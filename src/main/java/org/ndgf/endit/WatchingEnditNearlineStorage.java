/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2015 Gerd Behrmann
 * Modifications Copyright (C) 2025 Niklas Edmundsson
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ndgf.endit;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Variant of the Endit nearline storage using a WatchService.
 */
public class WatchingEnditNearlineStorage extends AbstractEnditNearlineStorage
{
    private final static Logger LOGGER = LoggerFactory.getLogger(WatchingEnditNearlineStorage.class);

    private final ConcurrentMap<Path,TaskFuture<?>> tasks = new ConcurrentHashMap<>();
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private Future<?> watchTask;

    protected ListeningScheduledExecutorService schedexec;

    protected int period;
    protected int watchtimeout;

    public WatchingEnditNearlineStorage(String type, String name)
    {
        super(type, name);
    }

    @Override
    public synchronized void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        int threads = Integer.parseInt(properties.getOrDefault("threads", "4"));
        int period = Integer.parseInt(properties.getOrDefault("period", "110")); /* ms */
        int watchtimeout = Integer.parseInt(properties.getOrDefault("watchtimeout", "300")); /* seconds */

        super.configure(properties);
        if (watchTask != null) {
            watchTask.cancel(true);
            watchTask = executor.submit(new WatchTask());
        }

        this.period = period;
        this.watchtimeout = watchtimeout;

        if (schedexec != null) {
            schedexec.shutdown();
        }
        schedexec = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(threads));
    }

    @Override
    protected ListeningExecutorService executor()
    {
        return executor;
    }

    @Override
    protected <T> ListenableFuture<T> schedule(PollingTask<T> task)
    {
        return new TaskFuture<>(task);
    }

    public synchronized void start()
    {
        if (watchTask == null) {
            watchTask = executor.submit(new WatchTask());
        }
    }

    @Override
    public synchronized void shutdown()
    {
        if (watchTask != null) {
            watchTask.cancel(true);
        }
        executor.shutdown();
        schedexec.shutdown();
    }

    private class WatchTask implements Runnable
    {
        @Override
        public void run()
        {
            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
                /* Watch event types are hard-coded per directory. Since the
                 * Java implementation doesn't support the inotify CLOSE_WRITE
                 * event we can only cover the basic create/delete events.
                 * Notably MODIFY can't be used since it causes event storms
                 * due to generating an event for each write operation to a
                 * file in the watched directory.
                 */
                outDir.register(watcher, StandardWatchEventKinds.ENTRY_DELETE); // Flushed files
                inDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE); // Staged files
                requestDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE); // Error files

                pollAll();

                while (!Thread.currentThread().isInterrupted()) {
                    WatchKey key = watcher.poll(watchtimeout, TimeUnit.SECONDS);
                    if(key == null) {
                        /* Watch poll timeout, double-check that we're in sync */
                        LOGGER.debug("WatchingEnditNearlineStorage run: Watch poll timeout, doing pollAll()");
                        pollAll();
                        continue;
                    }
                    Path dir = (Path) key.watchable();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (event.kind().equals(StandardWatchEventKinds.OVERFLOW)) {
                            LOGGER.debug("WatchingEnditNearlineStorage run: Watch event overflow, doing pollAll()");
                            pollAll();
                        } else {
                            Path fileName = (Path) event.context();
                            poll(dir.resolve(fileName));
                        }
                    }
                    if (!key.reset()) {
                        // TODO
                    }
                }
            } catch (InterruptedException ignored) {
            } catch (IOException e) {
                LOGGER.warn("WatchingEnditNearlineStorage run: I/O error while watching Endit directories: {}", e.toString());
            } finally {
                LOGGER.debug("WatchingEnditNearlineStorage run: Cancelling tasks");
                for (TaskFuture<?> task : tasks.values()) {
                    task.cancel(true);
                }
            }
        }

        private void poll(Path path)
        {
            TaskFuture<?> task = tasks.get(path);
            if (task != null) {
                task.poll();
            }
        }

        private void pollAll()
        {
            for (TaskFuture<?> task : tasks.values()) {
                task.poll();
            }
        }
    }

    /**
     * Represents the future result of a PollingTask.
     *
     * For this Watching provider, we leverage the task canWatch() and
     * getFilesToWatch() methods to enable watch events.
     *
     * If watch events can't be used, fall back to scheduled polling.
     *
     * If this Future is cancelled, the task is aborted.
     *
     * @param <V> The result type returned by this Future's <tt>get</tt> method
     */
    private class TaskFuture<V> extends AbstractFuture<V> implements Runnable
    {
        private final PollingTask<V> task;
        private ListenableScheduledFuture<?> future;

        TaskFuture(PollingTask<V> task)
        {
            this.task = task;
            if(task.canWatch()) {
                register();
                future = null;
            }
            else {
                future = schedexec.schedule(this, period, TimeUnit.MILLISECONDS);
            }
        }

        private void register()
        {
            for (Path path : task.getFilesToWatch()) {
                if (tasks.putIfAbsent(path, this) != null) {
                    setException(new IllegalStateException("Duplicate nearline requests on " + path));
                }
            }
        }

        private void unregister()
        {
            for (Path path : task.getFilesToWatch()) {
                tasks.remove(path, this);
            }
        }

        public synchronized void poll()
        {
            try {
                if (!isDone()) {
                    V result = task.poll();
                    if (result != null) {
                        unregister();
                        set(result);
                    }
                    else if(future != null) {
                        future = schedexec.schedule(this, period, TimeUnit.MILLISECONDS);
                    }
                }
            } catch (Exception e) {
                try {
                    LOGGER.debug("WatchingEnditNearlineStorage poll: calling task.abort()");
                    task.abort();
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
                unregister();
                setException(e);
            }
        }

        @Override
        public synchronized void run()
        {
            poll();
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning)
        {
            LOGGER.debug("WatchingEnditNearlineStorage cancel: called");
            if (isDone()) {
                LOGGER.debug("WatchingEnditNearlineStorage cancel: return false (isDone)");
                return false;
            }
            try {
                LOGGER.debug("WatchingEnditNearlineStorage cancel: calling task.abort()");
                if (!task.abort()) {
                    LOGGER.debug("WatchingEnditNearlineStorage cancel: return false (task.abort false)");
                    return false;
                }
                LOGGER.debug("WatchingEnditNearlineStorage cancel: calling super.cancel(mayInterruptIfRunning)");
                super.cancel(mayInterruptIfRunning);
            } catch (Exception e) {
                LOGGER.debug("WatchingEnditNearlineStorage cancel: exception: {}", e.toString());
                setException(e);
            }
            unregister();
            LOGGER.debug("WatchingEnditNearlineStorage cancel: return true");
            return true;
        }
    }
}
