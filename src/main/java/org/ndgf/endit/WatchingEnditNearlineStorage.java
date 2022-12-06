/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2015 Gerd Behrmann
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
import com.google.common.util.concurrent.ListeningExecutorService;
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

/**
 * Variant of the Endit nearline storage using a WatchService.
 */
public class WatchingEnditNearlineStorage extends AbstractEnditNearlineStorage
{
    private final static Logger LOGGER = LoggerFactory.getLogger(WatchingEnditNearlineStorage.class);

    private final ConcurrentMap<Path,TaskFuture<?>> tasks = new ConcurrentHashMap<>();
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private Future<?> watchTask;

    public WatchingEnditNearlineStorage(String type, String name)
    {
        super(type, name);
    }

    @Override
    public synchronized void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        super.configure(properties);
        if (watchTask != null) {
            watchTask.cancel(true);
            watchTask = executor.submit(new WatchTask());
        }
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
    }

    private class WatchTask implements Runnable
    {
        @Override
        public void run()
        {
            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
                outDir.register(watcher, StandardWatchEventKinds.ENTRY_DELETE);
                inDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
                requestDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);

                pollAll();

                while (!Thread.currentThread().isInterrupted()) {
                    WatchKey key = watcher.take();
                    Path dir = (Path) key.watchable();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (event.kind().equals(StandardWatchEventKinds.OVERFLOW)) {
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
                LOGGER.warn("I/O error while watching Endit directories: {}", e.toString());
            } finally {
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
     * Periodically polls the task to check whether it has completed. If this Future
     * is cancelled, the task is aborted.
     *
     * @param <V> The result type returned by this Future's <tt>get</tt> method
     */
    private class TaskFuture<V> extends AbstractFuture<V>
    {
        private final PollingTask<V> task;

        TaskFuture(PollingTask<V> task)
        {
            this.task = task;
            register();
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
                }
            } catch (Exception e) {
                try {
                    task.abort();
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
                unregister();
                setException(e);
            }
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning)
        {
            if (isDone()) {
                return false;
            }
            try {
                if (!task.abort()) {
                    return false;
                }
                super.cancel(mayInterruptIfRunning);
            } catch (Exception e) {
                setException(e);
            }
            unregister();
            return true;
        }
    }
}
