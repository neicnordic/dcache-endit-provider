/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2015 Gerd Behrmann
 * Modifications Copyright (C) 2023-2025 Niklas Edmundsson
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

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PollingEnditNearlineStorage extends AbstractEnditNearlineStorage
{
    private final static Logger LOGGER = LoggerFactory.getLogger(PollingEnditNearlineStorage.class);

    protected int period;

    protected ListeningScheduledExecutorService executor;

    public PollingEnditNearlineStorage(String type, String name)
    {
        super(type, name);
    }

    @Override
    public synchronized void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        int threads = Integer.parseInt(properties.getOrDefault("threads", "16"));
        int period = Integer.parseInt(properties.getOrDefault("period", "1100"));

        super.configure(properties);

        this.period = period;

        if (executor != null) {
            executor.shutdown();
        }
        executor = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(threads));

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

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    /**
     * Represents the future result of a PollingTask.
     *
     * Periodically polls the task to check whether it has completed. If this Future
     * is cancelled, the task is aborted.
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
            future = executor.schedule(this, period, TimeUnit.MILLISECONDS);
        }

        @Override
        public synchronized void run()
        {
            try {
                if (!isDone()) {
                    V result = task.poll();
                    if (result != null) {
                        set(result);
                    } else {
                        future = executor.schedule(this, period, TimeUnit.MILLISECONDS);
                    }
                }
            } catch (Exception e) {
                try {
                    task.abort();
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
                setException(e);
            }
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning)
        {
            LOGGER.debug("PollingEnditNearlineStorage cancel: called");
            if (isDone()) {
                LOGGER.debug("PollingEnditNearlineStorage cancel: return false (isDone)");
                return false;
            }
            try {
                if (!task.abort()) {
                    LOGGER.debug("PollingEnditNearlineStorage cancel: return false (task.abort false)");
                    return false;
                }
                super.cancel(mayInterruptIfRunning);
            } catch (Exception e) {
                setException(e);
            }
            future.cancel(false);
            LOGGER.debug("PollingEnditNearlineStorage cancel: return true");
            return true;
        }
    }
}
