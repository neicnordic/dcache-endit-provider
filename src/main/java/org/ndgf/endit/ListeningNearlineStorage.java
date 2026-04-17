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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;

public abstract class ListeningNearlineStorage implements NearlineStorage
{
    private final ConcurrentMap<UUID, Future<?>> tasks = new ConcurrentHashMap<>();

    private final static Logger LOGGER = LoggerFactory.getLogger(ListeningNearlineStorage.class);

    @Override
    public void cancel(UUID uuid)
    {
        Future<?> task = tasks.get(uuid);
        LOGGER.debug("ListeningNearlineStorage cancel: uuid {}: called", uuid);
        if (task != null) {
            LOGGER.debug("ListeningNearlineStorage cancel: uuid {}: calling task.cancel(true)", uuid);
            task.cancel(true);
        }
        else {
            LOGGER.debug("ListeningNearlineStorage cancel: uuid {}: no task found", uuid);
        }
    }

    @Override
    public synchronized void flush(Iterable<FlushRequest> requests)
    {
        for (FlushRequest request : requests) {
            add(request, flush(request));
        }
    }

    @Override
    public synchronized void stage(Iterable<StageRequest> requests)
    {
        for (StageRequest request : requests) {
            add(request, stage(request));
        }
    }

    @Override
    public synchronized void remove(Iterable<RemoveRequest> requests)
    {
        for (RemoveRequest request : requests) {
            add(request, remove(request));
        }
    }

    protected boolean hasTasks()
    {
        return !tasks.isEmpty();
    }

    protected abstract ListenableFuture<Set<URI>> flush(FlushRequest request);

    protected abstract ListenableFuture<Set<Checksum>> stage(StageRequest request);

    protected abstract ListenableFuture<Void> remove(RemoveRequest request);

    private <T> void add(final NearlineRequest<T> request, final ListenableFuture<T> future)
    {
        if (tasks.putIfAbsent(request.getId(), future) != null) {
            request.failed(new IllegalStateException("Duplicate nearline requests on uuid " + request.getId()));
        }
        LOGGER.debug("ListeningNearlineStorage add: put uuid {} in task list", request.getId());
        future.addListener(new Runnable()
        {
            @Override
            public void run()
            {
                tasks.remove(request.getId());
                try {
                    T result = Uninterruptibles.getUninterruptibly(future);
                    LOGGER.debug("ListeningNearlineStorage add run(): uuid {}: Calling request.completed(result)", request.getId());
                    request.completed(result);
                } catch (ExecutionException | CancellationException e) {
                    if (e instanceof CancellationException) {
                        LOGGER.debug("ListeningNearlineStorage add run(): uuid {}: CancellationException, calling request.failed(e)", request.getId());
                        request.failed(e);
                    } else if (e.getCause() instanceof EnditException) {
                        EnditException cause = (EnditException) e.getCause();
                        LOGGER.debug("ListeningNearlineStorage add run(): uuid {}: Calling request.failed(cause)", request.getId());
                        request.failed(cause.getReturnCode(), cause.getMessage());
                    } else {
                        LOGGER.debug("ListeningNearlineStorage add run(): uuid {}: Calling request.failed(e)", request.getId());
                        request.failed(e);
                    }
                }
            }
        }, MoreExecutors.directExecutor());
    }
}
