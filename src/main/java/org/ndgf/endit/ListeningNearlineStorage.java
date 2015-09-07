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

    @Override
    public void cancel(UUID uuid)
    {
        Future<?> task = tasks.get(uuid);
        if (task != null) {
            task.cancel(true);
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
        tasks.put(request.getId(), future);
        future.addListener(new Runnable()
        {
            @Override
            public void run()
            {
                tasks.remove(request.getId());
                try {
                    request.completed(Uninterruptibles.getUninterruptibly(future));
                } catch (ExecutionException | CancellationException e) {
                    if (e.getCause() instanceof EnditException) {
                        EnditException cause = (EnditException) e.getCause();
                        request.failed(cause.getReturnCode(), cause.getMessage());
                    } else {
                        request.failed(e);
                    }
                }
            }
        }, MoreExecutors.sameThreadExecutor());
    }
}
