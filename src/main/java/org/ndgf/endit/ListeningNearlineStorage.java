package org.ndgf.endit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

import java.net.URI;
import java.util.Set;
import java.util.UUID;
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
    public void flush(Iterable<FlushRequest> requests)
    {
        for (FlushRequest request : requests) {
            add(request, flush(request));
        }
    }

    @Override
    public void stage(Iterable<StageRequest> requests)
    {
        for (StageRequest request : requests) {
            add(request, stage(request));
        }
    }

    @Override
    public void remove(Iterable<RemoveRequest> requests)
    {
        for (RemoveRequest request : requests) {
            add(request, remove(request));
        }
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
                } catch (ExecutionException e) {
                    request.failed(e);
                }
            }
        }, MoreExecutors.sameThreadExecutor());
    }
}
