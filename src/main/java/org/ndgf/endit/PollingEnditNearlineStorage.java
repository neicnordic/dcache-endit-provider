package org.ndgf.endit;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;

import static com.google.common.base.Preconditions.checkArgument;

public class PollingEnditNearlineStorage extends ListeningNearlineStorage
{
    public static final int POLL_PERIOD = 5000;

    protected final ListeningScheduledExecutorService executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
    private final String type;
    private final String name;
    private Path inDir;
    private Path outDir;
    private Path requestDir;
    private Path trashDir;

    public PollingEnditNearlineStorage(String type, String name)
    {
        this.type = type;
        this.name = name;
    }

    @Override
    public ListenableFuture<Void> remove(final RemoveRequest request)
    {
        return executor.submit(new RemoveTask(request, trashDir));
    }

    @Override
    protected ListenableFuture<Set<URI>> flush(FlushRequest request)
    {
        final PollingTask<Set<URI>> task = new FlushTask(request, outDir, type, name);
        return Futures.transform(request.activate(),
                                 new AsyncFunction<Void, Set<URI>>()
                                 {
                                     @Override
                                     public ListenableFuture<Set<URI>> apply(Void ignored) throws Exception
                                     {
                                         task.start();
                                         return new TaskFuture<>(task);
                                     }
                                 }, executor);
    }

    @Override
    protected ListenableFuture<Set<Checksum>> stage(final StageRequest request)
    {
        final PollingTask<Set<Checksum>> task = new StageTask(request, requestDir, inDir);
        return Futures.transform(
                Futures.transform(request.activate(),
                                  new AsyncFunction<Void, Void>()
                                  {
                                      @Override
                                      public ListenableFuture<Void> apply(Void ignored) throws Exception
                                      {
                                          return request.allocate();
                                      }
                                  }),
                new AsyncFunction<Void, Set<Checksum>>()
                {
                    @Override
                    public ListenableFuture<Set<Checksum>> apply(Void ignored) throws Exception
                    {
                        task.start();
                        return new TaskFuture<>(task);
                    }
                }, executor);
    }

    @Override
    public void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        String path = properties.get("directory");
        checkArgument(path != null, "conf attribute is required");
        Path dir = FileSystems.getDefault().getPath(path);
        checkArgument(Files.isDirectory(dir), dir + " is not a directory.");
        requestDir = dir.resolve("request");
        outDir = dir.resolve("out");
        inDir = dir.resolve("in");
        trashDir = dir.resolve("trash");
        checkArgument(Files.isDirectory(requestDir), requestDir + " is not a directory.");
        checkArgument(Files.isDirectory(outDir), outDir + " is not a directory.");
        checkArgument(Files.isDirectory(inDir), inDir + " is not a directory.");
        checkArgument(Files.isDirectory(trashDir), trashDir + " is not a directory.");
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
            future = executor.schedule(this, POLL_PERIOD, TimeUnit.MILLISECONDS);
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
                        future = executor.schedule(this, POLL_PERIOD, TimeUnit.MILLISECONDS);
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
            future.cancel(false);
            return true;
        }
    }
}
