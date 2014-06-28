package org.ndgf.endit;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.sun.jna.Library;
import com.sun.jna.Native;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;

public class EnditNearlineStorage extends ListeningNearlineStorage
{
    public static final int POLL_PERIOD = 5000;
    public static final int ERROR_GRACE_PERIOD = 1000;

    protected final ListeningScheduledExecutorService executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
    private final String type;
    private final String name;
    private final int pid;
    private Path inDir;
    private Path outDir;
    private Path requestDir;
    private Path trashDir;

    private interface CLibrary extends Library
    {
        CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);
        int getpid();
    }

    public EnditNearlineStorage(String type, String name)
    {
        this.type = type;
        this.name = name;
        this.pid = CLibrary.INSTANCE.getpid();
    }

    @Override
    public ListenableFuture<Void> remove(final RemoveRequest request)
    {
        return executor.submit(new RemoveTask(request));
    }

    @Override
    protected ListenableFuture<Set<URI>> flush(FlushRequest request)
    {
        final PollingTask<Set<URI>> task = new FlushTask(request);
        return Futures.transform(request.activate(),
                                 new AsyncFunction<Void, Set<URI>>()
                                 {
                                     @Override
                                     public ListenableFuture<Set<URI>> apply(Void ignored) throws Exception
                                     {
                                         task.start();
                                         return new TaskFuture<>(task, POLL_PERIOD, TimeUnit.MILLISECONDS);
                                     }
                                 }, executor);
    }

    @Override
    protected ListenableFuture<Set<Checksum>> stage(final StageRequest request)
    {
        final PollingTask<Set<Checksum>> task = new StageTask(request);
        return Futures.transform(
                Futures.transform(request.activate(),
                                  new AsyncFunction<Void, Void>()
                                  {
                                      @Override
                                      public ListenableFuture<Void> apply(Void input) throws Exception
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
                        return new TaskFuture<>(task, POLL_PERIOD, TimeUnit.MILLISECONDS);
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
     * A polling task has an initiating action (start), followed by periodic polls
     * for the result.
     *
     * @param <T> The type of the result of the PollingTask
     */
    private interface PollingTask<T>
    {
        /** Called to initiate the task. */
        void start() throws Exception;

        /**
         *  Called periodically to poll for the result.
         *
         * @return The result or null if the task has not completed yet.
         */
        T poll() throws Exception;

        /**
         * Called to abort the task.
         *
         * @return true if the task was aborted, false if the task could not be aborted
         * (presumably because the task already completed).
         */
        boolean abort() throws Exception;
    }

    private class FlushTask implements PollingTask<Set<URI>>
    {
        private final Path outFile;
        private final File file;
        private final PnfsId pnfsId;

        public FlushTask(FlushRequest request)
        {
            file = request.getFile();
            outFile = outDir.resolve(file.getName());
            pnfsId = request.getFileAttributes().getPnfsId();
        }

        @Override
        public void start() throws IOException
        {
            try {
                Files.createLink(outFile, file.toPath());
            } catch (FileAlreadyExistsException ignored) {
            }
        }

        @Override
        public Set<URI> poll() throws URISyntaxException
        {
            if (!Files.exists(outFile)) {
                return Collections.singleton(new URI(type, name, null, "bfid=" + pnfsId.getId(), null));
            }
            return null;
        }

        @Override
        public boolean abort() throws IOException
        {
            return Files.deleteIfExists(outFile);
        }
    }

    private class StageTask implements PollingTask<Set<Checksum>>
    {
        private final Path file;
        private final Path inFile;
        private final Path errorFile;
        private final Path requestFile;
        private final long size;

        StageTask(StageRequest request)
        {
            file = request.getFile().toPath();
            FileAttributes fileAttributes = request.getFileAttributes();
            String id = fileAttributes.getPnfsId().toString();
            size = fileAttributes.getSize();
            inFile = inDir.resolve(id);
            errorFile = requestDir.resolve(id + ".err");
            requestFile = requestDir.resolve(id);
        }


        @Override
        public void start() throws Exception
        {
            String s = pid + " " + System.currentTimeMillis() / 1000;
            Files.write(requestFile, s.getBytes(Charsets.UTF_8));
        }

        @Override
        public Set<Checksum> poll() throws IOException, InterruptedException, CacheException
        {
            if (Files.exists(errorFile)) {
                Thread.sleep(ERROR_GRACE_PERIOD);
                try {
                    throw throwError(Files.readAllLines(errorFile, Charsets.UTF_8));
                } finally {
                    Files.deleteIfExists(inFile);
                    Files.deleteIfExists(errorFile);
                    Files.deleteIfExists(requestFile);
                }
            }
            if (Files.isRegularFile(inFile) && Files.size(inFile) == size) {
                Files.move(inFile, file, StandardCopyOption.ATOMIC_MOVE);
                Files.deleteIfExists(requestFile);
                return Collections.emptySet();
            }
            return null;
        }

        private CacheException throwError(List<String> lines) throws CacheException
        {
            String error;
            int errorCode;
            if (lines.isEmpty()) {
                errorCode = CacheException.DEFAULT_ERROR_CODE;
                error = "Endit reported a stage failure without providing a reason.";
            } else {
                try {
                    errorCode = Integer.parseInt(lines.get(0));
                    error = Joiner.on("\n").join(Iterables.skip(lines, 1));
                } catch (NumberFormatException e) {
                    errorCode = CacheException.DEFAULT_ERROR_CODE;
                    error = Joiner.on("\n").join(lines);
                }
            }
            throw new CacheException(errorCode, error);
        }

        @Override
        public boolean abort() throws Exception
        {
            if (Files.deleteIfExists(requestFile)) {
                Files.deleteIfExists(errorFile);
                Files.deleteIfExists(inFile);
                return true;
            }
            return false;
        }
    }

    private class RemoveTask implements Callable<Void>
    {
        private final RemoveRequest request;

        public RemoveTask(RemoveRequest request)
        {
            this.request = request;
        }

        @Override
        public Void call() throws IOException
        {
            URI uri = request.getUri();
            String id = getPnfsId(uri);

            /* Tell Endit to remove it from tape.
             */
            Files.write(trashDir.resolve(id), uri.toASCIIString().getBytes(Charsets.UTF_8));
            return null;
        }

        private String getPnfsId(URI uri)
        {
            String query = uri.getQuery();
            checkArgument(query != null, "URI lacks query part");
            String id = Splitter.on('&').withKeyValueSeparator('=').split(query).get("bfid");
            checkArgument(id != null, "Query part lacks bfid parameter");
            return id;
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
    private class TaskFuture<V> extends AbstractFuture<V> implements Runnable
    {
        private final long period;
        private final TimeUnit unit;
        private final PollingTask<V> task;
        private ListenableScheduledFuture<?> future;

        TaskFuture(PollingTask<V> task, long period, TimeUnit unit)
        {
            this.task = task;
            this.period = period;
            this.unit = unit;
            future = executor.schedule(this, this.period, this.unit);
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
                        future = executor.schedule(this, period, unit);
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
