package org.ndgf.endit;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
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
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;

/**
 * Incomplete variant of the Endit nearline storage using a WatchService.
 */
public class WatchingEnditNearlineStorage extends ListeningNearlineStorage
{
    public static final int ERROR_GRACE_PERIOD = 1000;

    private final ConcurrentMap<Path,TaskFuture<?>> tasks = new ConcurrentHashMap<>();
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private final String type;
    private final String name;
    private final int pid;
    private Path inDir;
    private Path outDir;
    private Path requestDir;
    private Path trashDir;
    private Future<?> watchTask;

    private interface CLibrary extends Library
    {
        CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);
        int getpid();
    }

    public WatchingEnditNearlineStorage(String type, String name)
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
        start();
        final PollingTask<Set<URI>> task = new FlushTask(request);
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
        start();
        final PollingTask<Set<Checksum>> task = new StageTask(request);
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

        // TODO: If the WatchTask is already running we would have to restart it
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

        public List<Path> getFilesToWatch()
        {
            return asList(outFile);
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
                tasks.remove(outFile);
                return Collections.singleton(new URI(type, name, null, "bfid=" + pnfsId.getId(), null));
            }
            return null;
        }

        @Override
        public boolean abort() throws IOException
        {
            if (Files.deleteIfExists(outFile)) {
                tasks.remove(outFile);
                return true;
            } else {
                return false;
            }
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
        public List<Path> getFilesToWatch()
        {
            return asList(errorFile, inFile);
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
                // TODO
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
     * A polling task has an initiating action (start), followed by periodic polls
     * for the result.
     *
     * @param <T> The type of the result of the PollingTask
     */
    private interface PollingTask<T>
    {
        /** Returns the list of files to watch for events. */
        List<Path> getFilesToWatch();

        /** Called to initiate the task. */
        void start() throws Exception;

        /**
         * Called when any of the files to watch have an event occur on them.
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
                    // TODO panic
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
