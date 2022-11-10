/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2014-2015 Gerd Behrmann
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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractEnditNearlineStorage extends ListeningNearlineStorage
{
    protected final String type;
    protected final String name;
    protected volatile Path inDir;
    protected volatile Path outDir;
    protected volatile Path requestDir;
    protected volatile Path trashDir;

    public AbstractEnditNearlineStorage(String type, String name)
    {
        this.type = type;
        this.name = name;
    }

    /**
     * Returns an executor suitable for background tasks and callbacks.
     */
    protected abstract ListeningExecutorService executor();

    /**
     * Schedules periodic execution of a PollingTask. The specific policy and mechanism
     * used depends on the implementation.
     *
     * @param task PollingTask to schedule
     * @param <T> Result type of the PollingTask
     * @return Future promise of a result of the polling task (a value of type T or an exception)
     */
    protected abstract <T> ListenableFuture<T> schedule(PollingTask<T> task);

    @Override
    public synchronized void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        checkState(!hasTasks(), "The nearline storage is busy and cannot be reconfigured.");

        String path = properties.get("directory");
        checkArgument(path != null, "conf attribute is required");
        Path dir = FileSystems.getDefault().getPath(path);
        checkArgument(Files.isDirectory(dir), dir + " is not a directory.");
        Path requestDir = dir.resolve("request");
        Path outDir = dir.resolve("out");
        Path inDir = dir.resolve("in");
        Path trashDir = dir.resolve("trash");
        checkArgument(Files.isDirectory(requestDir), requestDir + " is not a directory.");
        checkArgument(Files.isDirectory(outDir), outDir + " is not a directory.");
        checkArgument(Files.isDirectory(inDir), inDir + " is not a directory.");
        checkArgument(Files.isDirectory(trashDir), trashDir + " is not a directory.");

        try (DirectoryStream<Path> paths = Files.newDirectoryStream(requestDir)) {
            for (Path requestFile : paths) {
                Files.deleteIfExists(requestFile);
            }
        } catch (IOException e) {
            new RuntimeException(e);
            // Throwables.propagate(e);
        }

        this.requestDir = requestDir;
        this.outDir = outDir;
        this.inDir = inDir;
        this.trashDir = trashDir;
    }

    @Override
    public ListenableFuture<Void> remove(final RemoveRequest request)
    {
        return executor().submit(new RemoveTask(request, trashDir));
    }

    @Override
    protected ListenableFuture<Set<URI>> flush(FlushRequest request)
    {
        final PollingTask<Set<URI>> task = new FlushTask(request, requestDir, outDir, type, name);
        return Futures.transformAsync(request.activate(),
                                 new AsyncFunction<Void, Set<URI>>()
                                 {
                                     @Override
                                     public ListenableFuture<Set<URI>> apply(Void ignored) throws Exception
                                     {
                                         Set<URI> uris = task.start();
                                         if (uris != null) {
                                             return Futures.immediateFuture(uris);
                                         } else {
                                             return schedule(task);
                                         }
                                     }
                                 }, MoreExecutors.directExecutor());
    }

    @Override
    protected ListenableFuture<Set<Checksum>> stage(final StageRequest request)
    {
        final PollingTask<Set<Checksum>> task = new StageTask(request, requestDir, inDir);
        return Futures.transformAsync(
                Futures.transformAsync(request.activate(),
                                  new AsyncFunction<Void, Void>()
                                  {
                                      @Override
                                      public ListenableFuture<Void> apply(Void ignored) throws Exception
                                      {
                                          return request.allocate();
                                      }
                                  }, MoreExecutors.directExecutor()),
                new AsyncFunction<Void, Set<Checksum>>()
                {
                    @Override
                    public ListenableFuture<Set<Checksum>> apply(Void ignored) throws Exception
                    {
                        Set<Checksum> checksums = task.start();
                        if (checksums != null) {
                            return Futures.immediateFuture(checksums);
                        } else {
                            return schedule(task);
                        }
                    }
                }, MoreExecutors.directExecutor());
    }
}
