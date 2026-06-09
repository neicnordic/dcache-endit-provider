/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2014-2015 Gerd Behrmann
 * Modifications Copyright (C) 2018 Vincent Garonne
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

import java.nio.charset.StandardCharsets;
import com.sun.jna.Library;
import com.sun.jna.Native;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static java.util.Arrays.asList;
import com.google.gson.JsonObject;
import org.apache.commons.io.FileUtils;

class StageTask implements PollingStageTask<Boolean>
{
    public static final int ERROR_GRACE_PERIOD = 1000;

    public static final int START_FOUND_GRACE_PERIOD = 4*60*60*1000; // milliseconds

    private static final int PID = CLibrary.INSTANCE.getpid();

    private final static Logger LOGGER = LoggerFactory.getLogger(StageTask.class);

    private final Path requestDir;
    private final int graceperiod;
    private final Path file;
    private final Path inFile;
    private final Path errorFile;
    private final Path requestFile;
    private final String id;
    private final long size;
    private final String storageClass;
    private final String path;
    private long delayUntil;
    private boolean doStart;
    private boolean doComplete;
    private boolean doWatch;

    StageTask(StageRequest request, Path requestDir, Path inDir, int graceperiod)
    {
        this.requestDir = requestDir;
        this.graceperiod = graceperiod;
        file = Paths.get(request.getReplicaUri());
        FileAttributes fileAttributes = request.getFileAttributes();
        id = fileAttributes.getPnfsId().toString();
        size = fileAttributes.getSize();
        inFile = inDir.resolve(id);
        errorFile = requestDir.resolve(id + ".err");
        requestFile = requestDir.resolve(id);
        storageClass = fileAttributes.getStorageClass();
        path = request.getFileAttributes().getStorageInfo().getMap().get("path");
        delayUntil = -1;
        doStart = false;
        doComplete = false;
        doWatch = false;
    }

    @Override
    public boolean canWatch()
    {
        return this.doWatch;
    }

    @Override
    public List<Path> getFilesToWatch()
    {
        return asList(errorFile, inFile);
    }


    /* start() returns when inFile exists. */
    @Override
    public Boolean start() throws Exception
    {
        assert !doComplete : "Internal ENDIT provider bug: complete() called before start()";

        doStart = true;
        doWatch = true;

        if (Files.isRegularFile(inFile)) {
            LOGGER.debug("StageTask start: id {}: found {}", id, inFile);
            long fsize = Files.size(inFile);
            long flastmod = Files.getLastModifiedTime(inFile).toMillis();
            if(fsize == size) {
                LOGGER.debug("StageTask start: id {}: size {} is final size", id, fsize);
                return true;
            }
            LOGGER.debug("StageTask start: id {}: size {}", id, size);
            LOGGER.debug("StageTask start: id {}: last modified {}", id, flastmod/1000);
            if(flastmod + START_FOUND_GRACE_PERIOD > System.currentTimeMillis()) {
                // If inFile has been modified within START_FOUND_GRACE_PERIOD we consider this file to be
                // in progress and treat the request as being processed by the integration. This can happen when
                // a request has been cancelled and then resubmitted, since we don't enforce the integration to process
                // cancellations.
                LOGGER.debug("StageTask start: id {}: File modified within {} seconds", id, START_FOUND_GRACE_PERIOD/1000);
                return true;
            }
            LOGGER.debug("StageTask start: id {}: Deleting stale file {}", id, inFile);
            Files.deleteIfExists(inFile);
        }

        JsonObject jsObj = new JsonObject();
        jsObj.addProperty("file_size", size);
        jsObj.addProperty("parent_pid", PID);
        jsObj.addProperty("time", System.currentTimeMillis() / 1000);
        jsObj.addProperty("storage_class", storageClass);
        jsObj.addProperty("action", "recall");
        jsObj.addProperty("path", path);

        /* Create the file and rename it in place to avoid consumers
         * getting a 0-byte or half-written file.
         */
        Path tmpf = Files.createTempFile(requestDir, id + ".", ".stage.tmp");
        FileUtils.write(tmpf.toFile(), jsObj.toString(),  StandardCharsets.UTF_8);
        Files.move(tmpf, requestFile, StandardCopyOption.ATOMIC_MOVE);
        LOGGER.debug("StageTask start: id {}: wrote {}", id, requestFile);

        return null;
    }

    /* complete() completes processing with waiting for a
     * complete inFile, grace delay for file attributes to be set, and moves
     * it to outFile.
     */
    @Override
    public Boolean complete() throws Exception
    {
        assert !doStart : "Internal ENDIT provider bug: start() called before complete()";

        doComplete = true;

        LOGGER.debug("StageTask complete: id {}: called", id);

        return poll();
    }


    /* poll() handles both start() and complete() initiated tasks */
    @Override
    public Boolean poll() throws IOException, InterruptedException, EnditException
    {
        if (Files.exists(errorFile)) {
            List<String> lines;
            try {
                LOGGER.debug("StageTask poll: id {}: Error file " + errorFile + " detected, sleeping {} ms before handling error.", id, ERROR_GRACE_PERIOD);
                Thread.sleep(ERROR_GRACE_PERIOD); // Locks this execution thread
                lines = Files.readAllLines(errorFile, StandardCharsets.UTF_8);
            } finally {
                if(Files.deleteIfExists(inFile)) {
                    LOGGER.debug("StageTask poll: id {}: Deleted {}", id, inFile);
                }
                if(Files.deleteIfExists(errorFile)) {
                    LOGGER.debug("StageTask poll: id {}: Deleted {}", id, errorFile);
                }
                if(Files.deleteIfExists(requestFile)) {
                    LOGGER.debug("StageTask poll: id {}: Deleted {}", id, requestFile);
                }
            }
            throw EnditException.create(lines);
        }

        if(doStart) {
            if (Files.isRegularFile(inFile)) {
                LOGGER.debug("StageTask poll: id {}: found {}", id, inFile);
                return true;
            }
        }
        else if(doComplete) {
            if(Files.isRegularFile(inFile) && Files.size(inFile) == size) {
                LOGGER.debug("StageTask poll: id {}: inFile " + inFile + " size " + size, id);
                if(delayUntil < 0) {
                    if(Files.deleteIfExists(requestFile)) {
                        LOGGER.debug("StageTask poll: id {}: Deleted {}", id, requestFile);
                    }
                    if(graceperiod > 0) {
                        delayUntil = System.currentTimeMillis() + graceperiod;
                        LOGGER.debug("StageTask poll: id {}: inFile " + inFile + " delayUntil " + delayUntil, id);
                        return null;
                    }
                    else {
                        delayUntil = 0;
                    }
                }
                if(delayUntil > 0 && System.currentTimeMillis() < delayUntil) {
                    LOGGER.debug("StageTask poll: id {}: inFile {} delaying", id, inFile);
                    return null;
                }

                Files.move(inFile, file, StandardCopyOption.ATOMIC_MOVE);
                LOGGER.debug("StageTask poll: id {}: inFile " + inFile + " complete, moved to " + file, id);

                return true;
            }
        }
        else {
            // Neither start() or complete() called.
            List<String> err = List.of("Internal ENDIT provider bug.", "StageTask: neither start() nor complete() called before poll().");
            throw EnditException.create(err);
        }
        return null;
    }

    @Override
    public Set<Checksum> checksum() throws Exception
    {
        LOGGER.debug("StageTask checksum: id {}: called", id);
        // No proper checksum retention yet.
        return Collections.emptySet();
    }

    @Override
    public boolean abort() throws Exception
    {
       /* Only delete the requestFile and eventual errorFile. The rationale
        * behind this is that the request has likely timed out and will be
        * retried shortly, saving us from having the daemon stage it again.
        */
       if(Files.deleteIfExists(requestFile)) {
           LOGGER.debug("StageTask abort: id {}: Deleted {}", id, requestFile);
       }
       if(Files.deleteIfExists(errorFile)) {
           LOGGER.debug("StageTask abort: id {}: Deleted {}", id, errorFile);
       }

       LOGGER.debug("StageTask abort: id {}: return true", id);
       return true;
    }

    private interface CLibrary extends Library
    {
        CLibrary INSTANCE = Native.load("c", CLibrary.class);
        int getpid();
    }
}
