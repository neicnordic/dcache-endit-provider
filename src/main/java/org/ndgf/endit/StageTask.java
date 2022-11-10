/* dCache Endit Nearline Storage Provider
 *
 * Copyright (C) 2014-2015 Gerd Behrmann
 *
 * Modifications Copyright (C) 2018 Vincent Garonne
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

class StageTask implements PollingTask<Set<Checksum>>
{
    public static final int ERROR_GRACE_PERIOD = 1000;

    public static final int GRACE_PERIOD = 1000;

    private static final int PID = CLibrary.INSTANCE.getpid();

    private final static Logger LOGGER = LoggerFactory.getLogger(StageTask.class);
    
    private final Path file;
    private final Path inFile;
    private final Path errorFile;
    private final Path requestFile;
    private final long size;
    private final String storageClass;
    private final String path;

    StageTask(StageRequest request, Path requestDir, Path inDir)
    {
        file = request.getFile().toPath();
        FileAttributes fileAttributes = request.getFileAttributes();
        String id = fileAttributes.getPnfsId().toString();
        size = fileAttributes.getSize();
        inFile = inDir.resolve(id);
        errorFile = requestDir.resolve(id + ".err");
        requestFile = requestDir.resolve(id);
        storageClass = fileAttributes.getStorageClass();
        path = request.getFileAttributes().getStorageInfo().getMap().get("path");
    }

    @Override
    public List<Path> getFilesToWatch()
    {
        return asList(errorFile, inFile);
    }

    @Override
    public Set<Checksum> start() throws Exception
    {
        if (Files.isRegularFile(inFile) && Files.size(inFile) == size) {
            Files.move(inFile, file, StandardCopyOption.ATOMIC_MOVE);
            return Collections.emptySet();
        }
  
        JsonObject jsObj = new JsonObject();
        jsObj.addProperty("file_size", size);
        jsObj.addProperty("parent_pid", PID);
        jsObj.addProperty("time", System.currentTimeMillis() / 1000);
        jsObj.addProperty("storage_class", storageClass);
        jsObj.addProperty("action", "recall");
        jsObj.addProperty("path", path);
    	    	
        FileUtils.write(requestFile.toFile(), jsObj.toString(),  StandardCharsets.UTF_8);
 	
        return null;
    }

    @Override
    public Set<Checksum> poll() throws IOException, InterruptedException, EnditException
    {
        if (Files.exists(errorFile)) {
            List<String> lines;
            try {
                Thread.sleep(ERROR_GRACE_PERIOD);
                lines = Files.readAllLines(errorFile, StandardCharsets.UTF_8);
            } finally {
                Files.deleteIfExists(inFile);
                Files.deleteIfExists(errorFile);
                Files.deleteIfExists(requestFile);
            }
            throw EnditException.create(lines);
        }
        if (Files.isRegularFile(inFile) && Files.size(inFile) == size) {            
            Files.deleteIfExists(requestFile);            
            Thread.sleep(GRACE_PERIOD); 
            try {
                Files.move(inFile, file, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                System.err.println(e);
            }
            return Collections.emptySet();
        }
        return null;
    }

    @Override
    public boolean abort() throws Exception
    {
       return Files.deleteIfExists(requestFile) && Files.deleteIfExists(errorFile) && Files.deleteIfExists(inFile);
      
    }

    private interface CLibrary extends Library
    {
        CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);
        int getpid();
    }
}
