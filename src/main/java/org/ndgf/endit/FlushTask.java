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

import java.nio.charset.StandardCharsets;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.pool.nearline.spi.FlushRequest;

import diskCacheV111.util.PnfsId;

import static java.util.Arrays.asList;

import org.dcache.util.Checksum;

import com.google.gson.JsonObject;
import org.apache.commons.io.FileUtils;

class FlushTask implements PollingTask<Set<URI>>
{
    private final Path outFile;
    private final File file;
    private final PnfsId pnfsId;
    private final String type;
    private final String name;
    private final Path requestFile;
    private final long size;
    private final String storageClass;
    private final String path;
    
    private final Set<Checksum> checksums;

    private final static Logger LOGGER = LoggerFactory.getLogger(FlushTask.class);

    public FlushTask(FlushRequest request, Path requestDir, Path outDir, String type, String name)
    {
        this.type = type;
        this.name = name;
        file = new File(request.getReplicaUri().getPath());
        outFile = outDir.resolve(file.getName());
        pnfsId = request.getFileAttributes().getPnfsId();
        requestFile = requestDir.resolve(pnfsId.toString());
        size = request.getFileAttributes().getSize();
        storageClass =request.getFileAttributes().getStorageClass();
        path = request.getFileAttributes().getStorageInfo().getMap().get("path");
        checksums = request.getFileAttributes().getChecksums();
    }

    public List<Path> getFilesToWatch()
    {
        return asList(outFile);
    }

    @Override
    public Set<URI> start() throws IOException
    {
        String checksumType="";
        String checksumValue="";

        for (Checksum checksum: checksums) {
            checksumType = checksum.getType().getName().toLowerCase();
            checksumValue = checksum.getValue();           
        }


        JsonObject jsObj = new JsonObject();
        jsObj.addProperty("file_size", size);
        jsObj.addProperty("time", System.currentTimeMillis() / 1000);
        jsObj.addProperty("storage_class", storageClass);
        jsObj.addProperty("action", "migrate");
        jsObj.addProperty("path", path);
        jsObj.addProperty("checksumType", checksumType);
        jsObj.addProperty("checksumValue", checksumValue);
    	
        FileUtils.write(requestFile.toFile(), jsObj.toString(),  StandardCharsets.UTF_8);
         
        try {
            Files.createLink(outFile, file.toPath());
        } catch (FileAlreadyExistsException ignored) {
        }
        return null;
    }

    @Override
    public Set<URI> poll() throws URISyntaxException, IOException
    {
        if (!Files.exists(outFile)) {
           LOGGER.debug("File " + name + " deleted");
           URI uri = new URI(type, name, null, "bfid=" + pnfsId.toString(), null);
           // URI format: hsmType://hsmInstance/?store=storename&group=groupname&bfid=bfid  
           // <hsmType>: The type of the Tertiary Storage System  
           // <hsmInstance>: The name of the instance  
           // <storename> and <groupname> : The store and group name of the file as provided by the arguments to this executable.  
           // <bfid>: The unique identifier needed to restore or remove the file if necessary.   
           LOGGER.debug("Send back uri: " + uri.toString());
           Files.deleteIfExists(requestFile);
           
	   return Collections.singleton(uri);
        }
        return null;
    }

    @Override
    public boolean abort() throws IOException
    {
       return Files.deleteIfExists(outFile) && Files.deleteIfExists(requestFile);
    }
}
