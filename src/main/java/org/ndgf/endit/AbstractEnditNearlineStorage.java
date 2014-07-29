/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

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

    public AbstractEnditNearlineStorage(
            String type, String name)
    {
        this.type = type;
        this.name = name;
    }

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
            Throwables.propagate(e);
        }

        this.requestDir = requestDir;
        this.outDir = outDir;
        this.inDir = inDir;
        this.trashDir = trashDir;
    }
}
