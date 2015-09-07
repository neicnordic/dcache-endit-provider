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

import diskCacheV111.util.PnfsId;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.dcache.pool.nearline.spi.FlushRequest;

import static java.util.Arrays.asList;

class FlushTask implements PollingTask<Set<URI>>
{
    private final Path outFile;
    private final File file;
    private final PnfsId pnfsId;
    private final String type;
    private final String name;

    public FlushTask(FlushRequest request, Path outDir, String type, String name)
    {
        this.type = type;
        this.name = name;
        file = request.getFile();
        outFile = outDir.resolve(file.getName());
        pnfsId = request.getFileAttributes().getPnfsId();
    }

    public List<Path> getFilesToWatch()
    {
        return asList(outFile);
    }

    @Override
    public Set<URI> start() throws IOException
    {
        try {
            Files.createLink(outFile, file.toPath());
        } catch (FileAlreadyExistsException ignored) {
        }
        return null;
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
