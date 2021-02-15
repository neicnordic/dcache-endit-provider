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

import com.google.common.base.Splitter;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.dcache.pool.nearline.spi.RemoveRequest;

import static com.google.common.base.Preconditions.checkArgument;

class RemoveTask implements Callable<Void>
{
    private final RemoveRequest request;
    private final Path trashDir;

    public RemoveTask(RemoveRequest request, Path trashDir)
    {
        this.request = request;
        this.trashDir = trashDir;
    }

    @Override
    public Void call() throws IOException
    {
        URI uri = request.getUri();
        String id = getPnfsId(uri);

        /* Tell Endit to remove it from tape.
         */
        Files.write(trashDir.resolve(id), uri.toASCIIString().getBytes(StandardCharsets.UTF_8));
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
