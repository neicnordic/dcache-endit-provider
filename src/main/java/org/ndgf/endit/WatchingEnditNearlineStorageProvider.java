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

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;

public class WatchingEnditNearlineStorageProvider implements NearlineStorageProvider
{
    @Override
    public String getName()
    {
        return "endit-watching";
    }

    @Override
    public String getDescription()
    {
        return "Endit TSM integration provider.";
    }

    @Override
    public NearlineStorage createNearlineStorage(String type, String name)
    {
        return new WatchingEnditNearlineStorage(type, name);
    }
}
