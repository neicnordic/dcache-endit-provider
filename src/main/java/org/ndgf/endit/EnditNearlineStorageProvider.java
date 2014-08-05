package org.ndgf.endit;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;

public class EnditNearlineStorageProvider implements NearlineStorageProvider
{
    @Override
    public String getName()
    {
        return "endit";
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
