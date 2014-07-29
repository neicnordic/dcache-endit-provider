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
