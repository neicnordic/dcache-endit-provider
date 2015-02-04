dCache Endit Provider
==============================================

This [dCache] plugin interfaces with the [Endit] TSM integration system.

To compile the plugin, run:

    mvn package

To install the plugin, unpack the resulting tarball in the dCache
plugin directory (usually `/usr/local/share/dcache/plugins`).


To use, define a nearline storage in the dCache admin interface:

    hsm create osm osm endit -directory=/path/to/endit/directory

The endit directory must be on the same file system as the pool's
data directory.

[dCache]: http://www.dcache.org/
[Endit]: https://github.com/maswan/endit
