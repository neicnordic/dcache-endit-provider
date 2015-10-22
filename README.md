dCache Endit Provider
==============================================

This [dCache] plugin interfaces with the [Endit] TSM integration system.

To compile the plugin, run:

    mvn package

To install the plugin, unpack the resulting tarball in the dCache
plugin directory (usually `/usr/local/share/dcache/plugins`).

## Configuration

To use, define a nearline storage in the dCache admin interface:

    hsm create osm the-hsm-name endit -directory=/path/to/endit/directory

The endit directory must be on the same file system as the pool's
data directory.

The above will create a provider that uses the JVMs file event
notification feature which in most cases maps directly to a native
file event notification facility of the operating system. 

## Polling provider

To use a provider that polls for changes, use:

    hsm create osm osm the-hsm-name -directory=/path/to/endit/directory

This provider accepts two additional options with the following default
values:

    -threads=1
    -period=5000

The first is the number of threads used for polling for file changes
and the second is the poll period in milliseconds.

[dCache]: http://www.dcache.org/
[Endit]: https://github.com/maswan/endit
