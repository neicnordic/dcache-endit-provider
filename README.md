# dCache ENDIT Provider

This is the Efficient Northern Dcache Interface to TSM (ENDIT) [dCache]
provider plugin. It interfaces with the
[ENDIT daemons] to form an integration for the IBM Storage Protect
(Spectrum Protect, TSM) storage system.

## Installation

To install the plugin, unpack the tarball in the dCache
plugin directory (usually `/usr/local/share/dcache/plugins`).

## Configuration

There are two flavors of the ENDIT provider: The polling provider and
the watching provider.

The polling provider is the most performant, this is what's used in
production on NDGF and what we recommend to use.

The watching provider uses the least system resources, but should not be
used in any situation where performance is of interest.

The pool must be configured to leave free space for [ENDIT daemons]
(pre)staging, leave at least 1 TiB free. If you have very large files
and/or use many tape drives you might need even more space, remember to
also modify the `retriever_buffersize` [ENDIT daemons] option.

### Polling provider

To use a provider that polls for changes, use:
```
hsm create osm the-hsm-name endit-polling -directory=/path/to/endit/directory
```

The endit directory must be on the same file system as the pool's
data directory.

This provider accepts two additional options with the following default
values:

    -threads=20
    -period=5000

The first is the number of threads used for polling for file changes
and the second is the poll period in milliseconds.

For sites with large request queues we recommend to increase the thread
count further, 200 threads are used in production on NDGF.

#### Notes on the provider behaviour

* The polling provider does *not* monitor the request files, once they are created.
  Editing or deleting them has no consequences from the perspective of dCache.
* The polling provider will check whether a requested file does exist already in the `/in` folder,
  before it writes a new request file and, if so, move it into the pool's inventory without staging anything.
* The polling provider will *overwrite* existing request files, when the pool receives a request
  (that isn't satisfied by the content of the `/in` folder).
  That is important regarding *retries* of recalls from the pool and *pool restarts*!
* The polling provider will check for *error files* with every poll.
  If such a file exists for a requested file, it's content is read and verbatim raised as an
  exception from the staging task. Because the exception is raised, the task will be aborted
  and all related files should get purged.
* The error file's path has to be `/request/<pnfsid>.err`
* Shutting down the polling provider and/or the pool does clean up existing request files.

### Watching provider

To use, define a nearline storage in the dCache admin interface:

```
hsm create osm the-hsm-name endit -directory=/path/to/endit/directory
```

The endit directory must be on the same file system as the pool's
data directory.

The above will create a provider that uses the JVMs file event
notification feature which in most cases maps directly to a native
file event notification facility of the operating system.

## More documentation

More verbose instructions are available at
https://wiki.neic.no/wiki/DCache_TSM_interface.

# Collaboration

Patches, suggestions, and general improvements are most welcome.

We use the
[GitHub issue tracker](https://github.com/neicnordic/dcache-endit-provider/issues)
to track and discuss proposed improvements.

When submitting code, open an issue to track/discuss pull-request(s) and
refer to that issue in the pull-request. Pull-requests should be based
on the master branch.

## License

AGPL-3.0, see [LICENSE](LICENSE.txt)

## Versioning

[Semantic Versioning 2.0.0](https://semver.org/)

## Building

To compile the plugin, run:
```
mvn package
```

## API

FIXME: The file-based API between the ENDIT dCache plugin and the ENDIT
daemons needs to be formally documented. For now, read the source of
both for documentation.

[dCache]: http://www.dcache.org/
[ENDIT daemons]:  https://github.com/neicnordic/endit
