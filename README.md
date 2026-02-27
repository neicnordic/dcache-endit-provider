# dCache ENDIT Provider

This is the Efficient Northern Dcache Interface to TSM (ENDIT) [dCache]
provider plugin. It was originaly designed to interface with the
[ENDIT daemons] to form an integration for the IBM Storage Protect
(Spectrum Protect, TSM) storage system, since then additional
integrations have materialized.

## Installation

To install the dCache ENDIT provider plugin, unpack the tarball in the dCache
plugin directory (usually `/usr/local/share/dcache/plugins`).

Since the provider configuration is dependant on the integration setup
we recommend installing and setting up the integration with your
tape/nearline storage system first, be it the [ENDIT daemons] or
whatever fits your system.

## Configuration

There are two flavors of the ENDIT provider: The polling provider and
the watching provider.

The watching provider uses the least system resources, and should be the
most performant today when having huge queues.

The polling provider is a good alternative if the watching provider has
issues due to OS limitations.

The endit directory must be on the same file system as the pool's
data directory. The reason for this is that files must be able to be
moved from the staging `in/` directory using rename, and
duplicated to the `out/` directory using hardlinks.

Note that since ENDIT v2 a late allocation scheme is used in order to
expose all pending read requests to the pools. This minimizes tape
remounts and thus optimizes access. It also reduces the storage needed
for staging. For new installations, and when upgrading from ENDIT v1 to
v2, note that:

- The dCache pool size needs to be set lower than the actual file space
  size, at least 100 GiB lower if the default [ENDIT daemons]
  `retriever_buffersize` is used.
- You need to allow a really large amount of concurrent restores and
  thus might need an even larger restore timeout. ENDIT has been verified with
  1 million requests on a single tape pool with
  [modest hardware](#development-performance-tests), central
  dCache resources on your site might well limit this number.
- It is highly recommended to set up a migration job that moves staged
  files from the tape pool onto disk pools and set up a link group that
  only allows clients to fetch data from those disk pools.

### Watching provider

To use, define a nearline storage in the dCache admin interface:

```
hsm create osm the-hsm-name endit-watching -directory=/path/to/endit/directory
```

The endit directory must be on the same file system as the pool's
data directory.

The above will create a provider that uses the JVMs file event
notification feature to detect files created/deleted in the stage/flush
processes. File completion is monitored using polling in the same way as
the polling provider.

The watching provider accepts additional options with the following default
values:

    -threads=4 - number of threads used for polling for file changes
    -period=110 - poll period in milliseconds
    -graceperiod=1000 - grace period in milliseconds between detecting file complete and moving to destination
    -watchtimeout=300 - Timeout in seconds of inactivity before a double-check of watch directory is done
    -lateallocate=true - Allocate space when staging start is detected (false to allocate before submitting staging request)

The number of threads and default poll period for the watching provider
is lower compared to the polling provider, this is due to the fact that
only files in progress are monitored using polling.

### Polling provider

To use a provider that polls for changes, define a nearline storage in
the dCache admin interface:

```
hsm create osm the-hsm-name endit-polling -directory=/path/to/endit/directory
```

The endit directory must be on the same file system as the pool's
data directory.

The polling provider accepts additional options with the following default
values:

    -threads=16 - number of threads used for polling for file changes
    -period=1100 - poll period in milliseconds
    -graceperiod=1000 - grace period in milliseconds between detecting file complete and moving to destination
    -lateallocate=true - Allocate space when staging start is detected (false to allocate before submitting staging request)

### Notes on the provider behaviour

**FIXME:** Move this to the API section, the details pertain to the cooperation between the ENDIT provider
and the integration with the tape/nearline storage system.

* The request files are *not* monitored once they are created.
  Editing or deleting them has no consequences from the perspective of dCache.
* Provider will check whether a requested file already exists with the correct size in the `/in` folder,
  before it writes a new request file and, if so, move it into the pool's inventory without staging anything.
* Existing request file(s) are *overwritten* when the pool receives a request
  (that isn't satisfied by the content of the `/in` folder as stated above).
  This is important regarding *retries* of recalls from the pool and *pool restarts*!
* Provider will check for *error files* with every poll.
  If such a file exists for a requested file, it's content is read verbatim and raised as an
  exception from the staging task. Because the exception is raised, the task will be aborted
  and all related files should get purged.
  The error file's path has to be `$enditdirectory/request/<pnfsid>.err`
* Shutting down the provider and/or the pool does clean up existing request files.

## Performance tuning

In order for the dCache ENDIT provider to be able to handle million(s)
of pending stage requests a performant `$enditdirectory/request/`
directory is required. Since this is volatile data that is recreated on
dCache restart a simple solution is to use a `tmpfs` file system.

There are more tuning considerations to be made depending on what is
used to integrate with the ENDIT provider. When using the [ENDIT daemons]
with million(s) of pending stage requests it might frequently consume a
CPU core in order to make scheduling decisions. Also, any IO
requirements of your data transfers apply.

### Development performance tests

The performance tests done during development are made on a dedicated
test system with modest hardware specifications and a complete stack
with the ENDIT provider and [ENDIT daemons] communicating with a
production TSM server.

When benchmarking the ENDIT staging rate a dedicated set of small files
is used that are residing on FILE pools on the TSM server. This is done
to avoid the need of having a very IO-bandwidth heavy, and thus
expensive, test system.

With this setup we expect to be able to handle 1M requests queued while
staging requests at a rate in excess of 200 Hz.

The test hardware basics:

- Intel E2275G 4-core CPU
- 64 G RAM
- 4 x 960G SSD in RAID5
- 25G Ethernet

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

**FIXME:** The file-based API between the ENDIT dCache plugin and the
[ENDIT daemons] needs to be formally documented. For now, read the source of
both for documentation.

[dCache]: http://www.dcache.org/
[ENDIT daemons]:  https://github.com/neicnordic/endit
