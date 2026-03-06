# dCache ENDIT Provider

This is the Efficient Northern Dcache Interface to TSM (ENDIT) [dCache]
provider plugin. It was originally designed to interface with the
[ENDIT daemons] to form an integration for the IBM Storage Protect
(Spectrum Protect, TSM) storage system, since then additional
integrations have materialized.

See [Known ENDIT Integrations](#known-endit-integrations) for a list of
the ENDIT integrations known to us.

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
- It is highly advised to not allow client reads from the tape read
  pool and instead having reads trigger a pool-to-pool copy to a
  suitable set of other pools with enough size to hold the staged files
  for the time needed. This reduces the requirements on the tape read
  pool since it only has to handle the staging data transfers in flight
  and a single additional transfer for each staged file. For use cases
  where staged files are not read immediately by clients a dCache
  migration job might be needed to copy the files to the designated
  pools doing the caching.

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

More verbose instructions are available in the NDGF Tier1 Wiki at
https://wiki.neic.no/wiki/DCache_TSM_interface - however do note that
those instructions are site instructions tailored to NDGF so some
details might be convoluted for historical reasons.

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

**FIXME:** The JSON file-based API between the ENDIT provider dCache
plugin and the [ENDIT daemons] needs to be formally documented. For now,
read the source of both for documentation.

### Notes on the provider behaviour

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

# Known ENDIT Integrations

The ENDIT [dCache] provider plugin needs to interface with a storage
system specific integration to be able to do actual work.

These integrations are known to us, but this list is likely incomplete.

## ENDIT daemons

[ENDIT daemons] - IBM Storage Protect (formerly Spectrum Protect, TSM, ADSM)

The [ENDIT daemons] is the original ENDIT integration. It has evolved to
be highly optimized for sparse recalls and has been in use within NDGF
from its inception. It is to be regarded as a reference for an
implementation of the ENDIT provider/integration file based API.

Although the [ENDIT daemons] was originally implemented to interface
with the `dsmc` command it should be fairly easy to implement support
for other storage system specific command-line stage/flush/delete
utilities, either by utilizing a wrapper script/command or by augmenting
the [ENDIT daemons].

Among the features are:

- Dynamic multi-drive/session support. The number of sessions scales
  with the amount of data that needs to be flushed or staged.
- Tape-aware scheduling with the use of hint files to provide file-tape
  mappings.
- Stages are only scheduled when the request set for a specific tape is
  stable and there are no additional requests trickling in, the time
  threshold for this is configurable.
- Configurable tape mount/reuse delay to avoid load/unload wear due to
  extremely sparse requests hitting a single tape.
- Comfortably handles a 1 million entry request queue. This is achieved
  by caching parsed JSON request file data.
- Built in metric generation support that can be exported with
  `node_exporter` and thus be visualized with for example Grafana.
- On-the-fly temporary reconfiguration using a JSON file, this can for
  example be leveraged to dynamically change the balance of read/write
  sessions with a standalone script.
- Deletes are queued and executed periodically (by default monthly) in
  order to avoid partial tape reclamations.

### emulate-dsmc

The [ENDIT daemons] provides the `emulate-dsmc` utility that can be used
instead of a real storage system to emulate it using local storage. It
is intended for testing and use in CI/CD pipelines, but can serve as
inspiration for minimal-effort integrations with other storage systems.

## HPSS

Sites are known to use the ENDIT provider together with a local
integration with the HPSS storage system.

**FIXME:** Info/link?

## Tape guy

Sites are known to use the ENDIT provider together with a local
integration with the Tape guy storage system.

**FIXME:** Info/link?

## NESE automata

This is a site-specific integration with the local storage system.

**FIXME:** Info/link?

[dCache]: http://www.dcache.org/
[ENDIT daemons]:  https://github.com/neicnordic/endit
