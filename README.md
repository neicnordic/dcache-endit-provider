# dCache ENDIT Provider

This is the Efficient Northern Dcache Interface to TSM (ENDIT) [dCache]
provider plugin. It was originally designed to interface with the
[ENDIT daemons] to form an integration for the IBM Storage Protect
(Spectrum Protect, TSM) storage system, since then additional
integrations have materialized.

See [Known ENDIT Integrations](#known-endit-integrations) for a list of
the ENDIT integrations known to us.

## Sizing and performance

These recommendations are focused on using the ENDIT provider together
with the [ENDIT daemons] integration using late allocation (ie ENDIT
v2), but most conclusions should be valid for other integrations as
well.

The list is not considered to be complete as there might be other
considerations to be made, especially when another integration than
the [ENDIT daemons] is used.

### Pool size

The dCache pool size requirement is rather modest for a tape pool with
ENDIT, especially when leveraging other dCache pools to handle
workload-induced load variations. Tape technology is best suited to
workloads with a steady-state data rate during long time periods, a
bursty/periodic workload adds extra challenges to the setup that can be
solved in numerous ways.

For most environments a pool size around 4-10TB is sufficient, however
you need to take into account a number of factors that may influence
whether you fall into the lower or higher number of that range.

For write pools, doing the flush/write of data from dCache to tape,
factors might include:

- Write bursts - data ingested during short periods and flushed to
  tape before the next burst happens.
  - If the burst cycle is long the required storage/buffer space can
    become huge and thus expensive. Consider taking data to other
    storage such as dCache disk pools to allow streaming to tape in
    orderly fashion. dCache migration jobs can be used to move incoming
    data onto the tape pool.
- The number of tape drives used
  - More/faster tape drives requires more storage to be able to write
    data to tape in reasonably sized chunks. One estimate is to allow
    for at least 1 TB per tape drive.
- Endurance - storage lifetime TBW/PBW
  - Compare the lifetime Terabyte written (TBW) for the server storage
    with the amount of data you expect to transfer during the server
    lifetime. For example, if you expect to migrate 10 PB to tape during
    the 5 year server life that server storage needs an endurance of at
    least 10 PBW or 10000 TBW.
  - It might be cheaper to use bigger lower-endurance (ie
    Read-Intensive/RI) storage than using smaller amount of
    higher-endurance storage (ie Mixed-Use/MU or Write-Intensive/WI).
  - Be careful to use TBW endurance numbers as stated by the
    manufacturer. Watch out when trying to deduce the endurance from a
    Drive Writes Per Day (DWPD) number, as those are dependant on the
    stated lifetime of the storage device that can range between 2-5
    years.
- Tape system outage tolerance
  - More storage means the system can take more data during a tape
    system outage/maintenance.
  - An alternative is to take data to disk pools instead, similar to the
    write burst scenario.

For read pools, doing the stage/read of data from tape to dCache,
factors might be:

- Read bursts - data is staged from tape in bursts/campaigns and then
  used over a longer period of time while other data is read from tape.
  - If the burst total data size is big or the cycle is long the
    required storage/buffer space can become huge and thus expensive.
    Consider using a dCache migration job to move the data to other
    storage such as dCache disk pools.
  - Having a subset of clients with low transfer rates can also be
    considered to be this kind of workload.

### Transfer rate and bandwidth

**FIXME:**

The combined tape drive transfer rate is usually the minimal 
transfer rate to cater for when sizing the storage, since forcing tape
drives to slow down can be detrimental to the performance. Tape is a
streaming media where its physical properties place limits on how it can
adapt and perform, so it's of high importance to allow tape drives to
continuously stream data at their preferred rate.

A common mistake is to not cater for the network transfer rate when
estimating the total bandwidth needed. The goal is to not allow incoming
or outgoing data transfers affect the tape drive transfer activity.

The workload will always be simultaneous read+write, either network+tape
or tape+network transfer-rate wise. The local storage needs to be able
to handle these scenarios without affecting the tape drive streaming
rate.

When using hardware RAID controllers there are a few mitigations known
to have some effect of balancing the read/write priority to favor
streaming tape drive transfers:

- Read pool: Use a write-back cache to favor tape IO, even for
  flash/NVMe.
- Write-pool: Tune system with HUGE read-ahead buffers to better allow
  for tape drive streaming.

Another common pitfall is allowing client reads increasing the network
transfer rate.  It is highly advised to not allow client reads from the
tape read pool and instead having reads trigger a pool-to-pool copy to a
suitable set of other pools with enough size to hold the staged files
for the time needed. This reduces the requirements on the tape read pool
since it only has to handle the staging data transfers in flight and a
single additional transfer for each staged file. For use cases where
staged files are not read immediately by clients a dCache migration job
might be needed to copy the files to the designated pools doing the
caching.

### Server hardware

It is recommended to commission a separate read and write pool on
dedicated physical servers with internal storage. Reasons for this
include:

- IO requirements are isolated to a single server.
- If a server breaks it's easy to reconfigure a read pool to also
  perform the duties of a write pool and vice versa.
- Low risk of cascading performance issues or failures caused by shared
  infrastructure such as backplane/enclosures, storage, virtualization
  components etc.

Server CPU/memory sizing is mostly dependant on the expected IO transfer
rate. In general fewer faster cores is better than many slow ones,
especially when per-core (PVU) licensing is used by the tape storage
software. When using the [ENDIT daemons] with million(s) of pending
stage requests it might frequently consume a CPU core in order to make
scheduling decisions.  RAM requirements are modest and depends mostly on
the number of files in the system combined with the IO workload.  

As an example, an NDGF tape pool with a 16 core CPU, 64G RAM and a 25G
NIC typically has a lot of CPU idling and around 50GB RAM free for
buffer/cache when pushing data to tape at 1 GB/s while handling 3 GB/s
incoming data.

In order for the dCache ENDIT provider to be able to handle million(s)
of pending stage requests a performant `$enditdirectory/request/`
directory is required. Since this is volatile data that is recreated on
dCache restart a simple solution is to use a `tmpfs` file system.

## Installation

**FIXME:** Seems the plugin goes into the classes directory, so this is
wrong. Investigate what's the sane way to do it without mixing with
dcache-shipped files.

To install the dCache ENDIT provider plugin, unpack the tarball in the dCache
plugin directory (usually `/usr/local/share/dcache/plugins`).

Since the provider configuration is dependant on the integration setup
we recommend installing and setting up the integration with your
tape/nearline storage system first, be it the [ENDIT daemons] or
whatever fits your system.

## Configuration

**FIXME:** Move stuff here

Note that since ENDIT v2 a late allocation scheme is used in order to
expose all pending read requests to the pools. This minimizes tape
remounts and thus optimizes access. It also reduces the storage needed
for staging. For new installations, and when upgrading from ENDIT v1 to
v2, note that:

- The dCache pool size needs to be set lower than the usable file space
  size, at least 100 GiB lower if the default [ENDIT daemons]
  `retriever_buffersize` is used.
  - With usable file space we mean the space available to the non-root
    user running dCache/ENDIT considering the maximum recommended file
    system fill level, in our experience approx 98% for XFS, 95% för ZFS
    and 85% for CEPH.
- You need to allow a really large amount of concurrent restores and
  thus might need an even larger restore timeout. ENDIT has been verified with
  1 million requests on a single tape pool with
  [modest hardware](#development-performance-tests), central
  dCache resources on your site might well limit this number.

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

Use the `hsm ls` command in the dCache admin interface to view all
defined nearline storage instances including their parameters.

### Watching provider

**FIXME:** Example of how to see which parameters are defined, print at
startup too?

To use, define a nearline storage in the dCache admin interface:

**FIXME:** Fix command syntax

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

**FIXME:** Fix command syntax

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

**FIXME:** Document mailing list!

## License

AGPL-3.0, see [LICENSE](LICENSE.txt)

## Versioning

[Semantic Versioning 2.0.0](https://semver.org/)


## Contributors

The ENDIT project started in April 2006 using the original script-based
HSM interface in dCache. This ENDIT dCache provider plugin was created
together with the dCache nearline storage SPI in 2014 to improve
performance and reduce resource usage when handling many concurrent
requests. This list of contributors refers to this provider plugin.

* Gerd Behrmann - original author
* Vincent Garonne - previous maintainer
* Krishnaveni Chitrapu - previous maintainer
* Niklas Edmundsson <nikke@hpc2n.umu.se> - current maintainer
* Tigran Mkrtchyan - various contributions and fixes

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

## Development performance tests

The performance tests done during development are made on a dedicated
test system with modest hardware specifications and a complete stack
with the ENDIT provider and [ENDIT daemons] communicating with a
production TSM server.

When benchmarking the ENDIT staging rate a dedicated set of small files
is used that are residing on FILE pools on the TSM server. This is done
to avoid the need of having a very IO-bandwidth heavy, and thus
expensive, test system.

With this setup we expect to be able to handle 100000 requests queued while
staging requests at a rate in excess of 200 Hz. Very occasionally we also
test with 1 million requests queued, since this takes a lot of time to
prepare/test.

The test hardware basics:

- Intel E2275G 4-core CPU
- 64 G RAM
- 4 x 960G SSD in RAID5
- 25G Ethernet

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
