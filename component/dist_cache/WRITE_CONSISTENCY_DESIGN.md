# Distributed Cache — Write Consistency Design

## Overview

The `dist_cache` component is an L2 caching layer that sits between the local cache
(`file_cache` or `block_cache`) and Azure Storage (`azstorage`). It provides a shared
distributed cache across multiple blobfuse2 nodes.

This document describes how write operations maintain consistency between the L2 cache
and the source of truth (Azure Blob Storage), specifically addressing the challenges
posed by the cache server's **asynchronous `DeleteGroup`** semantics.

## Problem Statement

The distributed cache server processes `DeleteGroup` requests asynchronously — the RPC
returns success immediately while chunk deletion happens in the background. This creates
a race condition:

```
Time →
Node:     DeleteGroup("file\x00v0")    UploadChunk("file", offset=0, gid="file\x00v0")
Server:   ACK (queues deletion)         Stores chunk
Server:   ...async delete executes...   ← deletes the chunk we just uploaded!
```

Without mitigation, a write that invalidates old L2 data and then uploads new data could
have the new data deleted by the still-in-progress async deletion from its own invalidation.

## Solution: Versioned Group IDs

### Core Mechanism

Each file has a monotonically increasing **version counter** (per-node, in-memory). All
chunks uploaded for a file share a **versioned group ID** of the form:

```
"<filename>\x00v<N>"
```

The write sequence becomes:

1. **Delete** old group using the old version's group ID
2. **Bump** version to N+1
3. **Upload** new chunks under group ID `"file\x00v(N+1)"`

Since the async deletion targets `"file\x00vN"` and new uploads use `"file\x00v(N+1)"`,
the deletion cannot affect newly uploaded data.

### Per-Chunk Metadata

Every uploaded chunk carries its group ID in metadata:

```go
dcache.WithMetadata(map[string][]byte{"gid": gid})
```

This enables `resolveServerGroupID` (see below) to discover what group ID the server
actually holds, independent of local state.

## Cross-Restart Correctness

### Problem

The version counter is in-memory. After a crash/restart, the local counter resets to 0.
If the server has chunks under group ID `"file\x00v5"`, the restarted node would issue
`DeleteGroup("file\x00v0")` — deleting nothing — leaving stale data in L2.

### Solution: `resolveServerGroupID`

Before every `DeleteGroup` call, the node queries the server for the actual group ID
stored in chunk metadata:

```go
func (dc *DistCache) resolveServerGroupID(name string) []byte {
    serverGID, err := dc.client.GetChunkGroupID(ctx, name)
    if err == nil && serverGID != nil {
        return serverGID  // use what the server actually has
    }
    return fileGroupID(name, dc.getVersion(name))  // fallback to local
}
```

`GetChunkGroupID` issues a minimal `DownloadRequest` (Length=1) to fetch chunk metadata
without transferring significant data.

## Write Path Flows

### block_cache Path: `StageData` → `CommitData`

```
StageData(name, offset, data):
  1. Forward to azstorage (write-through)
  2. Buffer chunk in pendingWrites[name]
  3. markDirty(name) — local reads bypass L2

CommitData(name, blockList):
  1. Forward to azstorage (PutBlockList — atomic commit)
  2. cancelFlush(name) — stop any in-flight flush from a previous commit
  3. markDirty(name)
  4. DeleteGroup(resolveServerGroupID(name)) — invalidate old L2 data
  5. newVer = bumpVersion(name)
  6. Drain pendingWrites[name]
  7. Async: flushPendingToL2(name, chunks, newVer)
```

### file_cache Path: `CopyFromFile`

```
CopyFromFile(name, file):
  1. Forward to azstorage (PutBlob/upload)
  2. cancelFlush(name)
  3. markDirty(name)
  4. DeleteGroup(resolveServerGroupID(name))
  5. newVer = bumpVersion(name)
  6. Async: populateCache(name, filePath, newVer)
```

### Invalidation: `DeleteFile`, `RenameFile`, `TruncateFile`

```
DeleteFile/RenameFile/TruncateFile(name):
  1. markDirty(name)
  2. clearPending(name)
  3. DeleteGroup(resolveServerGroupID(name))
  4. bumpVersion(name)
  5. Forward to azstorage
```

## Read Path Behaviour

### block_cache Path: `ReadInBuffer`

```
ReadInBuffer(name, offset, buf):
  1. If isDirty(name) → bypass L2, read from azstorage
  2. DownloadChunk(name, offset) → L2 hit: return cached data
  3. L2 miss: read from azstorage, then async uploadChunkAsync(name, offset, data)
```

### file_cache Path: `CopyToFile`

```
CopyToFile(name, file):
  1. If isDirty(name) → bypass L2, download from azstorage
  2. DownloadWithSize(name, lock=true) → L2 hit: return
  3. L2 miss + got lock: download from azstorage, async populateCache
  4. L2 miss + locked: poll until another node finishes populating
```

## Dirty Marking

Files are marked "dirty" during writes to prevent the local node from reading stale L2
data during the invalidation/re-population window:

- **Set on**: every write, delete, rename, truncate
- **Duration**: 10 seconds (`dirtyTTL`), or cleared early when L2 re-population succeeds
- **Scope**: local to the node that performed the write
- **Effect**: reads for dirty files bypass L2 and go directly to azstorage

## Flush Cancellation

If a new write arrives while a previous flush is still uploading chunks to L2:

1. `cancelFlush(name)` cancels the in-flight goroutine's context
2. The cancelled flush stops uploading (checks `ctx.Done()` between chunks)
3. The new write proceeds with `DeleteGroup` + `bumpVersion` + its own flush

This prevents stale chunks from a cancelled flush from landing in L2 after the new
write's `DeleteGroup` has already cleaned up.

## Consistency Guarantees

| Property | Guaranteed? | Mechanism |
|----------|-------------|-----------|
| Single-node write consistency | ✅ Yes | Versioned group IDs isolate delete from upload |
| Cross-restart correctness | ✅ Yes | `resolveServerGroupID` queries server metadata |
| No stale reads on writing node | ✅ Yes | Dirty marking bypasses L2 for 10s |
| In-flight flush doesn't corrupt | ✅ Yes | `cancelFlush` stops racing goroutines |
| Cross-node last-writer-wins | ❌ No | Version counters are node-local; async flushes can race |
| Global read-after-write | ❌ No | Other nodes may read stale L2 until TTL/next invalidation |

## Known Limitations

1. **Cross-node staleness**: If Node A and Node B both write the same file, a third
   reading node may see stale data from the "losing" writer's flush until TTL expiry.
   Azure Storage remains correct; only L2 may be transiently stale.

2. **Version counter is volatile**: Resets on restart. `resolveServerGroupID` mitigates
   this but adds one network round-trip per write operation.

3. **Dirty marking is node-local**: Other nodes are unaware that a file was recently
   modified and may serve stale L2 data during the re-population window.

## Future Improvements

- **ETag-based versioning**: Use Azure's ETag as the group ID so all nodes agree on
  "current version" without coordination.
- **Conditional L2 writes**: Reject uploads if the server already has a newer version.
- **Cross-node dirty propagation**: Publish invalidation events so reading nodes can
  bypass stale L2 immediately.
