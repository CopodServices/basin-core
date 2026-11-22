# basin-core

This is the CopodServices fork of [vul-os/basin](https://github.com/vul-os/basin).
It adds one compatibility patch on top of upstream `main`; everything else is
the upstream engine.

## The patch: create-if-absent on stores without conditional PUTs

Basin keeps its bucket-native catalog and lease registry in the same object
store as the data. Commits append `v{N}.json` / `e{EPOCH}.json` with
`PutMode::Create`, which the AWS client turns into `If-None-Match: *`.

That precondition is what makes the append atomic: the loser of a race gets
`AlreadyExists`, reloads the manifest and retries. Tigris, Cloudflare R2 and AWS
S3 implement it. Backblaze B2 answers **501 Not Implemented** to the header, so
the first DDL statement on a B2 bucket fails with
`catalog error: object-store catalog put manifest ... 501` while plain PUTs of
the same object succeed.

`BASIN_STORAGE_CONDITIONAL_PUT` selects how that is handled:

| Value | Behaviour |
| --- | --- |
| `require` (default) | Keep the pinned `If-None-Match` behaviour. Use it on stores that implement the precondition. |
| `emulate` | Wrap the catalog store so `PutMode::Create` becomes HEAD, then an unconditional PUT, then a read-back ETag check. A concurrent writer that replaced the object is reported as `AlreadyExists`, so the commit loop retries. |

Unknown values abort startup rather than silently changing behaviour.

The emulation is not atomic. It turns a lost update into a visible conflict,
which is what the versioned-log protocol needs, and a single writer per project
never races at all. Keep one writer per project on B2: one engine replica, and
`BASIN_SHARD_ENABLED=1`.

## Image

`.github/workflows/image.yaml` builds `ghcr.io/copodservices/basin-core` on
every push to `main` (and on demand), tagged `<UTC date>-<short sha>` plus
`latest`. The build compiles the Rust workspace from source, so the first run is
slow.
