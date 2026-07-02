# Prepared Content Store

The Serving Layer reads a prepared content store from disk at startup when
`store_dir` is configured. The conversion pipeline writes this directory in a
future task; the serve path never harvests, extracts, renders, induces, drafts,
or signs content.

```text
<store_dir>/
  manifest.json
  views/<name>.json
  view-index.json
```

- `manifest.json` is the complete, already-signed Ajar manifest served at
  `/.well-known/ajar.json`.
- `views/<name>.json` files are complete, already-signed Ajar View objects. The
  Gateway builds its in-memory request map from each View object's `url` field.
- `view-index.json` is the machine sitemap referenced by `manifest.views.index`.
  Its shape is informational in protocol v0.1.

The Gateway loads and validates the whole store before binding listeners. It
fails startup if the manifest JSON is malformed, required manifest fields are
missing, the manifest lifetime is invalid or expired, required View fields are
missing, or multiple Views resolve to the same request path and query.

After startup, the store is in memory. There are no per-request disk reads.

## Atomic write and reload

Adapters and conversion jobs should publish a new prepared store by writing a
complete replacement directory first:

```text
<store_dir>.next/
  manifest.json
  views/<name>.json
  view-index.json
```

Write all files to the replacement directory, then rename it into place on the
same filesystem. After the rename succeeds, call the admin control API:

```sh
curl -i -X POST http://127.0.0.1:9090/reload
```

`POST /reload` re-runs the full load and validation from `store_dir`. On
success, the Gateway atomically swaps the in-memory store and responds:

```json
{"status":"reloaded","views":1,"manifest_sequence":2}
```

On failure, the Gateway keeps the old in-memory store serving and returns
`409 application/problem+json` with the typed validation detail. A reload whose
manifest sequence is lower than the currently served sequence is rejected as
rollback protection.

Do not mutate a live store in place. Write to a temporary directory, rename the
complete store into place, then reload.
