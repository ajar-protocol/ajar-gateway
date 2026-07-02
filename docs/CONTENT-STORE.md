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

After startup, the store is in memory. There are no per-request disk reads and
no hot reload yet; update workflows should restart the Gateway after replacing
the prepared store atomically.
