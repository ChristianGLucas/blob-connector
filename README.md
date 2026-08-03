# blob-connector

Generic **S3-compatible object storage** against a caller-set endpoint — ONE
connector that speaks the S3 API for **AWS S3, Cloudflare R2, GCS interop
mode, MinIO, Backblaze B2, and DigitalOcean Spaces** alike. Bring-your-own
access key + secret key via a named tenant secret (recommended) or raw
fields (publicly-documented/throwaway credentials only) — the BLOB/STATE
pillar alongside
[`christiangeorgelucas/sql-connector`](https://github.com/ChristianGLucas/sql-connector).
Full CRUD + pagination + server-side copy (unary), expiry-bounded presigned
GET/PUT URLs that bypass the flow envelope entirely (the app-platform
upload/download flagship), and a true incremental streaming read (pipeline)
for large objects. Built for the [Axiom](https://axiomide.com) marketplace,
MIT licensed.

## Use it from your agent or app

Every node in this package is a **live, auto-scaling API endpoint** on the
[Axiom](https://axiomide.com) marketplace — call it from an AI agent or your
own code, with nothing to self-host.

**📦 See it on the marketplace:**
https://dev.axiomide.com/marketplace/christiangeorgelucas/blob-connector@0.1.0

**Hook it up to an AI agent (MCP).** Add Axiom's hosted MCP server to any MCP
client and every node becomes a typed tool your agent can call — search the
catalog, inspect a schema, and invoke it directly.

```bash
# Claude Code
claude mcp add --transport http axiom https://api.axiomide.com/mcp \
  --header "Authorization: Bearer $AXIOM_API_KEY"
```

Claude Desktop, Cursor, or any config-based client:

```json
{
  "mcpServers": {
    "axiom": {
      "type": "http",
      "url": "https://api.axiomide.com/mcp",
      "headers": { "Authorization": "Bearer YOUR_AXIOM_API_KEY" }
    }
  }
}
```

**Call it from the CLI.**

```bash
axiom invoke christiangeorgelucas/blob-connector/HeadObject --input '{"connection":{"endpoint":"s3.amazonaws.com","bucket":"my-bucket","accessKeySecretName":"MY_S3_ACCESS_KEY","secretKeySecretName":"MY_S3_SECRET_KEY"},"key":"reports/2026/q1.csv"}'
```

**Call it over HTTP.**

```bash
curl -X POST https://api.axiomide.com/invocations/v1/nodes/christiangeorgelucas/blob-connector/0.1.0/HeadObject \
  -H "Authorization: Bearer $AXIOM_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"connection":{"endpoint":"s3.amazonaws.com","bucket":"my-bucket","accessKeySecretName":"MY_S3_ACCESS_KEY","secretKeySecretName":"MY_S3_SECRET_KEY"},"key":"reports/2026/q1.csv"}'
```

### Get started free

Install the CLI:

```bash
# macOS / Linux — Homebrew
brew install axiomide/tap/axiom

# macOS / Linux — install script
curl -fsSL https://raw.githubusercontent.com/AxiomIDE/axiom-releases/main/install.sh | sh
```

**Windows:** download the `windows/amd64` `.zip` from the
[releases page](https://github.com/AxiomIDE/axiom-releases/releases), unzip
it, and put `axiom.exe` on your `PATH`.

Then `axiom version` to verify, `axiom login` (GitHub or Google) to
authenticate, and create an API key under **Console → API Keys**. Docs and
sign-up at **[axiomide.com](https://axiomide.com)**.

## Connecting to your object store

Every node takes a `connection` naming an **endpoint** (no scheme, e.g.
`"s3.amazonaws.com"`, `"play.min.io"`,
`"<accountid>.r2.cloudflarestorage.com"`, `"storage.googleapis.com"`,
`"<region>.digitaloceanspaces.com"`), a **region** (SigV4 requires one even
against providers with no real regional concept — `"us-east-1"` is a
harmless default), and a **bucket**.

**Addressing style:** set `path_style: true` for MinIO and most self-hosted
or non-AWS endpoints ("`https://endpoint/bucket/key`"); AWS S3 works either
way. Non-AWS/GCS endpoints already default to path-style automatically even
when left `false` — `path_style` mainly matters as an explicit override.

**Credentials — two ways to supply them, set exactly one pair:**

- **`access_key_secret_name` + `secret_key_secret_name`** (recommended for
  any real credential). Set two tenant secrets under **Console → Secrets**
  holding your access key ID and secret access key, then reference their
  NAMES (never the values). Resolved server-side at invocation time —
  real credentials never appear in flow manifests, node inputs, or logs.
  Takes precedence when both modes are set.
- **`access_key` + `secret_key`** — raw values, for publicly-documented or
  throwaway credentials only (e.g. play.min.io's published demo keys). A
  value placed here is visible in flow definitions and execution history —
  never put a real credential here.

Set `insecure: true` only against a local/dev MinIO that isn't
TLS-terminated (plain HTTP); every real provider uses the default (HTTPS).

## Nodes

**Unary** (request/response):

- **PutObject** — upload an object's full body inline, with content type and
  optional metadata. Bounded by the platform's request envelope size; for
  larger uploads use `PresignPut`. Last-writer-wins idempotent.
- **GetObject** — download an object's full body inline, with content type,
  metadata, ETag, size, and last-modified time. For larger downloads use
  `StreamObjectBody` or `PresignGet`.
- **HeadObject** — check existence + size/ETag/content-type/metadata
  WITHOUT downloading the body. A missing key reports `exists: false`
  rather than an error.
- **DeleteObject** — delete one object. IDEMPOTENT: deleting an
  already-gone key is a normal success, matching S3's own semantics.
- **ListObjects** — list objects under a prefix, one page at a time
  (`continuation_token` in/out), with an optional `"/"` delimiter for a
  directory-like grouped listing.
- **CopyObject** — server-side copy within or between buckets on the same
  connection; the bytes never transit through this connector.
- **PresignGet** / **PresignPut** — expiry-bounded, pre-signed URLs for
  downloading/uploading an object directly against the object store,
  bypassing Axiom's payload path entirely. **The app-platform flagship**:
  hand these URLs to an end-user client for direct browser/app
  upload/download. Pure local SigV4 signing — no network call, safe to call
  any number of times.

**Pipeline** (streaming):

- **StreamObjectBody** — stream one object's body incrementally, one chunk
  per frame, via a true streaming read (the object is never materialized
  whole in memory). Frame field names (`data`, `byte_offset`, `error_code`,
  `error`, `is_final`) mirror
  [`christiangeorgelucas/http-tools`](https://github.com/ChristianGLucas/http-tools)'
  `StreamBodyChunk`, so a CSV object streams straight into
  `record-stream-tools`' `StreamCsvRecords` downstream in a flow.

## Idempotency

Axiom invokes nodes **at-least-once**. `PutObject` is last-writer-wins
idempotent (a retried delivery simply re-uploads the same bytes to the same
key); `CopyObject` and `DeleteObject` are idempotent by construction;
`PresignGet`/`PresignPut` are pure (generating a URL never touches the
object). `ListObjects`/`GetObject`/`HeadObject`/`StreamObjectBody` are
read-only.

## Limitations

- `ListObjects.delimiter` supports only `""` (flat/recursive) or `"/"`
  (single-level grouping) — a real constraint of the underlying S3 client
  library, not a policy choice.
- `PutObject`'s body travels through the platform's normal request
  envelope; there is no chunked/multipart inline upload. Use `PresignPut`
  for large uploads.
