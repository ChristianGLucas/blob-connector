# blob-connector

Generic **S3-compatible object storage** template — the BLOB/STATE pillar alongside
[`christiangeorgelucas/sql-connector`](https://github.com/ChristianGLucas/sql-connector). Speaks
the S3 API for **AWS S3, Cloudflare R2, GCS interop mode, MinIO, Backblaze B2, and DigitalOcean
Spaces** alike. Built for the [Axiom](https://axiomide.com) marketplace, MIT licensed.

**This package is a template, not a directly-callable API.** The 8 CRUD/list/copy/presign nodes
each publish a generalized (`kind: generic`) node — real, deployed code whose connection details
aren't fixed yet. A consumer binds one to a real bucket via `axiom instance create`, pinning the
endpoint, region, addressing style, and credentials (a literal key pair for a public/demo target,
or a tenant-declared secret for a private one) so that Instance's own callers never see connection
details at all — only the object-shaped fields (`key`, `data`, `prefix`, etc). `StreamObjectBody`
(the pipeline node) is the one exception: pipeline Instances aren't supported by the platform yet,
so it stays directly invocable with the full connection in its input, like an ordinary node.

## Use it from your agent or app

Every Instance you (or anyone) creates from this template is a **live, auto-scaling API
endpoint** on the [Axiom](https://axiomide.com) marketplace — call it from an AI agent or your own
code, with nothing to self-host.

**📦 See the template on the marketplace:**
https://dev.axiomide.com/marketplace/christiangeorgelucas/blob-connector@0.1.0

**Hook it up to an AI agent (MCP).** Add Axiom's hosted MCP server to any MCP client — once you
(or someone) creates an Instance, it becomes a typed tool your agent can call.

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

**Bind it to your own bucket (one-time, no code, no build, no deploy).** An Instance is pure
server-side type-binding against this package's already-deployed image:

```bash
axiom instance create your-handle/my-bucket/PutObject \
  --generic-package christiangeorgelucas/blob-connector --generic-node PutObject \
  --description "Upload an object to my-bucket." \
  --input-message PutObjectInput --input-field key=string --input-field data=bytes --input-field content_type=string \
  --input-map "key=key" --input-map "data=data" --input-map "content_type=content_type" \
  --input-map "connection.endpoint='s3.amazonaws.com'" \
  --input-map "connection.region='us-east-1'" \
  --input-map "connection.bucket='my-bucket'" \
  --input-map "connection.access_key_secret_name='MY_S3_ACCESS_KEY'" \
  --input-map "connection.secret_key_secret_name='MY_S3_SECRET_KEY'" \
  --required-secret MY_S3_ACCESS_KEY --required-secret MY_S3_SECRET_KEY
```

Repeat per node (`GetObject`, `HeadObject`, `DeleteObject`, `ListObjects`, `CopyObject`,
`PresignGet`, `PresignPut`) — append to the same package name to build a full typed surface for
your bucket. See [`axiom-instance-authoring`](https://axiomide.com) docs, or
`axiom instance create --help`, for the full field/secret/nested-message options.

**Call your Instance from the CLI** (once created — the caller only ever supplies the object-shaped
fields, never connection details):

```bash
axiom invoke your-handle/my-bucket/PutObject --input '{"key":"reports/2026/q1.csv","data":"<base64>","content_type":"text/csv"}'
```

**Call it over HTTP.**

```bash
curl -X POST https://api.axiomide.com/invocations/v1/nodes/your-handle/my-bucket/0.1.0/PutObject \
  -H "Authorization: Bearer $AXIOM_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"key":"reports/2026/q1.csv","data":"<base64>","content_type":"text/csv"}'
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

## Binding an Instance to your object store

Every node's REAL (template) input takes a `connection` naming an **endpoint** (no scheme, e.g.
`"s3.amazonaws.com"`, `"play.min.io"`, `"<accountid>.r2.cloudflarestorage.com"`,
`"storage.googleapis.com"`, `"<region>.digitaloceanspaces.com"`), a **region** (SigV4 requires one
even against providers with no real regional concept — `"us-east-1"` is a harmless default), and a
**bucket**. When you create an Instance, you bake all of this in as constants via `--input-map` —
your Instance's own callers never see or set it.

**Addressing style:** bake `connection.path_style=true` for MinIO and most self-hosted or non-AWS
endpoints (`"https://endpoint/bucket/key"`); AWS S3 works either way. Non-AWS/GCS endpoints already
default to path-style automatically even when left unset — `path_style` mainly matters as an
explicit override.

**Credentials — two ways to bake them into an Instance, set exactly one pair:**

- **`connection.access_key_secret_name` + `connection.secret_key_secret_name`** (recommended for
  any real credential), paired with `--required-secret <NAME>` on the Instance. Whoever invokes
  your Instance configures those secret names under **Console → Secrets**; the values are resolved
  server-side at invocation time and never appear in the Instance's mapping, flow manifests, or
  logs.
- **`connection.access_key` + `connection.secret_key`** — raw values baked as CEL constants, for
  publicly-documented or throwaway credentials only (e.g. play.min.io's published demo keys). A
  value baked this way is visible via `axiom instance inspect` — never bake a real credential here.

Bake `connection.insecure=true` only for an Instance pointed at a local/dev MinIO that isn't
TLS-terminated (plain HTTP); every real provider uses the default (HTTPS).

## Nodes (generic templates — bind via an Instance before use)

**Unary:**

- **PutObject** — upload an object's full body inline, with content type and optional metadata.
  Bounded by the platform's request envelope size; for larger uploads use `PresignPut`.
  Last-writer-wins idempotent.
- **GetObject** — download an object's full body inline, with content type, metadata, ETag, size,
  and last-modified time. For larger downloads use `StreamObjectBody` or `PresignGet`.
- **HeadObject** — check existence + size/ETag/content-type/metadata WITHOUT downloading the body.
  A missing key reports `exists: false` rather than an error.
- **DeleteObject** — delete one object. IDEMPOTENT: deleting an already-gone key is a normal
  success, matching S3's own semantics.
- **ListObjects** — list objects under a prefix, one page at a time (`continuation_token` in/out),
  with an optional `"/"` delimiter for a directory-like grouped listing.
- **CopyObject** — server-side copy within or between buckets on the same connection; the bytes
  never transit through this connector.
- **PresignGet** / **PresignPut** — expiry-bounded, pre-signed URLs for downloading/uploading an
  object directly against the object store, bypassing Axiom's payload path entirely. **The
  app-platform flagship**: hand these URLs to an end-user client for direct browser/app
  upload/download. Pure local SigV4 signing — no network call, safe to call any number of times.

**Pipeline (directly invocable, not a generic template):**

- **StreamObjectBody** — stream one object's body incrementally, one chunk per frame, via a true
  streaming read (the object is never materialized whole in memory). Frame field names (`data`,
  `byte_offset`, `error_code`, `error`, `is_final`) mirror
  [`christiangeorgelucas/http-tools`](https://github.com/ChristianGLucas/http-tools)'
  `StreamBodyChunk`, so a CSV object streams straight into `record-stream-tools`' `StreamCsvRecords`
  downstream in a flow. Takes the full `connection` directly, unlike the 8 templates above.

## Idempotency

Axiom invokes nodes **at-least-once**. `PutObject` is last-writer-wins idempotent (a retried
delivery simply re-uploads the same bytes to the same key); `CopyObject` and `DeleteObject` are
idempotent by construction; `PresignGet`/`PresignPut` are pure (generating a URL never touches the
object). `ListObjects`/`GetObject`/`HeadObject`/`StreamObjectBody` are read-only.

## Limitations

- `ListObjects.delimiter` supports only `""` (flat/recursive) or `"/"` (single-level grouping) — a
  real constraint of the underlying S3 client library, not a policy choice.
- `PutObject`'s body travels through the platform's normal request envelope; there is no
  chunked/multipart inline upload. Use `PresignPut` for large uploads.
