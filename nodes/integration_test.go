//go:build integration

// Local live-MinIO oracle: run with
//
//	go test -tags integration ./nodes/... -run Integration -v
//
// against the docker MinIO container started per RETRO-NOTES.md
// (localhost:19000, path-style, minioadmin/minioadmin). Excluded from the
// default `go test ./...` / `axiom test` run (no build tag) so the
// deterministic unit-test gate never depends on a live container being
// present.
package nodes_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"christiangeorgelucas/blob-connector/nodes"

	gen "christiangeorgelucas/blob-connector/gen"
)

const (
	minioEndpoint  = "localhost:19000"
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
)

func testConnection(bucket string) *gen.Connection {
	return &gen.Connection{
		Endpoint:  minioEndpoint,
		Region:    "us-east-1",
		Bucket:    bucket,
		Insecure:  true,
		PathStyle: true,
		AccessKey: minioAccessKey,
		SecretKey: minioSecretKey,
	}
}

// ensureBucket creates the bucket if it doesn't already exist, using
// minio-go directly (independent of the package under test's own client
// wiring, other than sharing the same dependency).
func ensureBucket(t *testing.T, bucket string) {
	t.Helper()
	cl, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("setup: minio.New: %v", err)
	}
	exists, err := cl.BucketExists(context.Background(), bucket)
	if err != nil {
		t.Fatalf("setup: BucketExists: %v", err)
	}
	if !exists {
		if err := cl.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
			t.Fatalf("setup: MakeBucket: %v", err)
		}
	}
}

// TestIntegration_PutGetHeadDelete_RoundTrip is the independent oracle for
// the full object lifecycle against a REAL S3-compatible server: PutObject's
// reported etag/size, GetObject's returned bytes/content-type/metadata,
// HeadObject's existence + metadata (without downloading the body), and
// DeleteObject's idempotent removal (delete-again still succeeds).
func TestIntegration_PutGetHeadDelete_RoundTrip(t *testing.T) {
	bucket := "blob-connector-it-crud"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()
	key := "round-trip/hello.txt"
	body := []byte("hello, MinIO — café — 日本語 — 🎉")

	putResp, err := nodes.PutObject(ctx, nil, &gen.PutObjectRequest{
		Connection:  conn,
		Key:         key,
		Data:        body,
		ContentType: "text/plain; charset=utf-8",
		Metadata:    map[string]string{"owner": "blob-connector-it", "unit": "café"},
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if putResp.GetEtag() == "" {
		t.Error("PutObject: expected a non-empty etag")
	}
	if putResp.GetSize() != int64(len(body)) {
		t.Errorf("PutObject: size = %d, want %d", putResp.GetSize(), len(body))
	}

	headResp, err := nodes.HeadObject(ctx, nil, &gen.HeadObjectRequest{Connection: conn, Key: key})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if !headResp.GetExists() {
		t.Fatal("HeadObject: expected exists=true for a just-uploaded key")
	}
	if headResp.GetSize() != int64(len(body)) {
		t.Errorf("HeadObject: size = %d, want %d", headResp.GetSize(), len(body))
	}
	if headResp.GetMetadata()["owner"] != "blob-connector-it" {
		t.Errorf("HeadObject: metadata[owner] = %q, want %q (full map: %v)", headResp.GetMetadata()["owner"], "blob-connector-it", headResp.GetMetadata())
	}
	if headResp.GetMetadata()["unit"] != "café" {
		t.Errorf("HeadObject: metadata[unit] (non-ASCII) = %q, want %q", headResp.GetMetadata()["unit"], "café")
	}

	getResp, err := nodes.GetObject(ctx, nil, &gen.GetObjectRequest{Connection: conn, Key: key})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if !bytes.Equal(getResp.GetData(), body) {
		t.Errorf("GetObject: data = %q, want %q", getResp.GetData(), body)
	}
	if getResp.GetContentType() != "text/plain; charset=utf-8" {
		t.Errorf("GetObject: content_type = %q, want %q", getResp.GetContentType(), "text/plain; charset=utf-8")
	}
	if getResp.GetEtag() != putResp.GetEtag() {
		t.Errorf("GetObject: etag = %q, want it to match PutObject's %q", getResp.GetEtag(), putResp.GetEtag())
	}

	delResp, err := nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: key})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if !delResp.GetDeleted() {
		t.Error("DeleteObject: expected deleted=true")
	}

	headAfterDelete, err := nodes.HeadObject(ctx, nil, &gen.HeadObjectRequest{Connection: conn, Key: key})
	if err != nil {
		t.Fatalf("HeadObject after delete: unexpected error: %v", err)
	}
	if headAfterDelete.GetExists() {
		t.Error("HeadObject after delete: expected exists=false")
	}

	// Idempotency: deleting an already-gone key is still a plain success.
	delAgain, err := nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: key})
	if err != nil {
		t.Fatalf("DeleteObject (already gone): expected idempotent success, got error: %v", err)
	}
	if !delAgain.GetDeleted() {
		t.Error("DeleteObject (already gone): expected deleted=true (idempotent)")
	}
}

// TestIntegration_UnicodeAndSpecialCharacterKeys is an adversarial-key
// oracle: keys with spaces, percent signs, and non-ASCII characters must
// round-trip byte-for-byte through Put/Get/Head/Delete and appear verbatim
// (not double-encoded or mangled) in ListObjects.
func TestIntegration_UnicodeAndSpecialCharacterKeys(t *testing.T) {
	bucket := "blob-connector-it-crud"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()

	keys := []string{
		"weird/has spaces.txt",
		"weird/100%done.txt",
		"weird/café-日本語-🎉.txt",
		"weird/a+b&c=d.txt",
	}
	// Cleanup is registered on the OUTER test, not each subtest: a subtest's
	// t.Cleanup fires when that subtest ends, which would delete every key
	// before the parent-level ListObjects assertion below ever runs.
	t.Cleanup(func() {
		for _, key := range keys {
			nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: key})
		}
	})

	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			body := []byte("payload for " + key)
			if _, err := nodes.PutObject(ctx, nil, &gen.PutObjectRequest{Connection: conn, Key: key, Data: body}); err != nil {
				t.Fatalf("PutObject(%q): %v", key, err)
			}

			head, err := nodes.HeadObject(ctx, nil, &gen.HeadObjectRequest{Connection: conn, Key: key})
			if err != nil {
				t.Fatalf("HeadObject(%q): %v", key, err)
			}
			if !head.GetExists() {
				t.Fatalf("HeadObject(%q): expected exists=true", key)
			}

			get, err := nodes.GetObject(ctx, nil, &gen.GetObjectRequest{Connection: conn, Key: key})
			if err != nil {
				t.Fatalf("GetObject(%q): %v", key, err)
			}
			if !bytes.Equal(get.GetData(), body) {
				t.Errorf("GetObject(%q): data = %q, want %q", key, get.GetData(), body)
			}
		})
	}

	list, err := nodes.ListObjects(ctx, nil, &gen.ListObjectsRequest{Connection: conn, Prefix: "weird/"})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	for _, want := range keys {
		found := false
		for _, o := range list.GetObjects() {
			if o.GetKey() == want {
				found = true
			}
		}
		if !found {
			t.Errorf("key %q not found verbatim in ListObjects (got keys: %v)", want, listKeys(list))
		}
	}
}

func listKeys(resp *gen.ListObjectsResponse) []string {
	out := make([]string, len(resp.GetObjects()))
	for i, o := range resp.GetObjects() {
		out[i] = o.GetKey()
	}
	return out
}

// TestIntegration_GetObject_NotFound proves a genuinely missing key/bucket
// surfaces as a typed not-found error against a real server.
func TestIntegration_GetObject_NotFound(t *testing.T) {
	bucket := "blob-connector-it-crud"
	ensureBucket(t, bucket)
	_, err := nodes.GetObject(context.Background(), nil, &gen.GetObjectRequest{
		Connection: testConnection(bucket),
		Key:        "does/not/exist.txt",
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("got %v, want a \"not found\" error", err)
	}
}

// TestIntegration_BadCredentials proves wrong credentials against a real
// server surface as a typed invalid-credentials error, not a generic one.
func TestIntegration_BadCredentials(t *testing.T) {
	bucket := "blob-connector-it-crud"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	conn.SecretKey = "totally-wrong-secret"
	_, err := nodes.GetObject(context.Background(), nil, &gen.GetObjectRequest{Connection: conn, Key: "any"})
	if err == nil || !strings.Contains(err.Error(), "invalid credentials") {
		t.Errorf("got %v, want an \"invalid credentials\" error", err)
	}
}

// TestIntegration_PutObject_ZeroByteObject proves a 0-byte upload round-trips
// cleanly through Put/Get/Head, distinct from the pipeline's zero-byte
// single-frame case (see TestIntegration_StreamObjectBody_ZeroByte).
func TestIntegration_PutObject_ZeroByteObject(t *testing.T) {
	bucket := "blob-connector-it-crud"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()
	key := "zero-byte.bin"

	if _, err := nodes.PutObject(ctx, nil, &gen.PutObjectRequest{Connection: conn, Key: key, Data: []byte{}}); err != nil {
		t.Fatalf("PutObject (0 bytes): %v", err)
	}
	getResp, err := nodes.GetObject(ctx, nil, &gen.GetObjectRequest{Connection: conn, Key: key})
	if err != nil {
		t.Fatalf("GetObject (0 bytes): %v", err)
	}
	if len(getResp.GetData()) != 0 {
		t.Errorf("GetObject (0 bytes): got %d bytes, want 0", len(getResp.GetData()))
	}
	if getResp.GetSize() != 0 {
		t.Errorf("GetObject (0 bytes): size = %d, want 0", getResp.GetSize())
	}
	nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: key})
}

// TestIntegration_ListObjects_Pagination is the independent oracle for
// pagination: exactly 5 objects, max_keys=2 must yield pages of 2, 2, 1 with
// is_truncated correctly false only on the last page — the
// "exactly-page-boundary" case the brief calls out by name.
func TestIntegration_ListObjects_Pagination(t *testing.T) {
	bucket := "blob-connector-it-list"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()

	keys := []string{"page/a.txt", "page/b.txt", "page/c.txt", "page/d.txt", "page/e.txt"}
	for _, k := range keys {
		if _, err := nodes.PutObject(ctx, nil, &gen.PutObjectRequest{Connection: conn, Key: k, Data: []byte("x")}); err != nil {
			t.Fatalf("setup PutObject(%s): %v", k, err)
		}
	}
	t.Cleanup(func() {
		for _, k := range keys {
			nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: k})
		}
	})

	var seen []string
	token := ""
	pageSizes := []int{}
	for i := 0; i < 10; i++ { // hard cap so a pagination bug can't infinite-loop the test
		resp, err := nodes.ListObjects(ctx, nil, &gen.ListObjectsRequest{
			Connection:         conn,
			Prefix:             "page/",
			MaxKeys:            2,
			ContinuationToken:  token,
		})
		if err != nil {
			t.Fatalf("ListObjects page %d: %v", i, err)
		}
		pageSizes = append(pageSizes, len(resp.GetObjects()))
		for _, o := range resp.GetObjects() {
			seen = append(seen, o.GetKey())
		}
		if !resp.GetIsTruncated() {
			break
		}
		token = resp.GetNextContinuationToken()
		if token == "" {
			t.Fatal("is_truncated=true but next_continuation_token is empty")
		}
	}

	if len(seen) != len(keys) {
		t.Fatalf("got %d total keys across all pages, want %d (%v)", len(seen), len(keys), seen)
	}
	for _, k := range keys {
		found := false
		for _, s := range seen {
			if s == k {
				found = true
			}
		}
		if !found {
			t.Errorf("key %q never appeared in any page", k)
		}
	}
	wantPageSizes := []int{2, 2, 1}
	if fmt.Sprint(pageSizes) != fmt.Sprint(wantPageSizes) {
		t.Errorf("page sizes = %v, want %v (exact page-boundary behavior)", pageSizes, wantPageSizes)
	}
}

// TestIntegration_CopyObject proves a server-side copy (same bucket) lands
// the destination with matching content, independent of the source.
func TestIntegration_CopyObject(t *testing.T) {
	bucket := "blob-connector-it-crud"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()
	src, dst := "copy/src.txt", "copy/dst.txt"
	body := []byte("copy me")

	if _, err := nodes.PutObject(ctx, nil, &gen.PutObjectRequest{Connection: conn, Key: src, Data: body}); err != nil {
		t.Fatalf("setup PutObject: %v", err)
	}
	t.Cleanup(func() {
		nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: src})
		nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: dst})
	})

	copyResp, err := nodes.CopyObject(ctx, nil, &gen.CopyObjectRequest{
		Connection: conn, SourceKey: src, DestinationKey: dst,
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if copyResp.GetSize() != int64(len(body)) {
		t.Errorf("CopyObject: size = %d, want %d", copyResp.GetSize(), len(body))
	}

	getDst, err := nodes.GetObject(ctx, nil, &gen.GetObjectRequest{Connection: conn, Key: dst})
	if err != nil {
		t.Fatalf("GetObject(dst): %v", err)
	}
	if !bytes.Equal(getDst.GetData(), body) {
		t.Errorf("GetObject(dst): data = %q, want %q", getDst.GetData(), body)
	}
}

// TestIntegration_PresignGet_ActuallyFetchable and
// TestIntegration_PresignPut_ActuallyUploadable are the independent oracles
// for the app-platform flagship: a presigned URL must be usable by a PLAIN
// HTTP client with NO Axiom/Authorization involvement at all — that's the
// entire point of presigning.
func TestIntegration_PresignGet_ActuallyFetchable(t *testing.T) {
	bucket := "blob-connector-it-crud"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()
	key := "presign/get-me.txt"
	body := []byte("fetch me via presigned URL")

	if _, err := nodes.PutObject(ctx, nil, &gen.PutObjectRequest{Connection: conn, Key: key, Data: body}); err != nil {
		t.Fatalf("setup PutObject: %v", err)
	}
	t.Cleanup(func() { nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: key}) })

	presign, err := nodes.PresignGet(ctx, nil, &gen.PresignRequest{Connection: conn, Key: key, ExpirySeconds: 60})
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	url := strings.Replace(presign.GetUrl(), "https://", "http://", 1) // MinIO is running plain HTTP here
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET presigned URL: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET presigned URL: status = %d, want 200", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading presigned GET body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("presigned GET body = %q, want %q", got, body)
	}
}

func TestIntegration_PresignPut_ActuallyUploadable(t *testing.T) {
	bucket := "blob-connector-it-crud"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()
	key := "presign/put-me.txt"
	body := []byte("uploaded via presigned URL")

	presign, err := nodes.PresignPut(ctx, nil, &gen.PresignRequest{Connection: conn, Key: key, ExpirySeconds: 60})
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}
	url := strings.Replace(presign.GetUrl(), "https://", "http://", 1)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building PUT request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT presigned URL: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT presigned URL: status = %d, want 200", resp.StatusCode)
	}
	t.Cleanup(func() { nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: key}) })

	getResp, err := nodes.GetObject(ctx, nil, &gen.GetObjectRequest{Connection: conn, Key: key})
	if err != nil {
		t.Fatalf("GetObject after presigned PUT: %v", err)
	}
	if !bytes.Equal(getResp.GetData(), body) {
		t.Errorf("GetObject after presigned PUT: data = %q, want %q", getResp.GetData(), body)
	}
}

// TestIntegration_PresignGet_ExpiredURLIsRejected proves an actually-expired
// presigned URL is rejected by the server, not silently honored.
func TestIntegration_PresignGet_ExpiredURLIsRejected(t *testing.T) {
	bucket := "blob-connector-it-crud"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()
	key := "presign/expires-fast.txt"
	if _, err := nodes.PutObject(ctx, nil, &gen.PutObjectRequest{Connection: conn, Key: key, Data: []byte("x")}); err != nil {
		t.Fatalf("setup PutObject: %v", err)
	}
	t.Cleanup(func() { nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: key}) })

	presign, err := nodes.PresignGet(ctx, nil, &gen.PresignRequest{Connection: conn, Key: key, ExpirySeconds: 1})
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	url := strings.Replace(presign.GetUrl(), "https://", "http://", 1)
	time.Sleep(3 * time.Second) // let the 1s expiry genuinely elapse

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET expired presigned URL: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Error("expired presigned URL was honored (status 200); expected a rejection (403)")
	}
}

// TestIntegration_StreamObjectBody_MultiChunk is the independent oracle for
// true incremental streaming: a body large enough to span multiple 1 MiB
// chunks must arrive as multiple frames, in order, reassembling byte-for-
// byte to the original, with is_final true on ONLY the last frame.
func TestIntegration_StreamObjectBody_MultiChunk(t *testing.T) {
	bucket := "blob-connector-it-stream"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()
	key := "stream/multi-chunk.bin"

	body := make([]byte, 3*1024*1024+777) // >3 chunks at the ~1 MiB chunk size, with a partial tail
	for i := range body {
		body[i] = byte(i % 256)
	}
	if _, err := nodes.PutObject(ctx, nil, &gen.PutObjectRequest{Connection: conn, Key: key, Data: body, ContentType: "application/octet-stream"}); err != nil {
		t.Fatalf("setup PutObject: %v", err)
	}
	t.Cleanup(func() { nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: key}) })

	in := make(chan *gen.StreamObjectRequest, 1)
	in <- &gen.StreamObjectRequest{Connection: conn, Key: key}
	close(in)

	var frames []*gen.ObjectBodyChunk
	emit := func(f *gen.ObjectBodyChunk) error {
		frames = append(frames, f)
		return nil
	}
	if err := nodes.StreamObjectBody(ctx, nil, in, emit); err != nil {
		t.Fatalf("StreamObjectBody: %v", err)
	}
	if len(frames) < 2 {
		t.Fatalf("expected multiple chunks for a %d-byte body, got %d frame(s)", len(body), len(frames))
	}

	var reassembled []byte
	for i, f := range frames {
		if f.GetErrorCode() != "" {
			t.Fatalf("frame %d: unexpected error frame: %s / %s", i, f.GetErrorCode(), f.GetError())
		}
		wantFinal := i == len(frames)-1
		if f.GetIsFinal() != wantFinal {
			t.Errorf("frame %d: is_final = %v, want %v", i, f.GetIsFinal(), wantFinal)
		}
		if f.GetTotalSize() != int64(len(body)) {
			t.Errorf("frame %d: total_size = %d, want %d", i, f.GetTotalSize(), len(body))
		}
		reassembled = append(reassembled, f.GetData()...)
	}
	if !bytes.Equal(reassembled, body) {
		t.Errorf("reassembled body does not match the original (got %d bytes, want %d)", len(reassembled), len(body))
	}
}

// TestIntegration_StreamObjectBody_ZeroByte proves a 0-byte object emits
// exactly one final frame with empty data, per ObjectBodyChunk's contract.
func TestIntegration_StreamObjectBody_ZeroByte(t *testing.T) {
	bucket := "blob-connector-it-stream"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)
	ctx := context.Background()
	key := "stream/zero-byte.bin"

	if _, err := nodes.PutObject(ctx, nil, &gen.PutObjectRequest{Connection: conn, Key: key, Data: []byte{}}); err != nil {
		t.Fatalf("setup PutObject: %v", err)
	}
	t.Cleanup(func() { nodes.DeleteObject(ctx, nil, &gen.DeleteObjectRequest{Connection: conn, Key: key}) })

	in := make(chan *gen.StreamObjectRequest, 1)
	in <- &gen.StreamObjectRequest{Connection: conn, Key: key}
	close(in)

	var frames []*gen.ObjectBodyChunk
	emit := func(f *gen.ObjectBodyChunk) error {
		frames = append(frames, f)
		return nil
	}
	if err := nodes.StreamObjectBody(ctx, nil, in, emit); err != nil {
		t.Fatalf("StreamObjectBody: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame for a 0-byte object, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() {
		t.Error("lone frame for a 0-byte object must have is_final=true")
	}
	if len(frames[0].GetData()) != 0 {
		t.Errorf("lone frame for a 0-byte object must have empty data, got %d bytes", len(frames[0].GetData()))
	}
}

// TestIntegration_StreamObjectBody_NotFound proves a missing key surfaces as
// the in-band NOT_FOUND terminal frame against a real server.
func TestIntegration_StreamObjectBody_NotFound(t *testing.T) {
	bucket := "blob-connector-it-stream"
	ensureBucket(t, bucket)
	conn := testConnection(bucket)

	in := make(chan *gen.StreamObjectRequest, 1)
	in <- &gen.StreamObjectRequest{Connection: conn, Key: "does/not/exist.bin"}
	close(in)

	var frames []*gen.ObjectBodyChunk
	emit := func(f *gen.ObjectBodyChunk) error {
		frames = append(frames, f)
		return nil
	}
	if err := nodes.StreamObjectBody(context.Background(), nil, in, emit); err != nil {
		t.Fatalf("unexpected hard error: %v", err)
	}
	if len(frames) != 1 || frames[0].GetErrorCode() != "NOT_FOUND" {
		t.Fatalf("got frames %+v, want exactly one NOT_FOUND terminal frame", frames)
	}
}
