package nodes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// fakeSecrets is a minimal axiom.Secrets for internal tests — this file is
// `package nodes` (needs unexported helpers), so it can't reuse nodes_test's
// testContext fake from helpers_test.go.
type fakeSecrets map[string]string

func (s fakeSecrets) Get(name string) (string, bool) { v, ok := s[name]; return v, ok }
func (s fakeSecrets) Status(name string) axiom.SecretStatus {
	if _, ok := s[name]; ok {
		return axiom.SecretStatusAvailable
	}
	return axiom.SecretStatusUnset
}

// fakeContext implements only the axiom.Context methods these helpers touch.
type fakeContext struct{ secrets fakeSecrets }

func (c fakeContext) Log() axiom.Logger            { return nil }
func (c fakeContext) Secrets() axiom.Secrets       { return c.secrets }
func (c fakeContext) ExecutionID() string          { return "" }
func (c fakeContext) FlowID() string               { return "" }
func (c fakeContext) TenantID() string             { return "" }
func (c fakeContext) Reflection() axiom.Reflection { return nil }
func (c fakeContext) Mutation() axiom.Mutation     { return nil }

// TestResolveCredentials_Precedence is the independent oracle for
// Connection's dual-mode credential input: named secrets (both parts) win
// over raw fields when all are set; raw fields work alone; mixing one
// secret-named part with one raw part is a structured error; an
// unconfigured secret name is a structured error, never a silent fallback.
func TestResolveCredentials_Precedence(t *testing.T) {
	ax := fakeContext{secrets: fakeSecrets{"AK": "secret-ak", "SK": "secret-sk"}}

	ak, sk, err := resolveCredentials(ax, &gen.Connection{
		AccessKeySecretName: "AK", SecretKeySecretName: "SK",
		AccessKey: "raw-ak", SecretKey: "raw-sk",
	})
	if err != nil || ak != "secret-ak" || sk != "secret-sk" {
		t.Errorf("both set: got (%q, %q, %v), want the SECRET values to win", ak, sk, err)
	}

	ak, sk, err = resolveCredentials(ax, &gen.Connection{AccessKey: "raw-ak", SecretKey: "raw-sk"})
	if err != nil || ak != "raw-ak" || sk != "raw-sk" {
		t.Errorf("raw only: got (%q, %q, %v), want the raw values", ak, sk, err)
	}

	if _, _, err := resolveCredentials(ax, &gen.Connection{AccessKeySecretName: "AK"}); err == nil {
		t.Error("only access_key_secret_name set (secret_key_secret_name empty): expected a pairing error, got nil")
	}
	if _, _, err := resolveCredentials(ax, &gen.Connection{AccessKeySecretName: "AK", SecretKey: "raw-sk"}); err == nil {
		t.Error("mixed mode (secret access key + raw secret key): expected an error, got nil")
	}
	if _, _, err := resolveCredentials(ax, &gen.Connection{AccessKeySecretName: "NEVER_CONFIGURED", SecretKeySecretName: "SK"}); err == nil {
		t.Error("unconfigured secret name: expected an error, not a silent partial resolution")
	}
	if _, _, err := resolveCredentials(ax, &gen.Connection{}); err == nil {
		t.Error("all four credential fields empty: expected an error, got nil")
	}
	if _, _, err := resolveCredentials(ax, &gen.Connection{AccessKey: "raw-ak"}); err == nil {
		t.Error("only raw access_key set (secret_key empty): expected an error, got nil")
	}

	// An empty-string secret VALUE (Get returns ok=true, value="") must be
	// treated the same as "not configured".
	axEmpty := fakeContext{secrets: fakeSecrets{"AK": "", "SK": "sk"}}
	if _, _, err := resolveCredentials(axEmpty, &gen.Connection{AccessKeySecretName: "AK", SecretKeySecretName: "SK"}); err == nil {
		t.Error("secret resolves to an empty string: expected a not-configured error, got nil")
	}
}

// TestClampExpiry is the independent oracle for PresignRequest.expiry_seconds
// validation: 0 defaults to 1 hour, [1, 604800] passes through unchanged,
// and anything outside that AWS SigV4 hard ceiling is a structured error.
func TestClampExpiry(t *testing.T) {
	got, err := clampExpiry(0)
	if err != nil || got != time.Hour {
		t.Errorf("zero: got (%v, %v), want (1h, nil)", got, err)
	}
	got, err = clampExpiry(1)
	if err != nil || got != time.Second {
		t.Errorf("min boundary (1): got (%v, %v), want (1s, nil)", got, err)
	}
	got, err = clampExpiry(604800)
	if err != nil || got != 604800*time.Second {
		t.Errorf("max boundary (604800): got (%v, %v), want (604800s, nil)", got, err)
	}
	if _, err := clampExpiry(604801); err == nil {
		t.Error("604801 (one over the ceiling): expected an error, got nil")
	}
	if _, err := clampExpiry(-1); err == nil {
		t.Error("negative expiry: expected an error, got nil")
	}
}

// TestNormalizeMetadata strips the canonical S3 user-metadata header prefix
// case-insensitively, and leaves already-plain keys untouched.
func TestNormalizeMetadata(t *testing.T) {
	got := normalizeMetadata(map[string]string{
		"X-Amz-Meta-Owner": "alice",
		"x-amz-meta-team":  "platform",
		"plain-key":        "value",
	})
	want := map[string]string{"owner": "alice", "team": "platform", "plain-key": "value"}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q: got %q, want %q (full map: %v)", k, got[k], v, got)
		}
	}
}

// TestClassify_KnownErrorCodes is the independent oracle for error
// categorization: each S3 API error Code minio-go can hand back is
// hand-mapped to the expected message category — never left as an opaque
// Go error whose type a caller can't distinguish.
func TestClassify_KnownErrorCodes(t *testing.T) {
	cases := []struct {
		code       string
		wantSubstr string
	}{
		{"NoSuchBucket", "not found"},
		{"NoSuchKey", "not found"},
		{"AccessDenied", "access denied"},
		{"InvalidAccessKeyId", "invalid credentials"},
		{"SignatureDoesNotMatch", "invalid credentials"},
	}
	for _, c := range cases {
		err := classify(minio.ErrorResponse{Code: c.code, Message: "provider said so"}, "endpoint.example", "my-bucket", "my/key")
		if err == nil {
			t.Errorf("%s: expected a non-nil classified error", c.code)
			continue
		}
		if !strings.Contains(strings.ToLower(err.Error()), c.wantSubstr) {
			t.Errorf("%s: classified error %q does not contain %q", c.code, err.Error(), c.wantSubstr)
		}
	}

	// An unrecognized-but-real S3 error code still surfaces the provider's
	// own code and message rather than being swallowed into a generic string.
	err := classify(minio.ErrorResponse{Code: "SlowDown", Message: "please retry"}, "endpoint", "bucket", "key")
	if err == nil || !strings.Contains(err.Error(), "SlowDown") || !strings.Contains(err.Error(), "please retry") {
		t.Errorf("unrecognized S3 code: got %v, want it to name the code and message", err)
	}

	if classify(nil, "e", "b", "k") != nil {
		t.Error("nil error: expected classify to return nil")
	}
}

// TestIsCommonPrefixEntry is the independent oracle for distinguishing a
// "directory-like" common prefix (minio-go populates ONLY Key) from a real
// object — including the tricky case of a genuine 0-byte real object, which
// still carries a non-empty ETag and non-zero LastModified and must NOT be
// misclassified as a common prefix.
func TestIsCommonPrefixEntry(t *testing.T) {
	if !isCommonPrefixEntry(minio.ObjectInfo{Key: "reports/2026/"}) {
		t.Error("prefix-only entry (no ETag/LastModified): expected true")
	}
	zeroByteObject := minio.ObjectInfo{
		Key:          "reports/2026/.keep",
		ETag:         "d41d8cd98f00b204e9800998ecf8427e",
		LastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	if isCommonPrefixEntry(zeroByteObject) {
		t.Error("genuine 0-byte object with a real ETag+LastModified: expected false, misclassified as a common prefix")
	}
}

// TestResolveSourceBucket confirms CopyObjectRequest's source_bucket default:
// empty means "same as the destination bucket" (same-bucket copy/re-key).
func TestResolveSourceBucket(t *testing.T) {
	if got := resolveSourceBucket(&gen.CopyObjectRequest{SourceBucket: "explicit"}, "dest"); got != "explicit" {
		t.Errorf("explicit source_bucket: got %q, want %q", got, "explicit")
	}
	if got := resolveSourceBucket(&gen.CopyObjectRequest{}, "dest"); got != "dest" {
		t.Errorf("empty source_bucket: got %q, want the destination bucket %q", got, "dest")
	}
}

// TestClassifyStreamError is the independent oracle for StreamObjectBody's
// in-band terminal error frame token vocabulary.
func TestClassifyStreamError(t *testing.T) {
	cases := []struct {
		err      error
		wantCode string
	}{
		{minio.ErrorResponse{Code: "NoSuchKey"}, "NOT_FOUND"},
		{minio.ErrorResponse{Code: "NoSuchBucket"}, "NOT_FOUND"},
		{minio.ErrorResponse{Code: "AccessDenied"}, "ACCESS_DENIED"},
		{minio.ErrorResponse{Code: "InternalError"}, "BODY_READ_FAILED"},
	}
	for _, c := range cases {
		code, msg := classifyStreamError(c.err)
		if code != c.wantCode {
			t.Errorf("%v: got code %q, want %q", c.err, code, c.wantCode)
		}
		if msg == "" {
			t.Errorf("%v: expected a non-empty message", c.err)
		}
	}
}

// TestNewClient_PathStyleForcesBucketIntoPath is a signing/addressing oracle
// using a real HTTP server (httptest, not a mock of our own code): with
// path_style=true, the client MUST send requests as
// "http(s)://endpoint/bucket/key" (bucket in the path, plain endpoint as
// Host) rather than "http(s)://bucket.endpoint/key" — the addressing style
// MinIO, R2, and most self-hosted endpoints require.
func TestNewClient_PathStyleForcesBucketIntoPath(t *testing.T) {
	var gotHost, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	endpoint := strings.TrimPrefix(srv.URL, "http://")
	ax := fakeContext{secrets: fakeSecrets{}}
	cl, err := newClient(ax, &gen.Connection{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		Bucket:    "test-bucket",
		Insecure:  true,
		PathStyle: true,
		AccessKey: "AKIAEXAMPLE",
		SecretKey: "secretkeyexample",
	})
	if err != nil {
		t.Fatalf("newClient: unexpected error: %v", err)
	}
	// Any request is enough to observe addressing; StatObject is cheap and
	// its (expected, injected) NotFound error is irrelevant here.
	_, _ = cl.StatObject(context.Background(), "test-bucket", "some/key", minio.StatObjectOptions{})

	if gotHost != endpoint {
		t.Errorf("path-style Host: got %q, want the plain endpoint %q (no bucket subdomain)", gotHost, endpoint)
	}
	if !strings.HasPrefix(gotPath, "/test-bucket/") {
		t.Errorf("path-style URL path: got %q, want it to start with /test-bucket/", gotPath)
	}
}
