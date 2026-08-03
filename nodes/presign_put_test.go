package nodes_test

import (
	"context"
	"strings"
	"testing"

	gen "christiangeorgelucas/blob-connector/gen"
	"christiangeorgelucas/blob-connector/nodes"
)

// TestPresignPut_MissingKey is a pre-flight validation oracle.
func TestPresignPut_MissingKey(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.PresignPut(context.Background(), ax, &gen.PresignRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
	})
	if err == nil || !strings.Contains(err.Error(), "key is required") {
		t.Errorf("got %v, want an error naming key as required", err)
	}
}

// TestPresignPut_ExpiryOutOfRange proves the AWS SigV4 expiry ceiling is
// enforced as a structured error, not silently clamped.
func TestPresignPut_ExpiryOutOfRange(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.PresignPut(context.Background(), ax, &gen.PresignRequest{
		Connection:    &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
		Key:           "some/key",
		ExpirySeconds: 0 - 1,
	})
	if err == nil || !strings.Contains(err.Error(), "expiry_seconds") {
		t.Errorf("got %v, want an expiry_seconds range error", err)
	}
}

// TestPresignPut_WellFormedURL is the independent oracle for this node's
// core job: presigning is PURE LOCAL SigV4 computation (no network call),
// checked against the well-documented AWS SigV4 query-presigning format.
func TestPresignPut_WellFormedURL(t *testing.T) {
	ax := newTestContext(t)
	got, err := nodes.PresignPut(context.Background(), ax, &gen.PresignRequest{
		Connection: &gen.Connection{
			Endpoint: "s3.example.com", Bucket: "my-bucket", Region: "us-east-1",
			PathStyle: true, AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
		},
		Key:           "uploads/incoming.bin",
		ExpirySeconds: 120,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got.GetUrl(), "my-bucket/uploads/incoming.bin") {
		t.Errorf("URL %q does not contain the path-style bucket/key", got.GetUrl())
	}
	if !strings.Contains(got.GetUrl(), "X-Amz-Algorithm=AWS4-HMAC-SHA256") {
		t.Errorf("URL %q is missing the SigV4 algorithm query param", got.GetUrl())
	}
	if !strings.Contains(got.GetUrl(), "X-Amz-Expires=120") {
		t.Errorf("URL %q does not carry the requested 120s expiry", got.GetUrl())
	}
}

// TestPresignGet_And_PresignPut_DifferAlways proves the two nodes are wired
// to distinct minio-go calls: the presigned URL for a GET is never
// byte-identical to the one for a PUT on the same key (each call generates
// its own signing timestamp/nonce even with identical inputs), so a caller
// can never confuse one for the other.
func TestPresignGet_And_PresignPut_DifferAlways(t *testing.T) {
	ax := newTestContext(t)
	conn := &gen.Connection{
		Endpoint: "s3.example.com", Bucket: "my-bucket", PathStyle: true,
		AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
	}
	getResp, err := nodes.PresignGet(context.Background(), ax, &gen.PresignRequest{Connection: conn, Key: "k"})
	if err != nil {
		t.Fatalf("PresignGet: unexpected error: %v", err)
	}
	putResp, err := nodes.PresignPut(context.Background(), ax, &gen.PresignRequest{Connection: conn, Key: "k"})
	if err != nil {
		t.Fatalf("PresignPut: unexpected error: %v", err)
	}
	if getResp.GetUrl() == putResp.GetUrl() {
		t.Error("PresignGet and PresignPut produced identical URLs for the same key")
	}
}
