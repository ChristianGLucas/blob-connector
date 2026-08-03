package nodes_test

import (
	"context"
	"strings"
	"testing"
	"time"

	gen "christiangeorgelucas/blob-connector/gen"
	"christiangeorgelucas/blob-connector/nodes"
)

// TestPresignGet_MissingKey is a pre-flight validation oracle.
func TestPresignGet_MissingKey(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.PresignGet(context.Background(), ax, &gen.PresignRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
	})
	if err == nil || !strings.Contains(err.Error(), "key is required") {
		t.Errorf("got %v, want an error naming key as required", err)
	}
}

// TestPresignGet_ExpiryOutOfRange proves the AWS SigV4 expiry ceiling is
// enforced as a structured error, not silently clamped.
func TestPresignGet_ExpiryOutOfRange(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.PresignGet(context.Background(), ax, &gen.PresignRequest{
		Connection:     &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
		Key:            "some/key",
		ExpirySeconds:  604801,
	})
	if err == nil || !strings.Contains(err.Error(), "expiry_seconds") {
		t.Errorf("got %v, want an expiry_seconds range error", err)
	}
}

// TestPresignGet_WellFormedURL is the independent oracle for this node's
// core job: presigning is PURE LOCAL SigV4 computation (minio-go never
// makes a network call for it), so this runs with no live infrastructure
// and checks the result against the well-documented AWS SigV4
// query-presigning format — a structural spec independent of this
// package's own code.
func TestPresignGet_WellFormedURL(t *testing.T) {
	ax := newTestContext(t)
	got, err := nodes.PresignGet(context.Background(), ax, &gen.PresignRequest{
		Connection: &gen.Connection{
			Endpoint: "s3.example.com", Bucket: "my-bucket", Region: "us-east-1",
			PathStyle: true, AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
		},
		Key:           "reports/2026/q1.csv",
		ExpirySeconds: 900,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.GetUrl() == "" {
		t.Fatal("expected a non-empty presigned URL")
	}
	if !strings.Contains(got.GetUrl(), "my-bucket/reports/2026/q1.csv") {
		t.Errorf("URL %q does not contain the path-style bucket/key", got.GetUrl())
	}
	if !strings.Contains(got.GetUrl(), "X-Amz-Algorithm=AWS4-HMAC-SHA256") {
		t.Errorf("URL %q is missing the SigV4 algorithm query param", got.GetUrl())
	}
	if !strings.Contains(got.GetUrl(), "X-Amz-Expires=900") {
		t.Errorf("URL %q does not carry the requested 900s expiry", got.GetUrl())
	}
	if !strings.Contains(got.GetUrl(), "X-Amz-Signature=") {
		t.Errorf("URL %q is missing a signature", got.GetUrl())
	}
	expiresAt, err := time.Parse(time.RFC3339, got.GetExpiresAt())
	if err != nil {
		t.Fatalf("expires_at %q is not RFC 3339: %v", got.GetExpiresAt(), err)
	}
	if d := time.Until(expiresAt); d < 890*time.Second || d > 900*time.Second {
		t.Errorf("expires_at %v is not ~900s from now (delta %v)", expiresAt, d)
	}
}

// TestPresignGet_DefaultExpiry proves expiry_seconds=0 defaults to 1 hour
// rather than an immediately-expired or unbounded URL.
func TestPresignGet_DefaultExpiry(t *testing.T) {
	ax := newTestContext(t)
	got, err := nodes.PresignGet(context.Background(), ax, &gen.PresignRequest{
		Connection: &gen.Connection{
			Endpoint: "s3.example.com", Bucket: "my-bucket", PathStyle: true,
			AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
		},
		Key: "some/key",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got.GetUrl(), "X-Amz-Expires=3600") {
		t.Errorf("URL %q does not carry the default 3600s (1h) expiry", got.GetUrl())
	}
}
