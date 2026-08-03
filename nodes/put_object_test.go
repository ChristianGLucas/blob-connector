package nodes_test

import (
	"context"
	"strings"
	"testing"

	"christiangeorgelucas/blob-connector/nodes"

	gen "christiangeorgelucas/blob-connector/gen"
)

// TestPutObject_MissingKey is a pre-flight validation oracle: no key means
// no network attempt at all, and a clear structured error naming the field.
func TestPutObject_MissingKey(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.PutObject(context.Background(), ax, &gen.PutObjectRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
	})
	if err == nil || !strings.Contains(err.Error(), "key is required") {
		t.Errorf("got %v, want an error naming key as required", err)
	}
}

// TestPutObject_MissingCredentials proves a connection with no credential
// mode configured fails BEFORE any network attempt, with a structured error.
func TestPutObject_MissingCredentials(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.PutObject(context.Background(), ax, &gen.PutObjectRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket"},
		Key:        "some/key",
	})
	if err == nil || !strings.Contains(err.Error(), "credentials are required") {
		t.Errorf("got %v, want a credentials-required error", err)
	}
}

// TestPutObject_UnreachableEndpoint proves a connection-refused endpoint
// (nothing listening) surfaces as a typed, bounded error rather than
// hanging — the "unreachable endpoint" oracle the brief calls for, without
// needing live infrastructure.
func TestPutObject_UnreachableEndpoint(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.PutObject(context.Background(), ax, &gen.PutObjectRequest{
		Connection: &gen.Connection{
			Endpoint: "127.0.0.1:1", // nothing listens on port 1: connection refused, fast
			Bucket:   "test-bucket", Insecure: true, PathStyle: true,
			AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
		},
		Key:  "some/key",
		Data: []byte("hello"),
	})
	if err == nil || !strings.Contains(err.Error(), "unreachable endpoint") {
		t.Errorf("got %v, want an \"unreachable endpoint\" error", err)
	}
}
