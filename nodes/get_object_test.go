package nodes_test

import (
	"context"
	"strings"
	"testing"

	gen "christiangeorgelucas/blob-connector/gen"
	"christiangeorgelucas/blob-connector/nodes"
)

// TestGetObject_MissingKey is a pre-flight validation oracle.
func TestGetObject_MissingKey(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.GetObject(context.Background(), ax, &gen.GetObjectRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
	})
	if err == nil || !strings.Contains(err.Error(), "key is required") {
		t.Errorf("got %v, want an error naming key as required", err)
	}
}

// TestGetObject_UnreachableEndpoint proves an unreachable endpoint fails
// with a typed, bounded error instead of hanging.
func TestGetObject_UnreachableEndpoint(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.GetObject(context.Background(), ax, &gen.GetObjectRequest{
		Connection: &gen.Connection{
			Endpoint: "127.0.0.1:1", Bucket: "test-bucket", Insecure: true, PathStyle: true,
			AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
		},
		Key: "some/key",
	})
	if err == nil || !strings.Contains(err.Error(), "unreachable endpoint") {
		t.Errorf("got %v, want an \"unreachable endpoint\" error", err)
	}
}
