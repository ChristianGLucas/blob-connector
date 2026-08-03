package nodes_test

import (
	"context"
	"strings"
	"testing"

	gen "christiangeorgelucas/blob-connector/gen"
	"christiangeorgelucas/blob-connector/nodes"
)

// TestHeadObject_MissingKey is a pre-flight validation oracle.
func TestHeadObject_MissingKey(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.HeadObject(context.Background(), ax, &gen.HeadObjectRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
	})
	if err == nil || !strings.Contains(err.Error(), "key is required") {
		t.Errorf("got %v, want an error naming key as required", err)
	}
}

// TestHeadObject_UnreachableEndpoint proves a genuine failure (as opposed to
// a missing object) still surfaces as a structured error, not exists=false —
// only a real NoSuchKey response maps to exists=false.
func TestHeadObject_UnreachableEndpoint(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.HeadObject(context.Background(), ax, &gen.HeadObjectRequest{
		Connection: &gen.Connection{
			Endpoint: "127.0.0.1:1", Bucket: "test-bucket", Insecure: true, PathStyle: true,
			AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
		},
		Key: "some/key",
	})
	if err == nil || !strings.Contains(err.Error(), "unreachable endpoint") {
		t.Errorf("got %v, want an \"unreachable endpoint\" error, not a plain exists=false response", err)
	}
}
