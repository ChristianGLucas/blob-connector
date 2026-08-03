package nodes_test

import (
	"context"
	"strings"
	"testing"

	gen "christiangeorgelucas/blob-connector/gen"
	"christiangeorgelucas/blob-connector/nodes"
)

// TestCopyObject_MissingSourceKey is a pre-flight validation oracle.
func TestCopyObject_MissingSourceKey(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.CopyObject(context.Background(), ax, &gen.CopyObjectRequest{
		Connection:     &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
		DestinationKey: "dst",
	})
	if err == nil || !strings.Contains(err.Error(), "source_key is required") {
		t.Errorf("got %v, want an error naming source_key as required", err)
	}
}

// TestCopyObject_MissingDestinationKey is a pre-flight validation oracle.
func TestCopyObject_MissingDestinationKey(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.CopyObject(context.Background(), ax, &gen.CopyObjectRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
		SourceKey:  "src",
	})
	if err == nil || !strings.Contains(err.Error(), "destination_key is required") {
		t.Errorf("got %v, want an error naming destination_key as required", err)
	}
}

// TestCopyObject_UnreachableEndpoint proves an unreachable endpoint fails
// with a typed, bounded error instead of hanging.
func TestCopyObject_UnreachableEndpoint(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.CopyObject(context.Background(), ax, &gen.CopyObjectRequest{
		Connection: &gen.Connection{
			Endpoint: "127.0.0.1:1", Bucket: "test-bucket", Insecure: true, PathStyle: true,
			AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
		},
		SourceKey:      "src",
		DestinationKey: "dst",
	})
	if err == nil || !strings.Contains(err.Error(), "unreachable endpoint") {
		t.Errorf("got %v, want an \"unreachable endpoint\" error", err)
	}
}
