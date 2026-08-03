package nodes_test

import (
	"context"
	"strings"
	"testing"

	gen "christiangeorgelucas/blob-connector/gen"
	"christiangeorgelucas/blob-connector/nodes"
)

// TestListObjects_InvalidDelimiter proves an unsupported delimiter is a
// structured, pre-flight error naming the real client-library constraint —
// never a silently-wrong grouping.
func TestListObjects_InvalidDelimiter(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.ListObjects(context.Background(), ax, &gen.ListObjectsRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
		Delimiter:  ",",
	})
	if err == nil || !strings.Contains(err.Error(), "delimiter must be") {
		t.Errorf("got %v, want an error naming the delimiter constraint", err)
	}
}

// TestListObjects_NegativeMaxKeys is a pre-flight validation oracle.
func TestListObjects_NegativeMaxKeys(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.ListObjects(context.Background(), ax, &gen.ListObjectsRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
		MaxKeys:    -1,
	})
	if err == nil || !strings.Contains(err.Error(), "max_keys must not be negative") {
		t.Errorf("got %v, want an error naming max_keys as invalid", err)
	}
}

// TestListObjects_UnreachableEndpoint proves an unreachable endpoint fails
// with a typed, bounded error instead of hanging.
func TestListObjects_UnreachableEndpoint(t *testing.T) {
	ax := newTestContext(t)
	_, err := nodes.ListObjects(context.Background(), ax, &gen.ListObjectsRequest{
		Connection: &gen.Connection{
			Endpoint: "127.0.0.1:1", Bucket: "test-bucket", Insecure: true, PathStyle: true,
			AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "unreachable endpoint") {
		t.Errorf("got %v, want an \"unreachable endpoint\" error", err)
	}
}
