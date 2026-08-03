package nodes_test

import (
	"context"
	"strings"
	"testing"

	gen "christiangeorgelucas/blob-connector/gen"
	"christiangeorgelucas/blob-connector/nodes"
)

// TestStreamObjectBody_EmptyInput proves a closed, never-seeded input
// channel returns cleanly and emits nothing, rather than hanging or
// panicking.
func TestStreamObjectBody_EmptyInput(t *testing.T) {
	ctx := context.Background()
	ax := newTestContext(t)

	in := make(chan *gen.StreamObjectRequest)
	close(in)

	var frames []*gen.ObjectBodyChunk
	emit := func(f *gen.ObjectBodyChunk) error {
		frames = append(frames, f)
		return nil
	}

	if err := nodes.StreamObjectBody(ctx, ax, in, emit); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("expected 0 frames for empty input, got %d", len(frames))
	}
}

// TestStreamObjectBody_MissingKey proves a missing key is a pre-flight
// validation failure — the node returns a hard error before emitting any
// frame, per this package's documented contract.
func TestStreamObjectBody_MissingKey(t *testing.T) {
	ctx := context.Background()
	ax := newTestContext(t)

	in := make(chan *gen.StreamObjectRequest, 1)
	in <- &gen.StreamObjectRequest{
		Connection: &gen.Connection{Endpoint: "example.invalid", Bucket: "test-bucket", AccessKey: "a", SecretKey: "s"},
	}
	close(in)

	var frames []*gen.ObjectBodyChunk
	emit := func(f *gen.ObjectBodyChunk) error {
		frames = append(frames, f)
		return nil
	}

	err := nodes.StreamObjectBody(ctx, ax, in, emit)
	if err == nil || !strings.Contains(err.Error(), "key is required") {
		t.Errorf("got %v, want an error naming key as required", err)
	}
	if len(frames) != 0 {
		t.Errorf("expected no frames emitted for a pre-flight validation failure, got %d", len(frames))
	}
}

// TestStreamObjectBody_UnreachableEndpoint proves a runtime failure
// (unlike a pre-flight validation failure) is surfaced as the in-band
// terminal error frame — never a hard node error — matching
// ObjectBodyChunk's documented contract.
func TestStreamObjectBody_UnreachableEndpoint(t *testing.T) {
	ctx := context.Background()
	ax := newTestContext(t)

	in := make(chan *gen.StreamObjectRequest, 1)
	in <- &gen.StreamObjectRequest{
		Connection: &gen.Connection{
			Endpoint: "127.0.0.1:1", Bucket: "test-bucket", Insecure: true, PathStyle: true,
			AccessKey: "AKIAEXAMPLE", SecretKey: "secretkeyexample",
		},
		Key: "some/key",
	}
	close(in)

	var frames []*gen.ObjectBodyChunk
	emit := func(f *gen.ObjectBodyChunk) error {
		frames = append(frames, f)
		return nil
	}

	if err := nodes.StreamObjectBody(ctx, ax, in, emit); err != nil {
		t.Fatalf("unexpected hard error (runtime failures must be in-band frames): %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 terminal error frame, got %d", len(frames))
	}
	f := frames[0]
	if f.GetErrorCode() != "UNREACHABLE_ENDPOINT" {
		t.Errorf("error_code = %q, want UNREACHABLE_ENDPOINT", f.GetErrorCode())
	}
	if !f.GetIsFinal() {
		t.Error("terminal error frame must have is_final=true")
	}
	if len(f.GetData()) != 0 {
		t.Error("terminal error frame must carry no body data")
	}
}
