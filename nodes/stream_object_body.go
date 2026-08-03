package nodes

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"

	"github.com/minio/minio-go/v7"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// streamChunkSize is the nominal size of each emitted ObjectBodyChunk, well
// under the platform's 24 MiB/frame ceiling.
const streamChunkSize = 1 << 20 // 1 MiB

// StreamObjectBody streams one object's body incrementally, one chunk per
// frame, using a true streaming read from the provider — the object is
// never materialized whole in memory. Frame field names mirror
// christiangeorgelucas/http-tools' StreamBodyChunk (data, byte_offset,
// error_code, error, is_final) so downstream pipeline nodes that already
// compose with an HTTP byte stream compose with a bucket-backed one
// identically. Idempotent: a redelivered execution restarts the read from
// byte 0.
// inputs: stream of gen.StreamObjectRequest frames from the previous node.
// For the start node of a pipeline flow, the channel yields exactly one item.
func StreamObjectBody(ctx context.Context, ax axiom.Context, in <-chan *gen.StreamObjectRequest, emit func(*gen.ObjectBodyChunk) error) error {
	for input := range in {
		conn := input.GetConnection()
		key := strings.TrimSpace(input.GetKey())
		// Pre-flight validation failures (missing key, unresolvable
		// connection/credentials) fail the node before any frame is
		// emitted — the same contract as a unary node's default error
		// behavior, and distinct from a runtime failure reaching the
		// provider (see below).
		if key == "" {
			return errors.New("key is required")
		}
		cl, err := newClient(ax, conn)
		if err != nil {
			return err
		}
		if err := streamOneObject(ctx, cl, conn.GetBucket(), key, emit); err != nil {
			return err
		}
	}
	return nil
}

// streamOneObject reads and emits one object's body in chunks. A runtime
// failure reaching or reading the object (not-found, access denied,
// unreachable endpoint, mid-read failure) is surfaced as the in-band
// terminal error frame — a pipeline node has no unary error envelope once
// frames may have started — and this function returns nil so the caller
// moves on to the next queued item rather than aborting the whole node. It
// returns a non-nil error only when emit() itself fails (e.g. the platform
// is tearing the stream down), which must propagate and stop everything.
func streamOneObject(ctx context.Context, cl *minio.Client, bucket, key string, emit func(*gen.ObjectBodyChunk) error) error {
	obj, err := cl.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return emitStreamError(emit, err)
	}
	defer obj.Close()

	info, err := obj.Stat()
	if err != nil {
		return emitStreamError(emit, err)
	}
	totalSize := info.Size
	contentType := info.ContentType

	buf := make([]byte, streamChunkSize)
	var offset int64
	for {
		n, readErr := obj.Read(buf)
		if n > 0 {
			isFinal := readErr == io.EOF
			chunk := &gen.ObjectBodyChunk{
				Data:        append([]byte(nil), buf[:n]...),
				ByteOffset:  offset,
				TotalSize:   totalSize,
				ContentType: contentType,
				IsFinal:     isFinal,
			}
			if err := emit(chunk); err != nil {
				return err
			}
			offset += int64(n)
			if isFinal {
				return nil
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				if offset == 0 {
					// Zero-byte object: emit the lone final frame.
					return emit(&gen.ObjectBodyChunk{
						TotalSize:   totalSize,
						ContentType: contentType,
						IsFinal:     true,
					})
				}
				return nil
			}
			return emitStreamError(emit, readErr)
		}
	}
}

// emitStreamError classifies a runtime failure into the terminal
// ObjectBodyChunk error frame and emits it. See ObjectBodyChunk.error_code
// for the token vocabulary.
func emitStreamError(emit func(*gen.ObjectBodyChunk) error, err error) error {
	code, message := classifyStreamError(err)
	return emit(&gen.ObjectBodyChunk{
		ErrorCode: code,
		Error:     message,
		IsFinal:   true,
	})
}

func classifyStreamError(err error) (code, message string) {
	if errors.Is(err, context.DeadlineExceeded) {
		return "TIMEOUT", "the platform's node-stream time ceiling was reached"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return "UNREACHABLE_ENDPOINT", err.Error()
	}
	resp := minio.ToErrorResponse(err)
	switch resp.Code {
	case "NoSuchBucket", "NoSuchKey", "NoSuchVersion":
		return "NOT_FOUND", err.Error()
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
		return "ACCESS_DENIED", err.Error()
	}
	return "BODY_READ_FAILED", err.Error()
}
