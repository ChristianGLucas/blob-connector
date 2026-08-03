package nodes

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// GetObject downloads an object's full body inline, with its content type,
// metadata, ETag, size, and last-modified time. A nonexistent bucket or key
// is a structured error, never a response with an empty body. For objects
// too large to comfortably return inline, use StreamObjectBody (incremental
// frames) or PresignGet (a direct download URL bypassing this connector's
// payload path entirely).
func GetObject(ctx context.Context, ax axiom.Context, input *gen.GetObjectRequest) (*gen.GetObjectResponse, error) {
	conn := input.GetConnection()
	key := strings.TrimSpace(input.GetKey())
	if key == "" {
		return nil, errors.New("key is required")
	}
	cl, err := newClient(ax, conn)
	if err != nil {
		return nil, err
	}

	opCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	obj, err := cl.GetObject(opCtx, conn.GetBucket(), key, minio.GetObjectOptions{})
	if err != nil {
		return nil, classify(err, conn.GetEndpoint(), conn.GetBucket(), key)
	}
	defer obj.Close()

	// GetObject never itself contacts the server — Stat (or the first Read)
	// is where a nonexistent key/bucket actually surfaces.
	info, err := obj.Stat()
	if err != nil {
		return nil, classify(err, conn.GetEndpoint(), conn.GetBucket(), key)
	}
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, classify(err, conn.GetEndpoint(), conn.GetBucket(), key)
	}

	return &gen.GetObjectResponse{
		Data:         data,
		ContentType:  info.ContentType,
		Metadata:     normalizeMetadata(info.UserMetadata),
		Etag:         info.ETag,
		Size:         info.Size,
		LastModified: info.LastModified.UTC().Format(time.RFC3339),
	}, nil
}
