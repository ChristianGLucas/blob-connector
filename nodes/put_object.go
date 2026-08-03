package nodes

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/minio/minio-go/v7"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// PutObject uploads an object's full body inline, with content type and
// optional user metadata. The body travels through the platform's normal
// request/response envelope, so it is bounded by that envelope's size limit
// — for uploads too large to fit inline, use PresignPut instead. Last-writer-
// wins idempotent: a retried at-least-once delivery simply re-uploads the
// same bytes to the same key.
func PutObject(ctx context.Context, ax axiom.Context, input *gen.PutObjectRequest) (*gen.PutObjectResponse, error) {
	conn := input.GetConnection()
	key := strings.TrimSpace(input.GetKey())
	if key == "" {
		return nil, errors.New("key is required")
	}
	cl, err := newClient(ax, conn)
	if err != nil {
		return nil, err
	}
	contentType := input.GetContentType()
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	opCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	data := input.GetData()
	info, err := cl.PutObject(opCtx, conn.GetBucket(), key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType:  contentType,
		UserMetadata: input.GetMetadata(),
	})
	if err != nil {
		return nil, classify(err, conn.GetEndpoint(), conn.GetBucket(), key)
	}
	return &gen.PutObjectResponse{
		Etag: info.ETag,
		Size: info.Size,
	}, nil
}
