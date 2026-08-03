package nodes

import (
	"context"
	"errors"
	"strings"

	"github.com/minio/minio-go/v7"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// DeleteObject deletes one object by key. Deletion is IDEMPOTENT: deleting a
// key that does not exist is reported as an ordinary success
// (deleted=true), matching S3's own DELETE semantics — so a retried
// at-least-once delivery safely re-deletes an already-gone key instead of
// surfacing a spurious not-found error.
func DeleteObject(ctx context.Context, ax axiom.Context, input *gen.DeleteObjectRequest) (*gen.DeleteObjectResponse, error) {
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

	if err := cl.RemoveObject(opCtx, conn.GetBucket(), key, minio.RemoveObjectOptions{}); err != nil {
		return nil, classify(err, conn.GetEndpoint(), conn.GetBucket(), key)
	}
	return &gen.DeleteObjectResponse{Deleted: true}, nil
}
