package nodes

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// HeadObject checks whether an object exists and reads its size, ETag,
// content type, metadata, and last-modified time — WITHOUT downloading its
// body. Unlike GetObject, a missing key is reported as exists=false rather
// than a structured error, since checking existence is the whole point of
// this node; a genuine failure (bad credentials, unreachable endpoint,
// nonexistent bucket) is still a structured error.
func HeadObject(ctx context.Context, ax axiom.Context, input *gen.HeadObjectRequest) (*gen.HeadObjectResponse, error) {
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

	info, err := cl.StatObject(opCtx, conn.GetBucket(), key, minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == "NoSuchKey" {
			return &gen.HeadObjectResponse{Exists: false}, nil
		}
		return nil, classify(err, conn.GetEndpoint(), conn.GetBucket(), key)
	}

	return &gen.HeadObjectResponse{
		Exists:       true,
		Size:         info.Size,
		Etag:         info.ETag,
		ContentType:  info.ContentType,
		Metadata:     normalizeMetadata(info.UserMetadata),
		LastModified: info.LastModified.UTC().Format(time.RFC3339),
	}, nil
}
