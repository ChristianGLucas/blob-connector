package nodes

import (
	"context"
	"errors"
	"strings"

	"github.com/minio/minio-go/v7"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// CopyObject performs a server-side copy of one object within a bucket or
// between two buckets on the same connection/provider — the bytes never
// transit through this connector or the caller. Idempotent: re-running the
// same copy simply overwrites the destination with the same result.
func CopyObject(ctx context.Context, ax axiom.Context, input *gen.CopyObjectRequest) (*gen.CopyObjectResponse, error) {
	conn := input.GetConnection()
	srcKey := strings.TrimSpace(input.GetSourceKey())
	if srcKey == "" {
		return nil, errors.New("source_key is required")
	}
	dstKey := strings.TrimSpace(input.GetDestinationKey())
	if dstKey == "" {
		return nil, errors.New("destination_key is required")
	}
	srcBucket := strings.TrimSpace(input.GetSourceBucket())
	if srcBucket == "" {
		srcBucket = conn.GetBucket()
	}

	cl, err := newClient(ax, conn)
	if err != nil {
		return nil, err
	}

	opCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	if _, err := cl.CopyObject(opCtx,
		minio.CopyDestOptions{Bucket: conn.GetBucket(), Object: dstKey},
		minio.CopySrcOptions{Bucket: srcBucket, Object: srcKey},
	); err != nil {
		return nil, classify(err, conn.GetEndpoint(), srcBucket, srcKey)
	}

	// The S3 CopyObject response carries ETag and LastModified but never the
	// destination's size (live-verified against MinIO: minio-go's CopyObject
	// leaves UploadInfo.Size at its zero value) — a HEAD on the freshly
	// written destination is the only way to report a real size here.
	info, err := cl.StatObject(opCtx, conn.GetBucket(), dstKey, minio.StatObjectOptions{})
	if err != nil {
		return nil, classify(err, conn.GetEndpoint(), conn.GetBucket(), dstKey)
	}
	return &gen.CopyObjectResponse{
		Etag: info.ETag,
		Size: info.Size,
	}, nil
}
