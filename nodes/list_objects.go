package nodes

import (
	"context"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// defaultListPageSize is used when ListObjectsRequest.max_keys is 0 —
// matches the S3-compatible ecosystem's common default page size.
const defaultListPageSize = 1000

// isCommonPrefixEntry reports whether an ObjectInfo returned by minio-go's
// listing channel represents a "directory-like" common prefix rather than a
// real object. minio-go populates ONLY the Key field for a common prefix
// (see api-list.go); every real object — even a genuine 0-byte one — always
// carries a non-empty ETag and a non-zero LastModified from S3, so this
// combination reliably distinguishes the two without any explicit flag on
// ObjectInfo (there isn't one).
func isCommonPrefixEntry(obj minio.ObjectInfo) bool {
	return obj.ETag == "" && obj.LastModified.IsZero()
}

// ListObjects lists objects under a prefix, one page at a time. Pagination
// resumes by lexicographic key via S3's StartAfter parameter — minio-go's
// public API does not expose the raw ListObjectsV2 continuation token, so
// next_continuation_token is the last key/common-prefix string this page
// returned, fed back in as StartAfter for the next call. Page boundaries are
// enforced by this node itself (not the underlying provider's own HTTP-level
// page size), so max_keys is honored exactly regardless of how the provider
// batches its own requests.
func ListObjects(ctx context.Context, ax axiom.Context, input *gen.ListObjectsRequest) (*gen.ListObjectsResponse, error) {
	conn := input.GetConnection()
	// minio-go's public ListObjects API hardcodes the delimiter to "/" for a
	// non-recursive listing (or "" for recursive) and exposes no way to set
	// a custom delimiter string — a genuine client-library limitation, not
	// an Axiom one. Any other requested delimiter is a structured error
	// rather than a silently-wrong result.
	delimiter := input.GetDelimiter()
	if delimiter != "" && delimiter != "/" {
		return nil, fmt.Errorf(`delimiter must be "" (flat, fully recursive) or "/" (single-level grouping); got %q`, delimiter)
	}
	maxKeys := int(input.GetMaxKeys())
	if maxKeys < 0 {
		return nil, fmt.Errorf("max_keys must not be negative, got %d", maxKeys)
	}
	if maxKeys == 0 {
		maxKeys = defaultListPageSize
	}

	cl, err := newClient(ax, conn)
	if err != nil {
		return nil, err
	}

	opCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	objCh := cl.ListObjects(opCtx, conn.GetBucket(), minio.ListObjectsOptions{
		Prefix:     input.GetPrefix(),
		Recursive:  delimiter == "",
		StartAfter: input.GetContinuationToken(),
	})

	var objects []*gen.ObjectInfo
	var commonPrefixes []string
	var lastKey string
	truncated := false
	for obj := range objCh {
		if obj.Err != nil {
			return nil, classify(obj.Err, conn.GetEndpoint(), conn.GetBucket(), "")
		}
		if len(objects)+len(commonPrefixes) == maxKeys {
			truncated = true
			cancel() // stop the background listing goroutine early; see drain below
			break
		}
		if isCommonPrefixEntry(obj) {
			commonPrefixes = append(commonPrefixes, obj.Key)
		} else {
			objects = append(objects, &gen.ObjectInfo{
				Key:          obj.Key,
				Size:         obj.Size,
				Etag:         obj.ETag,
				LastModified: obj.LastModified.UTC().Format(time.RFC3339),
			})
		}
		lastKey = obj.Key
	}
	// Fully drain the channel per minio-go's contract, whether we stopped
	// early (cancel above) or the listing simply ran out. minio-go closes
	// the channel promptly once ctx is cancelled.
	for range objCh {
	}

	next := ""
	if truncated {
		next = lastKey
	}
	return &gen.ListObjectsResponse{
		Objects:               objects,
		CommonPrefixes:        commonPrefixes,
		IsTruncated:           truncated,
		NextContinuationToken: next,
	}, nil
}
