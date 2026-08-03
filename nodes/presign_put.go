package nodes

import (
	"context"
	"errors"
	"strings"
	"time"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// PresignPut issues an expiry-bounded, pre-signed URL for uploading one
// object directly to the object store via a plain HTTP PUT — the uploaded
// bytes never pass through Axiom. The app-platform upload path: hand this
// URL to an end-user client for direct browser/app uploads instead of
// proxying the body through a flow's request envelope.
// Pure and side-effect-free: generating a URL never touches the object
// itself (a local signing computation, no network round trip), so it is
// safe to call any number of times; whether an upload actually happens, and
// what bytes it carries, is entirely up to whoever uses the URL.
func PresignPut(ctx context.Context, ax axiom.Context, input *gen.PresignRequest) (*gen.PresignResponse, error) {
	conn := input.GetConnection()
	key := strings.TrimSpace(input.GetKey())
	if key == "" {
		return nil, errors.New("key is required")
	}
	expiry, err := clampExpiry(input.GetExpirySeconds())
	if err != nil {
		return nil, err
	}
	cl, err := newClient(ax, conn)
	if err != nil {
		return nil, err
	}

	u, err := cl.PresignedPutObject(ctx, conn.GetBucket(), key, expiry)
	if err != nil {
		return nil, classify(err, conn.GetEndpoint(), conn.GetBucket(), key)
	}
	return &gen.PresignResponse{
		Url:       u.String(),
		ExpiresAt: time.Now().Add(expiry).UTC().Format(time.RFC3339),
	}, nil
}
