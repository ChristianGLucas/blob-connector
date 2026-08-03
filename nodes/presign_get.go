package nodes

import (
	"context"
	"errors"
	"strings"
	"time"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// PresignGet issues an expiry-bounded, pre-signed URL for downloading one
// object directly from the object store — the object's bytes never pass
// through Axiom. The app-platform download path: hand this URL to an
// end-user client instead of proxying the download through a flow.
// Pure and side-effect-free: generating a URL never touches the object
// itself (it is a local signing computation, no network round trip), so it
// is safe to call any number of times, including for a key that doesn't
// exist — the URL simply 404s when someone tries to use it.
func PresignGet(ctx context.Context, ax axiom.Context, input *gen.PresignRequest) (*gen.PresignResponse, error) {
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

	u, err := cl.PresignedGetObject(ctx, conn.GetBucket(), key, expiry, nil)
	if err != nil {
		return nil, classify(err, conn.GetEndpoint(), conn.GetBucket(), key)
	}
	return &gen.PresignResponse{
		Url:       u.String(),
		ExpiresAt: time.Now().Add(expiry).UTC().Format(time.RFC3339),
	}, nil
}
