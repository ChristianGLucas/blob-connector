// Package nodes' engine.go holds the shared connection/client/error plumbing
// used by every node in this package. It is a helper file, not a node — it
// is not listed in axiom.yaml.
package nodes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"christiangeorgelucas/blob-connector/axiom"
	gen "christiangeorgelucas/blob-connector/gen"
)

// connectTimeout bounds a single request-response call (Put/Get/Head/Delete/
// List/Copy/Presign-precheck) end-to-end, so an unreachable endpoint fails
// with a clean typed error instead of hanging on an OS-level TCP timeout.
// StreamObjectBody deliberately does not use this for its body read — that
// runs under the platform's own 5-minute pipeline stream ceiling instead, so
// a legitimately large object isn't cut off by a fixed node-side timeout.
const connectTimeout = 30 * time.Second

// defaultRegion is sent when Connection.region is empty. AWS SigV4 signing
// requires a region string even against providers with no real regional
// concept; MinIO, R2, and GCS interop mode all accept this as a harmless
// default.
const defaultRegion = "us-east-1"

// metaAmzPrefix is the canonical S3 user-metadata header prefix. minio-go
// surfaces user metadata with this prefix still attached (case varies by
// provider); normalizeMetadata strips it so callers see plain keys
// regardless of provider.
const metaAmzPrefix = "x-amz-meta-"

// resolveCredentials implements Connection's dual-mode credential
// precedence: named secrets (recommended, for any real credential) win over
// raw fields (publicly-documented/throwaway credentials only) when both are
// set. The two halves of a mode must be paired — mixing a secret access key
// with a raw secret key (or vice versa) is a structured error rather than a
// silent partial resolution.
func resolveCredentials(ax axiom.Context, conn *gen.Connection) (accessKey, secretKey string, err error) {
	akName := strings.TrimSpace(conn.GetAccessKeySecretName())
	skName := strings.TrimSpace(conn.GetSecretKeySecretName())
	if akName != "" || skName != "" {
		if akName == "" || skName == "" {
			return "", "", errors.New("connection.access_key_secret_name and connection.secret_key_secret_name must both be set together")
		}
		ak, ok := ax.Secrets().Get(akName)
		if !ok || ak == "" {
			return "", "", fmt.Errorf("required secret %q is not configured", akName)
		}
		sk, ok := ax.Secrets().Get(skName)
		if !ok || sk == "" {
			return "", "", fmt.Errorf("required secret %q is not configured", skName)
		}
		return ak, sk, nil
	}
	ak := strings.TrimSpace(conn.GetAccessKey())
	sk := strings.TrimSpace(conn.GetSecretKey())
	if ak == "" || sk == "" {
		return "", "", errors.New("connection credentials are required: set access_key_secret_name+secret_key_secret_name (recommended) or access_key+secret_key")
	}
	return ak, sk, nil
}

// newClient resolves credentials and constructs a fresh S3 client for this
// invocation. Nodes are stateless — no client/connection pool is kept across
// invocations. Constructing a client never itself makes a network call; an
// unreachable endpoint or bad credential only surfaces once the caller
// issues the first real request against it.
func newClient(ax axiom.Context, conn *gen.Connection) (*minio.Client, error) {
	endpoint := strings.TrimSpace(conn.GetEndpoint())
	if endpoint == "" {
		return nil, errors.New("connection.endpoint is required")
	}
	bucket := strings.TrimSpace(conn.GetBucket())
	if bucket == "" {
		return nil, errors.New("connection.bucket is required")
	}
	ak, sk, err := resolveCredentials(ax, conn)
	if err != nil {
		return nil, err
	}
	region := strings.TrimSpace(conn.GetRegion())
	if region == "" {
		region = defaultRegion
	}
	lookup := minio.BucketLookupAuto
	if conn.GetPathStyle() {
		lookup = minio.BucketLookupPath
	}
	cl, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(ak, sk, ""),
		Secure:       !conn.GetInsecure(),
		Region:       region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, fmt.Errorf("constructing client for endpoint %q: %w", endpoint, err)
	}
	return cl, nil
}

// classify turns a raw minio-go/network error into a structured, categorized
// node error whose message names the failure class (credentials, not-found,
// unreachable endpoint, timeout) without ever echoing a secret. bucket/key
// are used only to make the message actionable, never to change behavior.
func classify(err error, endpoint, bucket, key string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("unreachable endpoint %q: connection timed out", endpoint)
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return fmt.Errorf("unreachable endpoint %q: %w", endpoint, err)
	}
	resp := minio.ToErrorResponse(err)
	switch resp.Code {
	case "NoSuchBucket":
		return fmt.Errorf("not found: bucket %q does not exist", bucket)
	case "NoSuchKey", "NoSuchVersion":
		return fmt.Errorf("not found: key %q does not exist in bucket %q", key, bucket)
	case "AccessDenied":
		return fmt.Errorf("access denied: credentials are valid but not authorized for bucket %q", bucket)
	case "InvalidAccessKeyId":
		return errors.New("invalid credentials: access key not recognized by the endpoint")
	case "SignatureDoesNotMatch":
		return errors.New("invalid credentials: secret key does not match the access key")
	}
	if resp.Code != "" {
		// A real S3 API error we don't special-case above — surface the
		// provider's own code and message rather than the raw Go error type.
		return fmt.Errorf("%s: %s", resp.Code, resp.Message)
	}
	return fmt.Errorf("endpoint %q: %w", endpoint, err)
}

// normalizeMetadata strips the canonical S3 user-metadata header prefix from
// every key and lower-cases what remains. Lower-casing is deliberate, not
// cosmetic: metadata keys travel as HTTP headers, and both real S3 (which
// documents storing user-metadata names lower-cased) and Go's own
// http.Header (which title-cases every hyphen-separated segment,
// e.g. "owner" -> "Owner") silently change casing in transit — live-proven
// against MinIO, whose client-observed casing follows Go's header
// canonicalization rather than the case PutObject was called with. Without
// this, a caller could round-trip "owner" as "Owner" depending on provider,
// which is worse than a documented, deterministic normalization.
func normalizeMetadata(raw map[string]string) map[string]string {
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, metaAmzPrefix) {
			out[lk[len(metaAmzPrefix):]] = v
			continue
		}
		out[lk] = v
	}
	return out
}

// resolveSourceBucket implements CopyObjectRequest's source_bucket default:
// empty means "same bucket as the destination" (a same-bucket copy/re-key).
func resolveSourceBucket(req interface{ GetSourceBucket() string }, destBucket string) string {
	if sb := strings.TrimSpace(req.GetSourceBucket()); sb != "" {
		return sb
	}
	return destBucket
}

// clampExpiry validates PresignRequest.expiry_seconds against the AWS SigV4
// hard ceiling (7 days), applying the 1-hour default for an unset (zero)
// value. Returns a structured error for anything outside [1, 604800].
func clampExpiry(seconds int32) (time.Duration, error) {
	const maxExpirySeconds = 7 * 24 * 60 * 60 // 604800
	if seconds == 0 {
		return time.Hour, nil
	}
	if seconds < 1 || seconds > maxExpirySeconds {
		return 0, fmt.Errorf("expiry_seconds must be between 1 and %d, got %d", maxExpirySeconds, seconds)
	}
	return time.Duration(seconds) * time.Second, nil
}
