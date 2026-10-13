package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// maxS3FetchBytes is the documented TemplateURL ceiling (api_op_CreateStack.go: max 1 MB).
const maxS3FetchBytes = 1 << 20

var errS3Fetch = errors.New("S3 error")

// s3ObjectFetcher is satisfied by *InMemoryBackend; the handler uses it to
// resolve TemplateURL, StackPolicyURL and AccountsUrl.
type s3ObjectFetcher interface {
	FetchS3URL(ctx context.Context, rawURL string) (string, error)
}

// splitS3URL extracts bucket and key from a virtual-hosted-style
// (bucket.s3.<region>.amazonaws.com/key) or path-style (host/bucket/key) URL.
func splitS3URL(rawURL string) (string, string, string, error) {
	u, parseErr := url.Parse(rawURL)
	if parseErr != nil || u.Host == "" {
		return "", "", "", fmt.Errorf("%w: invalid URL %q", errS3Fetch, rawURL)
	}

	var bucket, key string

	path := strings.TrimPrefix(u.Path, "/")
	if idx := strings.Index(u.Host, ".s3"); idx > 0 {
		bucket, key = u.Host[:idx], path
	} else {
		bucket, key, _ = strings.Cut(path, "/")
	}

	if bucket == "" || key == "" {
		return "", "", "", fmt.Errorf("%w: URL %q does not name an S3 object", errS3Fetch, rawURL)
	}

	return bucket, key, u.Query().Get("versionId"), nil
}

// FetchS3URL reads the object an S3 URL names through the wired S3 backend.
func (b *InMemoryBackend) FetchS3URL(ctx context.Context, rawURL string) (string, error) {
	if b.creator == nil || b.creator.backends == nil || b.creator.backends.S3 == nil {
		return "", fmt.Errorf("%w: no S3 backend available to fetch %q", errS3Fetch, rawURL)
	}

	bucket, key, versionID, err := splitS3URL(rawURL)
	if err != nil {
		return "", err
	}

	in := &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		in.VersionId = aws.String(versionID)
	}

	out, err := b.creator.backends.S3.Backend.GetObject(ctx, in)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errS3Fetch, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(io.LimitReader(out.Body, maxS3FetchBytes+1))
	if err != nil {
		return "", fmt.Errorf("%w: %w", errS3Fetch, err)
	}

	if len(data) > maxS3FetchBytes {
		return "", fmt.Errorf("%w: object %q exceeds %d bytes", errS3Fetch, rawURL, maxS3FetchBytes)
	}

	return string(data), nil
}

// bodyForURLKey maps a URL-valued form key to the inline-body key it replaces.
func bodyForURLKey(key string) (string, bool) {
	for _, pair := range [...][2]string{
		{"TemplateURL", "TemplateBody"},
		{"StackPolicyURL", "StackPolicyBody"},
		{"StackPolicyDuringUpdateURL", "StackPolicyDuringUpdateBody"},
	} {
		if prefix, ok := strings.CutSuffix(key, pair[0]); ok && (prefix == "" || strings.HasSuffix(prefix, ".")) {
			return prefix + pair[1], true
		}
	}

	return "", false
}

// resolveURLParams replaces TemplateURL/StackPolicyURL/StackPolicyDuringUpdateURL
// with the S3 object's contents in the matching *Body field, and expands
// DeploymentTargets.AccountsUrl into DeploymentTargets.Accounts members.
func (h *Handler) resolveURLParams(ctx context.Context, form url.Values) error {
	fetcher, _ := h.Backend.(s3ObjectFetcher)

	for key, vals := range form {
		bodyKey, ok := bodyForURLKey(key)
		if !ok || len(vals) == 0 || vals[0] == "" || form.Get(bodyKey) != "" {
			continue
		}

		body, err := fetchVia(ctx, fetcher, vals[0])
		if err != nil {
			return err
		}

		form.Set(bodyKey, body)
	}

	if accountsURL := form.Get("DeploymentTargets.AccountsUrl"); accountsURL != "" {
		body, err := fetchVia(ctx, fetcher, accountsURL)
		if err != nil {
			return err
		}

		appendAccounts(form, body)
	}

	return nil
}

func fetchVia(ctx context.Context, fetcher s3ObjectFetcher, rawURL string) (string, error) {
	if fetcher == nil {
		return "", fmt.Errorf("%w: cannot fetch %q", errS3Fetch, rawURL)
	}

	return fetcher.FetchS3URL(ctx, rawURL)
}

// appendAccounts adds the comma- or newline-separated account IDs in body
// after any DeploymentTargets.Accounts members already on the form.
func appendAccounts(form url.Values, body string) {
	n := len(parseMemberList(form, "DeploymentTargets.Accounts."))

	for _, id := range strings.FieldsFunc(body, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' }) {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}

		n++
		form.Set("DeploymentTargets.Accounts.member."+strconv.Itoa(n), id)
	}
}
