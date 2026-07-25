package athena

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// pageTokenCodec produces opaque, tamper-evident pagination tokens for
// ListQueryExecutions.
//
// AWS pagination tokens are opaque strings that clients must treat as blobs; a
// client must not be able to forge one or infer the next resource ID from it.
// The previous implementation returned the raw next query-execution ID as the
// token, which was both enumerable (it leaked a real ID) and forgeable (any
// caller could synthesise one).
//
// The token here is:
//
//	base64url( boundary + "|" + hex(HMAC-SHA256(key, boundary)) )
//
// where boundary is the sort key (query-execution ID) of the first item on the
// next page. The HMAC is keyed by a per-process random secret, so a token cannot
// be forged without the key, and any tampering is detected on decode. Because
// the boundary is a sort key rather than a positional index, tokens are
// mutation-stable: inserting or deleting executions between calls does not
// invalidate an outstanding token — decoding yields the boundary and the pager
// resumes at the first item whose ID is >= boundary.
type pageTokenCodec struct {
	key []byte
}

// pageTokenKeyLen is the length in bytes of the per-process HMAC key.
const pageTokenKeyLen = 32

// newPageTokenCodec returns a codec seeded with a fresh random signing key.
func newPageTokenCodec() *pageTokenCodec {
	key := make([]byte, pageTokenKeyLen)
	// crypto/rand.Read never returns a short read on success; ignore n.
	if _, err := rand.Read(key); err != nil {
		// A failure here means the OS CSPRNG is unavailable, which is fatal for
		// any secure operation. Panicking at construction time (process start)
		// surfaces the problem immediately rather than emitting weak tokens.
		panic(fmt.Sprintf("athena: unable to seed pagination token key: %v", err))
	}

	return &pageTokenCodec{key: key}
}

// encode returns the opaque token for the given boundary sort key.
func (c *pageTokenCodec) encode(boundary string) string {
	raw := boundary + "|" + c.sign(boundary)

	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decode validates an opaque token and returns its boundary sort key. It returns
// an ErrValidation-wrapped error (surfaced to the client as
// InvalidRequestException) for any token that is not well-formed or fails the
// integrity check, matching how AWS rejects a corrupt/forged NextToken.
func (c *pageTokenCodec) decode(token string) (string, error) {
	rawBytes, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("%w: NextToken is not a valid pagination token", ErrValidation)
	}

	boundary, mac, ok := strings.Cut(string(rawBytes), "|")
	if !ok {
		return "", fmt.Errorf("%w: NextToken is malformed", ErrValidation)
	}

	if !hmac.Equal([]byte(mac), []byte(c.sign(boundary))) {
		return "", fmt.Errorf("%w: NextToken failed its integrity check", ErrValidation)
	}

	return boundary, nil
}

// sign returns the hex-encoded HMAC-SHA256 of s under the codec's key.
func (c *pageTokenCodec) sign(s string) string {
	m := hmac.New(sha256.New, c.key)
	_, _ = m.Write([]byte(s))

	return hex.EncodeToString(m.Sum(nil))
}

// paginationStart returns the index of the first of n sorted-ascending
// elements whose key (as returned by keyAt) is >= boundary, or n if every
// key sorts before boundary. An empty boundary always resumes at 0.
//
// This gives WorkGroups/NamedQueries/DataCatalogs/PreparedStatements listing
// mutation-stable pagination: resuming with a NextToken whose original
// boundary item was deleted (or never existed -- a stale or unrecognized
// token) lands on the first surviving item at or after that boundary instead
// of silently restarting the page from offset 0, which would re-emit items
// the caller already consumed. It mirrors the resume semantics
// pageTokenCodec.paginateQueryExecutionIDs already uses for
// ListQueryExecutions, applied here to the plain (non-opaque) boundary
// tokens the other four listings use.
func paginationStart(n int, boundary string, keyAt func(i int) string) int {
	if boundary == "" {
		return 0
	}

	return sort.Search(n, func(i int) bool { return keyAt(i) >= boundary })
}

// paginateQueryExecutionIDs applies AWS-style MaxResults/NextToken pagination to
// a sorted list of query-execution IDs, using opaque tokens. ids MUST be sorted
// ascending (ListQueryExecutions guarantees this). It returns the page of IDs
// and, when more remain, an opaque token for the next page. A malformed or
// forged token yields an ErrValidation error.
func (c *pageTokenCodec) paginateQueryExecutionIDs(
	ids []string,
	maxResults int,
	nextToken string,
) ([]string, string, error) {
	limit := maxListQueryExecutionsPageSize
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := 0

	if nextToken != "" {
		boundary, err := c.decode(nextToken)
		if err != nil {
			return nil, "", err
		}

		// Mutation-stable resume: first index with ID >= boundary. If the
		// boundary ID was deleted, we resume at the next surviving ID; if it
		// sorts past the end, start == len(ids) and we return an empty last page.
		start = sort.SearchStrings(ids, boundary)
	}

	ids = ids[start:]

	token := ""
	if len(ids) > limit {
		token = c.encode(ids[limit])
		ids = ids[:limit]
	}

	return ids, token, nil
}
