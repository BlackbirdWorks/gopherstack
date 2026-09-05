package cosmosdb

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// masterkey.go implements Cosmos DB's master-key HMAC authorization scheme.
// Per AZURE.md section 3, this lives in services/cosmosdb (not pkgs/azureauth)
// because Cosmos's scheme is structurally different enough from the
// SharedKey scheme pkgs/azureauth implements for Blob/Queue/Table to not be
// worth sharing: different header shape (a single URL-encoded
// "type=...&ver=...&sig=..." string, not "Scheme account:sig"), and a
// different, much simpler canonicalization (five fixed fields, no
// header-block canonicalization). It mirrors pkgs/azureauth's *internal*
// shape -- a parse function, a canonicalization function, a sign function,
// and an opt-in verify function -- just as unexported package-level code
// here instead of its own shared package.

// ErrMalformedAuthorization is returned by VerifyMasterKey when the
// request's Authorization header cannot be parsed.
var ErrMalformedAuthorization = errors.New("cosmosdb: malformed Authorization header")

// masterKeyAuthorization is the parsed form of a Cosmos DB Authorization
// header: "type=master&ver=1.0&sig=<base64 HMAC-SHA256>", itself URL-encoded
// as a whole when placed in the header value.
type masterKeyAuthorization struct {
	Type      string
	Version   string
	Signature string
}

// parseMasterKeyAuthorization parses header (the raw Authorization header
// value, URL-encoded per Cosmos's wire format) into its type/ver/sig
// fields. Returns ok=false for anything malformed: not URL-decodable,
// missing any of the three fields, or an empty field value. It never
// inspects the signature's correctness -- see verifyMasterKeySignature for
// that.
func parseMasterKeyAuthorization(header string) (masterKeyAuthorization, bool) {
	if header == "" {
		return masterKeyAuthorization{}, false
	}

	decoded, err := url.QueryUnescape(header)
	if err != nil {
		return masterKeyAuthorization{}, false
	}

	var auth masterKeyAuthorization

	for field := range strings.SplitSeq(decoded, "&") {
		key, value, ok := strings.Cut(field, "=")
		if !ok || value == "" {
			return masterKeyAuthorization{}, false
		}

		switch key {
		case "type":
			auth.Type = value
		case "ver":
			auth.Version = value
		case "sig":
			auth.Signature = value
		default:
			return masterKeyAuthorization{}, false
		}
	}

	if auth.Type == "" || auth.Version == "" || auth.Signature == "" {
		return masterKeyAuthorization{}, false
	}

	return auth, true
}

// masterKeyStringToSign builds Cosmos's master-key canonicalized
// string-to-sign for a request:
//
//	lowercase(verb) + "\n" + lowercase(resourceType) + "\n" + resourceId +
//	"\n" + lowercase(x-ms-date) + "\n" + lowercase(date-header) + "\n"
//
// resourceId is NOT lowercased (its casing is significant -- it identifies
// the exact resource path segment, e.g. "dbs/mydb/colls/mycoll").
func masterKeyStringToSign(verb, resourceType, resourceID, xmsDate, dateHeader string) string {
	return strings.ToLower(verb) + "\n" +
		strings.ToLower(resourceType) + "\n" +
		resourceID + "\n" +
		strings.ToLower(xmsDate) + "\n" +
		strings.ToLower(dateHeader) + "\n"
}

// signMasterKey returns base64(HMAC-SHA256(base64decode(masterKey),
// stringToSign)), matching real Cosmos DB's own signing algorithm.
func signMasterKey(masterKey, stringToSign string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(masterKey)
	if err != nil {
		return "", fmt.Errorf("cosmosdb: decode master key: %w", err)
	}

	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(stringToSign))

	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

// resourceTypeAndIDFor derives the (resourceType, resourceId) pair Cosmos's
// signature canonicalizes over from an HTTP request path, per Cosmos's own
// resource-path convention: resourceType is the last even-indexed (0-based)
// path segment kind ("dbs", "colls", or "docs"), and resourceId is the full
// path with the leading "/" trimmed -- UNLESS the request targets a
// collection resource (e.g. POST /dbs/mydb/colls to create a container, or
// POST .../docs to create/query a document), in which case resourceId is
// the path of the PARENT resource instead (the collection being posted
// into, not a not-yet-existent child) -- matching real Cosmos's own
// signing convention where a Create/Query-type POST signs against its
// parent's resource ID.
func resourceTypeAndIDFor(method, path string) (string, string) {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return "", ""
	}

	segments := strings.Split(trimmed, "/")

	// An odd number of segments means the path names a collection resource
	// (e.g. "dbs", "dbs/mydb/colls", "dbs/mydb/colls/mycoll/docs"); an even
	// number names a specific item within one (e.g. "dbs/mydb",
	// "dbs/mydb/colls/mycoll"). See real Cosmos's REST resource model.
	if len(segments)%2 == 1 {
		resourceType := segments[len(segments)-1]

		if method == http.MethodPost {
			// Posting into a collection (create or query): sign against the
			// parent, which is everything before the trailing collection
			// segment.
			return resourceType, strings.Join(segments[:len(segments)-1], "/")
		}

		// GET on a collection (list): the collection itself has no
		// meaningful "id" beyond its own path.
		return resourceType, trimmed
	}

	return segments[len(segments)-2], trimmed
}

// VerifyMasterKey recomputes the master-key signature for r using
// masterKey, and reports whether it matches the signature the client sent.
// This is an explicit, opt-in check: nothing in this package calls it
// implicitly -- see handler.go's checkAuth, which only calls this when
// Settings.ValidateAuth is true, mirroring pkgs/azureauth.VerifySharedKey's
// identical opt-in stance.
func VerifyMasterKey(masterKey string, r *http.Request) (bool, error) {
	auth, ok := parseMasterKeyAuthorization(r.Header.Get("Authorization"))
	if !ok {
		return false, ErrMalformedAuthorization
	}

	resourceType, resourceID := resourceTypeAndIDFor(r.Method, r.URL.Path)
	stringToSign := masterKeyStringToSign(
		r.Method, resourceType, resourceID, r.Header.Get("X-Ms-Date"), r.Header.Get("Date"),
	)

	expected, err := signMasterKey(masterKey, stringToSign)
	if err != nil {
		return false, err
	}

	return hmac.Equal([]byte(expected), []byte(auth.Signature)), nil
}
