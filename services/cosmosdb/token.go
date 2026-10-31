package cosmosdb

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	tokenTypeResource = "resource"
	tokenVersion      = "1"
	// tokenKeyContext domain-separates the resource-token key from the master key it is derived from.
	tokenKeyContext = "gopherstack/cosmosdb/resource-token/v1"

	headerExpirySeconds = "X-Ms-Documentdb-Expiry-Seconds"

	// Resource-token lifetime: 1 h default; 10 min minimum and 24 h maximum per "Service quotas" (Per-request limits).
	// The Create Permission REST page, last revised 2020, still says 18000 s.
	defaultTokenExpirySeconds = 3600
	minTokenExpirySeconds     = 600
	maxTokenExpirySeconds     = 86400
	// MinResourceTokenMaxSeconds and MaxResourceTokenMaxSeconds bound Settings.ResourceTokenMaxSeconds.
	MinResourceTokenMaxSeconds = minTokenExpirySeconds
	MaxResourceTokenMaxSeconds = maxTokenExpirySeconds
)

var (
	// ErrInsufficientPermissions is returned when a valid resource token does not cover the request.
	ErrInsufficientPermissions = errors.New("cosmosdb: resource token does not grant this operation")
	// ErrTokenExpired is returned when a resource token is used outside its validity window.
	ErrTokenExpired = errors.New("cosmosdb: resource token is not valid at the current time")
	// ErrInvalidTokenExpiry is returned for an x-ms-documentdb-expiry-seconds outside 600..the configured maximum.
	ErrInvalidTokenExpiry = errors.New("cosmosdb: invalid x-ms-documentdb-expiry-seconds")
)

// ErrInvalidResourceTokenMax is returned when --cosmosdb-resource-token-max-seconds is outside 600..86400.
var ErrInvalidResourceTokenMax = errors.New("cosmosdb: resource-token-max-seconds must be between 600 and 86400")

type tokenExpiryError struct{ maxSeconds int }

func (e *tokenExpiryError) Error() string {
	return fmt.Sprintf("cosmosdb: x-ms-documentdb-expiry-seconds must be between %d and %d",
		minTokenExpirySeconds, e.maxSeconds)
}

func (e *tokenExpiryError) Is(target error) bool { return target == ErrInvalidTokenExpiry }

// tokenClaims is the signed payload of a resource token. It names the permission; scope is read from the live
// permission on every request, so deleting or narrowing a permission takes effect immediately.
type tokenClaims struct {
	DB         string `json:"db"`
	User       string `json:"u"`
	Permission string `json:"p"`
	Issued     int64  `json:"iat"`
	Expires    int64  `json:"exp"`
}

// tokenWindowError carries the validity window so the 403 message can report it.
type tokenWindowError struct {
	start, end, now time.Time
}

func (e *tokenWindowError) Error() string { return ErrTokenExpired.Error() }

func (e *tokenWindowError) Is(target error) bool { return target == ErrTokenExpired }

// tokenKey derives the HMAC key for resource tokens from a master key.
func tokenKey(masterKey string) []byte {
	raw, err := base64.StdEncoding.DecodeString(masterKey)
	if err != nil {
		raw = []byte(masterKey)
	}

	mac := hmac.New(sha256.New, raw)
	mac.Write([]byte(tokenKeyContext))

	return mac.Sum(nil)
}

func signTokenPayload(masterKey string, payload []byte) []byte {
	mac := hmac.New(sha256.New, tokenKey(masterKey))
	mac.Write(payload)

	return mac.Sum(nil)
}

// issueResourceToken builds "type=resource&ver=1&sig=<mac>;<payload>", the value of a permission's _token.
func issueResourceToken(masterKey string, claims tokenClaims) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("cosmosdb: marshal token claims: %w", err)
	}

	sig := base64.StdEncoding.EncodeToString(signTokenPayload(masterKey, payload))

	return "type=" + tokenTypeResource + "&ver=" + tokenVersion + "&sig=" + sig + ";" +
		base64.StdEncoding.EncodeToString(payload), nil
}

// parseResourceToken verifies sig ("<mac>;<payload>") under masterKey and returns the claims.
func parseResourceToken(masterKey, sig string) (tokenClaims, bool) {
	macPart, payloadPart, ok := strings.Cut(sig, ";")
	if !ok {
		return tokenClaims{}, false
	}

	mac, err := base64.StdEncoding.DecodeString(macPart)
	if err != nil {
		return tokenClaims{}, false
	}

	payload, err := base64.StdEncoding.DecodeString(payloadPart)
	if err != nil {
		return tokenClaims{}, false
	}

	if !hmac.Equal(mac, signTokenPayload(masterKey, payload)) {
		return tokenClaims{}, false
	}

	var claims tokenClaims
	if json.Unmarshal(payload, &claims) != nil {
		return tokenClaims{}, false
	}

	return claims, true
}

// tokenExpiry reads x-ms-documentdb-expiry-seconds; absent means the one-hour default.
func (h *Handler) tokenExpiry(r *http.Request) (time.Duration, error) {
	raw := r.Header.Get(headerExpirySeconds)
	if raw == "" {
		return defaultTokenExpirySeconds * time.Second, nil
	}

	maxSeconds := h.ResourceTokenMaxSeconds
	if maxSeconds == 0 {
		maxSeconds = maxTokenExpirySeconds
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < minTokenExpirySeconds || n > maxSeconds {
		return 0, &tokenExpiryError{maxSeconds: maxSeconds}
	}

	return time.Duration(n) * time.Second, nil
}

// permissionToken signs a token for p that expires ttl from now.
func (h *Handler) permissionToken(p PermissionInfo, ttl time.Duration) (string, error) {
	now := h.now()

	return issueResourceToken(h.MasterKey, tokenClaims{
		DB: p.DBID, User: p.UserID, Permission: p.ID,
		Issued: now.Unix(), Expires: now.Add(ttl).Unix(),
	})
}

// authorizeResourceToken accepts a type=resource Authorization only when its signature verifies, it is inside its
// validity window and the live permission it names covers this exact request.
func (h *Handler) authorizeResourceToken(r *http.Request, sig string) error {
	var claims tokenClaims

	verified := false

	for _, k := range h.candidateKeys() {
		if c, ok := parseResourceToken(k, sig); ok {
			claims, verified = c, true

			break
		}
	}

	if !verified {
		return ErrBadSignature
	}

	now := h.now()
	start, end := time.Unix(claims.Issued, 0).UTC(), time.Unix(claims.Expires, 0).UTC()

	if now.Before(start.Add(-maxAuthClockSkew)) || !now.Before(end) {
		return &tokenWindowError{start: start, end: end, now: now.UTC()}
	}

	p, err := h.Backend.GetPermission(claims.DB, claims.User, claims.Permission)
	if err != nil {
		return ErrBadSignature
	}

	if !permissionCovers(p, r) {
		return ErrInsufficientPermissions
	}

	return nil
}

// permissionCovers reports whether p grants exactly the request r: its container or document, read or write per
// its mode, and for a partition key permission only documents of that key.
func permissionCovers(p PermissionInfo, r *http.Request) bool {
	kind, dbID, collID, docID := parseResourcePath(r.URL.Path)
	read := isReadRequest(r)

	if kind == resourceAccountRoot {
		return read
	}

	segs := strings.Split(p.Link, "/")
	if dbID != segs[1] || collID != segs[3] || (p.Mode == PermissionModeRead && !read) {
		return false
	}

	docPermission := len(segs) == documentLinkSegments
	if docPermission && segs[4] != docsSegment {
		return scriptPermissionCovers(segs, kind, docID) && partitionAllowed(p, r)
	}

	return childCovered(kind, docPermission, docPermission && docID == segs[5], read) && partitionAllowed(p, r)
}

// childCovered reports whether a container (docPermission false) or document permission reaches a request of kind.
func childCovered(kind resourceKind, docPermission, sameDoc, read bool) bool {
	switch kind {
	case resourceContainerItem:
		return !docPermission
	case resourcePKRanges:
		return !docPermission && read
	case resourceDocumentItem:
		return !docPermission || sameDoc
	case resourceDocuments, resourceSprocs, resourceSprocItem, resourceTriggers, resourceTriggerItem,
		resourceUDFs, resourceUDFItem:
		return !docPermission
	default:
		return false
	}
}

func partitionAllowed(p PermissionInfo, r *http.Request) bool {
	if p.PartitionKeyJSON == "" {
		return true
	}

	pk, err := partitionKeyFromHeader(r)

	return err == nil && pk == p.PartitionKeyJSON
}

// isReadRequest is true for GET, HEAD and SQL queries (a POST flagged as a query).
func isReadRequest(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return true
	case http.MethodPost:
		return isQueryRequest(r)
	default:
		return false
	}
}

// scriptPermissionCovers reports whether a stored procedure, trigger or UDF permission (segs) grants r: the item
// itself and nothing else. Executing a stored procedure is a write, so a Read permission never reaches it.
func scriptPermissionCovers(segs []string, kind resourceKind, itemID string) bool {
	var want resourceKind

	switch scriptKind(segs[4]) {
	case kindSproc:
		want = resourceSprocItem
	case kindTrigger:
		want = resourceTriggerItem
	case kindUDF:
		want = resourceUDFItem
	}

	return kind == want && itemID == segs[5]
}
