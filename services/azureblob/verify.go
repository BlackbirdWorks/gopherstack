package azureblob

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

// DevStoreAccount and DevStoreKey are the well-known development account and key every Azure Storage emulator
// and SDK documents; they are always accepted when signature verification is on.
const (
	DevStoreAccount = "devstoreaccount1"
	DevStoreKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

	sasEncryptionScopeFrom = "2020-12-06"
	sasMinVersion          = "2018-11-09"
	sasMaxVersion          = "2026-12-06"
	sasHTTPSOnly           = "https"
	sasHTTPSAndHTTP        = "https,http"
	sasVersionLayout       = "2006-01-02"
	maxDateSkew            = 15 * time.Minute
)

func errAuthFailed(msg string) *apiError {
	return &apiError{status: http.StatusForbidden, code: "AuthenticationFailed", message: msg}
}

func errAuthorizationFailure() *apiError {
	return &apiError{
		status: http.StatusForbidden, code: "AuthorizationPermissionMismatch",
		message: "This request is not authorized to perform this operation using this permission.",
	}
}

// ParseAccountKeys parses "account=base64key,account2=key2" into a map.
func ParseAccountKeys(spec string) map[string]string {
	out := make(map[string]string)

	for part := range strings.SplitSeq(spec, ",") {
		name, key, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && name != "" && key != "" {
			out[name] = key
		}
	}

	return out
}

func (h *Handler) accountKey(account string) (string, bool) {
	if k, ok := h.AccountKeys[account]; ok {
		return k, true
	}

	if account == DevStoreAccount {
		return DevStoreKey, true
	}

	return "", false
}

// authorize verifies the SharedKey signature or SAS token of a request when signature verification is on.
// Anonymous requests fall to the public-access rules; a bearer token is not validated (RBAC is not emulated).
func (h *Handler) authorize(r *http.Request, account, container, blob string) *apiError {
	if !h.VerifySignatures {
		return nil
	}

	key, known := h.accountKey(account)
	authz := r.Header.Get("Authorization")

	switch {
	case authz != "":
		return h.authorizeHeader(r, authz, key, account, known)
	case r.URL.Query().Get("sig") != "":
		if !known {
			return errAuthFailed(
				"Server failed to authenticate the request. The account is not known to this emulator.",
			)
		}

		return h.authorizeSAS(r, key, account, container, blob)
	default:
		return nil
	}
}

func (h *Handler) authorizeHeader(r *http.Request, authz, key, account string, known bool) *apiError {
	if strings.HasPrefix(authz, "Bearer ") {
		return nil
	}

	if !strings.HasPrefix(authz, "SharedKey ") && !strings.HasPrefix(authz, "SharedKeyLite ") {
		return errAuthFailed("Server failed to authenticate the request. The authorization scheme is not supported.")
	}

	if !known {
		return errAuthFailed(
			"Server failed to authenticate the request. The account is not known to this emulator.",
		)
	}

	if !h.verifySharedKeyRequest(key, account, r) {
		return errAuthFailed("Server failed to authenticate the request. " +
			"Make sure the value of Authorization header is formed correctly including the signature.")
	}

	return nil
}

type sasToken struct {
	q url.Values
}

func (t sasToken) get(name string) string { return t.q.Get(name) }

func sasSignature(key, stringToSign string) (string, bool) {
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return "", false
	}

	mac := hmac.New(sha256.New, raw)
	mac.Write([]byte(stringToSign))

	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), true
}

func (h *Handler) authorizeSAS(r *http.Request, key, account, container, blob string) *apiError {
	t := sasToken{q: r.URL.Query()}
	if !supportedSASVersion(t.get("sv")) {
		return errAuthFailed("Server failed to authenticate the request. The SAS version is not supported.")
	}

	if t.get("ses") != "" && t.get("sv") < sasEncryptionScopeFrom {
		return errAuthFailed("Server failed to authenticate the request. The SAS encryption scope needs version " +
			sasEncryptionScopeFrom + " or later.")
	}

	var (
		perms string
		err   *apiError
	)

	if t.get("ss") != "" {
		perms, err = h.verifyAccountSAS(r, t, key, account, container, blob)
	} else {
		perms, err = h.verifyServiceSAS(r, t, key, account, container, blob)
	}

	if err != nil {
		return err
	}

	need := h.requiredPermissions(r, container, blob, t.get("ss") != "")
	if need == "" || !strings.ContainsAny(perms, need) {
		return errAuthorizationFailure()
	}

	return nil
}

func supportedSASVersion(v string) bool {
	if _, err := time.Parse(sasVersionLayout, v); err != nil {
		return false
	}

	return v >= sasMinVersion && v <= sasMaxVersion
}

func (h *Handler) nowUTC() time.Time {
	if h.clock != nil {
		return h.clock().UTC()
	}

	return time.Now().UTC()
}

// SetClock replaces the clock used for SAS validity windows and request-date checks.
func (h *Handler) SetClock(clock func() time.Time) { h.clock = clock }

func sasEqual(want, got string) bool { return hmac.Equal([]byte(want), []byte(got)) }

func (h *Handler) verifyAccountSAS(
	r *http.Request, t sasToken, key, account, container, blob string,
) (string, *apiError) {
	fields := []string{
		account, t.get("sp"), t.get("ss"), t.get("srt"), t.get("st"), t.get("se"),
		t.get("sip"), t.get("spr"), t.get("sv"),
	}
	if t.get("sv") >= sasEncryptionScopeFrom {
		fields = append(fields, t.get("ses"))
	}

	fields = append(fields, "")

	sig, ok := sasSignature(key, strings.Join(fields, "\n"))
	if !ok || !sasEqual(sig, t.get("sig")) {
		return "", errAuthFailed("Server failed to authenticate the request. " +
			"Make sure the value of Authorization header is formed correctly including the signature.")
	}

	if err := h.checkSASWindow(r, t, t.get("st"), t.get("se")); err != nil {
		return "", err
	}

	rt := accountResourceType(operationFor(r), container, blob)
	if !strings.Contains(t.get("ss"), "b") || !strings.Contains(t.get("srt"), rt) {
		return "", errAuthorizationFailure()
	}

	return t.get("sp"), nil
}

func accountResourceType(op, container, blob string) string {
	switch {
	case op == opUndeleteBlob:
		return "c"
	case op == opFindBlobsByTags && container == "":
		return "o"
	case blob != "":
		return "o"
	case container != "":
		return "c"
	default:
		return "s"
	}
}

func (h *Handler) verifyServiceSAS(
	r *http.Request, t sasToken, key, account, container, blob string,
) (string, *apiError) {
	resource := t.get("sr")
	canonical := "/blob/" + account + "/" + container

	switch resource {
	case "c":
	case "b", "bs":
		if blob == "" {
			return "", errAuthorizationFailure()
		}

		canonical += "/" + strings.ReplaceAll(blob, "\\", "/")
	default:
		return "", errAuthFailed("Server failed to authenticate the request. The SAS resource type is not supported.")
	}

	snapshot := ""
	if resource == "bs" {
		snapshot = t.get("snapshot")
	}

	fields := []string{
		t.get("sp"), t.get("st"), t.get("se"), canonical, t.get("si"), t.get("sip"), t.get("spr"), t.get("sv"),
		resource, snapshot,
	}
	if t.get("sv") >= sasEncryptionScopeFrom {
		fields = append(fields, t.get("ses"))
	}

	fields = append(fields, t.get("rscc"), t.get("rscd"), t.get("rsce"), t.get("rscl"), t.get("rsct"))

	sig, ok := sasSignature(key, strings.Join(fields, "\n"))
	if !ok || !sasEqual(sig, t.get("sig")) {
		return "", errAuthFailed("Server failed to authenticate the request. " +
			"Make sure the value of Authorization header is formed correctly including the signature.")
	}

	perms, start, expiry := t.get("sp"), t.get("st"), t.get("se")

	if id := t.get("si"); id != "" {
		var perr *apiError
		if perms, start, expiry, perr = h.applyStoredPolicy(container, id, perms, start, expiry); perr != nil {
			return "", perr
		}
	}

	if expiry == "" {
		return "", errAuthFailed("Server failed to authenticate the request. The SAS has no expiry.")
	}

	if err := h.checkSASWindow(r, t, start, expiry); err != nil {
		return "", err
	}

	return perms, nil
}

// applyStoredPolicy fills the SAS fields the stored access policy supplies; a field in both is 400.
func (h *Handler) applyStoredPolicy(container, id, perms, start, expiry string) (string, string, string, *apiError) {
	pol, found := h.storedPolicy(container, id)
	if !found {
		return "", "", "", errAuthFailed(
			"Server failed to authenticate the request. The signed identifier was not found.",
		)
	}

	if (perms != "" && pol.Permission != "") || (start != "" && pol.Start != "") ||
		(expiry != "" && pol.Expiry != "") {
		return "", "", "", &apiError{
			status: http.StatusBadRequest, code: "InvalidAuthenticationInfo",
			message: "A field is set in both the SAS and the stored access policy.",
		}
	}

	return firstNonEmpty(perms, pol.Permission), firstNonEmpty(start, pol.Start),
		firstNonEmpty(expiry, pol.Expiry), nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

func (h *Handler) storedPolicy(container, id string) (SignedIdentifier, bool) {
	ci, err := h.Backend.GetContainer(container)
	if err != nil {
		return SignedIdentifier{}, false
	}

	for _, p := range ci.ACL {
		if p.ID == id {
			return p, true
		}
	}

	return SignedIdentifier{}, false
}

func parseSASTime(v string) (time.Time, bool) {
	layouts := []string{"2006-01-02T15:04:05.0000000Z", "2006-01-02T15:04:05Z", "2006-01-02T15:04Z", "2006-01-02"}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

func (h *Handler) checkSASWindow(r *http.Request, t sasToken, start, expiry string) *apiError {
	now := h.nowUTC()

	if start != "" {
		st, ok := parseSASTime(start)
		if !ok || now.Before(st) {
			return errAuthFailed("Server failed to authenticate the request. The SAS is not yet valid.")
		}
	}

	if expiry != "" {
		se, ok := parseSASTime(expiry)
		if !ok || !now.Before(se) {
			return errAuthFailed("Server failed to authenticate the request. The SAS has expired.")
		}
	}

	switch spr := t.get("spr"); spr {
	case "", sasHTTPSAndHTTP:
	case sasHTTPSOnly:
		if r.TLS == nil {
			return errAuthFailed("Server failed to authenticate the request. The SAS only permits HTTPS.")
		}
	default:
		return errAuthFailed("Server failed to authenticate the request. The SAS protocol is not valid.")
	}

	if sip := t.get("sip"); sip != "" && !ipInRange(sip, r.RemoteAddr) {
		return errAuthFailed("Server failed to authenticate the request. The caller IP is outside the SAS IP range.")
	}

	return nil
}

func ipInRange(spec, remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	lo, hi, isRange := strings.Cut(spec, "-")
	if !isRange {
		return ip.Equal(net.ParseIP(lo))
	}

	a, b := net.ParseIP(lo).To16(), net.ParseIP(hi).To16()
	ip = ip.To16()

	return a != nil && b != nil && ip != nil && bytesCompare(a, ip) <= 0 && bytesCompare(ip, b) <= 0
}

func bytesCompare(a, b net.IP) int { return strings.Compare(string(a), string(b)) }

// requiredPermissions lists the SAS permission letters of which any one authorises the request; empty means a
// SAS cannot authorise it (an account SAS adds the service and container operations a service SAS cannot).
// Letters follow the "Create an account SAS" operation table and "Create a service SAS" permission table.
func (h *Handler) requiredPermissions(r *http.Request, container, blob string, account bool) string {
	op := operationFor(r)

	switch op {
	case opPutBlob, opCopyBlob:
		if h.blobExists(r, op, container, blob) {
			return "w"
		}

		return "cw"
	}

	if perms := blobPermissions(op); perms != "" || !account {
		return perms
	}

	switch op {
	case opListContainers:
		return "l"
	case opGetServiceProperties, opGetContainerProperties:
		return "r"
	case opCreateContainer:
		return "cw"
	case opDeleteContainer:
		return "d"
	case opSetServiceProperties, opSetContainerMetadata:
		return "w"
	case opLeaseContainer:
		return "wd"
	default:
		return ""
	}
}

// blobExists reports whether the blob a Put Blob or Copy Blob targets already exists (overwrite needs Write).
func (h *Handler) blobExists(r *http.Request, op, container, blob string) bool {
	if op == opCopyBlob && r.Header.Get(headerCopySource) == "" {
		return false
	}

	_, err := h.Backend.HeadBlobAt(container, blob, "")

	return err == nil
}

func blobPermissions(op string) string {
	switch op {
	case opGetBlob, opGetBlobProperties, opGetBlobMetadata, opGetBlockList, opGetPageRanges:
		return "r"
	case opListBlobs:
		return "l"
	case opPutBlock, opPutBlockFromURL, opPutBlockList, opSetBlobProperties, opSetBlobMetadata,
		opSetBlobTier, opPutPage, opAbortCopyBlob, opUndeleteBlob:
		return "w"
	case opSnapshotBlob:
		return "cw"
	case opLeaseBlob:
		return "wd"
	case opAppendBlock:
		return "aw"
	case opDeleteBlob:
		return "d"
	case opSetBlobTags, opGetBlobTags:
		return "t"
	case opFindBlobsByTags:
		return "f"
	default:
		return ""
	}
}

// verifySharedKeyRequest checks the SharedKey or SharedKeyLite signature against the request. The header's
// account must be the addressed account and the request date must be within 15 minutes of now. Both canonical
// resource forms are accepted: the Azure SDK for Go signs "/<account>" plus the full URL path (the account twice
// for path-style URLs), other SDKs drop the path's account.
func (h *Handler) verifySharedKeyRequest(key, account string, r *http.Request) bool {
	auth, ok := azureauth.ParseAuthorizationHeader(r.Header.Get("Authorization"))
	if !ok || auth.Account != account || !h.requestDateFresh(r) {
		return false
	}

	stripped := azureauth.CanonicalizedResource(auth.Account, r.URL)

	for _, resource := range []string{stripped, fullPathResource(auth.Account, r.URL, stripped)} {
		toSign := sharedKeyStringToSign(r, resource)
		if auth.Scheme == azureauth.SchemeSharedKeyLite {
			toSign = sharedKeyLiteStringToSign(r, resource)
		}

		if want, signed := sasSignature(key, toSign); signed && sasEqual(want, auth.Signature) {
			return true
		}
	}

	return false
}

func (h *Handler) requestDateFresh(r *http.Request) bool {
	raw := r.Header.Get("X-Ms-Date")
	if raw == "" {
		raw = r.Header.Get("Date")
	}

	when, err := http.ParseTime(raw)
	if err != nil {
		return false
	}

	skew := h.nowUTC().Sub(when)

	return skew <= maxDateSkew && skew >= -maxDateSkew
}

// dateField is the string-to-sign Date value: empty whenever x-ms-date is present.
func dateField(r *http.Request) string {
	if r.Header.Get("X-Ms-Date") != "" {
		return ""
	}

	return r.Header.Get("Date")
}

func sharedKeyStringToSign(r *http.Request, resource string) string {
	length := ""
	if r.ContentLength > 0 {
		length = strconv.FormatInt(r.ContentLength, 10)
	}

	return strings.Join([]string{
		r.Method,
		r.Header.Get("Content-Encoding"),
		r.Header.Get("Content-Language"),
		length,
		r.Header.Get("Content-MD5"),
		r.Header.Get("Content-Type"),
		dateField(r),
		r.Header.Get("If-Modified-Since"),
		r.Header.Get("If-Match"),
		r.Header.Get("If-None-Match"),
		r.Header.Get("If-Unmodified-Since"),
		r.Header.Get("Range"),
		azureauth.CanonicalizedHeaders(r) + resource,
	}, "\n")
}

// sharedKeyLiteStringToSign signs the path and only the comp query parameter.
func sharedKeyLiteStringToSign(r *http.Request, resource string) string {
	path, _, _ := strings.Cut(resource, "\n")
	if comp := r.URL.Query().Get("comp"); comp != "" {
		path += "?comp=" + comp
	}

	return strings.Join([]string{
		r.Method,
		r.Header.Get("Content-MD5"),
		r.Header.Get("Content-Type"),
		dateField(r),
		azureauth.CanonicalizedHeaders(r) + path,
	}, "\n")
}

func fullPathResource(account string, u *url.URL, stripped string) string {
	_, query, _ := strings.Cut(stripped, "\n")
	if query != "" {
		query = "\n" + query
	}

	return "/" + account + u.EscapedPath() + query
}
