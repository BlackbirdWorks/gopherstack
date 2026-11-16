package azureblob

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"net/http"
	"net/url"
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
	sasHTTPSOnly           = "https"
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
// Anonymous requests and bearer tokens pass through; the former fall to the public-access rules.
func (h *Handler) authorize(r *http.Request, account, container, blob string) *apiError {
	if !h.VerifySignatures {
		return nil
	}

	key, known := h.accountKey(account)
	authz := r.Header.Get("Authorization")

	switch {
	case strings.HasPrefix(authz, "SharedKey"):
		if !known {
			return errAuthFailed(
				"Server failed to authenticate the request. The account is not known to this emulator.",
			)
		}

		if !verifySharedKeyRequest(key, r) {
			return errAuthFailed("Server failed to authenticate the request. " +
				"Make sure the value of Authorization header is formed correctly including the signature.")
		}

		return nil
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
	if v := t.get("sv"); v < sasMinVersion {
		return errAuthFailed("Server failed to authenticate the request. The SAS version is not supported.")
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

	need := requiredPermissions(operationFor(r), t.get("ss") != "")
	if need == "" || !strings.ContainsAny(perms, need) {
		return errAuthorizationFailure()
	}

	return nil
}

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

	if !strings.Contains(t.get("ss"), "b") || !strings.Contains(t.get("srt"), accountResourceType(container, blob)) {
		return "", errAuthorizationFailure()
	}

	return t.get("sp"), nil
}

func accountResourceType(container, blob string) string {
	switch {
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
		pol, found := h.storedPolicy(container, id)
		if !found {
			return "", errAuthFailed("Server failed to authenticate the request. The signed identifier was not found.")
		}

		perms, start, expiry = firstNonEmpty(
			perms,
			pol.Permission,
		), firstNonEmpty(
			start,
			pol.Start,
		), firstNonEmpty(
			expiry,
			pol.Expiry,
		)
	}

	if expiry == "" {
		return "", errAuthFailed("Server failed to authenticate the request. The SAS has no expiry.")
	}

	if err := h.checkSASWindow(r, t, start, expiry); err != nil {
		return "", err
	}

	return perms, nil
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
	now := time.Now().UTC()

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

	if spr := t.get("spr"); spr == sasHTTPSOnly && r.TLS == nil {
		return errAuthFailed("Server failed to authenticate the request. The SAS only permits HTTPS.")
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

// requiredPermissions lists the SAS permission letters of which any one authorises op; empty means a SAS
// cannot authorise it (account SAS covers the account and container operations service SAS cannot).
func requiredPermissions(op string, account bool) string {
	if perms := blobPermissions(op); perms != "" || !account {
		return perms
	}

	switch op {
	case opListContainers:
		return "l"
	case opGetServiceProperties, opGetContainerProperties, opGetContainerACL:
		return "r"
	case opCreateContainer:
		return "c"
	case opDeleteContainer:
		return "d"
	case opSetServiceProperties, opSetContainerMetadata, opSetContainerACL, opLeaseContainer:
		return "w"
	default:
		return ""
	}
}

func blobPermissions(op string) string {
	switch op {
	case opGetBlob, opGetBlobProperties, opGetBlobMetadata, opGetBlockList, opGetPageRanges:
		return "r"
	case opListBlobs:
		return "l"
	case opPutBlob, opPutBlock, opPutBlockFromURL, opPutBlockList, opSetBlobProperties, opSetBlobMetadata,
		opSetBlobTier, opSnapshotBlob, opLeaseBlob, opPutPage, opCopyBlob, opUndeleteBlob:
		return "wcau"
	case opAppendBlock:
		return "aw"
	case opAbortCopyBlob:
		return "w"
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

// verifySharedKeyRequest accepts a signature over either canonical resource form: the Azure SDK for Go signs
// "/<account>" plus the full URL path (the account twice for path-style URLs), other SDKs drop the path's account.
func verifySharedKeyRequest(key string, r *http.Request) bool {
	auth, ok := azureauth.ParseAuthorizationHeader(r.Header.Get("Authorization"))
	if !ok {
		return false
	}

	if sigOK, err := azureauth.VerifySharedKey(key, r); err == nil && sigOK {
		return true
	}

	if auth.Scheme != azureauth.SchemeSharedKey {
		return false
	}

	stripped := azureauth.CanonicalizedResource(auth.Account, r.URL)
	full := fullPathResource(auth.Account, r.URL, stripped)

	prefix, found := strings.CutSuffix(azureauth.StringToSign(r, auth.Account), stripped)
	if !found {
		return false
	}

	want, ok := sasSignature(key, prefix+full)

	return ok && sasEqual(want, auth.Signature)
}

func fullPathResource(account string, u *url.URL, stripped string) string {
	_, query, _ := strings.Cut(stripped, "\n")
	if query != "" {
		query = "\n" + query
	}

	return "/" + account + u.EscapedPath() + query
}
