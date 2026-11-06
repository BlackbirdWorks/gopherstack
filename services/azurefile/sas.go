package azurefile

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

const accountSASEncScopeVersion = "2020-12-06"

// sasRule is what an operation demands of a SAS: the account SAS resource
// type, the permission letters (any one suffices) and which service SAS
// resources (share, file) may drive it.
type sasRule struct {
	perms string
	rtype byte
	svc   sasScope
}

type sasScope int

const (
	scopeAccountOnly sasScope = iota
	scopeShare
	scopeShareOrFile
)

const (
	rtService = 's'
	rtShare   = 'c'
	rtObject  = 'o'
)

// sasRules follows "Account SAS permissions by operation" (File service) and
// the service SAS permission tables in the Azure Storage REST reference.
var sasRules = map[string]sasRule{ //nolint:gochecknoglobals // immutable lookup table
	opListShares:        {rtype: rtService, perms: "l"},
	opGetServiceProps:   {rtype: rtService, perms: "r"},
	opSetServiceProps:   {rtype: rtService, perms: "w"},
	opCreateShare:       {rtype: rtShare, perms: "cw"},
	opGetShareProps:     {rtype: rtShare, perms: "r"},
	opSetShareProps:     {rtype: rtShare, perms: "w"},
	opSetShareMetadata:  {rtype: rtShare, perms: "w"},
	opDeleteShare:       {rtype: rtShare, perms: "d"},
	opSnapshotShare:     {rtype: rtShare, perms: "cw"},
	opRestoreShare:      {rtype: rtShare, perms: "w"},
	opGetShareACL:       {rtype: rtShare, perms: "r"},
	opSetShareACL:       {rtype: rtShare, perms: "w"},
	opGetShareStats:     {rtype: rtShare, perms: "r"},
	opLeaseShare:        {rtype: rtShare, perms: "wd"},
	opCreatePermission:  {rtype: rtShare, perms: "cw"},
	opGetPermission:     {rtype: rtShare, perms: "r"},
	opListFilesDirs:     {rtype: rtShare, perms: "l", svc: scopeShare},
	opCreateDirectory:   {rtype: rtObject, perms: "cw", svc: scopeShare},
	opGetDirProps:       {rtype: rtObject, perms: "r", svc: scopeShare},
	opSetDirProps:       {rtype: rtObject, perms: "w", svc: scopeShare},
	opSetDirMetadata:    {rtype: rtObject, perms: "w", svc: scopeShare},
	opDeleteDirectory:   {rtype: rtObject, perms: "d", svc: scopeShare},
	opRenameDirectory:   {rtype: rtObject, perms: "dw", svc: scopeShare},
	opListHandles:       {rtype: rtObject, perms: "r", svc: scopeShareOrFile},
	opForceCloseHandles: {rtype: rtObject, perms: "w", svc: scopeShareOrFile},
	opCreateFile:        {rtype: rtObject, perms: "cw", svc: scopeShareOrFile},
	opDeleteFile:        {rtype: rtObject, perms: "d", svc: scopeShareOrFile},
	opGetFile:           {rtype: rtObject, perms: "r", svc: scopeShareOrFile},
	opGetFileProps:      {rtype: rtObject, perms: "r", svc: scopeShareOrFile},
	opSetFileProps:      {rtype: rtObject, perms: "w", svc: scopeShareOrFile},
	opSetFileMetadata:   {rtype: rtObject, perms: "w", svc: scopeShareOrFile},
	opPutRange:          {rtype: rtObject, perms: "w", svc: scopeShareOrFile},
	opPutRangeFromURL:   {rtype: rtObject, perms: "w", svc: scopeShareOrFile},
	opListRanges:        {rtype: rtObject, perms: "r", svc: scopeShareOrFile},
	opCopyFile:          {rtype: rtObject, perms: "w", svc: scopeShareOrFile},
	opAbortCopyFile:     {rtype: rtObject, perms: "w", svc: scopeShareOrFile},
	opRenameFile:        {rtype: rtObject, perms: "dw", svc: scopeShareOrFile},
	opLeaseFile:         {rtype: rtObject, perms: "wd", svc: scopeShareOrFile},
	opCreateHardLink:    {rtype: rtObject, perms: "cw", svc: scopeShareOrFile},
	opCreateSymLink:     {rtype: rtObject, perms: "cw", svc: scopeShareOrFile},
	opGetSymLink:        {rtype: rtObject, perms: "r", svc: scopeShareOrFile},
}

func authzErr(code, msg string) *apiError { return newErr(http.StatusForbidden, code, msg) }

func errPermissionMismatch() *apiError {
	return authzErr("AuthorizationPermissionMismatch",
		"This request is not authorized to perform this operation using this permission.")
}

// verifySAS authorises a SAS request. Signatures are only checkable for the
// development account; every other check applies to all accounts.
func (h *Handler) verifySAS(rc *reqCtx, op string) *apiError {
	q := rc.q
	account := q.Get("ss") != ""

	if q.Get("sv") == "" || (!account && q.Get("sr") == "") {
		return authFailed(msgBadSignature)
	}

	sp, st, se, aerr := h.resolveSASPolicy(rc, account)
	if aerr != nil {
		return aerr
	}

	if aerr = checkSASWindow(rc.now, st, se); aerr != nil {
		return aerr
	}

	if strings.EqualFold(rc.acct, azureauth.DefaultAccountName) {
		if aerr = verifySASSignature(rc, account); aerr != nil {
			return aerr
		}
	}

	if aerr = checkSASOrigin(rc); aerr != nil {
		return aerr
	}

	rule, ok := sasRules[op]
	if !ok {
		return errPermissionMismatch()
	}

	if op == opCreateFile && h.Backend.fileExists(rc.acct, rc.share, rc.path) {
		rule.perms = "w"
	}

	if account {
		return checkAccountSAS(q.Get("ss"), q.Get("srt"), sp, rule)
	}

	return checkServiceSAS(q.Get("sr"), sp, rule)
}

// resolveSASPolicy returns the effective permissions, start and expiry,
// taking them from the stored access policy named by si when there is one.
func (h *Handler) resolveSASPolicy(rc *reqCtx, account bool) (string, string, string, *apiError) {
	q := rc.q
	sp, st, se := q.Get("sp"), q.Get("st"), q.Get("se")

	id := q.Get("si")
	if id == "" || account {
		if sp == "" || se == "" {
			return "", "", "", authFailed(msgBadSignature)
		}

		return sp, st, se, nil
	}

	pol, ok := h.Backend.sharePolicy(rc.acct, rc.share, id)
	if !ok {
		return "", "", "", authFailed("Server failed to authenticate the request. The signed identifier was not found.")
	}

	for _, pair := range [][2]string{{sp, pol.Permission}, {st, pol.Start}, {se, pol.Expiry}} {
		if pair[0] != "" && pair[1] != "" {
			return "", "", "", authFailed(msgBadSignature)
		}
	}

	sp, st, se = pick(sp, pol.Permission), pick(st, pol.Start), pick(se, pol.Expiry)
	if sp == "" || se == "" {
		return "", "", "", authFailed(msgBadSignature)
	}

	return sp, st, se, nil
}

func pick(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

func checkSASWindow(now time.Time, st, se string) *apiError {
	end, ok := parseSASTime(se)
	if !ok {
		return authFailed(msgBadSignature)
	}

	var start time.Time

	if st != "" {
		if start, ok = parseSASTime(st); !ok {
			return authFailed(msgBadSignature)
		}
	}

	if now.Before(start) || !now.Before(end) {
		return authFailed(
			"Server failed to authenticate the request. Signature not valid in the specified time frame: " +
				"Start [" + st + "] - Expiry [" + se + "] - Current [" + fmtSASTime(now) + "]",
		)
	}

	return nil
}

func fmtSASTime(t time.Time) string { return t.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT") }

// checkSASOrigin enforces the spr and sip restrictions.
func checkSASOrigin(rc *reqCtx) *apiError {
	if strings.EqualFold(rc.q.Get("spr"), "https") && rc.r.TLS == nil {
		return authzErr("AuthorizationProtocolMismatch",
			"This request is not authorized to perform this operation using this protocol.")
	}

	if sip := rc.q.Get("sip"); sip != "" && !ipAllowed(sip, rc.r.RemoteAddr) {
		return authzErr("AuthorizationSourceIPMismatch",
			"This request is not authorized to perform this operation using this source IP address.")
	}

	return nil
}

// ipAllowed reports whether the peer is the sip address or inside its inclusive a-b range (IPv4 only).
func ipAllowed(sip, remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}

	peer := net.ParseIP(host).To4()
	if peer == nil {
		return false
	}

	lo, hi, isRange := strings.Cut(sip, "-")
	if !isRange {
		hi = lo
	}

	from, to := net.ParseIP(strings.TrimSpace(lo)).To4(), net.ParseIP(strings.TrimSpace(hi)).To4()
	if from == nil || to == nil {
		return false
	}

	return bytesCmp(peer, from) >= 0 && bytesCmp(peer, to) <= 0
}

func bytesCmp(a, b net.IP) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}

			return 1
		}
	}

	return 0
}

func checkAccountSAS(ss, srt, sp string, rule sasRule) *apiError {
	if !strings.Contains(ss, "f") {
		return authzErr("AuthorizationServiceMismatch",
			"This request is not authorized to perform this operation using this service.")
	}

	if !strings.ContainsRune(srt, rune(rule.rtype)) {
		return authzErr("AuthorizationResourceTypeMismatch",
			"This request is not authorized to perform this operation using this resource type.")
	}

	return needAnyPerm(sp, rule.perms)
}

func checkServiceSAS(sr, sp string, rule sasRule) *apiError {
	ok := false

	switch rule.svc {
	case scopeShare:
		ok = sr == "s"
	case scopeShareOrFile:
		ok = sr == "s" || sr == "f"
	case scopeAccountOnly:
	}

	if !ok {
		return authzErr("AuthorizationResourceTypeMismatch",
			"This request is not authorized to perform this operation using this resource type.")
	}

	return needAnyPerm(sp, rule.perms)
}

func needAnyPerm(have, want string) *apiError {
	if strings.ContainsAny(have, want) {
		return nil
	}

	return errPermissionMismatch()
}

func verifySASSignature(rc *reqCtx, account bool) *apiError {
	key, err := base64.StdEncoding.DecodeString(azureauth.DefaultAccountKey)
	if err != nil {
		return authFailed("The development account key is unusable.")
	}

	q := rc.q

	var toSign string

	if account {
		parts := []string{
			strings.ToLower(rc.acct), q.Get("sp"), q.Get("ss"), q.Get("srt"), q.Get("st"), q.Get("se"),
			q.Get("sip"), q.Get("spr"), q.Get("sv"),
		}
		if q.Get("sv") >= accountSASEncScopeVersion {
			parts = append(parts, q.Get("ses"))
		}

		toSign = strings.Join(append(parts, ""), "\n")
	} else {
		resource := "/file/" + strings.ToLower(rc.acct) + "/" + rc.share
		if q.Get("sr") == "f" && rc.path != "" {
			resource += "/" + strings.Trim(rc.path, "/")
		}

		toSign = strings.Join([]string{
			q.Get("sp"), q.Get("st"), q.Get("se"), resource, q.Get("si"), q.Get("sip"), q.Get("spr"), q.Get("sv"),
			q.Get("rscc"), q.Get("rscd"), q.Get("rsce"), q.Get("rscl"), q.Get("rsct"),
		}, "\n")
	}

	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(toSign))

	if !hmac.Equal([]byte(base64.StdEncoding.EncodeToString(mac.Sum(nil))), []byte(q.Get("sig"))) {
		return authFailed(msgBadSignature)
	}

	return nil
}

func parseSASTime(s string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04Z", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}
