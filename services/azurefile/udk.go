package azurefile

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"encoding/xml"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

const (
	udkMaxWindow          = 7 * 24 * time.Hour
	udkService            = "f"
	sasTimeLayout         = "2006-01-02T15:04:05Z"
	compUserDelegationKey = "userdelegationkey"
	bearerPrefix          = "Bearer "
	jwtParts              = 3
	guidHexLen            = 32
)

type keyInfoXML struct {
	XMLName xml.Name `xml:"KeyInfo"`
	Start   string   `xml:"Start"`
	Expiry  string   `xml:"Expiry"`
}

type userDelegationKeyXML struct {
	XMLName       xml.Name `xml:"UserDelegationKey"`
	SignedOID     string   `xml:"SignedOid"`
	SignedTID     string   `xml:"SignedTid"`
	SignedStart   string   `xml:"SignedStart"`
	SignedExpiry  string   `xml:"SignedExpiry"`
	SignedService string   `xml:"SignedService"`
	SignedVersion string   `xml:"SignedVersion"`
	Value         string   `xml:"Value"`
}

func errInvalidKeyInfo(msg string) *apiError {
	return newErr(http.StatusBadRequest, "InvalidXmlNodeValue", msg)
}

// principalFromBearer reads oid and tid from the (unverified) bearer token, falling back to values derived from it.
func (h *Handler) principal(authz string) (string, string) {
	if h.ValidateToken != nil {
		if oid, tid, err := h.ValidateToken(strings.TrimPrefix(authz, bearerPrefix)); err == nil && oid != "" {
			return oid, tid
		}
	}

	return principalFromBearer(authz)
}

func principalFromBearer(authz string) (string, string) {
	tok := strings.TrimPrefix(authz, bearerPrefix)

	var claims struct {
		OID string `json:"oid"`
		TID string `json:"tid"`
	}

	if parts := strings.Split(tok, "."); len(parts) == jwtParts {
		if raw, err := base64.RawURLEncoding.DecodeString(parts[1]); err == nil {
			_ = json.Unmarshal(raw, &claims)
		}
	}

	sum := sha256.Sum256([]byte(tok))
	if claims.OID == "" {
		claims.OID = guidFrom(sum[:16])
	}

	if claims.TID == "" {
		claims.TID = guidFrom(sum[16:])
	}

	return claims.OID, claims.TID
}

func guidFrom(b []byte) string {
	h := hex.EncodeToString(b)

	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:guidHexLen]
}

type udkFields struct{ oid, tid, start, expiry, service, version string }

// deriveUDK is stateless: the key is an HMAC of the account key over the issued fields, so a SAS can be
// verified without storing keys.
func deriveUDK(f udkFields) []byte {
	key, _ := base64.StdEncoding.DecodeString(azureauth.DefaultAccountKey)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(strings.Join([]string{"udk", f.oid, f.tid, f.start, f.expiry, f.service, f.version}, "\n")))

	return mac.Sum(nil)
}

func (h *Handler) getUserDelegationKey(rc *reqCtx) (*resp, *apiError) {
	authz := rc.r.Header.Get("Authorization")
	if !strings.HasPrefix(authz, bearerPrefix) {
		return nil, authFailed("Only authentication with Microsoft Entra ID is supported for this operation.")
	}

	var ki keyInfoXML

	body, aerr := rc.body()
	if aerr != nil {
		return nil, aerr
	}

	if err := xml.Unmarshal(body, &ki); err != nil || ki.Start == "" || ki.Expiry == "" {
		return nil, newErr(http.StatusBadRequest, "InvalidXmlDocument", "XML specified is not syntactically valid.")
	}

	start, ok1 := parseSASTime(ki.Start)
	expiry, ok2 := parseSASTime(ki.Expiry)

	if !ok1 || !ok2 {
		return nil, errInvalidKeyInfo("The value for one of the XML nodes is not in the correct format.")
	}

	start, expiry = start.UTC(), expiry.UTC()

	if !expiry.After(start) {
		return nil, errInvalidKeyInfo("Key expiry time must be after the key start time.")
	}

	for _, t := range []time.Time{start, expiry} {
		if t.Sub(rc.now).Abs() > udkMaxWindow {
			return nil, errInvalidKeyInfo("Key times must be within seven days of the current date.")
		}
	}

	oid, tid := h.principal(authz)
	f := udkFields{oid, tid, start.Format(sasTimeLayout), expiry.Format(sasTimeLayout), udkService, azureFileVersion}

	return newResp(http.StatusOK).xml(userDelegationKeyXML{
		SignedOID: f.oid, SignedTID: f.tid, SignedStart: f.start, SignedExpiry: f.expiry,
		SignedService: f.service, SignedVersion: f.version,
		Value: base64.StdEncoding.EncodeToString(deriveUDK(f)),
	}), nil
}

// verifyUserDelegationSAS checks a SAS signed with a user delegation key; the string-to-sign is the
// azfile SDK's SignWithUserDelegation layout.
func verifyUserDelegationSAS(rc *reqCtx) *apiError {
	q := rc.q
	f := udkFields{
		oid: q.Get("skoid"), tid: q.Get("sktid"), start: q.Get("skt"), expiry: q.Get("ske"),
		service: q.Get("sks"), version: q.Get("skv"),
	}

	if aerr := checkSASWindow(rc.now, f.start, f.expiry); aerr != nil {
		return aerr
	}

	resource := "/file/" + strings.ToLower(rc.acct) + "/" + rc.share
	if q.Get("sr") == "f" && rc.path != "" {
		resource += "/" + strings.Trim(rc.path, "/")
	}

	toSign := strings.Join([]string{
		q.Get("sp"), q.Get("st"), q.Get("se"), resource, f.oid, f.tid, f.start, f.expiry, f.service, f.version,
		q.Get("saoid"), q.Get("suoid"), q.Get("scid"), "", q.Get("sduoid"), q.Get("sip"), q.Get("spr"),
		q.Get("sv"), q.Get("sr"), "", "", q.Get("rscc"), q.Get("rscd"), q.Get("rsce"), q.Get("rscl"), q.Get("rsct"),
	}, "\n")

	mac := hmac.New(sha256.New, deriveUDK(f))
	_, _ = mac.Write([]byte(toSign))

	if !hmac.Equal([]byte(base64.StdEncoding.EncodeToString(mac.Sum(nil))), []byte(q.Get("sig"))) {
		return authFailed(msgBadSignature)
	}

	return nil
}
