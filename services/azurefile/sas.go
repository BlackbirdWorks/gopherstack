package azurefile

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

// verifySAS checks the validity window and, for the development account, the
// signature of a service (share or file) SAS. Account SAS tokens are only
// checked for expiry.
func verifySAS(rc *reqCtx) *apiError {
	q := rc.q

	if se := q.Get("se"); se != "" {
		t, ok := parseSASTime(se)
		if !ok || !rc.now.Before(t) {
			return authFailed(msgBadSignature)
		}
	}

	if st := q.Get("st"); st != "" {
		if t, ok := parseSASTime(st); ok && rc.now.Before(t) {
			return authFailed("Server failed to authenticate the request. The SAS is not yet valid.")
		}
	}

	if !strings.EqualFold(rc.acct, azureauth.DefaultAccountName) || q.Get("sr") == "" {
		return nil
	}

	key, err := base64.StdEncoding.DecodeString(azureauth.DefaultAccountKey)
	if err != nil {
		return authFailed("The development account key is unusable.")
	}

	resource := "/file/" + strings.ToLower(rc.acct) + "/" + rc.share
	if q.Get("sr") == "f" && rc.path != "" {
		resource += "/" + strings.Trim(rc.path, "/")
	}

	toSign := strings.Join([]string{
		q.Get("sp"), q.Get("st"), q.Get("se"), resource, q.Get("si"), q.Get("sip"), q.Get("spr"), q.Get("sv"),
		q.Get("rscc"), q.Get("rscd"), q.Get("rsce"), q.Get("rscl"), q.Get("rsct"),
	}, "\n")

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
