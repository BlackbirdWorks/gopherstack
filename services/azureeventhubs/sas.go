package azureeventhubs

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/url"
	"strconv"
	"strings"
)

const (
	sasPrefix    = "SharedAccessSignature "
	sasTokenType = "servicebus.windows.net:sastoken"
	maxSASLen    = 4096
)

// SignSAS computes the SAS signature for an already URL-encoded resource.
func SignSAS(encodedResource string, expiry int64, key string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(encodedResource + "\n" + strconv.FormatInt(expiry, 10)))

	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func parseSAS(token string) map[string]string {
	token = strings.TrimPrefix(token, sasPrefix)
	out := map[string]string{}

	for pair := range strings.SplitSeq(token, "&") {
		if k, v, found := strings.Cut(pair, "="); found {
			out[k] = v
		}
	}

	return out
}

// claimRights validates a SAS token against the authorization rules and
// returns the rights it grants.
func (b *InMemoryBackend) claimRights(token string) ([]string, bool) {
	if len(token) > maxSASLen {
		return nil, false
	}

	fields := parseSAS(token)

	expiry, err := strconv.ParseInt(fields["se"], 10, 64)
	if err != nil || b.now().Unix() > expiry {
		return nil, false
	}

	sig, err := url.QueryUnescape(fields["sig"])
	if err != nil || fields["sr"] == "" {
		return nil, false
	}

	name := fields["skn"]
	if name == "" {
		name = DefaultKeyName
	}

	return b.matchRule(name, fields["sr"], expiry, sig)
}

func (r *rule) signs(resource string, expiry int64, sig string) bool {
	for _, key := range []string{r.PrimaryKey, r.SecondaryKey} {
		if hmac.Equal([]byte(SignSAS(resource, expiry, key)), []byte(sig)) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) matchRule(name, resource string, expiry int64, sig string) ([]string, bool) {
	b.mu.RLock("matchRule")
	defer b.mu.RUnlock()

	for _, ns := range b.namespaces {
		for _, r := range ns.Rules {
			if r.Name == name && r.signs(resource, expiry, sig) {
				return append([]string(nil), r.Rights...), true
			}
		}
	}

	if name == DefaultKeyName && hmac.Equal([]byte(SignSAS(resource, expiry, DefaultKeyValue)), []byte(sig)) {
		return []string{RightListen, RightSend, RightManage}, true
	}

	return nil, false
}

// PlainAuthorized reports whether a SASL PLAIN user/password pair is a rule key.
func (b *InMemoryBackend) PlainAuthorized(user, password string) bool {
	b.mu.RLock("PlainAuthorized")
	defer b.mu.RUnlock()

	for _, ns := range b.namespaces {
		for _, r := range ns.Rules {
			if r.Name == user && (hmac.Equal([]byte(r.PrimaryKey), []byte(password)) ||
				hmac.Equal([]byte(r.SecondaryKey), []byte(password))) {
				return true
			}
		}
	}

	return user == DefaultKeyName && hmac.Equal([]byte(DefaultKeyValue), []byte(password))
}

func hasRight(rights []string, want string) bool {
	for _, r := range rights {
		if r == want || r == RightManage {
			return true
		}
	}

	return false
}
