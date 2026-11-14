package azureeventhubs

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
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

// sasClaim is a validated SAS token: the hub its audience names ("" for the
// whole namespace) and the rights of the signing rule.
type sasClaim struct {
	Audience string
	Rights   []string
}

// audienceHub returns the entity named by a SAS audience URL
// (sb://host/<hub>[/...]); "" addresses the namespace.
func audienceHub(resource string) (string, bool) {
	raw, err := url.QueryUnescape(resource)
	if err != nil {
		return "", false
	}

	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", false
	}

	hub, _, _ := strings.Cut(strings.Trim(u.Path, "/"), "/")

	return hub, true
}

// audienceCovers reports whether a claim for audience (a hub, or "" for the
// namespace) authorizes access to entity.
func audienceCovers(audience, entity string) bool {
	return audience == "" || strs.Equal(audience, entity)
}

// claimRights validates a SAS token against the authorization rules; a
// hub-scoped rule only signs tokens whose audience is that hub.
func (b *InMemoryBackend) claimRights(token string) (sasClaim, bool) {
	if len(token) > maxSASLen {
		return sasClaim{}, false
	}

	fields := parseSAS(token)

	expiry, err := strconv.ParseInt(fields["se"], 10, 64)
	if err != nil || b.now().Unix() > expiry {
		return sasClaim{}, false
	}

	sig, err := url.QueryUnescape(fields["sig"])
	if err != nil {
		return sasClaim{}, false
	}

	hub, ok := audienceHub(fields["sr"])
	if !ok {
		return sasClaim{}, false
	}

	name := fields["skn"]
	if name == "" {
		name = DefaultKeyName
	}

	rights, ok := b.matchRule(name, hub, fields["sr"], expiry, sig)
	if !ok {
		return sasClaim{}, false
	}

	return sasClaim{Audience: hub, Rights: rights}, true
}

func (r *rule) signs(resource string, expiry int64, sig string) bool {
	for _, key := range []string{r.PrimaryKey, r.SecondaryKey} {
		if hmac.Equal([]byte(SignSAS(resource, expiry, key)), []byte(sig)) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) matchRule(name, hub, resource string, expiry int64, sig string) ([]string, bool) {
	b.mu.RLock("matchRule")
	defer b.mu.RUnlock()

	for _, ns := range b.namespaces {
		for _, r := range ns.Rules {
			if r.Name == name && audienceCovers(r.Hub, hub) && r.signs(resource, expiry, sig) {
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
