package azureeventgrid

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ErrUnauthorized is returned when validation is on and no credential matches.
var ErrUnauthorized = errors.New("azureeventgrid: unauthorized")

const (
	hdrSASKey     = "Aeg-Sas-Key"
	hdrSAS        = "Aeg-Sas-Token"
	maxTokenBytes = 4096
	sasTimeLayout = "1/2/2006 3:04:05 PM"
)

type credential struct {
	key   string
	token string
}

type holder struct {
	name   string
	key1   string
	key2   string
	domain bool
}

func credentialOf(r *http.Request) credential {
	c := credential{key: r.Header.Get(hdrSASKey), token: r.Header.Get(hdrSAS)}

	auth := r.Header.Get("Authorization")

	switch {
	case strings.HasPrefix(auth, "SharedAccessKey "):
		c.key = strings.TrimPrefix(auth, "SharedAccessKey ")
	case strings.HasPrefix(auth, "SharedAccessSignature "):
		c.token = strings.TrimPrefix(auth, "SharedAccessSignature ")
	case strings.HasPrefix(auth, "Bearer "):
		if v := strings.TrimPrefix(auth, "Bearer "); strings.Contains(v, "&s=") {
			c.token = v
		}
	}

	return c
}

// SignToken builds an Event Grid SAS token for resource, valid until expiry.
func SignToken(resource string, expiry time.Time, key string) string {
	unsigned := "r=" + url.QueryEscape(resource) + "&e=" + url.QueryEscape(expiry.UTC().Format(sasTimeLayout))

	return unsigned + "&s=" + url.QueryEscape(sign(unsigned, key))
}

func sign(unsigned, key string) string {
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		raw = []byte(key)
	}

	mac := hmac.New(sha256.New, raw)
	mac.Write([]byte(unsigned))

	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func parseExpiry(s string) (time.Time, bool) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(n, 0), true
	}

	for _, layout := range []string{sasTimeLayout, time.RFC3339Nano, "01/02/2006 15:04:05", "1/2/2006 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// verifyToken checks a SAS token's signature against either key and its expiry.
func verifyToken(token string, h holder, now time.Time) bool {
	if len(token) > maxTokenBytes {
		return false
	}

	parts := map[string]string{}

	for pair := range strings.SplitSeq(token, "&") {
		if k, v, ok := strings.Cut(pair, "="); ok {
			parts[k] = v
		}
	}

	rawR, rawE, rawS := parts["r"], parts["e"], parts["s"]
	if rawR == "" || rawE == "" || rawS == "" {
		return false
	}

	e, err := url.QueryUnescape(rawE)
	if err != nil {
		return false
	}

	exp, ok := parseExpiry(e)
	if !ok || now.After(exp) {
		return false
	}

	sig, err := url.QueryUnescape(rawS)
	if err != nil {
		return false
	}

	unsigned := "r=" + rawR + "&e=" + rawE

	for _, k := range []string{h.key1, h.key2} {
		if k != "" && hmac.Equal([]byte(sign(unsigned, k)), []byte(sig)) {
			return true
		}
	}

	return false
}

func (h holder) accepts(c credential, now time.Time) bool {
	if keyEqual(h.key1, c.key) || keyEqual(h.key2, c.key) {
		return true
	}

	return c.token != "" && verifyToken(c.token, h, now)
}

// pick selects the holder (topic or namespace) a request addresses: the one named
// by the host label, else the one whose key or token matches, else the only one.
// With validate on, the credential must match.
func pick(holders []holder, hostLabel string, c credential, validate bool, now time.Time) (string, error) {
	h, err := pickHolder(holders, hostLabel, c, validate, now)

	return h.name, err
}

func pickHolder(holders []holder, hostLabel string, c credential, validate bool, now time.Time) (holder, error) {
	if len(holders) == 0 {
		return holder{}, ErrTopicNotFound
	}

	sort.Slice(holders, func(i, j int) bool { return holders[i].name < holders[j].name })

	for _, h := range holders {
		if hostLabel != "" && strings.EqualFold(h.name, hostLabel) {
			if validate && !h.accepts(c, now) {
				return holder{}, ErrUnauthorized
			}

			return h, nil
		}
	}

	for _, h := range holders {
		if h.accepts(c, now) {
			return h, nil
		}
	}

	switch {
	case validate:
		return holder{}, ErrUnauthorized
	case len(holders) == 1:
		return holders[0], nil
	}

	return holder{}, ErrAmbiguousTopic
}

// ResolveTopic picks the custom topic a publish request is for.
func (b *InMemoryBackend) ResolveTopic(hostLabel string, r *http.Request, validate bool) (string, error) {
	b.mu.RLock("ResolveTopic")
	defer b.mu.RUnlock()

	holders := make([]holder, 0, len(b.topics))
	for _, t := range b.topics {
		holders = append(holders, holder{name: t.Name, key1: t.Key1, key2: t.Key2})
	}

	return pick(holders, hostLabel, credentialOf(r), validate, b.now())
}

// Publisher is the custom topic or domain a publish request addresses.
type Publisher struct {
	Name   string
	Domain bool
}

// ResolvePublisher picks the custom topic or domain a publish request is for.
func (b *InMemoryBackend) ResolvePublisher(hostLabel string, r *http.Request, validate bool) (Publisher, error) {
	b.mu.RLock("ResolvePublisher")
	defer b.mu.RUnlock()

	holders := make([]holder, 0, len(b.topics)+len(b.domains))
	for _, t := range b.topics {
		holders = append(holders, holder{name: t.Name, key1: t.Key1, key2: t.Key2})
	}

	for _, d := range b.domains {
		holders = append(holders, holder{name: d.Name, key1: d.Key1, key2: d.Key2, domain: true})
	}

	h, err := pickHolder(holders, hostLabel, credentialOf(r), validate, b.now())

	return Publisher{Name: h.name, Domain: h.domain}, err
}

// ResolveNamespace picks the namespace that holds topicName for a namespace data-plane request.
func (b *InMemoryBackend) ResolveNamespace(
	hostLabel, topicName string,
	r *http.Request,
	validate bool,
) (string, error) {
	b.mu.RLock("ResolveNamespace")
	defer b.mu.RUnlock()

	holders := make([]holder, 0, len(b.namespaces))

	for _, ns := range b.namespaces {
		if _, ok := ns.Topics[topicName]; ok {
			holders = append(holders, holder{name: ns.Name, key1: ns.Key1, key2: ns.Key2})
		}
	}

	name, err := pick(holders, hostLabel, credentialOf(r), validate, b.now())
	if errors.Is(err, ErrTopicNotFound) {
		return "", ErrNamespaceTopicNotFound
	}

	return name, err
}
