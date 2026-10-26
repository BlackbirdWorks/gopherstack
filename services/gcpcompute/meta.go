package gcpcompute

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"strings"
	"time"
)

const (
	apiBase     = "https://www.googleapis.com/compute/v1/"
	timeLayout  = "2006-01-02T15:04:05.000-07:00"
	idFloor     = 1_000_000_000_000_000_000
	idSpan      = 8_000_000_000_000_000_000
	fpBytes     = 8
	scopeGlobal = "global"
	scopeRegion = "regions"
	scopeZone   = "zones"
)

func newID() uint64 {
	var buf [8]byte

	_, _ = rand.Read(buf[:])

	return idFloor + binary.BigEndian.Uint64(buf[:])%idSpan
}

func (b *InMemoryBackend) stamp() string { return b.now().UTC().Format(timeLayout) }

func fingerprint(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))

	return base64.URLEncoding.EncodeToString(sum[:fpBytes])
}

func fingerprintOf(v any) string {
	data, _ := json.Marshal(v)

	return fingerprint(string(data))
}

func newFingerprint() string {
	var buf [fpBytes]byte

	_, _ = rand.Read(buf[:])

	return base64.URLEncoding.EncodeToString(buf[:])
}

func relPath(project, scopeKind, loc, collection, name string) string {
	var sb strings.Builder

	sb.WriteString("projects/")
	sb.WriteString(project)

	switch scopeKind {
	case scopeGlobal:
		sb.WriteString("/global")
	case scopeRegion, scopeZone:
		sb.WriteString("/" + scopeKind + "/" + loc)
	}

	sb.WriteString("/" + collection)

	if name != "" {
		sb.WriteString("/" + name)
	}

	return sb.String()
}

func link(project, scopeKind, loc, collection, name string) string {
	return apiBase + relPath(project, scopeKind, loc, collection, name)
}

func scopePath(project, scope, loc string) string {
	return "projects/" + project + "/" + scope + "/" + loc
}

func scopeURL(project, scope, loc string) string {
	return apiBase + "projects/" + project + "/" + scope + "/" + loc
}

func globalLink(project, collection, name string) string {
	return link(project, scopeGlobal, "", collection, name)
}

func zoneLink(project, zone, collection, name string) string {
	return link(project, scopeZone, zone, collection, name)
}

func regionLink(project, region, collection, name string) string {
	return link(project, scopeRegion, region, collection, name)
}

func clone[T any](v *T) *T {
	if v == nil {
		return nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		return v
	}

	out := new(T)
	if err = json.Unmarshal(data, out); err != nil {
		return v
	}

	return out
}

func cloneAll[T any](in []*T) []*T {
	out := make([]*T, len(in))
	for i, v := range in {
		out[i] = clone(v)
	}

	return out
}

func key(parts ...string) string { return strings.Join(parts, "/") }

func lastSegment(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}

	return s
}

func regionOfZone(zone string) string {
	if i := strings.LastIndexByte(zone, '-'); i > 0 {
		return zone[:i]
	}

	return zone
}

func since(now, t time.Time) time.Duration { return now.Sub(t) }

// mergePatch applies a JSON merge patch (RFC 7386) to dst.
func mergePatch(dst, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)

			continue
		}

		pm, isMap := v.(map[string]any)
		dm, dIsMap := dst[k].(map[string]any)

		if isMap && dIsMap {
			mergePatch(dm, pm)

			continue
		}

		dst[k] = v
	}
}

func fromDoc[T any](m map[string]any) (*T, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	out := new(T)
	if err = json.Unmarshal(data, out); err != nil {
		return nil, err
	}

	return out, nil
}
