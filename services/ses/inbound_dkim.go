package ses

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

var dkimBTag = regexp.MustCompile(`([;\s]b=)[^;]*`)

type rawHeader struct {
	name string
	raw  string
}

// splitMessage returns the raw header fields (folds kept) and the body.
func splitMessage(data []byte) ([]rawHeader, []byte) {
	head, body, found := bytes.Cut(data, []byte("\r\n\r\n"))
	if !found {
		head, body = bytes.TrimSuffix(data, []byte("\r\n")), nil
	}

	var out []rawHeader

	for line := range strings.SplitSeq(string(head), "\r\n") {
		if line != "" && (line[0] == ' ' || line[0] == '\t') && len(out) > 0 {
			out[len(out)-1].raw += "\r\n" + line

			continue
		}

		if name, _, ok := strings.Cut(line, ":"); ok {
			out = append(out, rawHeader{name: strings.TrimSpace(name), raw: line})
		}
	}

	return out, body
}

func collapseWSP(s string) string {
	return strings.Join(strings.FieldsFunc(s, func(r rune) bool { return r == ' ' || r == '\t' }), " ")
}

func relaxHeader(raw string) string {
	name, val, _ := strings.Cut(raw, ":")
	val = collapseWSP(strings.ReplaceAll(val, "\r\n", ""))

	return strs.Fold(strings.TrimSpace(name)) + ":" + val
}

func canonBody(body []byte, relaxed bool) []byte {
	lines := strings.Split(string(body), "\r\n")

	for i, l := range lines {
		if relaxed {
			lines[i] = collapseWSP(l)
		}
	}

	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		if relaxed {
			return nil
		}

		return []byte("\r\n")
	}

	return []byte(strings.Join(lines, "\r\n") + "\r\n")
}

// dkimSigned builds the header hash input for one signature.
func dkimSigned(hdrs []rawHeader, sig rawHeader, tags map[string]string, relaxed bool) []byte {
	used := make([]bool, len(hdrs))

	var buf bytes.Buffer

	for want := range strings.SplitSeq(tags["h"], ":") {
		want = strings.TrimSpace(want)

		for i := len(hdrs) - 1; i >= 0; i-- {
			if used[i] || !strs.Equal(hdrs[i].name, want) {
				continue
			}

			used[i] = true

			writeCanonHeader(&buf, hdrs[i].raw, relaxed, true)

			break
		}
	}

	stripped := dkimBTag.ReplaceAllString(sig.raw, "${1}")

	writeCanonHeader(&buf, stripped, relaxed, false)

	return buf.Bytes()
}

func writeCanonHeader(buf *bytes.Buffer, raw string, relaxed, crlf bool) {
	if relaxed {
		buf.WriteString(relaxHeader(raw))
	} else {
		buf.WriteString(raw)
	}

	if crlf {
		buf.WriteString("\r\n")
	}
}

func decodeB64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.Join(strings.Fields(s), ""))
}

// verifyDKIMSig verifies one DKIM-Signature; it reports the signing domain on success.
func verifyDKIMSig(
	ctx context.Context,
	res InboundTXTResolver,
	hdrs []rawHeader,
	body []byte,
	sig rawHeader,
) (string, bool) {
	_, val, _ := strings.Cut(sig.raw, ":")
	tags := parseTags(strings.ReplaceAll(val, "\r\n", ""))
	headerC, bodyC, _ := strings.Cut(strs.Fold(tags["c"]), "/")

	if tags["v"] != "1" || tags["d"] == "" || tags["s"] == "" || tags["h"] == "" {
		return "", false
	}

	cb := canonBody(body, bodyC == "relaxed")
	if l, err := strconv.Atoi(tags["l"]); err == nil && l >= 0 && l < len(cb) {
		cb = cb[:l]
	}

	sum := sha256.Sum256(cb)
	if want, err := decodeB64(tags["bh"]); err != nil || !bytes.Equal(want, sum[:]) {
		return "", false
	}

	sigBytes, err := decodeB64(tags["b"])
	if err != nil {
		return "", false
	}

	hash := sha256.Sum256(dkimSigned(hdrs, sig, tags, headerC == "relaxed"))
	if !verifyKey(ctx, res, tags, hash[:], sigBytes) {
		return "", false
	}

	return strs.Fold(tags["d"]), true
}

func verifyKey(ctx context.Context, res InboundTXTResolver, tags map[string]string, hash, sig []byte) bool {
	recs := txtRecords(ctx, res, tags["s"]+"._domainkey."+tags["d"], "")
	if len(recs) == 0 {
		return false
	}

	kt := parseTags(strings.Join(recs, ""))

	pub, err := decodeB64(kt["p"])
	if err != nil || len(pub) == 0 {
		return false
	}

	switch strs.Fold(tags["a"]) {
	case "rsa-sha256":
		pk, perr := x509.ParsePKIXPublicKey(pub)
		rk, ok := pk.(*rsa.PublicKey)

		return perr == nil && ok && rsa.VerifyPKCS1v15(rk, crypto.SHA256, hash, sig) == nil
	case "ed25519-sha256":
		return len(pub) == ed25519.PublicKeySize && ed25519.Verify(pub, hash, sig)
	}

	return false
}

// checkDKIM verifies every DKIM-Signature header and returns the verdict plus the passing signing domains.
func checkDKIM(ctx context.Context, res InboundTXTResolver, data []byte) (string, []string) {
	hdrs, body := splitMessage(data)

	var (
		domains []string
		seen    bool
	)

	for _, h := range hdrs {
		if !strs.Equal(h.name, "dkim-signature") {
			continue
		}

		seen = true

		if d, ok := verifyDKIMSig(ctx, res, hdrs, body, h); ok {
			domains = append(domains, d)
		}
	}

	switch {
	case !seen:
		return verdictGray, nil
	case len(domains) == 0:
		return verdictFail, nil
	default:
		return verdictPass, domains
	}
}
