package cloudfront

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // CloudFront signed URLs mandate RSA-SHA1
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func isSignedQueryKey(name string) bool {
	switch name {
	case "Expires", "Signature", "Key-Pair-Id", "Policy":
		return true
	}

	return false
}

const candidateURLCount = 6

type signedParams struct {
	keyPairID string
	signature string
	policy    string
	expires   string
}

func signedFromRequest(r *http.Request) (signedParams, bool) {
	q := r.URL.Query()
	p := signedParams{
		keyPairID: q.Get("Key-Pair-Id"), signature: q.Get("Signature"),
		policy: q.Get("Policy"), expires: q.Get("Expires"),
	}

	if p.keyPairID != "" {
		return p, true
	}

	get := func(n string) string {
		if c, err := r.Cookie(n); err == nil {
			return c.Value
		}

		return ""
	}

	p = signedParams{
		keyPairID: get("CloudFront-Key-Pair-Id"), signature: get("CloudFront-Signature"),
		policy: get("CloudFront-Policy"), expires: get("CloudFront-Expires"),
	}

	return p, p.keyPairID != ""
}

func cfBase64Decode(s string) ([]byte, error) {
	r := strings.NewReplacer("-", "+", "_", "=", "~", "/")

	return base64.StdEncoding.DecodeString(r.Replace(s))
}

func (h *Handler) trustedKey(beh *dpBehavior, keyPairID string) *rsa.PublicKey {
	for _, gid := range beh.KeyGroups.Groups {
		kg, err := h.Backend.GetKeyGroup(gid)
		if err != nil {
			continue
		}

		for _, item := range kg.Items {
			if item != keyPairID {
				continue
			}

			pk, pkErr := h.Backend.GetPublicKey(item)
			if pkErr != nil {
				return nil
			}

			return parseRSAPublicKey(pk.EncodedKey)
		}
	}

	return nil
}

func parseRSAPublicKey(encoded string) *rsa.PublicKey {
	block, _ := pem.Decode([]byte(encoded))
	if block == nil {
		return nil
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil
	}

	k, _ := pub.(*rsa.PublicKey)

	return k
}

func strippedQuery(raw string) string {
	var keep []string

	for part := range strings.SplitSeq(raw, "&") {
		name, _, _ := strings.Cut(part, "=")
		if part == "" || isSignedQueryKey(name) {
			continue
		}

		keep = append(keep, part)
	}

	return strings.Join(keep, "&")
}

func candidateURLs(v *viewerReq, d *Distribution) []string {
	out := make([]string, 0, candidateURLCount)

	q := strippedQuery(v.r.URL.RawQuery)
	suffix := v.rawPath

	if q != "" {
		suffix += "?" + q
	}

	for _, scheme := range []string{v.scheme, schemeHTTPS, schemeHTTP} {
		for _, host := range []string{v.r.Host, d.DomainName} {
			out = append(out, scheme+"://"+host+suffix)
		}
	}

	return out
}

type epochCondition struct {
	Epoch int64 `json:"AWS:EpochTime"`
}

type sourceIPCondition struct {
	Source string `json:"AWS:SourceIp"`
}

type signedCondition struct {
	IPAddress       sourceIPCondition `json:"IpAddress"`
	DateLessThan    epochCondition    `json:"DateLessThan"`
	DateGreaterThan epochCondition    `json:"DateGreaterThan"`
}

type signedStatement struct {
	Resource  string          `json:"Resource"`
	Condition signedCondition `json:"Condition"`
}

type signedPolicy struct {
	Statement []signedStatement `json:"Statement"`
}

func (h *Handler) verifySigned(v *viewerReq, d *Distribution, beh *dpBehavior) bool {
	p, ok := signedFromRequest(v.r)
	if !ok || p.signature == "" {
		return false
	}

	key := h.trustedKey(beh, p.keyPairID)
	if key == nil {
		return false
	}

	sig, err := cfBase64Decode(p.signature)
	if err != nil {
		return false
	}

	if p.policy != "" {
		return verifyCustomPolicy(v, d, key, p.policy, sig)
	}

	exp, err := strconv.ParseInt(p.expires, 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return false
	}

	for _, u := range candidateURLs(v, d) {
		doc, mErr := cannedPolicyDoc(u, exp)
		if mErr != nil {
			continue
		}

		if rsaSHA1Verify(key, doc, sig) {
			return true
		}
	}

	return false
}

type cannedPolicy struct {
	Statement []cannedStatement `json:"Statement"`
}

type cannedStatement struct {
	Resource  string          `json:"Resource"`
	Condition cannedCondition `json:"Condition"`
}

type cannedCondition struct {
	DateLessThan cannedEpoch `json:"DateLessThan"`
}

type cannedEpoch struct {
	EpochTime int64 `json:"AWS:EpochTime"`
}

// cannedPolicyDoc renders the compact canned policy CloudFront signs.
func cannedPolicyDoc(resource string, expires int64) ([]byte, error) {
	return json.Marshal(cannedPolicy{Statement: []cannedStatement{{
		Resource:  resource,
		Condition: cannedCondition{DateLessThan: cannedEpoch{EpochTime: expires}},
	}}})
}

func rsaSHA1Verify(key *rsa.PublicKey, doc, sig []byte) bool {
	sum := sha1.Sum(doc) //nolint:gosec // see import

	return rsa.VerifyPKCS1v15(key, crypto.SHA1, sum[:], sig) == nil
}

func verifyCustomPolicy(v *viewerReq, d *Distribution, key *rsa.PublicKey, policyB64 string, sig []byte) bool {
	doc, err := cfBase64Decode(policyB64)
	if err != nil || !rsaSHA1Verify(key, doc, sig) {
		return false
	}

	var pol signedPolicy
	if json.Unmarshal(doc, &pol) != nil || len(pol.Statement) == 0 {
		return false
	}

	st := pol.Statement[0]
	now := time.Now().Unix()

	if st.Condition.DateLessThan.Epoch == 0 || now > st.Condition.DateLessThan.Epoch ||
		now < st.Condition.DateGreaterThan.Epoch {
		return false
	}

	if src := st.Condition.IPAddress.Source; src != "" && !ipInCIDR(clientIP(v.r), src) {
		return false
	}

	for _, u := range candidateURLs(v, d) {
		if globMatchCS(st.Resource, u) {
			return true
		}
	}

	return false
}

func ipInCIDR(ipStr, cidr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	if !strings.Contains(cidr, "/") {
		return ip.Equal(net.ParseIP(cidr))
	}

	_, n, err := net.ParseCIDR(cidr)

	return err == nil && n.Contains(ip)
}
