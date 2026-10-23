package ses_test

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/textproto"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errNXDomain = errors.New("nxdomain")

type fakeTXT map[string][]string

func (f fakeTXT) LookupTXT(_ context.Context, name string) ([]string, error) {
	if v, ok := f[name]; ok {
		return v, nil
	}

	return nil, errNXDomain
}

var wspRun = regexp.MustCompile(`[ \t]+`)

func relaxedHdr(name, value string) string {
	return strings.ToLower(name) + ":" + strings.TrimSpace(wspRun.ReplaceAllString(value, " ")) + "\r\n"
}

func relaxedBody(body string) string {
	lines := strings.Split(body, "\r\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(wspRun.ReplaceAllString(l, " "), " ")
	}

	return strings.TrimRight(strings.Join(lines, "\r\n"), "\r\n") + "\r\n"
}

type dkimSigner struct {
	rsaKey *rsa.PrivateKey
	dom    string
	edKey  ed25519.PrivateKey
}

func (s dkimSigner) sign(t *testing.T, hdrs [][2]string, body string) string {
	t.Helper()

	alg := "rsa-sha256"
	if s.edKey != nil {
		alg = "ed25519-sha256"
	}

	sum := sha256.Sum256([]byte(relaxedBody(body)))
	names := make([]string, 0, len(hdrs))
	signed := ""

	var signedSb67 strings.Builder
	for _, h := range hdrs {
		names = append(names, h[0])
		signedSb67.WriteString(relaxedHdr(h[0], h[1]))
	}
	signed += signedSb67.String()

	val := fmt.Sprintf("v=1; a=%s; c=relaxed/relaxed; d=%s; s=sel; h=%s; bh=%s; b=",
		alg, s.dom, strings.Join(names, ":"), base64.StdEncoding.EncodeToString(sum[:]))
	hash := sha256.Sum256([]byte(signed + strings.TrimSuffix(relaxedHdr("DKIM-Signature", val), "\r\n")))

	var sig []byte

	var err error
	if s.edKey != nil {
		sig = ed25519.Sign(s.edKey, hash[:])
	} else {
		sig, err = rsa.SignPKCS1v15(rand.Reader, s.rsaKey, crypto.SHA256, hash[:])
		require.NoError(t, err)
	}

	out := "DKIM-Signature: " + val + base64.StdEncoding.EncodeToString(sig) + "\r\n"
	var outSb87 strings.Builder
	for _, h := range hdrs {
		outSb87.WriteString(h[0] + ": " + h[1] + "\r\n")
	}
	out += outSb87.String()

	return out + "\r\n" + body
}

func TestInboundAuthVerdicts(t *testing.T) {
	t.Parallel()

	rk, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	der, err := x509.MarshalPKIXPublicKey(&rk.PublicKey)
	require.NoError(t, err)

	edPub, edPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	rsaTXT := "v=DKIM1; k=rsa; p=" + base64.StdEncoding.EncodeToString(der)
	edTXT := "v=DKIM1; k=ed25519; p=" + base64.StdEncoding.EncodeToString(edPub)
	spfOK := []string{"v=spf1 ip4:127.0.0.0/8 -all"}
	spfBad := []string{"v=spf1 ip4:10.0.0.0/8 -all"}

	tests := []struct {
		dns        fakeTXT
		signer     *dkimSigner
		name       string
		tamper     string
		wantSPF    string
		wantDKIM   string
		wantDMARC  string
		wantPolicy string
		wantData   int
	}{
		{name: "no dns data", dns: fakeTXT{}, wantSPF: "GRAY", wantDKIM: "GRAY", wantDMARC: "GRAY"},
		{
			name: "spf pass", dns: fakeTXT{"sender.test": spfOK},
			wantSPF: "PASS", wantDKIM: "GRAY", wantDMARC: "GRAY",
		},
		{
			name: "spf fail", dns: fakeTXT{"sender.test": spfBad},
			wantSPF: "FAIL", wantDKIM: "GRAY", wantDMARC: "GRAY",
		},
		{
			name: "spf include and redirect",
			dns: fakeTXT{
				"sender.test": {"v=spf1 redirect=r.test"},
				"r.test":      {"v=spf1 include:inc.test -all"},
				"inc.test":    spfOK,
			},
			wantSPF: "PASS", wantDKIM: "GRAY", wantDMARC: "GRAY",
		},
		{
			name:    "dkim rsa pass",
			dns:     fakeTXT{"sel._domainkey.sender.test": {rsaTXT}},
			signer:  &dkimSigner{rsaKey: rk, dom: "sender.test"},
			wantSPF: "GRAY", wantDKIM: "PASS", wantDMARC: "GRAY",
		},
		{
			name:    "dkim ed25519 pass",
			dns:     fakeTXT{"sel._domainkey.sender.test": {edTXT}},
			signer:  &dkimSigner{edKey: edPriv, dom: "sender.test"},
			wantSPF: "GRAY", wantDKIM: "PASS", wantDMARC: "GRAY",
		},
		{
			name:   "dkim tampered body",
			dns:    fakeTXT{"sel._domainkey.sender.test": {rsaTXT}},
			signer: &dkimSigner{rsaKey: rk, dom: "sender.test"},
			tamper: "body line", wantSPF: "GRAY", wantDKIM: "FAIL", wantDMARC: "GRAY",
		},
		{
			name:    "dkim key missing",
			dns:     fakeTXT{},
			signer:  &dkimSigner{rsaKey: rk, dom: "sender.test"},
			wantSPF: "GRAY", wantDKIM: "FAIL", wantDMARC: "GRAY",
		},
		{
			name: "dmarc pass via spf",
			dns: fakeTXT{
				"sender.test": spfOK, "_dmarc.sender.test": {"v=DMARC1; p=reject"},
			},
			wantSPF: "PASS", wantDKIM: "GRAY", wantDMARC: "PASS", wantPolicy: "reject",
		},
		{
			name: "dmarc pass via dkim",
			dns: fakeTXT{
				"sender.test": spfBad, "sel._domainkey.sender.test": {rsaTXT},
				"_dmarc.sender.test": {"v=DMARC1; p=reject"},
			},
			signer:  &dkimSigner{rsaKey: rk, dom: "sender.test"},
			wantSPF: "FAIL", wantDKIM: "PASS", wantDMARC: "PASS", wantPolicy: "reject",
		},
		{
			name: "dmarc fail policy none delivers",
			dns: fakeTXT{
				"sender.test": spfBad, "_dmarc.sender.test": {"v=DMARC1; p=none"},
			},
			wantSPF: "FAIL", wantDKIM: "GRAY", wantDMARC: "FAIL", wantPolicy: "none",
		},
		{
			name: "dmarc fail policy reject refuses",
			dns: fakeTXT{
				"sender.test": spfBad, "_dmarc.sender.test": {"v=DMARC1; p=reject"},
			},
			wantData: 550,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, snsF, b := inboundHarness(t, "arn:t")
			b.SetInboundTXTResolver(tc.dns)

			msg := inboundBody
			if tc.signer != nil {
				msg = tc.signer.sign(t, [][2]string{
					{"From", "Sender <sender@sender.test>"}, {"To", "user@example.com"}, {"Subject", "Hello"},
				}, "body line\r\n")
			}

			if tc.tamper != "" {
				msg = strings.Replace(msg, tc.tamper, "evil line", 1)
			}

			serr := sendPlain(h.InboundAddr(), "sender@sender.test", "user@example.com", msg)
			if tc.wantData != 0 {
				var te *textproto.Error
				require.ErrorAs(t, serr, &te)
				assert.Equal(t, tc.wantData, te.Code)
				assert.Empty(t, snsF.msgs["arn:t"])

				return
			}

			require.NoError(t, serr)

			snsF.mu.Lock()
			defer snsF.mu.Unlock()
			require.Len(t, snsF.msgs["arn:t"], 1)

			var n struct {
				Receipt struct {
					SPF struct {
						Status string `json:"status"`
					} `json:"spfVerdict"`
					DKIM struct {
						Status string `json:"status"`
					} `json:"dkimVerdict"`
					DMARC struct {
						Status string `json:"status"`
					} `json:"dmarcVerdict"`

					Policy string `json:"dmarcPolicy"`
				} `json:"receipt"`
			}

			require.NoError(t, json.Unmarshal([]byte(snsF.msgs["arn:t"][0]), &n))
			assert.Equal(t, tc.wantSPF, n.Receipt.SPF.Status)
			assert.Equal(t, tc.wantDKIM, n.Receipt.DKIM.Status)
			assert.Equal(t, tc.wantDMARC, n.Receipt.DMARC.Status)
			assert.Equal(t, tc.wantPolicy, n.Receipt.Policy)
		})
	}
}
