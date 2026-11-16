package ses_test

import (
	"encoding/json/v2"
	"net/smtp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/ses"
)

func deliverWithHelo(t *testing.T, dns ses.InboundTXTResolver, helo, mailFrom string) string {
	t.Helper()

	h, snsF, b := inboundHarness(t, "arn:t")
	b.SetInboundTXTResolver(dns)

	c, err := smtp.Dial(h.InboundAddr())
	require.NoError(t, err)

	defer func() { _ = c.Close() }()

	require.NoError(t, c.Hello(helo))
	require.NoError(t, c.Mail(mailFrom))
	require.NoError(t, c.Rcpt("user@example.com"))

	w, err := c.Data()
	require.NoError(t, err)
	_, err = w.Write([]byte(inboundBody))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	snsF.mu.Lock()
	defer snsF.mu.Unlock()
	require.Len(t, snsF.msgs["arn:t"], 1)

	var n struct {
		Receipt struct {
			SPF struct {
				Status string `json:"status"`
			} `json:"spfVerdict"`
		} `json:"receipt"`
	}

	require.NoError(t, json.Unmarshal([]byte(snsF.msgs["arn:t"][0]), &n, jsonx.V1()))

	return n.Receipt.SPF.Status
}

func TestInboundSPFUsesHELOIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dns      fakeDNS
		mailFrom string
		want     string
	}{
		{
			name:     "null_sender_checks_helo_domain",
			dns:      fakeDNS{txt: map[string][]string{"mta.helo.test": {"v=spf1 ip4:127.0.0.1 -all"}}},
			mailFrom: "",
			want:     "PASS",
		},
		{
			name:     "null_sender_helo_fail",
			dns:      fakeDNS{txt: map[string][]string{"mta.helo.test": {"v=spf1 ip4:10.0.0.1 -all"}}},
			mailFrom: "",
			want:     "FAIL",
		},
		{
			name: "h_macro_is_helo_name",
			dns: fakeDNS{
				txt:   map[string][]string{"sender.test": {"v=spf1 exists:%{h}.chk.test -all"}},
				addrs: map[string][]string{"mta.helo.test.chk.test": {"127.0.0.2"}},
			},
			mailFrom: "bounce@sender.test",
			want:     "PASS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, deliverWithHelo(t, tt.dns, "mta.helo.test", tt.mailFrom))
		})
	}
}
