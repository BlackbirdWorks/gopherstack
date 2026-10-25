package dns_test

import (
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
)

func TestTransparentSkipPatterns(t *testing.T) {
	t.Parallel()

	upstream, upAddr := startWithConfig(t, gopherDNS.Config{ResolveIP: "127.0.0.1"})
	for _, n := range []string{"ecr.us-east-1.amazonaws.com", "lambda.eu-west-1.amazonaws.com", "sqs.us-east-1.amazonaws.com"} {
		upstream.RegisterRecord(n, "A", []string{"9.9.9.9"})
	}

	_, addr := startWithConfig(t, gopherDNS.Config{
		ResolveIP: "10.1.2.3",
		Transparent: gopherDNS.TransparentConfig{
			Suffixes:     gopherDNS.DefaultTransparentSuffixes(),
			SkipPatterns: []string{`.*(ecr|lambda)\..*\.amazonaws\.com`, " "},
			Upstreams:    []string{upAddr},
		},
	})

	tests := []struct {
		name   string
		qname  string
		wantIP string
	}{
		{"ecr upstream", "ecr.us-east-1.amazonaws.com.", "9.9.9.9"},
		{"lambda upstream", "lambda.eu-west-1.amazonaws.com.", "9.9.9.9"},
		{"upper case upstream", "ECR.us-east-1.amazonaws.com.", "9.9.9.9"},
		{"sqs stays local", "sqs.us-east-1.amazonaws.com.", "10.1.2.3"},
		{"partial match stays local", "ecr-x.us-east-1.amazonaws.com.", "10.1.2.3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := new(dns.Msg)
			m.SetQuestion(tt.qname, dns.TypeA)

			resp, _, err := (&dns.Client{}).Exchange(m, addr)
			require.NoError(t, err)
			require.Len(t, resp.Answer, 1)

			a, ok := resp.Answer[0].(*dns.A)
			require.True(t, ok)
			assert.Equal(t, tt.wantIP, a.A.String())
		})
	}
}

func TestTransparentSkipPatternInvalid(t *testing.T) {
	t.Parallel()

	_, err := gopherDNS.New(gopherDNS.Config{
		Transparent: gopherDNS.TransparentConfig{
			Suffixes:     gopherDNS.DefaultTransparentSuffixes(),
			SkipPatterns: []string{"(unclosed"},
		},
	})
	require.Error(t, err)
}
