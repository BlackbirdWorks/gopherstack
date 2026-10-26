package ses_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

var errTempFail = errors.New("servfail")

// fakeDNS serves TXT, A/AAAA, MX and PTR data; names listed in failing answer with a temporary error.
type fakeDNS struct {
	txt     map[string][]string
	addrs   map[string][]string
	mx      map[string][]string
	ptr     map[string][]string
	failing map[string]bool
}

func (f fakeDNS) LookupTXT(_ context.Context, name string) ([]string, error) {
	if v, ok := f.txt[name]; ok {
		return v, nil
	}

	return nil, errNXDomain
}

func (f fakeDNS) LookupIP(_ context.Context, name string) ([]netip.Addr, error) {
	if f.failing[name] {
		return nil, errTempFail
	}

	v, ok := f.addrs[name]
	if !ok {
		return nil, ses.ErrDNSNotFound
	}

	out := make([]netip.Addr, 0, len(v))
	for _, s := range v {
		out = append(out, netip.MustParseAddr(s))
	}

	return out, nil
}

func (f fakeDNS) LookupMX(_ context.Context, name string) ([]string, error) {
	if v, ok := f.mx[name]; ok {
		return v, nil
	}

	return nil, ses.ErrDNSNotFound
}

func (f fakeDNS) LookupAddr(_ context.Context, ip netip.Addr) ([]string, error) {
	if v, ok := f.ptr[ip.String()]; ok {
		return v, nil
	}

	return nil, ses.ErrDNSNotFound
}

type verdictSet struct{ spf, dmarc string }

// deliver sends one message from the loopback client and returns the recorded verdicts.
func deliver(t *testing.T, dns ses.InboundTXTResolver, mailFrom, msg string) verdictSet {
	t.Helper()

	h, snsF, b := inboundHarness(t, "arn:t")
	b.SetInboundTXTResolver(dns)

	require.NoError(t, sendPlain(h.InboundAddr(), mailFrom, "user@example.com", msg))

	snsF.mu.Lock()
	defer snsF.mu.Unlock()
	require.Len(t, snsF.msgs["arn:t"], 1)

	var n struct {
		Receipt struct {
			SPF struct {
				Status string `json:"status"`
			} `json:"spfVerdict"`
			DMARC struct {
				Status string `json:"status"`
			} `json:"dmarcVerdict"`
		} `json:"receipt"`
	}

	require.NoError(t, json.Unmarshal([]byte(snsF.msgs["arn:t"][0]), &n))

	return verdictSet{spf: n.Receipt.SPF.Status, dmarc: n.Receipt.DMARC.Status}
}

func TestInboundSPFMechanisms(t *testing.T) {
	t.Parallel()

	spf := func(rec string) map[string][]string { return map[string][]string{"sender.test": {rec}} }
	loop := []string{"127.0.0.1"}

	chain := func(n int) map[string][]string {
		txt := spf("v=spf1 include:i1.test -all")
		for i := 1; i <= n; i++ {
			next := "-all"
			if i < n {
				next = fmt.Sprintf("include:i%d.test -all", i+1)
			}

			rec := "v=spf1 " + next
			if i == n {
				rec = "v=spf1 ip4:127.0.0.1 -all"
			}

			txt[fmt.Sprintf("i%d.test", i)] = []string{rec}
		}

		return txt
	}

	tests := []struct {
		name string
		dns  fakeDNS
		want string
	}{
		{"a_match", fakeDNS{txt: spf("v=spf1 a -all"), addrs: map[string][]string{"sender.test": loop}}, "PASS"},
		{
			"a_other_ip",
			fakeDNS{txt: spf("v=spf1 a -all"), addrs: map[string][]string{"sender.test": {"10.0.0.1"}}},
			"FAIL",
		},
		{"a_domain_arg", fakeDNS{txt: spf("v=spf1 a:h.test -all"), addrs: map[string][]string{"h.test": loop}}, "PASS"},
		{
			"a_cidr",
			fakeDNS{txt: spf("v=spf1 a/8 -all"), addrs: map[string][]string{"sender.test": {"127.9.9.9"}}},
			"PASS",
		},
		{
			"a_domain_and_cidr",
			fakeDNS{txt: spf("v=spf1 a:h.test/8 -all"), addrs: map[string][]string{"h.test": {"127.5.5.5"}}},
			"PASS",
		},
		{
			"a_dual_cidr",
			fakeDNS{txt: spf("v=spf1 a/8//64 -all"), addrs: map[string][]string{"sender.test": {"127.2.2.2"}}},
			"PASS",
		},
		{
			"a_only_v6_record",
			fakeDNS{txt: spf("v=spf1 a -all"), addrs: map[string][]string{"sender.test": {"2001:db8::1"}}},
			"FAIL",
		},
		{
			"mx_match",
			fakeDNS{
				txt: spf("v=spf1 mx -all"), mx: map[string][]string{"sender.test": {"mail.sender.test."}},
				addrs: map[string][]string{"mail.sender.test": loop},
			}, "PASS",
		},
		{
			"mx_cidr",
			fakeDNS{
				txt: spf("v=spf1 mx/8 -all"), mx: map[string][]string{"sender.test": {"mail.sender.test"}},
				addrs: map[string][]string{"mail.sender.test": {"127.1.1.1"}},
			}, "PASS",
		},
		{"mx_none", fakeDNS{txt: spf("v=spf1 mx -all")}, "FAIL"},
		{
			"mx_too_many_names",
			fakeDNS{
				txt: spf("v=spf1 mx -all"),
				mx: map[string][]string{"sender.test": {
					"1.t", "2.t", "3.t", "4.t", "5.t", "6.t", "7.t", "8.t", "9.t", "10.t", "11.t",
				}},
			}, "PROCESSING_FAILED",
		},
		{
			"exists_macro",
			fakeDNS{
				txt:   spf("v=spf1 exists:%{i}.rbl.sender.test -all"),
				addrs: map[string][]string{"127.0.0.1.rbl.sender.test": {"127.0.0.2"}},
			},
			"PASS",
		},
		{
			"exists_reversed_macro",
			fakeDNS{
				txt:   spf("v=spf1 exists:%{ir}.rbl.test -all"),
				addrs: map[string][]string{"1.0.0.127.rbl.test": {"127.0.0.2"}},
			},
			"PASS",
		},
		{
			"exists_sender_macros",
			fakeDNS{
				txt:   spf("v=spf1 exists:%{l}.%{o}.z.test -all"),
				addrs: map[string][]string{"bounce.sender.test.z.test": {"127.0.0.2"}},
			},
			"PASS",
		},
		{"exists_missing", fakeDNS{txt: spf("v=spf1 exists:nope.test -all")}, "FAIL"},
		{
			"ptr_match",
			fakeDNS{
				txt: spf("v=spf1 ptr -all"), ptr: map[string][]string{"127.0.0.1": {"host.sender.test."}},
				addrs: map[string][]string{"host.sender.test": loop},
			}, "PASS",
		},
		{
			"ptr_wrong_domain",
			fakeDNS{
				txt: spf("v=spf1 ptr -all"), ptr: map[string][]string{"127.0.0.1": {"host.evil.test."}},
				addrs: map[string][]string{"host.evil.test": loop},
			}, "FAIL",
		},
		{
			"ptr_forward_mismatch",
			fakeDNS{
				txt: spf("v=spf1 ptr -all"), ptr: map[string][]string{"127.0.0.1": {"host.sender.test."}},
				addrs: map[string][]string{"host.sender.test": {"10.9.9.9"}},
			}, "FAIL",
		},
		{
			"ptr_domain_arg",
			fakeDNS{
				txt: spf("v=spf1 ptr:other.test -all"), ptr: map[string][]string{"127.0.0.1": {"x.other.test"}},
				addrs: map[string][]string{"x.other.test": loop},
			}, "PASS",
		},
		{"softfail_a", fakeDNS{txt: spf("v=spf1 ~a")}, "GRAY"},
		{"neutral_all", fakeDNS{txt: spf("v=spf1 ?all")}, "GRAY"},
		{"no_match_neutral", fakeDNS{txt: spf("v=spf1 ip4:10.0.0.1")}, "GRAY"},
		{"include_ten_lookups", fakeDNS{txt: chain(10)}, "PASS"},
		{"include_eleven_lookups", fakeDNS{txt: chain(11)}, "PROCESSING_FAILED"},
		{"include_no_record", fakeDNS{txt: spf("v=spf1 include:gone.test -all")}, "PROCESSING_FAILED"},
		{"include_not_pass_continues", fakeDNS{txt: map[string][]string{
			"sender.test": {"v=spf1 include:i.test ip4:127.0.0.1 -all"}, "i.test": {"v=spf1 -all"},
		}}, "PASS"},
		{"redirect_no_record", fakeDNS{txt: spf("v=spf1 redirect=gone.test")}, "PROCESSING_FAILED"},
		{"unknown_mechanism", fakeDNS{txt: spf("v=spf1 bogus:x -all")}, "PROCESSING_FAILED"},
		{"unknown_modifier_ignored", fakeDNS{txt: spf("v=spf1 foo=bar ip4:127.0.0.1 -all")}, "PASS"},
		{"void_lookups_two", fakeDNS{txt: spf("v=spf1 a:n1.test a:n2.test -all")}, "FAIL"},
		{"void_lookups_three", fakeDNS{txt: spf("v=spf1 a:n1.test a:n2.test a:n3.test -all")}, "PROCESSING_FAILED"},
		{
			"dns_temp_error",
			fakeDNS{txt: spf("v=spf1 a -all"), failing: map[string]bool{"sender.test": true}}, "PROCESSING_FAILED",
		},
		{"bad_macro", fakeDNS{txt: spf("v=spf1 exists:%{x}.test -all")}, "PROCESSING_FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := deliver(t, tt.dns, "bounce@sender.test", inboundBody)
			assert.Equal(t, tt.want, got.spf)
		})
	}
}

func TestInboundSPFWithTXTOnlyResolver(t *testing.T) {
	t.Parallel()

	dns := fakeTXT{"sender.test": {"v=spf1 a mx ptr exists:x.test ip4:127.0.0.1 -all"}}
	got := deliver(t, dns, "bounce@sender.test", inboundBody)
	assert.Equal(t, "PASS", got.spf, "address mechanisms are skipped, ip4 still matches")
}

func TestInboundDMARCOrganizationalDomain(t *testing.T) {
	t.Parallel()

	msg := func(fromDomain string) string {
		return "From: Sender <sender@" + fromDomain + ">\r\nTo: user@example.com\r\nSubject: Hello\r\n\r\nbody line\r\n"
	}

	tests := []struct {
		name       string
		fromHeader string
		mailFrom   string
		dmarcAt    string
		want       string
	}{
		{"co_uk_same_org", "mail.example.co.uk", "bounce@other.example.co.uk", "example.co.uk", "PASS"},
		{"co_uk_other_org", "a.example.co.uk", "bounce@b.other.co.uk", "example.co.uk", "FAIL"},
		{"private_suffix_distinct_orgs", "user1.github.io", "bounce@user2.github.io", "user1.github.io", "FAIL"},
		{"private_suffix_same_org", "x.user1.github.io", "bounce@y.user1.github.io", "user1.github.io", "PASS"},
		{"k12_suffix", "a.example.pvt.k12.ma.us", "bounce@b.example.pvt.k12.ma.us", "example.pvt.k12.ma.us", "PASS"},
		{
			"k12_suffix_other_school",
			"a.school1.pvt.k12.ma.us",
			"bounce@b.school2.pvt.k12.ma.us",
			"school1.pvt.k12.ma.us",
			"FAIL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			senderDomain := tt.mailFrom[strings.Index(tt.mailFrom, "@")+1:]
			dns := fakeTXT{
				senderDomain:           {"v=spf1 ip4:127.0.0.0/8 -all"},
				"_dmarc." + tt.dmarcAt: {"v=DMARC1; p=none"},
			}

			got := deliver(t, dns, tt.mailFrom, msg(tt.fromHeader))
			assert.Equal(t, "PASS", got.spf)
			assert.Equal(t, tt.want, got.dmarc)
		})
	}
}
