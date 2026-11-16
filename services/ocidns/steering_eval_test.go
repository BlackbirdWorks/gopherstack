package ocidns_test

import (
	"testing"

	"github.com/miekg/dns"
	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func filterRule(cond string) ocidnssdk.SteeringPolicyFilterRule {
	return ocidnssdk.SteeringPolicyFilterRule{
		DefaultAnswerData: []ocidnssdk.SteeringPolicyFilterAnswerData{
			{AnswerCondition: new(cond), ShouldKeep: new(true)},
		},
	}
}

func prioRule(cases ...ocidnssdk.SteeringPolicyPriorityRuleCase) ocidnssdk.SteeringPolicyPriorityRule {
	return ocidnssdk.SteeringPolicyPriorityRule{Cases: cases}
}

func prioData(pool string, v int) ocidnssdk.SteeringPolicyPriorityAnswerData {
	return ocidnssdk.SteeringPolicyPriorityAnswerData{
		AnswerCondition: new("answer.pool == '" + pool + "'"),
		Value:           new(v),
	}
}

func limitRule(n int) ocidnssdk.SteeringPolicyLimitRule {
	return ocidnssdk.SteeringPolicyLimitRule{DefaultCount: new(n)}
}

func TestSteeringPolicyEvaluation(t *testing.T) {
	t.Parallel()

	ans := []ocidnssdk.SteeringPolicyAnswer{
		{Name: new("a"), Rtype: new("A"), Rdata: new("192.0.2.1"), Pool: new("primary")},
		{Name: new("b"), Rtype: new("A"), Rdata: new("192.0.2.2"), Pool: new("backup")},
		{Name: new("c"), Rtype: new("A"), Rdata: new("192.0.2.3"), Pool: new("off"), IsDisabled: new(true)},
	}

	tests := []struct {
		name  string
		rules []ocidnssdk.SteeringPolicyRule
		want  []string
	}{
		{"failover", []ocidnssdk.SteeringPolicyRule{
			filterRule("answer.isDisabled != true"),
			prioRule(ocidnssdk.SteeringPolicyPriorityRuleCase{
				AnswerData: []ocidnssdk.SteeringPolicyPriorityAnswerData{prioData("backup", 2), prioData("primary", 1)},
			}),
			limitRule(1),
		}, []string{"192.0.2.1"}},
		{"filter drops disabled", []ocidnssdk.SteeringPolicyRule{
			filterRule("answer.isDisabled != true"),
			prioRule(ocidnssdk.SteeringPolicyPriorityRuleCase{
				AnswerData: []ocidnssdk.SteeringPolicyPriorityAnswerData{prioData("backup", 1), prioData("primary", 2)},
			}),
		}, []string{"192.0.2.2", "192.0.2.1"}},
		{"case by subnet", []ocidnssdk.SteeringPolicyRule{
			filterRule("answer.isDisabled != true"),
			prioRule(
				ocidnssdk.SteeringPolicyPriorityRuleCase{
					CaseCondition: new("query.client.address in (subnet '10.0.0.0/8')"),
					AnswerData: []ocidnssdk.SteeringPolicyPriorityAnswerData{
						prioData("primary", 1),
						prioData("backup", 2),
					},
				},
				ocidnssdk.SteeringPolicyPriorityRuleCase{
					CaseCondition: new("query.client.address in (subnet '127.0.0.0/8', subnet '::1/128')"),
					AnswerData: []ocidnssdk.SteeringPolicyPriorityAnswerData{
						prioData("backup", 1),
						prioData("primary", 2),
					},
				},
			),
			limitRule(1),
		}, []string{"192.0.2.2"}},
		{"no case matches", []ocidnssdk.SteeringPolicyRule{
			prioRule(ocidnssdk.SteeringPolicyPriorityRuleCase{CaseCondition: new("query.client.asn == 3")}),
			filterRule("answer.name == 'b'"),
		}, []string{"192.0.2.2"}},
		{"weighted zero never first", []ocidnssdk.SteeringPolicyRule{
			ocidnssdk.SteeringPolicyWeightedRule{DefaultAnswerData: []ocidnssdk.SteeringPolicyWeightedAnswerData{
				{AnswerCondition: new("answer.name == 'a'"), Value: new(0)},
				{AnswerCondition: new("answer.name == 'b'"), Value: new(5)},
				{AnswerCondition: new("answer.name == 'c'"), Value: new(0)},
			}},
			limitRule(1),
		}, []string{"192.0.2.2"}},
		{"filter removes all", []ocidnssdk.SteeringPolicyRule{filterRule("answer.name == 'zzz'")}, []string{}},
	}

	e := newEnv(t, envOpts{})

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			zone := "steer" + string(rune('a'+i)) + ".example"
			z := e.createZone(t, zone, nil)

			pol, err := e.client.CreateSteeringPolicy(t.Context(), ocidnssdk.CreateSteeringPolicyRequest{
				CompartmentId: new(testCompartment), DisplayName: new("p"),
				Template: ocidnssdk.CreateSteeringPolicyDetailsTemplateCustom,
				Answers:  ans, Rules: tt.rules, Ttl: new(45),
			})
			require.NoError(t, err)

			_, err = e.client.CreateSteeringPolicyAttachment(
				t.Context(),
				ocidnssdk.CreateSteeringPolicyAttachmentRequest{
					SteeringPolicyId: pol.Id, ZoneId: z.Id, DomainName: new("www." + zone),
				},
			)
			require.NoError(t, err)

			e.mustPatch(t, zone, add("www."+zone, "A", "198.51.100.9", 300))

			r := e.query(t, "udp", "www."+zone+".", dns.TypeA)

			got := make([]string, 0, len(r.Answer))
			for _, rr := range r.Answer {
				a, ok := rr.(*dns.A)
				require.True(t, ok)
				assert.Equal(t, uint32(45), a.Hdr.Ttl)

				got = append(got, a.A.String())
			}

			assert.Equal(t, tt.want, got)
			assert.Equal(t, dns.RcodeSuccess, r.Rcode)
		})
	}
}
