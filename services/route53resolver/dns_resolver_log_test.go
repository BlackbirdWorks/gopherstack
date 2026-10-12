package route53resolver_test

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

const queryLogGroup = "/r53/queries"

func (e *dnsEnv) logConfig(t *testing.T, destARN, vpc string) {
	t.Helper()

	ctx := context.Background()
	cfg, err := e.b.CreateResolverQueryLogConfig(ctx, "qlc", "", destARN)
	require.NoError(t, err)

	_, err = e.b.AssociateResolverQueryLogConfig(ctx, cfg.ID, vpc)
	require.NoError(t, err)
}

func (e *dnsEnv) loggedMessages(t *testing.T, vpc string) []string {
	t.Helper()

	events, _, _, err := e.logs.GetLogEvents(context.Background(), queryLogGroup, vpc, nil, nil, 0, "", true)
	if err != nil {
		return nil
	}

	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Message)
	}

	return out
}

func TestDNSResolverQueryLogging(t *testing.T) {
	t.Parallel()

	logARN := "arn:aws:logs:us-east-1:000000000000:log-group:" + queryLogGroup + ":*"

	tests := []struct {
		setup        func(t *testing.T, e *dnsEnv)
		name         string
		src          string
		wantFirewall string
		wantEvents   int
	}{
		{
			name: "logged_to_cloudwatch",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.logConfig(t, logARN, vpc1)
			},
			src: vpc1Client, wantEvents: 1,
		},
		{
			name: "firewall_action_recorded",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.logConfig(t, logARN, vpc1)
				e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "NXDOMAIN", domains: []string{"q.example"},
					priority: 100, groupPriority: 100,
				})
			},
			src: vpc1Client, wantEvents: 1, wantFirewall: "BLOCK",
		},
		{
			name: "other_vpc_not_logged",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.logConfig(t, logARN, vpc2)
			},
			src: vpc1Client, wantEvents: 0,
		},
		{
			name: "unassociated_not_logged",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				_, err := e.b.CreateResolverQueryLogConfig(context.Background(), "qlc", "", logARN)
				require.NoError(t, err)
			},
			src: vpc1Client, wantEvents: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDNSEnv(t)
			_, err := e.logs.CreateLogGroup(context.Background(), queryLogGroup, "", "")
			require.NoError(t, err)

			tt.setup(t, e)
			e.ask(tt.src, "q.example", dns.TypeA)

			msgs := e.loggedMessages(t, vpc1)
			require.Len(t, msgs, tt.wantEvents)

			if tt.wantEvents == 0 {
				return
			}

			var rec map[string]any
			require.NoError(t, json.Unmarshal([]byte(msgs[0]), &rec))
			assert.Equal(t, vpc1, rec["vpc_id"])
			assert.Equal(t, "q.example.", rec["query_name"])
			assert.Equal(t, "A", rec["query_type"])
			assert.Equal(t, "000000000000", rec["account_id"])
			assert.Equal(t, vpc1Client, rec["srcaddr"])

			if tt.wantFirewall != "" {
				assert.Equal(t, tt.wantFirewall, rec["firewall_rule_action"])
				assert.Equal(t, "NXDOMAIN", rec["rcode"])
			}
		})
	}
}

func TestDNSResolverEC2VPCLocator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     string
		wantFwd bool
	}{
		{name: "subnet_cidr", src: "10.0.1.77", wantFwd: true},
		{name: "outside_vpc", src: "10.8.1.77"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ec2 := ec2backend.NewInMemoryBackend(testAccount, testRegion)
			vpc, err := ec2.CreateVpc("10.0.0.0/16", "")
			require.NoError(t, err)

			_, err = ec2.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
			require.NoError(t, err)

			b := route53resolver.NewInMemoryBackend(testAccount, testRegion)
			b.SetAppConfig(ec2Config{h: ec2backend.NewHandler(ec2)})

			up := startUpstream(t, "192.0.2.10")
			rule, err := b.CreateResolverRule(context.Background(), "r", "example.com", "FORWARD", "", "", "",
				[]route53resolver.TargetIP{up.target()})
			require.NoError(t, err)

			_, err = b.AssociateResolverRule(context.Background(), rule.ID, vpc.ID, "a")
			require.NoError(t, err)

			req := new(dns.Msg)
			req.SetQuestion("www.example.com.", dns.TypeA)

			resp := b.NewDNSResolver(nil).
				Intercept(&net.UDPAddr{IP: net.ParseIP(tt.src), Port: 1}, req, func() *dns.Msg {
					return localAnswer(req)
				})

			if tt.wantFwd {
				require.NotNil(t, resp)
				assert.Equal(t, []string{"192.0.2.10"}, answerIPs(resp))
			} else {
				assert.Nil(t, resp)
			}
		})
	}
}
