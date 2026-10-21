package ec2

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

type captureCompute struct {
	reqs []LaunchRequest
	mu   sync.Mutex
}

func (c *captureCompute) Launch(_ context.Context, req LaunchRequest) (LaunchResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reqs = append(c.reqs, req)

	return LaunchResult{ProviderID: "ctr-" + req.InstanceID}, nil
}

func (*captureCompute) Terminate(context.Context, string, string) error { return nil }
func (*captureCompute) Start(context.Context, string, string) error     { return nil }
func (*captureCompute) Stop(context.Context, string, string) error      { return nil }

func TestLaunchRequestAdmitFollowsInstanceGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enforce bool
	}{
		{"flag off publishes world ports only", false},
		{"flag on relays declared ports", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := NewInMemoryBackend("000000000000", "us-east-1")
			cc := &captureCompute{}
			bk.WithCompute(cc)

			h := NewHandler(bk)
			if tt.enforce {
				h.SetSecurityGroupEnforcer(newSourceEnforcer(h))
			}

			web, err := bk.CreateSecurityGroup("web", "web", "vpc-default")
			require.NoError(t, err)
			require.NoError(t, bk.AuthorizeSecurityGroupIngress(web.ID, []SecurityGroupRule{
				{Protocol: "tcp", IPRange: "203.0.113.0/24", FromPort: 8080, ToPort: 8080},
			}))
			other, err := bk.CreateSecurityGroup("other", "other", "vpc-default")
			require.NoError(t, err)

			runIn(t, h, "subnet-default", web.ID)
			require.Len(t, cc.reqs, 1)
			req := cc.reqs[0]

			if !tt.enforce {
				assert.Nil(t, req.Admit)
				assert.Empty(t, req.PublishPorts)

				return
			}

			require.NotNil(t, req.Admit)
			assert.Equal(t, []PortSpec{{Protocol: protoTCP, Port: 8080}}, req.PublishPorts)

			admit := req.Admit(protoTCP, 8080)
			loop := &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}
			assert.False(t, admit(loop))

			require.NoError(t, bk.AuthorizeSecurityGroupIngress(other.ID, []SecurityGroupRule{
				{Protocol: "tcp", IPRange: "127.0.0.1/32", FromPort: 8080, ToPort: 8080},
			}))
			assert.False(t, admit(loop), "other group is not attached yet")

			require.NoError(t, bk.SetInstanceSecurityGroups(instanceIDOf(t, bk), []string{web.ID, other.ID}))
			assert.True(t, admit(loop), "ModifyInstanceAttribute groups apply to the next connection")
		})
	}
}

func newSourceEnforcer(h *Handler) *sgfilter.Enforcer {
	enf := sgfilter.New()
	enf.SetSource(h.SecurityGroupSource())

	return enf
}

func instanceIDOf(t *testing.T, bk *InMemoryBackend) string {
	t.Helper()

	insts := bk.DescribeInstances(nil, "")
	require.Len(t, insts, 1)

	return insts[0].ID
}
