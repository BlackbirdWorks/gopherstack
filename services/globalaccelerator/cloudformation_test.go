package globalaccelerator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/globalaccelerator"
)

type fakeEndpoints map[string]bool

func (f fakeEndpoints) EndpointExists(_, id string) bool { return f[id] }

func cfnStack(t *testing.T, bk *globalaccelerator.InMemoryBackend) (string, string, string) {
	t.Helper()

	accARN, _, err := bk.CreateCFNResource(globalaccelerator.CFNAccelerator, map[string]any{
		"Name": "cfn-acc", "IpAddressType": "DUAL_STACK",
		"Tags": []any{map[string]any{"Key": "env", "Value": "t"}},
	})
	require.NoError(t, err)

	lisARN, _, err := bk.CreateCFNResource(globalaccelerator.CFNListener, map[string]any{
		"AcceleratorArn": accARN, "Protocol": "TCP",
		"PortRanges": []any{map[string]any{"FromPort": float64(80), "ToPort": "80"}},
	})
	require.NoError(t, err)

	grpARN, _, err := bk.CreateCFNResource(globalaccelerator.CFNEndpointGroup, map[string]any{
		"ListenerArn": lisARN, "EndpointGroupRegion": "us-west-2",
		"EndpointConfigurations": []any{map[string]any{"EndpointId": "i-0abc", "Weight": float64(50)}},
	})
	require.NoError(t, err)

	return accARN, lisARN, grpARN
}

func TestCFN_Lifecycle(t *testing.T) {
	t.Parallel()

	bk := globalaccelerator.NewInMemoryBackend("123456789012", "")
	accARN, lisARN, grpARN := cfnStack(t, bk)

	a, err := bk.GetAccelerator(accARN, false)
	require.NoError(t, err)
	assert.Equal(t, "cfn-acc", a.Name)
	assert.True(t, a.Enabled)
	assert.Len(t, a.IPv6, 2)

	g, err := bk.GetEndpointGroup(grpARN, false)
	require.NoError(t, err)
	assert.Equal(t, int32(50), g.Endpoints[0].Weight)

	require.NoError(t, bk.UpdateCFNResource(globalaccelerator.CFNAccelerator, accARN, map[string]any{
		"Name": "renamed", "IpAddressType": "DUAL_STACK",
	}))
	require.NoError(t, bk.UpdateCFNResource(globalaccelerator.CFNEndpointGroup, grpARN, map[string]any{
		"ListenerArn": lisARN, "EndpointGroupRegion": "us-west-2", "ThresholdCount": "5",
	}))

	a, _ = bk.GetAccelerator(accARN, false)
	assert.Equal(t, "renamed", a.Name)
	assert.Empty(t, a.Tags)

	g, _ = bk.GetEndpointGroup(grpARN, false)
	assert.Empty(t, g.Endpoints)
	assert.Equal(t, int32(5), g.ThresholdCount)

	for _, d := range []struct{ typ, arn string }{
		{globalaccelerator.CFNEndpointGroup, grpARN},
		{globalaccelerator.CFNListener, lisARN},
		{globalaccelerator.CFNAccelerator, accARN},
		{globalaccelerator.CFNAccelerator, accARN},
	} {
		require.NoError(t, bk.DeleteCFNResource(d.typ, d.arn))
	}

	_, err = bk.GetAccelerator(accARN, false)
	require.Error(t, err)
}

func TestCFN_Attributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props map[string]any
		attrs map[string]string
		name  string
		typ   string
	}{
		{
			name: "attachment", typ: globalaccelerator.CFNCrossAccountAttachment,
			props: map[string]any{"Name": "att", "Principals": []any{"111122223333"}},
			attrs: map[string]string{"AttachmentArn": ":attachment/"},
		},
		{
			name: "accelerator", typ: globalaccelerator.CFNAccelerator,
			props: map[string]any{"Name": "acc", "IpAddressType": "DUAL_STACK"},
			attrs: map[string]string{
				"AcceleratorArn":   ":accelerator/",
				"DnsName":          ".awsglobalaccelerator.com",
				"DualStackDnsName": ".dualstack.awsglobalaccelerator.com",
				"Ipv4Addresses":    "75.2.",
				"Ipv6Addresses":    "2600:",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := globalaccelerator.NewInMemoryBackend("123456789012", "")
			id, attrs, err := bk.CreateCFNResource(tt.typ, tt.props)
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(id, "arn:aws:globalaccelerator::123456789012:"))

			for k, want := range tt.attrs {
				assert.Contains(t, attrs[k], want, k)
			}
		})
	}
}

func TestEndpointExistenceChecks(t *testing.T) {
	t.Parallel()

	const alb = "arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/app/web/abc"

	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{name: "known alb", id: alb},
		{name: "unknown alb", id: strings.Replace(alb, "web", "gone", 1), wantErr: true},
		{name: "known instance", id: "i-0aaa"},
		{name: "unknown instance", id: "i-0bbb", wantErr: true},
		{name: "known eip", id: "eipalloc-0aaa"},
		{name: "unknown eip", id: "eipalloc-0bbb", wantErr: true},
		{name: "foreign arn unchecked", id: "arn:aws:ec2:us-west-2:999999999999:thing/x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := globalaccelerator.NewInMemoryBackend("123456789012", "")
			bk.SetEndpointResolver(fakeEndpoints{alb: true, "i-0aaa": true, "eipalloc-0aaa": true})

			acc, err := bk.CreateAccelerator(globalaccelerator.AcceleratorInput{Name: "chk", Enabled: true})
			require.NoError(t, err)

			lis, err := bk.CreateListener(acc.ARN, globalaccelerator.ListenerInput{
				Protocol: "TCP", PortRanges: []globalaccelerator.PortRange{{FromPort: 80, ToPort: 80}},
			})
			require.NoError(t, err)

			lisARN := lis.ARN
			cfg := []globalaccelerator.EndpointConfig{{EndpointID: tt.id}}

			grp, err := bk.CreateEndpointGroup(lisARN, globalaccelerator.EndpointGroupInput{Region: "eu-west-1"})
			require.NoError(t, err)

			_, _, err = bk.AddEndpoints(grp.ARN, cfg)
			if tt.wantErr {
				require.ErrorIs(t, err, globalaccelerator.ErrInvalidArgument)

				_, err = bk.UpdateEndpointGroup(grp.ARN, globalaccelerator.EndpointGroupInput{Endpoints: cfg})
				require.ErrorIs(t, err, globalaccelerator.ErrInvalidArgument)

				_, err = bk.CreateEndpointGroup(
					lisARN, globalaccelerator.EndpointGroupInput{Region: "ap-south-1", Endpoints: cfg},
				)
				require.ErrorIs(t, err, globalaccelerator.ErrInvalidArgument)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestTaggedResources(t *testing.T) {
	t.Parallel()

	bk := globalaccelerator.NewInMemoryBackend("123456789012", "")
	accARN, _, _ := cfnStack(t, bk)

	_, _, err := bk.CreateCFNResource(globalaccelerator.CFNCrossAccountAttachment, map[string]any{"Name": "untagged"})
	require.NoError(t, err)

	got := bk.TaggedResources()
	require.Len(t, got, 1)
	assert.Equal(t, accARN, got[0].ARN)
	assert.Equal(t, map[string]string{"env": "t"}, got[0].Tags)
}
