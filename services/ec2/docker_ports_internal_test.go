package ec2

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	mobycontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorldOpenPorts(t *testing.T) {
	t.Parallel()

	rule := func(proto, v4, v6 string, from, to int) SecurityGroupRule {
		return SecurityGroupRule{Protocol: proto, IPRange: v4, IPv6Range: v6, FromPort: from, ToPort: to}
	}

	tests := []struct {
		name  string
		rules []SecurityGroupRule
		want  []PortSpec
	}{
		{
			name:  "tcp-world",
			rules: []SecurityGroupRule{rule("tcp", worldIPv4, "", 8080, 8080)},
			want:  []PortSpec{{"tcp", 8080}},
		},
		{
			name:  "udp-ipv6-world",
			rules: []SecurityGroupRule{rule("udp", "", worldIPv6, 53, 53)},
			want:  []PortSpec{{"udp", 53}},
		},
		{
			name:  "protocol-number",
			rules: []SecurityGroupRule{rule("6", worldIPv4, "", 80, 80)},
			want:  []PortSpec{{"tcp", 80}},
		},
		{name: "restricted-cidr", rules: []SecurityGroupRule{rule("tcp", "10.0.0.0/8", "", 8080, 8080)}},
		{name: "ssh-excluded", rules: []SecurityGroupRule{rule("tcp", worldIPv4, "", 22, 22)}},
		{name: "all-protocols", rules: []SecurityGroupRule{rule("-1", worldIPv4, "", 0, 0)}},
		{name: "icmp", rules: []SecurityGroupRule{rule("icmp", worldIPv4, "", -1, -1)}},
		{name: "huge-range-skipped", rules: []SecurityGroupRule{rule("tcp", worldIPv4, "", 1, 65535)}},
		{
			name:  "range-sorted-without-ssh",
			rules: []SecurityGroupRule{rule("tcp", worldIPv4, "", 21, 24), rule("tcp", worldIPv4, "", 23, 23)},
			want:  []PortSpec{{"tcp", 21}, {"tcp", 23}, {"tcp", 24}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, worldOpenPorts([]*SecurityGroup{{IngressRules: tt.rules}}))
		})
	}
}

func TestDockerComputePublishesPorts(t *testing.T) {
	t.Parallel()

	api := newFakeNetDocker()
	dc := newDockerComputeWithAPI(api, DockerComputeConfig{})

	res, err := dc.Launch(t.Context(), LaunchRequest{
		InstanceID: "i-1", PublishPorts: []PortSpec{{"tcp", 8080}, {"udp", 53}, {"tcp", 22}},
	})
	require.NoError(t, err)

	created := api.created[0]
	tcp := mobynetwork.MustParsePort("8080/tcp")
	assert.Contains(t, created.Config.ExposedPorts, tcp)
	require.Len(t, created.HostConfig.PortBindings[tcp], 1)
	assert.Equal(t, "127.0.0.1", created.HostConfig.PortBindings[tcp][0].HostIP.String())
	assert.Empty(t, created.HostConfig.PortBindings[tcp][0].HostPort)
	assert.Empty(t, created.HostConfig.PortBindings[containerSSHPort])

	require.Len(t, res.PublishedPorts, 2)
	assert.Equal(t, "udp", res.PublishedPorts[0].Protocol)
	assert.Equal(t, 53, res.PublishedPorts[0].ContainerPort)
	assert.Equal(t, 8080, res.PublishedPorts[1].ContainerPort)
	assert.Equal(t, 40001, res.PublishedPorts[0].HostPort)
	assert.Equal(t, 40002, res.PublishedPorts[1].HostPort)
	assert.Equal(t, "127.0.0.1", res.PublishedPorts[1].HostIP)

	got, err := dc.PublishedPorts(t.Context(), "ctr-1")
	require.NoError(t, err)
	assert.Equal(t, res.PublishedPorts, got)
}

func TestPublishedPortsFromIgnoresSSH(t *testing.T) {
	t.Parallel()

	pm := mobycontainer.InspectResponse{NetworkSettings: &mobycontainer.NetworkSettings{
		Ports: mobynetwork.PortMap{containerSSHPort: {{HostPort: "2222"}}},
	}}

	assert.Empty(t, publishedOf(pm))
}

func newVPCDockerHandler(t *testing.T) (*Handler, *fakeNetDockerAPI, *InMemoryBackend) {
	t.Helper()

	api := newFakeNetDocker()
	bk := NewInMemoryBackend("000000000000", "us-east-1")
	bk.WithCompute(newDockerComputeWithAPI(api, DockerComputeConfig{}))

	return NewHandler(bk), api, bk
}

func runIn(t *testing.T, h *Handler, subnetID string, sgIDs ...string) {
	t.Helper()

	img, err := h.Backend.RegisterImage("plain", "", "")
	require.NoError(t, err)

	vals := map[string][]string{
		"ImageId": {img.ImageID}, "InstanceType": {"t3.micro"}, "MinCount": {"1"}, "SubnetId": {subnetID},
	}
	if len(sgIDs) > 0 {
		vals["SecurityGroupId.1"] = sgIDs
	}

	_, err = h.handleRunInstances(vals, "req")
	require.NoError(t, err)
}

func TestHandlerVPCNetworkLifecycle(t *testing.T) {
	t.Parallel()

	h, api, bk := newVPCDockerHandler(t)

	vpc, err := bk.CreateVpc("10.20.0.0/16", "")
	require.NoError(t, err)

	sub, err := bk.CreateSubnet(vpc.ID, "10.20.1.0/24", "us-east-1a")
	require.NoError(t, err)

	runIn(t, h, sub.ID)

	require.Len(t, api.networks, 1)
	assert.Equal(t, vpcNetworkName(vpc.ID), api.networks[0].name)
	assert.Equal(t, "10.20.0.0/16", api.networks[0].opts.IPAM.Config[0].Subnet.String())

	insts := bk.DescribeInstances(nil, "")
	require.Len(t, insts, 1)

	ep := api.created[0].NetworkingConfig.EndpointsConfig[vpcNetworkName(vpc.ID)]
	require.NotNil(t, ep.IPAMConfig)
	assert.Equal(t, "10.20.1.4", ep.IPAMConfig.IPv4Address.String())
	assert.Equal(t, "10.20.1.4", insts[0].PrivateIP)

	_, err = bk.TerminateInstances([]string{insts[0].ID})
	require.NoError(t, err)
	require.NoError(t, bk.DeleteSubnet(sub.ID))

	_, err = h.handleDeleteVpc(map[string][]string{"VpcId": {vpc.ID}}, "req")
	require.NoError(t, err)
	assert.Empty(t, api.networks)
	assert.Equal(t, []string{vpcNetworkName(vpc.ID)}, api.removedNames)
}

func TestHandlerSecurityGroupPortPublishing(t *testing.T) {
	t.Parallel()

	h, api, bk := newVPCDockerHandler(t)

	vpc, err := bk.CreateVpc("10.30.0.0/16", "")
	require.NoError(t, err)

	sub, err := bk.CreateSubnet(vpc.ID, "10.30.1.0/24", "us-east-1a")
	require.NoError(t, err)

	sg, err := bk.CreateSecurityGroup("web", "web", vpc.ID)
	require.NoError(t, err)

	open := SecurityGroupRule{Protocol: "tcp", IPRange: worldIPv4, FromPort: 8080, ToPort: 8080}
	require.NoError(t, bk.AuthorizeSecurityGroupIngress(sg.ID, []SecurityGroupRule{open}))

	runIn(t, h, sub.ID, sg.ID)

	tcp := mobynetwork.MustParsePort("8080/tcp")
	require.Len(t, api.created[0].HostConfig.PortBindings[tcp], 1)

	insts := bk.DescribeInstances(nil, "")
	require.Len(t, insts, 1)

	tags := bk.DescribeTags([]string{insts[0].ID})
	found := map[string]string{}
	for _, tg := range tags {
		found[tg.Key] = tg.Value
	}

	assert.Contains(t, found[tagKeyPorts], "8080/tcp=127.0.0.1:")

	_, _, err = bk.RevokeSecurityGroupIngress(sg.ID, []SecurityGroupRule{open})
	require.NoError(t, err)

	runIn(t, h, sub.ID, sg.ID)
	assert.Empty(t, api.created[1].HostConfig.PortBindings[tcp])
}

func TestServePublishedPorts(t *testing.T) {
	t.Parallel()

	h, _, bk := newVPCDockerHandler(t)

	vpc, err := bk.CreateVpc("10.40.0.0/16", "")
	require.NoError(t, err)

	sub, err := bk.CreateSubnet(vpc.ID, "10.40.1.0/24", "us-east-1a")
	require.NoError(t, err)

	sg, err := bk.CreateSecurityGroup("web", "web", vpc.ID)
	require.NoError(t, err)
	require.NoError(t, bk.AuthorizeSecurityGroupIngress(sg.ID, []SecurityGroupRule{
		{Protocol: "tcp", IPRange: worldIPv4, FromPort: 8080, ToPort: 8080},
	}))

	runIn(t, h, sub.ID, sg.ID)

	insts := bk.DescribeInstances(nil, "")
	require.Len(t, insts, 1)

	tests := []struct {
		name       string
		instance   string
		wantStatus int
		wantPorts  int
	}{
		{name: "mapped", instance: insts[0].ID, wantStatus: http.StatusOK, wantPorts: 1},
		{name: "unknown", instance: "i-missing", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)
			c.SetPath("/_gopherstack/ec2/instances/:instance/ports")
			c.SetPathValues(echo.PathValues{{Name: "instance", Value: tt.instance}})

			require.NoError(t, h.ServePublishedPorts(c))
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var body publishedPortsResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.instance, body.InstanceID)
			require.Len(t, body.Ports, tt.wantPorts)
			assert.Equal(t, 8080, body.Ports[0].ContainerPort)
		})
	}
}
