package ec2_test

import (
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestSecurityGroupSourceLiveRules(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	sg, err := b.CreateSecurityGroup("db", "db", "vpc-default")
	require.NoError(t, err)

	enf := sgfilter.New()
	enf.SetSource(h.SecurityGroupSource())

	loop := &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}
	ids := []string{sg.ID}

	assert.False(t, enf.AdmitIngress(ids, "tcp", 5432, loop), "default ingress is deny")

	rule := ec2.SecurityGroupRule{Protocol: "tcp", IPRange: "127.0.0.1/32", FromPort: 5432, ToPort: 5432}
	require.NoError(t, b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{rule}))
	assert.True(t, enf.AdmitIngress(ids, "tcp", 5432, loop))
	assert.False(t, enf.AdmitIngress(ids, "tcp", 3306, loop))

	_, _, err = b.RevokeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{rule})
	require.NoError(t, err)
	assert.False(t, enf.AdmitIngress(ids, "tcp", 5432, loop))
}

func TestSecurityGroupSourceReferenceAndEgress(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	src := h.SecurityGroupSource()

	app, err := b.CreateSecurityGroup("app", "app", "vpc-default")
	require.NoError(t, err)

	db, err := b.CreateSecurityGroup("db", "db", "vpc-default")
	require.NoError(t, err)

	require.NoError(t, b.AuthorizeSecurityGroupIngress(db.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", SourceGroupID: app.ID, FromPort: 5432, ToPort: 5432},
	}))

	groups := src.SecurityGroups([]string{db.ID, "sg-missing"})
	require.Len(t, groups, 1)
	assert.Equal(t, []string{app.ID}, groups[0].Ingress[0].Groups)
	assert.True(t, sgfilter.AllowEgress(
		src.SecurityGroups([]string{db.ID}),
		sgfilter.Flow{
			Protocol: "tcp",
			Port:     443,
			Peer:     sgfilter.Peer{Addr: addr("8.8.8.8")},
		},
	), "default egress is allow-all")
}

func addr(s string) netip.Addr { return netip.MustParseAddr(s) }

func TestDefaultSecurityGroupSelfReference(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	enf := sgfilter.New()
	enf.SetSource(ec2.NewHandler(b).SecurityGroupSource())

	member, err := b.CreateNetworkInterfaceWithOptions("subnet-default", ec2.NetworkInterfaceOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"sg-default"}, member.SecurityGroupIDs, "no groups means the VPC default group")

	custom, err := b.CreateSecurityGroup("custom", "custom", "vpc-default")
	require.NoError(t, err)

	other, err := b.CreateNetworkInterfaceWithOptions("subnet-default",
		ec2.NetworkInterfaceOptions{SecurityGroupIDs: []string{custom.ID}})
	require.NoError(t, err)

	fromMember := &net.TCPAddr{IP: net.ParseIP(member.PrivateIP)}
	fromOther := &net.TCPAddr{IP: net.ParseIP(other.PrivateIP)}
	fromOutside := &net.TCPAddr{IP: net.ParseIP("203.0.113.9")}

	tests := []struct {
		remote net.Addr
		name   string
		ids    []string
		want   bool
	}{
		{name: "default group admits its own members", ids: []string{"sg-default"}, remote: fromMember, want: true},
		{name: "default group refuses other groups", ids: []string{"sg-default"}, remote: fromOther, want: false},
		{name: "default group refuses outsiders", ids: []string{"sg-default"}, remote: fromOutside, want: false},
		{name: "no groups behaves as the default group", ids: nil, remote: fromMember, want: true},
		{name: "no groups refuses outsiders", ids: nil, remote: fromOutside, want: false},
		{name: "custom group has no implicit self rule", ids: []string{custom.ID}, remote: fromOther, want: false},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.want, enf.AdmitIngress(tt.ids, "tcp", 5432, tt.remote), tt.name)
	}
}
