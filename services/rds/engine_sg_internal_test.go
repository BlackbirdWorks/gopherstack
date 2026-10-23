package rds

import (
	"net"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

type groupSource struct {
	groups atomic.Pointer[map[string]sgfilter.Group]
}

func (s *groupSource) SecurityGroups(ids []string) []sgfilter.Group {
	var out []sgfilter.Group

	for _, id := range ids {
		if g, ok := (*s.groups.Load())[id]; ok {
			out = append(out, g)
		}
	}

	return out
}

func (*groupSource) GroupsOfAddr(netip.Addr) []string { return nil }

func openTo(port int, cidr string) sgfilter.Group {
	return sgfilter.Group{Ingress: []sgfilter.Rule{{
		Protocol: "tcp", FromPort: port, ToPort: port, CIDRs: []netip.Prefix{netip.MustParsePrefix(cidr)},
	}}}
}

func TestAdmitClientInstanceGroups(t *testing.T) {
	t.Parallel()

	loop := &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}
	groups := map[string]sgfilter.Group{
		"sg-open":   openTo(5432, "127.0.0.0/8"),
		"sg-closed": openTo(5432, "203.0.113.0/24"),
		"sg-port":   openTo(3306, "127.0.0.0/8"),
	}

	tests := []struct {
		name   string
		sgs    []string
		enable bool
		want   bool
	}{
		{"disabled admits", nil, false, true},
		{"open group", []string{"sg-open"}, true, true},
		{"foreign cidr", []string{"sg-closed"}, true, false},
		{"wrong port", []string{"sg-port"}, true, false},
		{"no groups", nil, true, false},
		{"second group admits", []string{"sg-closed", "sg-open"}, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1")
			_, err := b.CreateDBInstance("i1", "postgres", "db.t3.micro", "d", "u", "", 20,
				DBInstanceOptions{MasterUserPassword: "passw0rdx", VpcSecurityGroupIDs: tt.sgs})
			require.NoError(t, err)

			if tt.enable {
				src := &groupSource{}
				src.groups.Store(&groups)

				enf := sgfilter.New()
				enf.SetSource(src)
				b.SetSecurityGroupEnforcer(enf)
			}

			lb := &liveDB{key: unitInstancePrefix + "i1", resourceID: "i1", kind: enginePostgres}
			assert.Equal(t, tt.want, b.admitClient(lb, loop))
		})
	}
}

func TestAdmitClientFollowsGroupMembership(t *testing.T) {
	t.Parallel()

	groups := map[string]sgfilter.Group{"sg-open": openTo(5432, "127.0.0.0/8"), "sg-closed": openTo(5432, "10.0.0.0/8")}
	src := &groupSource{}
	src.groups.Store(&groups)

	enf := sgfilter.New()
	enf.SetSource(src)

	b := NewInMemoryBackend("123456789012", "us-east-1")
	b.SetSecurityGroupEnforcer(enf)

	_, err := b.CreateDBInstance("i1", "postgres", "db.t3.micro", "d", "u", "", 20,
		DBInstanceOptions{MasterUserPassword: "passw0rdx", VpcSecurityGroupIDs: []string{"sg-closed"}})
	require.NoError(t, err)

	lb := &liveDB{key: unitInstancePrefix + "i1", resourceID: "i1", kind: enginePostgres}
	loop := &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}
	require.False(t, b.admitClient(lb, loop))

	_, err = b.ModifyDBInstance(
		"i1",
		"",
		0,
		DBInstanceOptions{VpcSecurityGroupIDs: []string{"sg-open"}, ApplyImmediately: true},
	)
	require.NoError(t, err)
	assert.True(t, b.admitClient(lb, loop), "ModifyDBInstance VpcSecurityGroupIds applies to the next connection")

	lb.dbPort.Store(3306)
	assert.False(t, b.admitClient(lb, loop), "rules are matched against the database port, not the host port")
}

func TestAdmitClientClusterGroups(t *testing.T) {
	t.Parallel()

	groups := map[string]sgfilter.Group{"sg-open": openTo(5432, "127.0.0.0/8")}
	src := &groupSource{}
	src.groups.Store(&groups)

	enf := sgfilter.New()
	enf.SetSource(src)

	b := NewInMemoryBackend("123456789012", "us-east-1")
	b.SetSecurityGroupEnforcer(enf)

	_, err := b.CreateDBCluster("c1", "aurora-postgresql", "u", "d", "", 0, nil,
		DBClusterOptions{MasterUserPassword: "passw0rdx", VpcSecurityGroupIDs: []string{"sg-open"}})
	require.NoError(t, err)

	lb := &liveDB{key: unitClusterPrefix + "c1", resourceID: "c1", kind: enginePostgres}
	assert.True(t, b.admitClient(lb, &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}))
	assert.False(t, b.admitClient(lb, &net.TCPAddr{IP: net.ParseIP("192.0.2.1")}))
}
