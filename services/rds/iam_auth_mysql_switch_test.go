package rds //nolint:testpackage // white-box access to the proxy codec and fake engine.

import (
	"crypto/tls"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (p *myProxyFixture) rawTLSClient(t *testing.T, plugin string, auth []byte) (net.Conn, byte) {
	t.Helper()

	addr := net.JoinHostPort(p.inst.Endpoint, strconv.Itoa(p.inst.Port))

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))

	_, _, err = readMyPacket(conn)
	require.NoError(t, err)

	caps := uint32(
		myCapProtocol41 | myCapSecureConn | myCapPluginAuth | myCapConnectWithDB |
			myCapSSL | myCapDeprecateEOF | myCapLenencData,
	)
	require.NoError(t, writeMyPacket(conn, 1, buildMyResponse(caps, "", nil, "", "")[:32]))

	tc := tls.Client(conn, &tls.Config{RootCAs: p.pool, ServerName: p.inst.Endpoint, MinVersion: tls.VersionTLS12})
	require.NoError(t, tc.HandshakeContext(t.Context()))
	require.NoError(t, writeMyPacket(tc, 2, buildMyResponse(caps, "bob", auth, "appdb", plugin)))

	return tc, 2
}

func TestMySQLProxyAuthSwitch(t *testing.T) {
	t.Parallel()

	allow := allowPolicy("arn:aws:rds-db:us-east-1:" + authTestAccount + ":dbuser:mydb/bob")

	tests := []struct {
		name    string
		plugin  string
		badTok  bool
		wantErr bool
	}{
		{name: "native offered valid token", plugin: fakeMyNativePlugin},
		{name: "native offered bad token", plugin: fakeMyNativePlugin, badTok: true, wantErr: true},
		{name: "clear offered bad token", plugin: myPluginClear, badTok: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newMyProxyFixture(t, true, allow)
			token := p.token(t, time.Now().UTC())

			if tt.badTok {
				token = p.token(t, time.Now().UTC().Add(-time.Hour))
			}

			first := []byte("scrambled-not-a-token")
			if tt.plugin == myPluginClear {
				first = append([]byte(token), 0)
			}

			c, seq := p.rawTLSClient(t, tt.plugin, first)

			if tt.plugin != myPluginClear {
				s, body, err := readMyPacket(c)
				require.NoError(t, err)
				require.Equal(t, seq+1, s)
				require.Equal(t, byte(myAuthSwitchMarker), body[0])
				assert.Equal(t, myPluginClear+"\x00", string(body[1:]))
				require.NoError(t, writeMyPacket(c, s+1, append([]byte(token), 0)))
			}

			_, body, err := readMyPacket(c)
			require.NoError(t, err)

			if tt.wantErr {
				require.Equal(t, byte(myErrMarker), body[0])
				assert.Equal(t, []byte{0x15, 0x04}, body[1:3], "ER_ACCESS_DENIED_ERROR 1045")
				assert.Equal(t, "#28000", string(body[3:9]))

				return
			}

			assert.Equal(t, byte(0), body[0])
		})
	}
}

func TestMySQLProxyBadTokenErrorCode(t *testing.T) {
	t.Parallel()

	p := newMyProxyFixture(t, true, "")

	err := p.ping(t, "bob", p.token(t, time.Now().UTC()), true)
	require.Error(t, err)

	var me *mysql.MySQLError
	require.ErrorAs(t, err, &me)
	assert.Equal(t, uint16(myErrAccessDenied), me.Number)
	assert.Equal(t, "28000", string(me.SQLState[:]))
}
