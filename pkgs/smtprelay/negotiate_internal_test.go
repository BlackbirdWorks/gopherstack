package smtprelay

import (
	"net"
	"net/smtp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/smtprelay/smtptest"
)

func TestNegotiateRefusesPlaintextCredentialsToRemoteHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hostname string
		wantErr  bool
	}{
		{name: "remote host refused", hostname: "mail.example.com", wantErr: true},
		{name: "localhost allowed", hostname: "localhost"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := smtptest.Start(t, smtptest.Options{AdvertiseAuth: true})
			conn, err := net.Dial("tcp", srv.Addr())
			require.NoError(t, err)

			c, err := smtp.NewClient(conn, tt.hostname)
			require.NoError(t, err)

			r := &Relay{cfg: Config{User: "u", Pass: "p"}, hostname: tt.hostname}
			err = r.negotiate(c)
			_ = c.Close()

			sess := <-srv.Sessions()

			if tt.wantErr {
				require.ErrorIs(t, err, ErrPlaintextCredentials)
				assert.Empty(t, sess.AuthLines)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, sess.AuthLines)
		})
	}
}
