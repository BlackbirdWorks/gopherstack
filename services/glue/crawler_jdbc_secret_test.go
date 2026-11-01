package glue_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

const sslRequestLen = 8

var errSecretNotFound = errors.New("ResourceNotFoundException: secret not found")

type fakeSecrets map[string]string

func (f fakeSecrets) SecretString(_ context.Context, _, id string) (string, error) {
	v, ok := f[id]
	if !ok {
		return "", errSecretNotFound
	}

	return v, nil
}

// startupCapture accepts one connection and reports the first bytes the
// client sends, which for a PostgreSQL client include the user name.
func startupCapture(t *testing.T) (string, <-chan string) {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	got := make(chan string, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			buf := make([]byte, 512)
			n, _ := c.Read(buf)

			if n == sslRequestLen {
				_, _ = c.Write([]byte("N"))
				n, _ = c.Read(buf)
			}

			if n > 0 {
				select {
				case got <- string(buf[:n]):
				default:
				}
			}

			_ = c.Close()
		}
	}()

	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	return ln.Addr().String(), got
}

func TestCrawler_JDBCSecretID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		secrets   glue.CrawlerSecrets
		name      string
		secretID  string
		wantErr   string
		wantUser  string
		useServer bool
	}{
		{
			name: "credentials from secret", secretID: "db-creds", useServer: true, wantUser: "svc_user",
			secrets: fakeSecrets{"db-creds": `{"username":"svc_user","password":"pw"}`},
			wantErr: "connect to JDBC data store",
		},
		{
			name: "missing secret fails the crawl", secretID: "absent", secrets: fakeSecrets{},
			wantErr: `cannot resolve the connection's SECRET_ID "absent"`,
		},
		{
			name: "secret without username", secretID: "bad", secrets: fakeSecrets{"bad": `{"password":"x"}`},
			wantErr: "secret has no username and password",
		},
		{name: "secrets manager unavailable", secretID: "x", wantErr: "Secrets Manager is not available"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			if tt.secrets != nil {
				env.glue.SetSecretsBackend(tt.secrets)
			}

			addr, got := "127.0.0.1:1", (<-chan string)(nil)
			if tt.useServer {
				addr, got = startupCapture(t)
			}

			_, err := env.client.CreateConnection(t.Context(), &gluesdk.CreateConnectionInput{
				ConnectionInput: &gluetypes.ConnectionInput{
					Name: aws.String("conn"), ConnectionType: gluetypes.ConnectionTypeJdbc,
					ConnectionProperties: map[string]string{
						"JDBC_CONNECTION_URL": "jdbc:postgresql://" + addr + "/appdb", "SECRET_ID": tt.secretID,
					},
				},
			})
			require.NoError(t, err)

			env.create(&gluetypes.CrawlerTargets{JdbcTargets: []gluetypes.JdbcTarget{{
				ConnectionName: aws.String("conn"), Path: aws.String("appdb/%"),
			}}}, nil)

			c := env.crawl()
			assert.Equal(t, gluetypes.LastCrawlStatusFailed, c.LastCrawl.Status)
			assert.Contains(t, aws.ToString(c.LastCrawl.ErrorMessage), tt.wantErr)
			assert.Equal(t, gluetypes.CrawlerHistoryStateFailed, env.lastCrawl().State)

			if got != nil {
				assert.Contains(t, <-got, tt.wantUser)
			}
		})
	}
}
