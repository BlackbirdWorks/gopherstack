package rds_test

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

const emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func generateAuthToken(t *testing.T, endpoint, user string, creds aws.Credentials, signedAt time.Time) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+endpoint, nil)
	require.NoError(t, err)

	q := req.URL.Query()
	q.Set("Action", "connect")
	q.Set("DBUser", user)
	q.Set("X-Amz-Expires", "900")
	req.URL.RawQuery = q.Encode()

	signed, _, err := v4.NewSigner().PresignHTTP(t.Context(), creds, req, emptyPayloadSHA256, "rds-db",
		"us-east-1", signedAt)
	require.NoError(t, err)

	return strings.TrimPrefix(signed, "https://")
}

func TestEngineDockerPostgresIAMAuth(t *testing.T) {
	t.Parallel()

	b, _ := newDockerEngineBackend(t)

	ib := iam.NewInMemoryBackend()
	_, err := ib.CreateUser("alice", "/", "")
	require.NoError(t, err)

	ak, err := ib.CreateAccessKey("alice")
	require.NoError(t, err)
	require.NoError(t, ib.PutUserPolicy("alice", "p", `{"Version":"2012-10-17","Statement":[`+
		`{"Effect":"Allow","Action":"rds-db:connect",`+
		`"Resource":"arn:aws:rds-db:us-east-1:123456789012:dbuser:iamdb/iamuser"}]}`))

	b.SetIAMAuth(func() rds.IAMAuthConfig { return rds.IAMAuthConfig{Identities: ib, Enforce: true} })

	_, err = b.CreateDBInstance("iamdb", "postgres", "db.t3.micro", "appdb", "master", "", 20,
		rds.DBInstanceOptions{MasterUserPassword: "firstpass1", IAMDatabaseAuthenticationEnabled: true})
	require.NoError(t, err)

	var inst rds.DBInstance
	require.Eventually(t, func() bool {
		got, derr := b.DescribeDBInstances("iamdb")
		require.NoError(t, derr)
		inst = got[0]
		require.NotEqual(t, "failed", inst.DBInstanceStatus)

		return inst.DBInstanceStatus == "available"
	}, dockerEngineWait, 500*time.Millisecond)

	master := openEngineDB(t, "postgres", inst, "firstpass1")
	_, err = master.ExecContext(t.Context(), "CREATE ROLE iamuser LOGIN")
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.EngineCAPEM()))

	endpoint := net.JoinHostPort(inst.Endpoint, strconv.Itoa(inst.Port))
	creds := aws.Credentials{AccessKeyID: ak.AccessKeyID, SecretAccessKey: ak.SecretAccessKey}

	connect := func(token string) (*pgconn.PgConn, error) {
		cfg, perr := pgconn.ParseConfig("postgres://iamuser@" + endpoint + "/appdb?sslmode=disable")
		require.NoError(t, perr)

		cfg.Password = token
		cfg.TLSConfig = &tls.Config{RootCAs: pool, ServerName: inst.Endpoint, MinVersion: tls.VersionTLS12}

		return pgconn.ConnectConfig(t.Context(), cfg)
	}

	pc, err := connect(generateAuthToken(t, endpoint, "iamuser", creds, time.Now().UTC()))
	require.NoError(t, err)

	t.Cleanup(func() { _ = pc.Close(t.Context()) })

	rs, err := pc.Exec(t.Context(), "select current_user").ReadAll()
	require.NoError(t, err)
	assert.Equal(t, "iamuser", string(rs[0].Rows[0][0]))

	_, err = pc.Exec(t.Context(), "RESET SESSION AUTHORIZATION").ReadAll()

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "42501", pgErr.Code)

	_, err = connect(generateAuthToken(t, endpoint, "iamuser", creds, time.Now().UTC().Add(-time.Hour)))
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "28000", pgErr.Code)

	wrong := generateAuthToken(t, "127.0.0.1:1", "iamuser", creds, time.Now().UTC())
	_, err = connect(wrong)
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "28000", pgErr.Code)
}
