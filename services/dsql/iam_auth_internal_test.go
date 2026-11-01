package dsql

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	dsqlauth "github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthenticateToken(t *testing.T) {
	t.Parallel()

	const (
		endpoint = "abcdefghijklmnopqrstuvwxyz.dsql.us-east-1.on.aws"
		region   = "us-east-1"
		port     = 49152
	)

	clusterARN := "arn:aws:dsql:us-east-1:123456789012:cluster/abcdefghijklmnopqrstuvwxyz"
	allowAdmin := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dsql:DbConnectAdmin","Resource":"*"}]}`
	allowUser := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dsql:DbConnect","Resource":"*"}]}`
	target := authTarget{endpoint: endpoint, region: region, arn: clusterARN, port: port}
	creds := credentials.NewStaticCredentialsProvider(fakeAccessKey, fakeSecretKey, "")
	badCreds := credentials.NewStaticCredentialsProvider(fakeAccessKey, "not-the-secret", "")
	unknownCreds := credentials.NewStaticCredentialsProvider("AKIAUNKNOWN", fakeSecretKey, "")

	gen := func(admin bool, host, reg string, c *credentials.StaticCredentialsProvider, ttl time.Duration) string {
		fn := dsqlauth.GenerateDbConnectAuthToken
		if admin {
			fn = dsqlauth.GenerateDBConnectAdminAuthToken
		}

		tok, err := fn(context.Background(), host, reg, c, func(o *dsqlauth.TokenOptions) { o.ExpiresIn = ttl })
		require.NoError(t, err)

		return tok
	}

	granted := func(role, arn string) bool { return role == "app" && arn == fakeUserARN }

	tests := []struct {
		wantErr  error
		cfg      IAMAuthConfig
		name     string
		user     string
		token    string
		wantRole string
		skew     time.Duration
	}{
		{name: "admin token", user: "admin", token: gen(true, endpoint, region, &creds, time.Hour), wantRole: "admin"},
		{name: "admin token with port", user: "admin", wantRole: "admin",
			token: gen(true, endpoint+":49152", region, &creds, time.Hour)},
		{name: "admin token via loopback port", user: "admin", wantRole: "admin",
			token: gen(true, "127.0.0.1:49152", region, &creds, time.Hour)},
		{name: "loopback wrong port", user: "admin", wantErr: errTokenEndpoint,
			token: gen(true, "127.0.0.1:1", region, &creds, time.Hour)},
		{name: "granted user token", user: "app", wantRole: "app", token: gen(false, endpoint, region, &creds, time.Hour)},
		{name: "user token without grant", user: "other", wantErr: errTokenNoGrant,
			token: gen(false, endpoint, region, &creds, time.Hour)},
		{name: "admin token for non-admin role", user: "app", wantErr: errTokenRole,
			token: gen(true, endpoint, region, &creds, time.Hour)},
		{name: "user token for admin role", user: "admin", wantErr: errTokenRole,
			token: gen(false, endpoint, region, &creds, time.Hour)},
		{name: "expired", user: "admin", wantErr: errTokenExpired, skew: 2 * time.Hour,
			token: gen(true, endpoint, region, &creds, time.Hour)},
		{name: "future dated", user: "admin", wantErr: errTokenMalformed, skew: -time.Hour,
			token: gen(true, endpoint, region, &creds, time.Hour)},
		{name: "other cluster", user: "admin", wantErr: errTokenEndpoint,
			token: gen(true, "zzzzzzzzzzzzzzzzzzzzzzzzzz.dsql.us-east-1.on.aws", region, &creds, time.Hour)},
		{name: "other region", user: "admin", wantErr: errTokenRegion,
			token: gen(true, endpoint, "eu-west-1", &creds, time.Hour)},
		{name: "wrong secret", user: "admin", wantErr: errTokenSignature,
			token: gen(true, endpoint, region, &badCreds, time.Hour)},
		{name: "unknown access key", user: "admin", wantErr: errTokenUnknownID,
			token: gen(true, endpoint, region, &unknownCreds, time.Hour)},
		{name: "garbage", user: "admin", wantErr: errTokenMalformed, token: "hunter2"},
		{name: "rds token", user: "admin", wantErr: errTokenMalformed,
			token: "db.x.us-east-1.rds.amazonaws.com:5432/?Action=connect&DBUser=admin&X-Amz-Signature=ab"},
		{name: "enforced allow", user: "admin", wantRole: "admin", cfg: IAMAuthConfig{Enforce: true},
			token: gen(true, endpoint, region, &creds, time.Hour)},
		{name: "enforced without policy", user: "admin", wantErr: errTokenDenied, cfg: IAMAuthConfig{Enforce: true},
			token: gen(true, endpoint, region, &creds, time.Hour)},
		{name: "enforced wrong action", user: "app", wantErr: errTokenDenied, cfg: IAMAuthConfig{Enforce: true},
			token: gen(false, endpoint, region, &creds, time.Hour)},
		{name: "enforced user allow", user: "app", wantRole: "app", cfg: IAMAuthConfig{Enforce: true},
			token: gen(false, endpoint, region, &creds, time.Hour)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.cfg
			cfg.Identities = fakeIdentities{policies: []string{allowAdmin, allowUser}}

			switch tt.name {
			case "enforced without policy":
				cfg.Identities = fakeIdentities{}
			case "enforced wrong action":
				cfg.Identities = fakeIdentities{policies: []string{allowAdmin}}
			}

			sess, err := authenticateToken(cfg, target, tt.user, tt.token, "127.0.0.1", time.Now().Add(tt.skew), granted)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, sess)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantRole, sess.role)
		})
	}
}

func TestGrantARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{name: "user", arn: fakeUserARN, want: fakeUserARN},
		{name: "assumed role", arn: "arn:aws:sts::123456789012:assumed-role/dev/session1",
			want: "arn:aws:iam::123456789012:role/dev"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, (&authPrincipal{arn: tt.arn}).grantARN())
		})
	}
}
