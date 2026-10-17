package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSMSecretsManagerReference(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	sm := secretsmanager.NewFromConfig(fx.cfg)

	_, err := sm.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
		Name: aws.String("app/db"), SecretString: aws.String("hunter2"),
	})
	require.NoError(t, err)

	tests := []struct {
		name       string
		param      string
		want       string
		wantErrSub string
		decrypt    bool
	}{
		{name: "current", param: "/aws/reference/secretsmanager/app/db", decrypt: true, want: "hunter2"},
		{name: "stage", param: "/aws/reference/secretsmanager/app/db:AWSCURRENT", decrypt: true, want: "hunter2"},
		{name: "no_decryption", param: "/aws/reference/secretsmanager/app/db", wantErrSub: "WithDecryption"},
		{
			name: "missing", param: "/aws/reference/secretsmanager/app/none",
			decrypt: true, wantErrSub: "ParameterNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, gErr := ssm.NewFromConfig(fx.cfg).GetParameter(t.Context(), &ssm.GetParameterInput{
				Name: aws.String(tt.param), WithDecryption: aws.Bool(tt.decrypt),
			})

			if tt.wantErrSub != "" {
				require.Error(t, gErr)
				assert.Contains(t, gErr.Error(), tt.wantErrSub)

				return
			}

			require.NoError(t, gErr)
			assert.Equal(t, tt.want, aws.ToString(out.Parameter.Value))
		})
	}
}

func TestSSMSecretsManagerReferenceIAM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		actions string
		wantErr bool
	}{
		{name: "allowed", actions: `"ssm:GetParameter","secretsmanager:GetSecretValue"`},
		{name: "denied", actions: `"ssm:GetParameter"`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, true)
			sm := secretsmanager.NewFromConfig(fx.cfg)
			_, err := sm.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
				Name: aws.String("app/db"), SecretString: aws.String("hunter2"),
			})
			require.NoError(t, err)

			iamc := iam.NewFromConfig(fx.cfg)
			_, err = iamc.CreateUser(t.Context(), &iam.CreateUserInput{UserName: aws.String("reader")})
			require.NoError(t, err)

			_, err = iamc.PutUserPolicy(t.Context(), &iam.PutUserPolicyInput{
				UserName: aws.String("reader"), PolicyName: aws.String("p"),
				PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[` +
					tt.actions + `],"Resource":"*"}]}`),
			})
			require.NoError(t, err)

			key, err := iamc.CreateAccessKey(t.Context(), &iam.CreateAccessKeyInput{UserName: aws.String("reader")})
			require.NoError(t, err)

			cfg := fx.cfg.Copy()
			cfg.Credentials = credentials.NewStaticCredentialsProvider(
				aws.ToString(key.AccessKey.AccessKeyId), aws.ToString(key.AccessKey.SecretAccessKey), "",
			)

			out, gErr := ssm.NewFromConfig(cfg).GetParameter(t.Context(), &ssm.GetParameterInput{
				Name: aws.String("/aws/reference/secretsmanager/app/db"), WithDecryption: aws.Bool(true),
			})

			if tt.wantErr {
				require.Error(t, gErr)
				assert.Contains(t, gErr.Error(), "AccessDenied")

				return
			}

			require.NoError(t, gErr)
			assert.Equal(t, "hunter2", aws.ToString(out.Parameter.Value))
		})
	}
}

func TestIAMPrincipalArn(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, in, want string }{
		{"user", "arn:aws:iam::1:user/bob", "arn:aws:iam::1:user/bob"},
		{"role", "arn:aws:iam::1:role/r", "arn:aws:iam::1:role/r"},
		{"assumed", "arn:aws:sts::1:assumed-role/r/sess", "arn:aws:iam::1:role/r"},
		{"root", "arn:aws:iam::1:root", ""},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, iamPrincipalArn(tt.in))
		})
	}
}
