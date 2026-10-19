package replicator_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/replicator"
)

// Mutates process env (AWS_CONFIG_FILE), so it cannot run in parallel.
func TestReplicateSourceProfile(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		creds   string
		profile string
		wantErr string
	}{
		{
			name:    "credentials file profile",
			config:  "[profile src]\nregion = us-east-1\n",
			creds:   "[src]\naws_access_key_id = " + srcAccount + "\naws_secret_access_key = secret\n",
			profile: "src",
		},
		{
			name: "config file profile with keys",
			config: "[profile src]\nregion = us-east-1\naws_access_key_id = " + srcAccount +
				"\naws_secret_access_key = secret\n",
			profile: "src",
		},
		{
			name:    "missing profile",
			config:  "[profile other]\nregion = us-east-1\n",
			profile: "src",
			wantErr: "source",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfgPath := filepath.Join(dir, "config")
			credPath := filepath.Join(dir, "credentials")
			require.NoError(t, os.WriteFile(cfgPath, []byte(tt.config), 0o600))
			require.NoError(t, os.WriteFile(credPath, []byte(tt.creds), 0o600))

			t.Setenv("AWS_CONFIG_FILE", cfgPath)
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credPath)
			t.Setenv("AWS_PROFILE", "")
			t.Setenv("AWS_ACCESS_KEY_ID", "")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "")
			t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

			p := newPair(t)
			seedSQS(t, p)

			cfg := p.config("sqs")
			cfg.Source = replicator.Endpoint{Profile: tt.profile, URL: p.src}

			rep, err := replicator.Run(t.Context(), cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.False(t, rep.Failed(), "%+v", rep.Results)
			assert.Equal(t, srcAccount, rep.SourceAccount)

			q, err := sqs.NewFromConfig(p.dstCfg).
				GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String("orders")})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(q.QueueUrl))
		})
	}
}

func TestReplicateExtrasDryRun(t *testing.T) {
	t.Parallel()

	p := newPair(t)
	seedLambdaVersions(t, p)

	cfg := p.config("lambda")
	cfg.DryRun = true

	rep, err := replicator.Run(t.Context(), cfg)
	require.NoError(t, err)
	require.False(t, rep.Failed(), "%+v", rep.Results)

	a := actions(rep)
	assert.Equal(t, replicator.ActionCreated, a["lambda/worker"])
	assert.Equal(t, replicator.ActionCreated, a["lambda/worker:1"])
	assert.Equal(t, replicator.ActionCreated, a["lambda/worker:live"])

	out, err := lambda.NewFromConfig(p.dstCfg).ListFunctions(t.Context(), &lambda.ListFunctionsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.Functions)
}
