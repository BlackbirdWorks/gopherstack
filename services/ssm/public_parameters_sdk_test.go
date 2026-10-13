package ssm_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func apiCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

func TestGetParameter_PublicCatalog_SDK(t *testing.T) {
	t.Parallel()

	const (
		amzn2   = "/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2"
		al2023  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
		ecsRec  = "/aws/service/ecs/optimized-ami/amazon-linux-2/recommended"
		ecsID   = ecsRec + "/image_id"
		eksID   = "/aws/service/eks/optimized-ami/1.31/amazon-linux-2/recommended/image_id"
		regions = "/aws/service/global-infrastructure/regions/us-east-1"
	)

	tests := []struct {
		check    func(t *testing.T, p *types.Parameter)
		name     string
		param    string
		wantCode string
	}{
		{name: "al2023", param: al2023, check: func(t *testing.T, p *types.Parameter) {
			t.Helper()
			assert.Regexp(t, `^ami-0[0-9a-f]{16}$`, aws.ToString(p.Value))
		}},
		{name: "amzn2", param: amzn2, check: func(t *testing.T, p *types.Parameter) {
			t.Helper()
			assert.Equal(t, "ami-0c55b159cbfafe1f0", aws.ToString(p.Value))
		}},
		{
			name:  "arm64",
			param: "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
		},
		{name: "ecs image id", param: ecsID},
		{name: "ecs json", param: ecsRec, check: func(t *testing.T, p *types.Parameter) {
			t.Helper()

			var doc map[string]any
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(p.Value)), &doc))
			assert.Contains(t, doc, "image_id")
			assert.Contains(t, doc, "ecs_agent_version")
		}},
		{name: "eks", param: eksID},
		{name: "bottlerocket", param: "/aws/service/bottlerocket/aws-ecs-1/x86_64/latest/image_id"},
		{
			name:  "windows",
			param: "/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base",
		},
		{
			name:  "ubuntu",
			param: "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
		},
		{name: "region", param: regions, check: func(t *testing.T, p *types.Parameter) {
			t.Helper()
			assert.Equal(t, "us-east-1", aws.ToString(p.Value))
		}},
		{name: "version selector", param: al2023 + ":1"},
		{name: "bad version", param: al2023 + ":9", wantCode: "ParameterVersionNotFound"},
		{name: "unknown", param: "/aws/service/nope/nothing", wantCode: "ParameterNotFound"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			out, err := client.GetParameter(
				t.Context(),
				&ssmsdk.GetParameterInput{Name: aws.String(tc.param)},
			)

			if tc.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tc.wantCode, apiCode(t, err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, types.ParameterTypeString, out.Parameter.Type)
			assert.NotEmpty(t, aws.ToString(out.Parameter.Value))
			assert.Equal(t, int64(1), out.Parameter.Version)
			assert.True(t, strings.HasPrefix(aws.ToString(out.Parameter.ARN), "arn:aws:ssm:"))
			assert.Contains(t, aws.ToString(out.Parameter.ARN), ":parameter/aws/service/")

			if tc.check != nil {
				tc.check(t, out.Parameter)
			}
		})
	}
}

func TestGetParameters_PublicCatalog_SDK(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	good := "/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2"
	bad := "/aws/service/ami-amazon-linux-latest/missing"

	out, err := client.GetParameters(
		t.Context(),
		&ssmsdk.GetParametersInput{Names: []string{good, bad}},
	)
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, good, aws.ToString(out.Parameters[0].Name))
	assert.Equal(t, []string{bad}, out.InvalidParameters)
}

func TestGetParametersByPath_PublicCatalog_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantName  string
		wantMin   int
		wantMax   int
		recursive bool
	}{
		{
			name: "ami aliases", path: "/aws/service/ami-amazon-linux-latest",
			wantName: "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64", wantMin: 5,
		},
		{
			name: "ecs recursive", path: "/aws/service/ecs/optimized-ami/amazon-linux-2", recursive: true,
			wantName: "/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id", wantMin: 2,
		},
		{
			name: "ecs flat excludes children", path: "/aws/service/ecs/optimized-ami/amazon-linux-2",
			wantName: "/aws/service/ecs/optimized-ami/amazon-linux-2/recommended", wantMin: 1, wantMax: 1,
		},
		{
			name: "region services", path: "/aws/service/global-infrastructure/regions/eu-west-1/services",
			wantName: "/aws/service/global-infrastructure/regions/eu-west-1/services/s3", wantMin: 10,
		},
		{
			name: "regions", path: "/aws/service/global-infrastructure/regions",
			wantName: "/aws/service/global-infrastructure/regions/us-east-1", wantMin: 30,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			names := map[string]string{}
			paginator := ssmsdk.NewGetParametersByPathPaginator(
				client,
				&ssmsdk.GetParametersByPathInput{
					Path: aws.String(
						tc.path,
					), Recursive: aws.Bool(tc.recursive), MaxResults: aws.Int32(10),
				},
			)

			for paginator.HasMorePages() {
				page, err := paginator.NextPage(t.Context())
				require.NoError(t, err)

				for _, p := range page.Parameters {
					names[aws.ToString(p.Name)] = aws.ToString(p.Value)
				}
			}

			assert.Contains(t, names, tc.wantName)
			assert.GreaterOrEqual(t, len(names), tc.wantMin)

			if tc.wantMax > 0 {
				assert.LessOrEqual(t, len(names), tc.wantMax)
			}
		})
	}
}

func TestDescribeParameters_PublicCatalog_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filters    []types.ParameterStringFilter
		wantPublic bool
	}{
		{name: "unfiltered hides public"},
		{
			name: "path filter",
			filters: []types.ParameterStringFilter{{
				Key: aws.String("Path"), Option: aws.String("Recursive"),
				Values: []string{"/aws/service/ami-amazon-linux-latest"},
			}},
			wantPublic: true,
		},
		{
			name: "name begins with",
			filters: []types.ParameterStringFilter{{
				Key: aws.String("Name"), Option: aws.String("BeginsWith"),
				Values: []string{"/aws/service/ecs/optimized-ami/amazon-linux-2/recommended"},
			}},
			wantPublic: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			putParam(t, client, "/mine")

			var got []string

			paginator := ssmsdk.NewDescribeParametersPaginator(
				client,
				&ssmsdk.DescribeParametersInput{
					ParameterFilters: tc.filters,
				},
			)

			for paginator.HasMorePages() {
				page, err := paginator.NextPage(t.Context())
				require.NoError(t, err)

				for _, p := range page.Parameters {
					got = append(got, aws.ToString(p.Name))
				}
			}

			hasPublic := false

			for _, n := range got {
				if strings.HasPrefix(n, "/aws/service/") {
					hasPublic = true
				}
			}

			assert.Equal(t, tc.wantPublic, hasPublic, got)

			if !tc.wantPublic {
				assert.Equal(t, []string{"/mine"}, got)
			}
		})
	}
}

func TestPublicCatalog_ReadOnly_SDK(t *testing.T) {
	t.Parallel()

	const name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"

	tests := []struct {
		call     func(c *ssmsdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "put",
			call: func(c *ssmsdk.Client) error {
				_, err := c.PutParameter(context.Background(), &ssmsdk.PutParameterInput{
					Name: aws.String(name), Value: aws.String("x"), Type: types.ParameterTypeString,
					Overwrite: aws.Bool(true),
				})

				return err
			},
			wantCode: "ParameterPatternMismatchException",
		},
		{
			name: "delete",
			call: func(c *ssmsdk.Client) error {
				_, err := c.DeleteParameter(
					context.Background(),
					&ssmsdk.DeleteParameterInput{Name: aws.String(name)},
				)

				return err
			},
			wantCode: "ParameterNotFound",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			err := tc.call(client)
			require.Error(t, err)
			assert.Equal(t, tc.wantCode, apiCode(t, err))

			out, gerr := client.GetParameter(
				t.Context(),
				&ssmsdk.GetParameterInput{Name: aws.String(name)},
			)
			require.NoError(t, gerr)
			assert.NotEmpty(t, aws.ToString(out.Parameter.Value))
		})
	}
}

type fakeSecrets struct {
	secrets map[string]*ssm.SecretValue
	err     error
	calls   int
}

func (f *fakeSecrets) ResolveSecret(
	_ context.Context,
	ref ssm.SecretRef,
) (*ssm.SecretValue, error) {
	f.calls++

	if f.err != nil {
		return nil, f.err
	}

	key := ref.ID
	if ref.VersionID != "" {
		key += "@id:" + ref.VersionID
	} else if ref.VersionStage != "" {
		key += "@stage:" + ref.VersionStage
	}

	if s, ok := f.secrets[key]; ok {
		return s, nil
	}

	return nil, ssm.ErrSecretNotFound
}

func TestSecretsManagerReference_SDK(t *testing.T) {
	t.Parallel()

	const (
		secretARN = "arn:aws:secretsmanager:us-east-1:000000000000:secret:db-AbCdEf"
		versionID = "11111111-2222-3333-4444-555555555555"
	)

	current := &ssm.SecretValue{
		ARN: secretARN, Name: "db", VersionID: versionID, SecretString: `{"user":"u","pass":"p"}`,
		VersionStages: []string{"AWSCURRENT"}, CreatedDate: 1700000000,
	}
	previous := &ssm.SecretValue{
		ARN: secretARN, Name: "db", VersionID: "99999999-2222-3333-4444-555555555555", SecretString: "old",
		VersionStages: []string{"AWSPREVIOUS"}, CreatedDate: 1600000000,
	}
	fake := func() *fakeSecrets {
		return &fakeSecrets{secrets: map[string]*ssm.SecretValue{
			"db":                            current,
			"db@stage:AWSPREVIOUS":          previous,
			"db@id:" + versionID:            current,
			secretARN:                       current,
			secretARN + "@stage:AWSCURRENT": current,
		}}
	}

	tests := []struct {
		resolver       *fakeSecrets
		name           string
		param          string
		wantCode       string
		wantValue      string
		withDecryption bool
		noResolver     bool
	}{
		{
			name:           "current",
			resolver:       fake(),
			param:          "db",
			withDecryption: true,
			wantValue:      current.SecretString,
		},
		{
			name:           "arn",
			resolver:       fake(),
			param:          secretARN,
			withDecryption: true,
			wantValue:      current.SecretString,
		},
		{
			name:           "arn stage",
			resolver:       fake(),
			param:          secretARN + ":AWSCURRENT",
			withDecryption: true,
			wantValue:      current.SecretString,
		},
		{
			name:           "stage",
			resolver:       fake(),
			param:          "db:AWSPREVIOUS",
			withDecryption: true,
			wantValue:      "old",
		},
		{
			name:           "version id",
			resolver:       fake(),
			param:          "db:" + versionID,
			withDecryption: true,
			wantValue:      current.SecretString,
		},
		{name: "no decryption", resolver: fake(), param: "db", wantCode: "ValidationException"},
		{
			name:           "missing",
			resolver:       fake(),
			param:          "nope",
			withDecryption: true,
			wantCode:       "ParameterNotFound",
		},
		{
			name: "denied", resolver: &fakeSecrets{err: ssm.ErrSecretAccessDenied}, param: "db",
			withDecryption: true, wantCode: "AccessDeniedException",
		},
		{
			name:           "no resolver",
			noResolver:     true,
			param:          "db",
			withDecryption: true,
			wantCode:       "ParameterNotFound",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			if !tc.noResolver {
				backend.SetSecretResolver(tc.resolver)
			}

			client := newTestSSMClient(t, ssm.NewHandler(backend))
			name := "/aws/reference/secretsmanager/" + tc.param

			got, err := client.GetParameter(t.Context(), &ssmsdk.GetParameterInput{
				Name: aws.String(name), WithDecryption: aws.Bool(tc.withDecryption),
			})

			if tc.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tc.wantCode, apiCode(t, err))

				return
			}

			require.NoError(t, err)
			p := got.Parameter
			assert.Equal(t, name, aws.ToString(p.Name))
			assert.Equal(t, types.ParameterTypeSecureString, p.Type)
			assert.Equal(t, tc.wantValue, aws.ToString(p.Value))
			assert.Contains(t, aws.ToString(p.ARN), ":parameter/aws/reference/secretsmanager/")

			var src map[string]any
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(p.SourceResult)), &src))
			assert.Equal(t, secretARN, src["ARN"])
			assert.Equal(t, "db", src["Name"])
			assert.Equal(t, tc.wantValue, src["SecretString"])
		})
	}
}

func TestGetParameters_SecretsManagerReference_SDK(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	backend.SetSecretResolver(&fakeSecrets{secrets: map[string]*ssm.SecretValue{
		"db": {
			ARN:          "arn:aws:secretsmanager:us-east-1:000000000000:secret:db-x",
			Name:         "db",
			SecretString: "s3cret",
		},
	}})
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	putParam(t, client, "/plain")

	good, gone := "/aws/reference/secretsmanager/db", "/aws/reference/secretsmanager/gone"

	out, err := client.GetParameters(t.Context(), &ssmsdk.GetParametersInput{
		Names: []string{"/plain", good, gone}, WithDecryption: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 2)
	assert.Equal(t, "/plain", aws.ToString(out.Parameters[0].Name))
	assert.Equal(t, good, aws.ToString(out.Parameters[1].Name))
	assert.Equal(t, "s3cret", aws.ToString(out.Parameters[1].Value))
	assert.Equal(t, []string{gone}, out.InvalidParameters)

	_, err = client.GetParameters(t.Context(), &ssmsdk.GetParametersInput{Names: []string{good}})
	require.Error(t, err)
	assert.Equal(t, "ValidationException", apiCode(t, err))
}
