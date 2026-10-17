package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	acctA = "111111111111"
	acctB = "222222222222"
)

type multiAccountStack struct {
	url string
}

func newMultiAccountStack(t *testing.T) *multiAccountStack {
	t.Helper()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(
		&service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()},
	)
	require.NoError(t, err)

	registry := service.NewRegistry()
	for _, svc := range services {
		require.NoError(t, registry.Register(svc))
	}

	registry.Use(service.Middleware(principalMiddleware(buildPrincipalResolvers(services))))

	e := echo.New()
	e.Pre(awsMetaMiddleware("us-east-1", "000000000000"))
	e.Use(
		service.NewServiceRouter(registry).
			WithTargetGates(routeTargetGates()).
			WithPathGates(routePathGates()).
			RouteHandler(),
	)

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return &multiAccountStack{url: srv.URL}
}

func (s *multiAccountStack) config(t *testing.T, key, token string, headers map[string]string) aws.Config {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithHTTPClient(http.DefaultClient),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, "secret", token)),
	)
	require.NoError(t, err)

	cfg.BaseEndpoint = aws.String(s.url)

	for k, v := range headers {
		cfg.APIOptions = append(cfg.APIOptions, smithyhttp.AddHeaderValue(k, v))
	}

	return cfg
}

func TestMultiAccount_CallerIdentityAccount(t *testing.T) {
	t.Parallel()

	stack := newMultiAccountStack(t)

	tests := []struct {
		headers map[string]string
		name    string
		key     string
		want    string
	}{
		{name: "account a", key: acctA, want: acctA},
		{name: "account b", key: acctB, want: acctB},
		{name: "non numeric key", key: "test", want: "000000000000"},
		{name: "eleven digits", key: "11111111111", want: "000000000000"},
		{name: "thirteen digits", key: "1111111111111", want: "000000000000"},
		{name: "akia key", key: "AKIAIOSFODNN7EXAMPLE", want: "000000000000"},
		{
			name:    "header beats key",
			key:     acctA,
			headers: map[string]string{"X-Amz-Account-Id": "333333333333"},
			want:    "333333333333",
		},
		{
			name:    "header with non numeric key",
			key:     "test",
			headers: map[string]string{"X-Amz-Account-Id": "444444444444"},
			want:    "444444444444",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := sts.NewFromConfig(stack.config(t, tt.key, "", tt.headers))

			out, err := client.GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(out.Account))
			assert.Contains(t, aws.ToString(out.Arn), ":"+tt.want+":")
		})
	}
}

func TestMultiAccount_CrossAccountAssumeRole(t *testing.T) {
	t.Parallel()

	stack := newMultiAccountStack(t)

	tests := []struct {
		name       string
		callerKey  string
		roleAcct   string
		wantAcct   string
		wantArnSub string
	}{
		{
			name:       "a into b",
			callerKey:  acctA,
			roleAcct:   acctB,
			wantAcct:   acctB,
			wantArnSub: ":" + acctB + ":assumed-role/x/sess",
		},
		{
			name:       "b into a",
			callerKey:  acctB,
			roleAcct:   acctA,
			wantAcct:   acctA,
			wantArnSub: ":" + acctA + ":assumed-role/x/sess",
		},
		{
			name:       "same account",
			callerKey:  acctA,
			roleAcct:   acctA,
			wantAcct:   acctA,
			wantArnSub: ":" + acctA + ":assumed-role/x/sess",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			caller := sts.NewFromConfig(stack.config(t, tt.callerKey, "", nil))

			assumed, err := caller.AssumeRole(t.Context(), &sts.AssumeRoleInput{
				RoleArn:         aws.String("arn:aws:iam::" + tt.roleAcct + ":role/x"),
				RoleSessionName: aws.String("sess"),
			})
			require.NoError(t, err)

			creds := assumed.Credentials
			session := sts.NewFromConfig(stack.config(t,
				aws.ToString(creds.AccessKeyId), aws.ToString(creds.SessionToken), nil))

			out, err := session.GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{})
			require.NoError(t, err)
			assert.Equal(t, tt.wantAcct, aws.ToString(out.Account))
			assert.Contains(t, aws.ToString(out.Arn), tt.wantArnSub)
		})
	}
}

func TestMultiAccount_SQSIsolation(t *testing.T) {
	t.Parallel()

	stack := newMultiAccountStack(t)
	ctx := t.Context()

	a := sqs.NewFromConfig(stack.config(t, acctA, "", nil))
	b := sqs.NewFromConfig(stack.config(t, acctB, "", nil))

	created, err := a.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("only-a")})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(created.QueueUrl), "/"+acctA+"/")

	tests := []struct {
		client    *sqs.Client
		name      string
		wantCount int
	}{
		{name: "owner lists queue", client: a, wantCount: 1},
		{name: "other account sees none", client: b, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			list, listErr := tt.client.ListQueues(t.Context(), &sqs.ListQueuesInput{})
			require.NoError(t, listErr)
			assert.Len(t, list.QueueUrls, tt.wantCount)
		})
	}
}

func TestMultiAccount_DynamoDBTableARN(t *testing.T) {
	t.Parallel()

	stack := newMultiAccountStack(t)

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "account a", key: acctA, want: acctA},
		{name: "account b", key: acctB, want: acctB},
		{name: "default account", key: "test", want: "000000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := dynamodb.NewFromConfig(stack.config(t, tt.key, "", nil))

			out, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
				TableName: aws.String("tbl-" + tt.want),
				AttributeDefinitions: []ddbtypes.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
				},
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
				},
				BillingMode: ddbtypes.BillingModePayPerRequest,
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.TableDescription.TableArn), ":"+tt.want+":table/")
		})
	}
}
