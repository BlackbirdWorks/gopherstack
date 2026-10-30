package app

import (
	"github.com/blackbirdworks/gopherstack/internal/routegates"

	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	defaultAccount = "000000000000"
	acctA          = "111111111111"
	acctB          = "222222222222"
)

type multiAccountStack struct {
	iamBackend *iambackend.InMemoryBackend
	url        string
}

func newMultiAccountStack(t *testing.T) *multiAccountStack {
	t.Helper()

	return newMultiAccountStackWith(t, false)
}

func newEnforcedMultiAccountStack(t *testing.T) *multiAccountStack {
	t.Helper()

	return newMultiAccountStackWith(t, true)
}

func newMultiAccountStackWith(t *testing.T, enforce bool) *multiAccountStack {
	t.Helper()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t,
		&service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()},
	)
	require.NoError(t, err)

	registry := service.NewRegistry()
	registry.Use(service.Middleware(principalMiddleware(buildPrincipalResolvers(services))))

	var iamBk *iambackend.InMemoryBackend

	for _, svc := range services {
		if h, ok := svc.(*iambackend.Handler); ok {
			iamBk, _ = h.Backend.(*iambackend.InMemoryBackend)
		}
	}

	if enforce {
		registry.Use(service.Middleware(iambackend.EnforcementMiddleware(
			findIAMBackend(services),
			iambackend.EnforcementConfig{ActionExtractors: buildActionExtractors(services)},
		)))
	}

	for _, svc := range services {
		require.NoError(t, registry.Register(svc))
	}

	e := echo.New()
	e.Pre(awsMetaMiddleware("us-east-1", "000000000000"))
	e.Use(
		service.NewServiceRouter(registry).
			WithTargetGates(routegates.TargetGates()).
			WithCBORGates(routegates.CBORGates()).
			WithPathGates(routegates.PathGates()).
			WithAltGates(routegates.AltGates()).
			RouteHandler(),
	)

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return &multiAccountStack{url: srv.URL, iamBackend: iamBk}
}

func (s *multiAccountStack) config(t *testing.T, key, token string, headers map[string]string) aws.Config {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithHTTPClient(testclient.New(t)),
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

const iamOpenTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}`

const iamExternalIDTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"AWS":"*"},"Action":"sts:AssumeRole",` +
	`"Condition":{"StringEquals":{"sts:ExternalId":"ext-b"}}}]}`

func TestMultiAccount_IAMRoleIsolation(t *testing.T) {
	t.Parallel()

	stack := newMultiAccountStack(t)
	ctx := t.Context()

	clients := map[string]*iam.Client{
		acctA:          iam.NewFromConfig(stack.config(t, acctA, "", nil)),
		acctB:          iam.NewFromConfig(stack.config(t, acctB, "", nil)),
		defaultAccount: iam.NewFromConfig(stack.config(t, "test", "", nil)),
	}

	for acct, c := range clients {
		out, err := c.CreateRole(ctx, &iam.CreateRoleInput{
			RoleName:                 aws.String("x"),
			AssumeRolePolicyDocument: aws.String(iamOpenTrust),
		})
		require.NoError(t, err, acct)
		assert.Equal(t, "arn:aws:iam::"+acct+":role/x", aws.ToString(out.Role.Arn))
	}

	_, err := clients[acctA].CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String("only-a"),
		AssumeRolePolicyDocument: aws.String(iamOpenTrust),
	})
	require.NoError(t, err)

	t.Run("duplicate in same account conflicts", func(t *testing.T) {
		t.Parallel()

		_, dupErr := clients[acctA].CreateRole(t.Context(), &iam.CreateRoleInput{
			RoleName:                 aws.String("x"),
			AssumeRolePolicyDocument: aws.String(iamOpenTrust),
		})
		require.Error(t, dupErr)

		var exists *iamtypes.EntityAlreadyExistsException
		require.ErrorAs(t, dupErr, &exists)
	})

	listTests := []struct {
		name  string
		acct  string
		names []string
	}{
		{name: "account a", acct: acctA, names: []string{"only-a", "x"}},
		{name: "account b", acct: acctB, names: []string{"x"}},
		{name: "default account", acct: defaultAccount, names: []string{"x"}},
	}

	for _, tt := range listTests {
		t.Run("list "+tt.name, func(t *testing.T) {
			t.Parallel()

			out, listErr := clients[tt.acct].ListRoles(t.Context(), &iam.ListRolesInput{})
			require.NoError(t, listErr)

			var got []string
			for _, r := range out.Roles {
				got = append(got, aws.ToString(r.RoleName))
				assert.Contains(t, aws.ToString(r.Arn), ":"+tt.acct+":role/")
			}

			assert.Equal(t, tt.names, got)
		})
	}

	getTests := []struct {
		name    string
		acct    string
		role    string
		wantErr bool
	}{
		{name: "owner gets", acct: acctA, role: "only-a"},
		{name: "other account no such entity", acct: acctB, role: "only-a", wantErr: true},
		{name: "default account no such entity", acct: defaultAccount, role: "only-a", wantErr: true},
	}

	for _, tt := range getTests {
		t.Run("get "+tt.name, func(t *testing.T) {
			t.Parallel()

			out, getErr := clients[tt.acct].GetRole(t.Context(), &iam.GetRoleInput{RoleName: aws.String(tt.role)})
			if tt.wantErr {
				var nse *iamtypes.NoSuchEntityException
				require.ErrorAs(t, getErr, &nse)

				return
			}

			require.NoError(t, getErr)
			assert.Equal(t, "arn:aws:iam::"+tt.acct+":role/"+tt.role, aws.ToString(out.Role.Arn))
		})
	}
}

func TestMultiAccount_IAMUsersGroupsPolicies(t *testing.T) {
	t.Parallel()

	stack := newMultiAccountStack(t)
	ctx := t.Context()

	a := iam.NewFromConfig(stack.config(t, acctA, "", nil))
	b := iam.NewFromConfig(stack.config(t, acctB, "", nil))

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`

	for acct, c := range map[string]*iam.Client{acctA: a, acctB: b} {
		u, err := c.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("u")})
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:iam::"+acct+":user/u", aws.ToString(u.User.Arn))

		g, err := c.CreateGroup(ctx, &iam.CreateGroupInput{GroupName: aws.String("g")})
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:iam::"+acct+":group/g", aws.ToString(g.Group.Arn))

		p, err := c.CreatePolicy(ctx, &iam.CreatePolicyInput{
			PolicyName: aws.String("p"), PolicyDocument: aws.String(doc),
		})
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:iam::"+acct+":policy/p", aws.ToString(p.Policy.Arn))

		ip, err := c.CreateInstanceProfile(ctx, &iam.CreateInstanceProfileInput{
			InstanceProfileName: aws.String("ip"),
		})
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:iam::"+acct+":instance-profile/ip", aws.ToString(ip.InstanceProfile.Arn))
	}

	_, err := a.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("only-a")})
	require.NoError(t, err)

	users, err := b.ListUsers(ctx, &iam.ListUsersInput{})
	require.NoError(t, err)
	assert.Len(t, users.Users, 1)

	_, err = b.GetUser(ctx, &iam.GetUserInput{UserName: aws.String("only-a")})
	var nse *iamtypes.NoSuchEntityException
	require.ErrorAs(t, err, &nse)

	policies, err := b.ListPolicies(ctx, &iam.ListPoliciesInput{Scope: iamtypes.PolicyScopeTypeLocal})
	require.NoError(t, err)
	require.Len(t, policies.Policies, 1)
	assert.Equal(t, "arn:aws:iam::"+acctB+":policy/p", aws.ToString(policies.Policies[0].Arn))

	managed, err := b.GetPolicy(ctx, &iam.GetPolicyInput{
		PolicyArn: aws.String("arn:aws:iam::aws:policy/AdministratorAccess"),
	})
	require.NoError(t, err)
	assert.Equal(t, "AdministratorAccess", aws.ToString(managed.Policy.PolicyName))
}

func TestMultiAccount_IAMAssumeRoleResolvesTargetAccount(t *testing.T) {
	t.Parallel()

	stack := newMultiAccountStack(t)
	ctx := t.Context()

	a := iam.NewFromConfig(stack.config(t, acctA, "", nil))
	b := iam.NewFromConfig(stack.config(t, acctB, "", nil))

	_, err := a.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName: aws.String("x"), AssumeRolePolicyDocument: aws.String(iamOpenTrust),
	})
	require.NoError(t, err)

	_, err = b.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName: aws.String("x"), AssumeRolePolicyDocument: aws.String(iamExternalIDTrust),
	})
	require.NoError(t, err)

	tests := []struct {
		name       string
		roleAcct   string
		externalID string
		wantErr    bool
	}{
		{name: "account a open trust", roleAcct: acctA},
		{name: "account b needs external id", roleAcct: acctB, wantErr: true},
		{name: "account b with external id", roleAcct: acctB, externalID: "ext-b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			caller := sts.NewFromConfig(stack.config(t, acctA, "", nil))
			in := &sts.AssumeRoleInput{
				RoleArn:         aws.String("arn:aws:iam::" + tt.roleAcct + ":role/x"),
				RoleSessionName: aws.String("sess"),
			}

			if tt.externalID != "" {
				in.ExternalId = aws.String(tt.externalID)
			}

			out, assumeErr := caller.AssumeRole(t.Context(), in)
			if tt.wantErr {
				require.Error(t, assumeErr)

				return
			}

			require.NoError(t, assumeErr)
			assert.Contains(t, aws.ToString(out.AssumedRoleUser.Arn), ":"+tt.roleAcct+":assumed-role/x/sess")
		})
	}
}

const allowSQSPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Action":"sqs:ListQueues","Resource":"*"}]}`

const allowAssumePolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Action":"sts:AssumeRole","Resource":"*"}]}`

func TestMultiAccount_EnforcementUsesPrincipalAccount(t *testing.T) {
	t.Parallel()

	stack := newEnforcedMultiAccountStack(t)
	bk := stack.iamBackend
	require.NotNil(t, bk)

	b := bk.ForAccount(acctB)

	mkUser := func(name string) string {
		_, err := b.CreateUser(name, "/", "")
		require.NoError(t, err)

		key, err := b.CreateAccessKey(name)
		require.NoError(t, err)

		return key.AccessKeyID
	}

	noPerm := mkUser("no-perm")

	inline := mkUser("inline")
	require.NoError(t, b.PutUserPolicy("inline", "p", allowSQSPolicy))

	crossPol, err := bk.ForAccount(acctA).CreatePolicy("shared", "/", allowSQSPolicy)
	require.NoError(t, err)

	cross := mkUser("cross")
	require.NoError(t, b.AttachUserPolicy("cross", crossPol.Arn))

	assumer := mkUser("assumer")
	require.NoError(t, b.PutUserPolicy("assumer", "p", allowAssumePolicy))

	_, err = b.CreateRole("allowed", "/", iamOpenTrust, "")
	require.NoError(t, err)
	require.NoError(t, b.PutRolePolicy("allowed", "p", allowSQSPolicy))

	_, err = b.CreateRole("bare", "/", iamOpenTrust, "")
	require.NoError(t, err)

	hdr := map[string]string{"X-Amz-Account-Id": acctB}

	assumeCreds := func(role string) (string, string) {
		out, assumeErr := sts.NewFromConfig(stack.config(t, assumer, "", hdr)).AssumeRole(t.Context(),
			&sts.AssumeRoleInput{
				RoleArn:         aws.String("arn:aws:iam::" + acctB + ":role/" + role),
				RoleSessionName: aws.String("sess"),
			})
		require.NoError(t, assumeErr)

		return aws.ToString(out.Credentials.AccessKeyId), aws.ToString(out.Credentials.SessionToken)
	}

	allowedKey, allowedTok := assumeCreds("allowed")
	bareKey, bareTok := assumeCreds("bare")

	tests := []struct {
		headers map[string]string
		name    string
		key     string
		token   string
		denied  bool
	}{
		{name: "user without permissions", key: noPerm, headers: hdr, denied: true},
		{name: "user with inline allow", key: inline, headers: hdr},
		{name: "user with cross account managed policy", key: cross, headers: hdr},
		{name: "assumed role with role policy", key: allowedKey, token: allowedTok},
		{name: "assumed role without policies", key: bareKey, token: bareTok, denied: true},
		{name: "unknown principal in account b", key: acctB, denied: true},
		{name: "unknown principal via header", key: "test", headers: hdr, denied: true},
		{name: "default account dummy key unchanged", key: "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := sqs.NewFromConfig(stack.config(t, tt.key, tt.token, tt.headers))
			_, listErr := client.ListQueues(t.Context(), &sqs.ListQueuesInput{})

			if !tt.denied {
				require.NoError(t, listErr)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, listErr, &apiErr)
			assert.Contains(t, apiErr.ErrorCode(), "AccessDenied")
		})
	}
}

func TestMultiAccount_WebIdentityFederationPerAccount(t *testing.T) {
	t.Parallel()

	stack := newMultiAccountStack(t)
	stack.iamBackend.ForAccount(acctB).DisableOutboundWebIdentityFederation()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "default enabled", key: "test"},
		{name: "untouched account", key: acctA},
		{name: "disabled account", key: acctB, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := sts.NewFromConfig(stack.config(t, tt.key, "", nil)).GetWebIdentityToken(t.Context(),
				&sts.GetWebIdentityTokenInput{
					Audience:         []string{"aud"},
					SigningAlgorithm: aws.String("RS256"),
				})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "OutboundWebIdentityFederationDisabled")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestMultiAccount_IAMUserKeyResolvesAccount(t *testing.T) {
	t.Parallel()

	stack := newMultiAccountStack(t)
	ctx := t.Context()

	admin := iam.NewFromConfig(stack.config(t, acctB, "", nil))

	_, err := admin.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("alice")})
	require.NoError(t, err)

	key, err := admin.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String("alice")})
	require.NoError(t, err)

	akia := aws.ToString(key.AccessKey.AccessKeyId)
	require.True(t, len(akia) > 4 && akia[:4] == "AKIA")

	userClient := sqs.NewFromConfig(stack.config(t, akia, "", nil))
	created, err := userClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("user-q")})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(created.QueueUrl), "/"+acctB+"/")

	tests := []struct {
		headers    map[string]string
		name       string
		key        string
		wantAcct   string
		wantArnSub string
	}{
		{
			name: "user key no header", key: akia, wantAcct: acctB,
			wantArnSub: ":" + acctB + ":user/alice",
		},
		{
			name: "unknown akia", key: "AKIAUNKNOWNKEY000000",
			wantAcct: defaultAccount, wantArnSub: ":" + defaultAccount + ":",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := sts.NewFromConfig(stack.config(t, tt.key, "", tt.headers))

			out, callErr := client.GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{})
			require.NoError(t, callErr)
			assert.Equal(t, tt.wantAcct, aws.ToString(out.Account))
			assert.Contains(t, aws.ToString(out.Arn), tt.wantArnSub)
		})
	}

	t.Run("header overrides user key", func(t *testing.T) {
		t.Parallel()

		client := sqs.NewFromConfig(stack.config(t, akia, "", map[string]string{"X-Amz-Account-Id": acctA}))

		out, qErr := client.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("hdr-q")})
		require.NoError(t, qErr)
		assert.Contains(t, aws.ToString(out.QueueUrl), "/"+acctA+"/")
	})

	visibility := []struct {
		name      string
		key       string
		wantCount int
	}{
		{name: "default account sees none", key: "test", wantCount: 0},
		{name: "account b via key sees queue", key: acctB, wantCount: 1},
		{name: "user key sees queue", key: akia, wantCount: 1},
	}

	for _, tt := range visibility {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			list, listErr := sqs.NewFromConfig(stack.config(t, tt.key, "", nil)).
				ListQueues(t.Context(), &sqs.ListQueuesInput{})
			require.NoError(t, listErr)
			assert.Len(t, list.QueueUrls, tt.wantCount)
		})
	}
}
