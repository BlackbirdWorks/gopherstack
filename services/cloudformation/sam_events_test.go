package cloudformation_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
)

type invocation struct {
	name    string
	payload string
}

// recordingInvoker records every Lambda invocation and answers with the function name as body.
type recordingInvoker struct {
	calls chan invocation
}

func (r *recordingInvoker) InvokeFunction(
	_ context.Context, name, _ string, payload []byte,
) ([]byte, int, error) {
	fn := name
	if _, after, ok := strings.CutLast(name, ":function:"); ok {
		fn = after
	}
	if strings.HasPrefix(fn, "AuthFn") {
		effect := "Deny"
		if strings.Contains(string(payload), "allow") {
			effect = "Allow"
		}
		policy, _ := json.Marshal(map[string]any{"principalId": "u", "policyDocument": map[string]any{
			"Version": "2012-10-17", "Statement": []any{map[string]any{
				"Action": "execute-api:Invoke", "Effect": effect, "Resource": "*",
			}},
		}})

		return policy, http.StatusOK, nil
	}
	r.calls <- invocation{name: fn, payload: string(payload)}
	resp, _ := json.Marshal(map[string]any{"statusCode": 200, "body": fn})

	return resp, http.StatusOK, nil
}

func (r *recordingInvoker) next(t *testing.T) invocation {
	t.Helper()

	select {
	case c := <-r.calls:
		return c
	case <-time.After(5 * time.Second):
		require.FailNow(t, "no Lambda invocation arrived")
	}

	return invocation{}
}

type samEventEnv struct {
	backends *cloudformation.ServiceBackends
	client   *cfnsdk.Client
	invoker  *recordingInvoker
}

func newSAMEventEnv(t *testing.T) *samEventEnv {
	t.Helper()

	backends := newLambdaServiceBackends(t)
	lambdaBk, ok := backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	require.True(t, ok)
	t.Cleanup(func() { lambdaBk.Close(context.Background()) })

	backends.Scheduler = schedulerbackend.NewHandler(schedulerbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	shutdownOnCleanup(t, backends.Scheduler)
	backends.APIGatewayV2 = apigatewayv2backend.NewHandler(apigatewayv2backend.NewInMemoryBackend())

	inv := &recordingInvoker{calls: make(chan invocation, 16)}
	backends.APIGatewayV2.SetLambdaInvoker(inv)
	backends.APIGateway.SetLambdaInvoker(inv)
	backends.S3.SetNotificationDispatcher(s3backend.NewNotificationDispatcher(
		&s3backend.NotificationTargets{LambdaInvoker: inv}, "us-east-1"))

	b := cloudformation.NewInMemoryBackendWithConfig(
		"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
	)

	return &samEventEnv{backends: backends, client: newTestClientForBackend(t, b), invoker: inv}
}

func (e *samEventEnv) physicalID(t *testing.T, logicalID string) string {
	t.Helper()

	out, err := e.client.DescribeStackResource(t.Context(), &cfnsdk.DescribeStackResourceInput{
		StackName: aws.String("sam-stack"), LogicalResourceId: aws.String(logicalID),
	})
	require.NoError(t, err)

	return aws.ToString(out.StackResourceDetail.PhysicalResourceId)
}

func (e *samEventEnv) fnName(t *testing.T, logicalID string) string {
	t.Helper()

	return lambdaFn(t, e.backends, logicalID).FunctionName
}

func (e *samEventEnv) proxy(t *testing.T, method, path, origin string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "POST")
	}
	rr := httptest.NewRecorder()
	require.NoError(t, e.backends.APIGatewayV2.Handler()(echo.New().NewContext(req, rr)))

	return rr
}

const samFnHeader = `      Handler: index.handler
      Runtime: python3.12
      InlineCode: "def handler(e, c): return 1"
`

type httpAPIRequest struct {
	method     string
	path       string
	origin     string
	wantFn     string
	wantHeader string
	wantStatus int
}

func TestSAMTransform_HttpApi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
		stage    string
		apiID    string
		requests []httpAPIRequest
	}{
		{
			name:  "implicit_default_stage_and_catch_all",
			stage: "$default",
			apiID: "ServerlessHttpApi",
			template: samHeader + "Resources:\n  Hello:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader +
				"      Events:\n        Get:\n          Type: HttpApi\n          Properties:\n            Path: /hello\n" +
				"            Method: get\n  Fallback:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader +
				"      Events:\n        Any:\n          Type: HttpApi\n",
			requests: []httpAPIRequest{
				{method: http.MethodGet, path: "/hello", wantFn: "Hello", wantStatus: http.StatusOK},
				{method: http.MethodPost, path: "/anything/else", wantFn: "Fallback", wantStatus: http.StatusOK},
			},
		},
		{
			name:  "explicit_named_stage_cors_object",
			stage: "Prod",
			apiID: "Api",
			template: samHeader + "Resources:\n  Api:\n    Type: AWS::Serverless::HttpApi\n    Properties:\n" +
				"      StageName: Prod\n      StageVariables:\n        Color: blue\n      CorsConfiguration:\n" +
				"        AllowOrigins: ['https://app.example.com']\n        AllowMethods: [GET, POST]\n        MaxAge: 600\n" +
				"  Items:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader +
				"      Events:\n        Item:\n          Type: HttpApi\n          Properties:\n            ApiId: !Ref Api\n" +
				"            Path: /items/{id}\n            Method: ANY\n            TimeoutInMillis: 10000\n",
			requests: []httpAPIRequest{
				{method: http.MethodPut, path: "/items/42", wantFn: "Items", wantStatus: http.StatusOK,
					origin: "https://app.example.com", wantHeader: "https://app.example.com"},
				{method: http.MethodOptions, path: "/items/42", wantStatus: http.StatusNoContent,
					origin: "https://app.example.com", wantHeader: "https://app.example.com"},
				{method: http.MethodGet, path: "/missing", wantStatus: http.StatusNotFound},
			},
		},
		{
			name:  "cors_string_origin",
			stage: "$default",
			apiID: "Api",
			template: samHeader + "Resources:\n  Api:\n    Type: AWS::Serverless::HttpApi\n    Properties:\n" +
				"      CorsConfiguration: https://only.example.com\n  Fn:\n    Type: AWS::Serverless::Function\n" +
				"    Properties:\n" + samFnHeader + "      Events:\n        E:\n          Type: HttpApi\n" +
				"          Properties:\n            ApiId: !Ref Api\n            Path: /x\n            Method: GET\n",
			requests: []httpAPIRequest{
				{method: http.MethodOptions, path: "/x", wantStatus: http.StatusNoContent,
					origin: "https://only.example.com", wantHeader: "https://only.example.com"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			deploySAM(t, env.client, tc.template)
			apiID := env.physicalID(t, tc.apiID)

			for _, r := range tc.requests {
				rr := env.proxy(t, r.method, "/v2proxy/"+apiID+"/"+tc.stage+r.path, r.origin)
				assert.Equal(t, r.wantStatus, rr.Code, r.method+" "+r.path)
				if r.wantFn != "" {
					assert.Equal(t, env.fnName(t, r.wantFn), rr.Body.String())
				}
				if r.wantHeader != "" {
					assert.Equal(t, r.wantHeader, rr.Header().Get("Access-Control-Allow-Origin"))
				}
			}
			assertDeleteClean(t, env.client)
		})
	}
}

func TestSAMTransform_HttpApiResources(t *testing.T) {
	t.Parallel()

	env := newSAMEventEnv(t)
	deploySAM(t, env.client, samHeader+"Resources:\n  Api:\n    Type: AWS::Serverless::HttpApi\n"+
		"    Properties:\n      StageName: Gamma\n      StageVariables:\n        Color: blue\n"+
		"  Fn:\n    Type: AWS::Serverless::Function\n    Properties:\n"+samFnHeader+
		"      Events:\n        E:\n          Type: HttpApi\n          Properties:\n            ApiId: !Ref Api\n"+
		"            Path: /x/{p}\n            Method: GET\n            TimeoutInMillis: 10000\n")

	ids := logicalIDs(t, env.client)
	assert.Equal(t, "AWS::ApiGatewayV2::Api", ids["Api"])
	assert.Equal(t, "AWS::ApiGatewayV2::Stage", ids["ApiGammaStage"])
	assert.Equal(t, "AWS::Lambda::Permission", ids["FnEPermission"])

	bk, ok := env.backends.APIGatewayV2.Backend.(*apigatewayv2backend.InMemoryBackend)
	require.True(t, ok)
	apiID := env.physicalID(t, "Api")
	integs, err := bk.GetIntegrations(apiID)
	require.NoError(t, err)
	require.Len(t, integs, 1)
	assert.Equal(t, "AWS_PROXY", integs[0].IntegrationType)
	assert.Equal(t, "2.0", integs[0].PayloadFormatVersion)
	assert.EqualValues(t, 10000, integs[0].TimeoutInMillis)
	assert.Contains(t, integs[0].IntegrationURI, ":function:"+env.fnName(t, "Fn")+"/invocations")

	routes, err := bk.GetRoutes(apiID)
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, "GET /x/{p}", routes[0].RouteKey)
	assert.Equal(t, "integrations/"+integs[0].IntegrationID, routes[0].Target)

	stage, err := bk.GetStage(apiID, "Gamma")
	require.NoError(t, err)
	assert.Equal(t, "blue", stage.StageVariables["Color"])
	assert.True(t, stage.AutoDeploy)
	assert.Equal(t, "SAM", stage.Tags["httpapi:createdBy"])

	policy, err := env.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend).GetPolicy(env.fnName(t, "Fn"), "")
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(policy.Policy), "/*/GET/x/*")
}

func s3Client(t *testing.T, env *samEventEnv) *awss3.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(env.backends.S3))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
}

func TestSAMTransform_S3Event(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		events  string
		filter  string
		skipKey string
		fireKey string
	}{
		{
			name:    "events_list_with_prefix_filter",
			events:  "[s3:ObjectCreated:*, s3:ObjectRemoved:*]",
			filter:  s3FilterRule("prefix", "incoming/"),
			skipKey: "other/a.txt", fireKey: "incoming/b.txt",
		},
		{
			name:    "single_event_suffix_filter",
			events:  "s3:ObjectCreated:Put",
			filter:  s3FilterRule("suffix", ".csv"),
			skipKey: "a.txt", fireKey: "b.csv",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			deploySAM(t, env.client, samHeader+"Resources:\n  Images:\n    Type: AWS::S3::Bucket\n"+
				"  Thumb:\n    Type: AWS::Serverless::Function\n    Properties:\n"+samFnHeader+
				"      Events:\n        Up:\n          Type: S3\n          Properties:\n            Bucket: !Ref Images\n"+
				"            Events: "+tc.events+"\n"+tc.filter)

			assert.Equal(t, "AWS::Lambda::Permission", logicalIDs(t, env.client)["ThumbUpPermission"])
			bucket := env.physicalID(t, "Images")
			cfgXML, err := env.backends.S3.Backend.GetBucketNotificationConfiguration(t.Context(), bucket)
			require.NoError(t, err)
			assert.Contains(t, cfgXML, ":function:"+env.fnName(t, "Thumb"))

			policy, err := env.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend).GetPolicy(
				env.fnName(t, "Thumb"),
				"",
			)
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(policy.Policy), "s3.amazonaws.com")

			client := s3Client(t, env)
			put := func(key string) {
				_, putErr := client.PutObject(t.Context(), &awss3.PutObjectInput{
					Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("x"),
				})
				require.NoError(t, putErr)
			}
			put(tc.skipKey)
			put(tc.fireKey)

			got := env.invoker.next(t)
			assert.Equal(t, env.fnName(t, "Thumb"), got.name)
			assert.Contains(t, got.payload, tc.fireKey)
			assert.NotContains(t, got.payload, tc.skipKey)
			for _, key := range []string{tc.skipKey, tc.fireKey} {
				_, delErr := env.backends.S3.Backend.DeleteObject(t.Context(), &awss3.DeleteObjectInput{
					Bucket: aws.String(bucket), Key: aws.String(key),
				})
				require.NoError(t, delErr)
			}
			assertDeleteClean(t, env.client)
		})
	}
}

func TestSAMTransform_ScheduleV2Event(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify   func(t *testing.T, s *schedulerbackend.Schedule)
		name     string
		props    string
		roleSelf bool
	}{
		{
			name: "generated_role_and_options",
			props: "            ScheduleExpression: cron(0 8 * * ? *)\n            ScheduleExpressionTimezone: Europe/London\n" +
				"            Input: '{\"k\":\"v\"}'\n            State: DISABLED\n            Description: nightly\n" +
				"            FlexibleTimeWindow:\n              Mode: FLEXIBLE\n              MaximumWindowInMinutes: 5\n" +
				"            StartDate: '2030-01-01T00:00:00Z'\n            EndDate: '2031-01-01T00:00:00Z'\n" +
				"            RetryPolicy:\n              MaximumRetryAttempts: 3\n              MaximumEventAgeInSeconds: 3600\n",
			roleSelf: true,
			verify: func(t *testing.T, s *schedulerbackend.Schedule) {
				t.Helper()
				assert.Equal(t, "cron(0 8 * * ? *)", s.ScheduleExpression)
				assert.Equal(t, "Europe/London", s.ScheduleExpressionTimezone)
				assert.Equal(t, "DISABLED", s.State)
				assert.Equal(t, "nightly", s.Description)
				assert.Equal(t, "FLEXIBLE", s.FlexibleTimeWindow.Mode)
				assert.Equal(t, 5, s.FlexibleTimeWindow.MaximumWindowInMinutes)
				assert.JSONEq(t, `{"k":"v"}`, s.Target.Input)
				require.NotNil(t, s.Target.RetryPolicy)
				assert.Equal(t, 3, s.Target.RetryPolicy.MaximumRetryAttempts)
				require.NotNil(t, s.StartDate)
				assert.Equal(t, 2030, s.StartDate.Year())
				require.NotNil(t, s.EndDate)
				assert.Equal(t, 2031, s.EndDate.Year())
			},
		},
		{
			name: "explicit_role_and_name",
			props: "            ScheduleExpression: rate(5 minutes)\n            Name: custom-name\n" +
				"            RoleArn: arn:aws:iam::000000000000:role/existing\n",
			verify: func(t *testing.T, s *schedulerbackend.Schedule) {
				t.Helper()
				assert.Equal(t, "custom-name", s.Name)
				assert.Equal(t, "arn:aws:iam::000000000000:role/existing", s.Target.RoleARN)
				assert.Equal(t, "ENABLED", s.State)
				assert.Equal(t, "OFF", s.FlexibleTimeWindow.Mode)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			deploySAM(
				t,
				env.client,
				samHeader+"Resources:\n  Cron:\n    Type: AWS::Serverless::Function\n"+
					"    Properties:\n"+samFnHeader+"      Events:\n        Nightly:\n          Type: ScheduleV2\n"+
					"          Properties:\n"+tc.props,
			)

			ids := logicalIDs(t, env.client)
			assert.Equal(t, "AWS::Scheduler::Schedule", ids["CronNightly"])
			if tc.roleSelf {
				assert.Equal(t, "AWS::IAM::Role", ids["CronNightlyRole"])
			}

			name := "CronNightly"
			if !tc.roleSelf {
				name = "custom-name"
			}
			sched, err := env.backends.Scheduler.Backend.GetSchedule(t.Context(), name, "")
			require.NoError(t, err)
			assert.True(t, strings.HasSuffix(sched.Target.ARN, ":function:"+env.fnName(t, "Cron")), sched.Target.ARN)
			if tc.roleSelf {
				assert.Contains(t, sched.Target.RoleARN, ":role/CronNightlyRole-")
			}
			tc.verify(t, sched)
			assertDeleteClean(t, env.client)

			_, err = env.backends.Scheduler.Backend.GetSchedule(t.Context(), name, "")
			require.Error(t, err)
		})
	}
}

func s3FilterRule(name, value string) string {
	return "            Filter:\n              S3Key:\n                Rules:\n                  - Name: " + name +
		"\n                    Value: " + value + "\n"
}

func s3Event(bucket string) string {
	return "        E:\n          Type: S3\n          Properties:\n            Bucket: " + bucket +
		"\n            Events: s3:ObjectCreated:*\n"
}

func TestSAMTransform_EventFailures(t *testing.T) {
	t.Parallel()

	fn := func(events string) string {
		return samHeader + "Resources:\n  F:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader +
			"      Events:\n" + events
	}
	tests := []struct {
		name     string
		template string
		reason   string
	}{
		{
			name: "s3_bucket_not_in_template",
			template: fn(
				s3Event("!Ref Missing"),
			),
			reason: "AWS::S3::Bucket",
		},
		{
			name: "s3_bucket_literal_name",
			template: fn(
				s3Event("plain-name"),
			),
			reason: "Ref to an AWS::S3::Bucket",
		},
		{
			name:     "httpapi_path_without_method",
			template: fn("        E:\n          Type: HttpApi\n          Properties:\n            Path: /x\n"),
			reason:   "both 'Path' and 'Method'",
		},
		{
			name: "httpapi_auth_unsupported",
			template: samHeader + "Resources:\n  A:\n    Type: AWS::Serverless::HttpApi\n" +
				"    Properties:\n      Auth:\n        DefaultAuthorizer: X\n",
			reason: "Auth",
		},
		{
			name: "httpapi_event_auth_unsupported",
			template: fn(
				"        E:\n          Type: HttpApi\n          Properties:\n            Auth:\n              Authorizer: X\n",
			),
			reason: "Auth",
		},
		{
			name: "schedulev2_dlq_needs_arn",
			template: fn(
				"        E:\n          Type: ScheduleV2\n          Properties:\n            ScheduleExpression: rate(1 minute)\n" +
					"            DeadLetterConfig:\n              Type: SQS\n",
			),
			reason: "DeadLetterConfig",
		},
		{
			name: "api_auth_unknown_authorizer",
			template: fn(
				"        E:\n          Type: Api\n          Properties:\n            Path: /x\n            Method: get\n" +
					"            Auth:\n              Authorizer: Nope\n",
			),
			reason: "Nope",
		},
		{
			name: "api_auth_aws_iam_unsupported",
			template: samHeader + "Resources:\n  A:\n    Type: AWS::Serverless::Api\n    Properties:\n      StageName: p\n" +
				"      Auth:\n        Authorizers:\n          I: AWS_IAM\n",
			reason: "AWS_IAM",
		},
		{
			name: "api_auth_api_key_unsupported",
			template: samHeader + "Resources:\n  A:\n    Type: AWS::Serverless::Api\n    Properties:\n      StageName: p\n" +
				"      Auth:\n        ApiKeyRequired: true\n",
			reason: "ApiKeyRequired",
		},
		{
			name:     "schedulev2_missing_expression",
			template: fn("        E:\n          Type: ScheduleV2\n          Properties:\n            State: ENABLED\n"),
			reason:   "ScheduleExpression",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			_, err := env.client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
				StackName: aws.String("bad"), ChangeSetName: aws.String("cs"), ChangeSetType: types.ChangeSetTypeCreate,
				TemplateBody: aws.String(tc.template),
				Capabilities: []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityAutoExpand},
			})
			require.NoError(t, err)
			cs, err := env.client.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
				StackName: aws.String("bad"), ChangeSetName: aws.String("cs"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.ChangeSetStatusFailed, cs.Status)
			assert.Contains(t, aws.ToString(cs.StatusReason), tc.reason)
		})
	}
}

func (e *samEventEnv) restProxy(t *testing.T, method, apiID, path string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, "/restapis/"+apiID+"/Prod/_user_request_"+path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rr := httptest.NewRecorder()
	require.NoError(t, e.backends.APIGateway.Handler()(echo.New().NewContext(req, rr)))

	return rr
}

func TestSAMTransform_ApiCors(t *testing.T) {
	t.Parallel()

	events := func(restAPIID string) string {
		return "  Fn:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader +
			"      Events:\n        Get:\n          Type: Api\n          Properties:\n            Path: /items\n" +
			"            Method: get\n" + restAPIID + "        Post:\n          Type: Api\n          Properties:\n" +
			"            Path: /items\n            Method: post\n" + restAPIID
	}

	tests := []struct {
		want     map[string]string
		name     string
		template string
		apiID    string
	}{
		{
			name:  "globals_cors_object_implicit_api",
			apiID: "ServerlessRestApi",
			template: samHeader + "Globals:\n  Api:\n    Cors:\n      AllowMethods: \"'GET,POST'\"\n" +
				"      AllowHeaders: \"'X-Forwarded-For'\"\n      AllowOrigin: \"'https://example.com'\"\n" +
				"      MaxAge: \"'600'\"\n      AllowCredentials: true\nResources:\n" + events(""),
			want: map[string]string{
				"Access-Control-Allow-Origin":      "https://example.com",
				"Access-Control-Allow-Methods":     "GET,POST",
				"Access-Control-Allow-Headers":     "X-Forwarded-For",
				"Access-Control-Max-Age":           "600",
				"Access-Control-Allow-Credentials": "true",
			},
		},
		{
			name:  "origin_string_derives_methods",
			apiID: "Api",
			template: samHeader + "Resources:\n  Api:\n    Type: AWS::Serverless::Api\n    Properties:\n" +
				"      StageName: Prod\n      Cors: \"'*'\"\n" + events("            RestApiId: !Ref Api\n"),
			want: map[string]string{
				"Access-Control-Allow-Origin":  "*",
				"Access-Control-Allow-Methods": "GET,POST,OPTIONS",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			deploySAM(t, env.client, tc.template)
			apiID := env.physicalID(t, tc.apiID)

			pre := env.restProxy(t, http.MethodOptions, apiID, "/items")
			assert.Equal(t, http.StatusOK, pre.Code, pre.Body.String())
			for h, v := range tc.want {
				assert.Equal(t, v, pre.Header().Get(h), h)
			}

			got := env.restProxy(t, http.MethodGet, apiID, "/items")
			assert.Equal(t, http.StatusOK, got.Code, got.Body.String())
			assert.Equal(t, env.invoker.next(t).name, env.fnName(t, "Fn"))
			assertDeleteClean(t, env.client)
		})
	}
}

const samAuthFn = "  AuthFn:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader

func samAuthTemplate(authBlock, secureAuth string) string {
	return samHeader + "Resources:\n  Api:\n    Type: AWS::Serverless::Api\n    Properties:\n      StageName: Prod\n" +
		authBlock + samAuthFn + "  Fn:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader +
		"      Events:\n        Secure:\n          Type: Api\n          Properties:\n            Path: /secure\n" +
		"            Method: get\n            RestApiId: !Ref Api\n" + secureAuth +
		"        Public:\n          Type: Api\n          Properties:\n            Path: /public\n" +
		"            Method: get\n            RestApiId: !Ref Api\n            Auth:\n              Authorizer: NONE\n"
}

func TestSAMTransform_ApiAuth(t *testing.T) {
	t.Parallel()

	type req struct {
		path       string
		header     []string
		wantStatus int
	}
	tests := []struct {
		name     string
		template string
		authType string
		permID   string
		reqs     []req
	}{
		{
			name:     "lambda_token_default_authorizer",
			authType: "TOKEN",
			permID:   "ApiTokenAuthAuthorizerPermission",
			template: samAuthTemplate("      Auth:\n        DefaultAuthorizer: TokenAuth\n        Authorizers:\n"+
				"          TokenAuth:\n            FunctionArn: !GetAtt AuthFn.Arn\n            Identity:\n"+
				"              Header: X-Token\n", ""),
			reqs: []req{
				{path: "/secure", header: []string{"X-Token", "deny"}, wantStatus: http.StatusForbidden},
				{path: "/secure", header: []string{"X-Token", "allow"}, wantStatus: http.StatusOK},
				{path: "/public", wantStatus: http.StatusOK},
			},
		},
		{
			name:     "lambda_request_per_event_authorizer",
			authType: "REQUEST",
			permID:   "ApiReqAuthAuthorizerPermission",
			template: samAuthTemplate("      Auth:\n        Authorizers:\n          ReqAuth:\n"+
				"            FunctionPayloadType: REQUEST\n            FunctionArn: !GetAtt AuthFn.Arn\n"+
				"            Identity:\n              Headers: [X-Token]\n",
				"            Auth:\n              Authorizer: ReqAuth\n"),
			reqs: []req{
				{path: "/secure", header: []string{"X-Token", "allow"}, wantStatus: http.StatusOK},
				{path: "/public", wantStatus: http.StatusOK},
			},
		},
		{
			name:     "cognito_authorizer",
			authType: "COGNITO_USER_POOLS",
			template: samAuthTemplate("      Auth:\n        DefaultAuthorizer: Pool\n        Authorizers:\n"+
				"          Pool:\n            UserPoolArn: arn:aws:cognito-idp:us-east-1:000000000000:userpool/p\n", ""),
			reqs: []req{
				{path: "/secure", wantStatus: http.StatusUnauthorized},
				{path: "/secure", header: []string{"Authorization", "not-a-jwt"}, wantStatus: http.StatusUnauthorized},
				{path: "/public", wantStatus: http.StatusOK},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			deploySAM(t, env.client, tc.template)
			ids := logicalIDs(t, env.client)
			if tc.permID != "" {
				assert.Equal(t, "AWS::Lambda::Permission", ids[tc.permID])
			}
			apiID := env.physicalID(t, "Api")

			auths, err := env.backends.APIGateway.Backend.GetAuthorizers(apiID)
			require.NoError(t, err)
			require.Len(t, auths, 1)
			assert.Equal(t, tc.authType, auths[0].Type)

			for _, r := range tc.reqs {
				rr := env.restProxy(t, http.MethodGet, apiID, r.path, r.header...)
				assert.Equal(t, r.wantStatus, rr.Code, r.path+" "+rr.Body.String())
				if r.wantStatus == http.StatusOK {
					assert.Equal(t, env.invoker.next(t).name, env.fnName(t, "Fn"))
				}
			}
			assertDeleteClean(t, env.client)
		})
	}
}
