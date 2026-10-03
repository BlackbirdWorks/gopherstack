package stepfunctions

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/smithy-go"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

const (
	errCodePermissions     = "States.Permissions"
	maxCachedClients       = 256
	credentialExpiryWindow = time.Minute
	sdkEndpoint            = "http://sfn-sdk-integration.local"
	sdkCredential          = "test"
	errCodeTaskFailed      = "States.TaskFailed"
	errCodeRuntime         = "States.Runtime"
	prefixStepFunctions    = "StepFunctions"
	exceptionSuffix        = "Exception"
	sdkClientException     = "SdkClientException"
	headerAccountID        = "X-Amz-Account-Id"
)

// modeledErrorCode maps wire codes an SDK cannot resolve to its modeled error
// to the modeled name Step Functions reports.
func modeledErrorCode(code string) string {
	if code == "AWS.SimpleQueueService.NonExistentQueue" {
		return "QueueDoesNotExist"
	}

	return code
}

type sdkService struct {
	build  func(aws.Config) any
	prefix string
}

// optimizedPrefixes name errors of optimized integrations, which differ from
// the aws-sdk prefix for a few services.
func optimizedPrefix(svc string) string {
	switch svc {
	case "dynamodb":
		return "DynamoDB"
	case "sqs":
		return "SQS"
	case "sns":
		return "SNS"
	case "sfn":
		return prefixStepFunctions
	case "bedrockruntime":
		return "Bedrock"
	default:
		return ""
	}
}

// inProcessClient is an aws.HTTPClient that serves requests from an
// in-process http.Handler instead of the network.
type inProcessClient struct {
	handler http.Handler
	account string
}

func (c *inProcessClient) Do(req *http.Request) (*http.Response, error) {
	if req.Body == nil {
		req.Body = http.NoBody
	}

	if c.account != "" {
		req.Header.Set(headerAccountID, c.account)
	}

	rec := httptest.NewRecorder()
	c.handler.ServeHTTP(rec, req)

	return rec.Result(), nil
}

// RoleCredentials are temporary credentials for an assumed execution role.
type RoleCredentials struct {
	Expires         time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// RoleAssumer issues credentials for a state machine's execution role.
type RoleAssumer interface {
	AssumeExecutionRole(roleArn string) (RoleCredentials, error)
}

var errAssumeExecutionRole = errors.New("cannot assume the execution role")

type sdkAdapter struct {
	handler  http.Handler
	roles    RoleAssumer
	services map[string]sdkService
	clients  *safemap.Map[string, any]
	region   string
}

var _ asl.SDKIntegration = (*sdkAdapter)(nil)

// NewSDKIntegration creates the generic AWS SDK integration, serving the real
// SDK clients from handler (the in-process server) with static credentials.
func NewSDKIntegration(handler http.Handler, region string) asl.SDKIntegration {
	return NewSDKIntegrationWithRoles(handler, region, nil)
}

// NewSDKIntegrationWithRoles is NewSDKIntegration but signs every call with
// temporary credentials of the state machine's execution role from roles.
func NewSDKIntegrationWithRoles(handler http.Handler, region string, roles RoleAssumer) asl.SDKIntegration {
	return &sdkAdapter{
		handler:  handler,
		region:   region,
		roles:    roles,
		services: sdkServiceTable(),
		clients:  safemap.New[string, any]("stepfunctions-sdk-clients"),
	}
}

func (a *sdkAdapter) credentials(roleArn string) aws.CredentialsProvider {
	if a.roles == nil {
		return credentials.NewStaticCredentialsProvider(sdkCredential, sdkCredential, "")
	}

	return aws.NewCredentialsCache(
		aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			if roleArn == "" {
				return aws.Credentials{}, fmt.Errorf("%w: state machine has no RoleArn", errAssumeExecutionRole)
			}

			c, err := a.roles.AssumeExecutionRole(roleArn)
			if err != nil {
				return aws.Credentials{}, fmt.Errorf("%w %s: %w", errAssumeExecutionRole, roleArn, err)
			}

			return aws.Credentials{
				AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken,
				CanExpire: true, Expires: c.Expires,
			}, nil
		}),
		func(o *aws.CredentialsCacheOptions) { o.ExpiryWindow = credentialExpiryWindow },
	)
}

func (a *sdkAdapter) client(svc sdkService, name string, call asl.SDKCall) any {
	region := call.Region
	if region == "" {
		region = a.region
	}

	account := call.AccountID
	if account == config.DefaultAccountID {
		account = ""
	}

	key := strings.Join([]string{name, region, account, call.RoleArn}, "|")
	if c, ok := a.clients.Get(key); ok {
		return c
	}

	if a.clients.Len() >= maxCachedClients {
		a.clients.Clear()
	}

	c := svc.build(aws.Config{
		Region:       region,
		Credentials:  a.credentials(call.RoleArn),
		HTTPClient:   &inProcessClient{handler: a.handler, account: account},
		Retryer:      func() aws.Retryer { return aws.NopRetryer{} },
		BaseEndpoint: aws.String(sdkEndpoint),
	})
	a.clients.Set(key, c)

	return c
}

// SFNCallSDK implements asl.SDKIntegration.
func (a *sdkAdapter) SFNCallSDK(ctx context.Context, call asl.SDKCall) (any, error) {
	svc, ok := a.services[call.Service]
	if !ok {
		return nil, &asl.FailError{
			ErrCode: errCodeTaskFailed,
			Cause:   "AWS SDK integration is not available for service " + call.Service,
		}
	}

	prefix := svc.prefix
	if p := optimizedPrefix(call.Service); call.Optimized && p != "" {
		prefix = p
	}

	client := a.client(svc, call.Service, call)

	out, err := invokeSDKMethod(ctx, client, call.Service, call.Action, call.Params)
	if err != nil {
		return nil, sdkFailure(prefix, err)
	}

	spec, isSync := syncSpecFor(call.Service, call.Action)
	if !isSync || !strings.HasPrefix(call.Pattern, "sync") {
		return out, nil
	}

	return a.waitSync(ctx, syncRequest{
		spec: spec, client: client, call: call, started: out, prefix: prefix,
	})
}

// invokeSDKMethod calls client.<Action>(ctx, input) by reflection, decoding
// params into the SDK input and encoding the SDK output.
func invokeSDKMethod(ctx context.Context, client any, svc, action string, params any) (any, error) {
	method := reflect.ValueOf(client).MethodByName(upperFirst(action))
	if !method.IsValid() || method.Type().NumIn() < 2 || method.Type().In(1).Kind() != reflect.Pointer {
		return nil, &asl.FailError{
			ErrCode: errCodeRuntime,
			Cause:   fmt.Sprintf("unknown AWS SDK action %q for service %q", action, svc),
		}
	}

	in := reflect.New(method.Type().In(1).Elem())

	if err := decodeSDKInput(in, prepareParams(svc, action, params)); err != nil {
		return nil, &asl.FailError{ErrCode: errCodeRuntime, Cause: err.Error()}
	}

	res := method.Call([]reflect.Value{reflect.ValueOf(ctx), in})
	if errv, _ := reflect.TypeAssert[error](res[1]); errv != nil {
		return nil, errv
	}

	return shapeOutput(svc, action, res[0]), nil
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}

	r := []rune(s)
	r[0] = unicode.ToUpper(r[0])

	return string(r)
}

// prepareParams adapts Step Functions parameter conventions the generic
// decoder cannot infer from types alone.
func prepareParams(svc, action string, params any) any {
	m, ok := params.(map[string]any)
	if !ok {
		return params
	}

	switch svc + "." + action {
	case "lambda.invoke":
		return withJSONString(m, "Payload", true)
	case "sfn.startExecution", "sfn.startSyncExecution":
		return withJSONString(m, "Input", false)
	default:
		return params
	}
}

// withJSONString returns a copy of m whose key holds its value as a JSON
// string (base64 for blobs), as the integrations accept object values there.
func withJSONString(m map[string]any, key string, blob bool) map[string]any {
	val, has := m[key]
	if !has {
		return m
	}

	raw, isStr := val.(string)
	if !isStr {
		b, _ := json.Marshal(val)
		raw = string(b)
	}

	if blob {
		raw = base64.StdEncoding.EncodeToString([]byte(raw))
	}

	out := make(map[string]any, len(m))
	maps.Copy(out, m)
	out[key] = raw

	return out
}

// shapeOutput encodes an SDK output; Lambda Invoke's Payload stays an escaped
// JSON string, as the aws-sdk integration returns it.
func shapeOutput(svc, action string, res reflect.Value) any {
	out := encodeSDKOutput(res)

	m, ok := out.(map[string]any)
	if !ok || svc != "lambda" || action != "invoke" {
		return out
	}

	if f := reflect.Indirect(res).FieldByName("Payload"); f.IsValid() && f.Len() > 0 {
		m["Payload"] = string(f.Bytes())
	}

	return m
}

// sdkFailure maps an SDK error to <ServicePrefix>.<ErrorName>.
func sdkFailure(prefix string, err error) error {
	if fe, ok := errors.AsType[*asl.FailError](err); ok {
		return fe
	}

	if errors.Is(err, errAssumeExecutionRole) {
		return &asl.FailError{ErrCode: errCodePermissions, Cause: err.Error()}
	}

	var apiErr smithy.APIError

	if !errors.As(err, &apiErr) {
		return &asl.FailError{ErrCode: prefix + "." + sdkClientException, Cause: err.Error()}
	}

	code := modeledErrorCode(apiErr.ErrorCode())

	if !strings.HasSuffix(code, exceptionSuffix) {
		code += exceptionSuffix
	}

	cause := apiErr.ErrorMessage()

	if respErr, ok := errors.AsType[*awshttp.ResponseError](err); ok {
		cause = fmt.Sprintf(
			"%s (Service: %s, Status Code: %d, Request ID: %s)",
			cause, prefix, respErr.HTTPStatusCode(), respErr.ServiceRequestID(),
		)
	}

	return &asl.FailError{ErrCode: prefix + "." + code, Cause: cause}
}
