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
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire"
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
	svcBedrockRuntime      = "bedrockruntime"
	actionInvoke           = "invoke"
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
	case svcBedrockRuntime:
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
	conns    ConnectionResolver
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
	return NewSDKIntegrationWithHTTP(handler, region, roles, nil)
}

// NewSDKIntegrationWithHTTP is NewSDKIntegrationWithRoles that also resolves
// EventBridge connections for http:invoke Task states from conns.
func NewSDKIntegrationWithHTTP(
	handler http.Handler, region string, roles RoleAssumer, conns ConnectionResolver,
) asl.SDKIntegration {
	return &sdkAdapter{
		handler:  handler,
		region:   region,
		roles:    roles,
		conns:    conns,
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

// sdkInvoker calls one action of a service with Step Functions parameters.
type sdkInvoker func(ctx context.Context, action string, params any) (any, error)

func (a *sdkAdapter) invoker(svc sdkService, name string, call asl.SDKCall) sdkInvoker {
	region := call.Region
	if region == "" {
		region = a.region
	}

	account := call.AccountID
	if account == config.DefaultAccountID {
		account = ""
	}

	if ws, ok := sdkwire.Lookup(name); ok {
		return a.wireInvoker(ws, name, sdkwire.Target{
			Handler:     a.handler,
			Credentials: a.cachedCredentials(call.RoleArn),
			Endpoint:    sdkEndpoint,
			Region:      region,
			Account:     account,
		})
	}

	client := a.legacyClient(svc, name, region, account, call.RoleArn)

	return func(ctx context.Context, action string, params any) (any, error) {
		return invokeSDKMethod(ctx, client, name, action, params)
	}
}

func (a *sdkAdapter) wireInvoker(ws *sdkwire.Service, name string, tgt sdkwire.Target) sdkInvoker {
	return func(ctx context.Context, action string, params any) (any, error) {
		out, err := ws.Call(ctx, tgt, upperFirst(action), prepareParams(name, action, params))
		if err != nil {
			return nil, wireError(name, action, err)
		}

		return shapeWireOutput(name, action, out), nil
	}
}

func wireError(svc, action string, err error) error {
	switch {
	case errors.Is(err, sdkwire.ErrUnknownOp):
		return &asl.FailError{
			ErrCode: errCodeRuntime,
			Cause:   fmt.Sprintf("unknown AWS SDK action %q for service %q", action, svc),
		}
	case errors.Is(err, sdkwire.ErrBadValue), errors.Is(err, sdkwire.ErrEventStream):
		return &asl.FailError{ErrCode: errCodeRuntime, Cause: err.Error()}
	default:
		return err
	}
}

func (a *sdkAdapter) cachedCredentials(roleArn string) aws.CredentialsProvider {
	key := "credentials|" + roleArn
	if c, found := a.clients.Get(key); found {
		if p, isProvider := c.(aws.CredentialsProvider); isProvider {
			return p
		}
	}

	p := a.credentials(roleArn)
	a.clients.Set(key, p)

	return p
}

func (a *sdkAdapter) legacyClient(svc sdkService, name, region, account, roleArn string) any {
	key := strings.Join([]string{name, region, account, roleArn}, "|")
	if c, ok := a.clients.Get(key); ok {
		return c
	}

	if a.clients.Len() >= maxCachedClients {
		a.clients.Clear()
	}

	c := svc.build(aws.Config{
		Region:       region,
		Credentials:  a.credentials(roleArn),
		HTTPClient:   &inProcessClient{handler: a.handler, account: account},
		Retryer:      func() aws.Retryer { return aws.NopRetryer{} },
		BaseEndpoint: aws.String(sdkEndpoint),
	})
	a.clients.Set(key, c)

	return c
}

// SFNCallSDK implements asl.SDKIntegration.
func (a *sdkAdapter) SFNCallSDK(ctx context.Context, call asl.SDKCall) (any, error) {
	if call.Service == httpService {
		return a.invokeHTTP(ctx, call)
	}

	if call.Service == "eks" && call.Action == eksCallAction {
		return a.invokeEKSCall(ctx, call)
	}

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

	invoke := a.invoker(svc, call.Service, call)

	out, err := invoke(ctx, call.Action, call.Params)
	if err != nil {
		return nil, sdkFailure(prefix, err)
	}

	out = withStartedFields(call, out)

	if failure := runTaskFailures(call, out); failure != nil {
		return nil, failure
	}

	spec, isSync := syncSpecFor(call.Service, call.Action)
	if !isSync || !strings.HasPrefix(call.Pattern, "sync") {
		return out, nil
	}

	return a.waitSync(ctx, syncRequest{
		spec: spec, invoke: invoke, call: call, started: out, prefix: prefix,
	})
}

// invokeSDKMethod calls action on a generated SDK client through the static
// table, decoding params into the SDK input and encoding the SDK output.
func invokeSDKMethod(ctx context.Context, client any, svc, action string, params any) (any, error) {
	call, ok := sdkOpTable(svc)[lowerFirst(action)]
	if !ok {
		return nil, &asl.FailError{
			ErrCode: errCodeRuntime,
			Cause:   fmt.Sprintf("unknown AWS SDK action %q for service %q", action, svc),
		}
	}

	return call(ctx, client, params)
}

func invokeTyped[I, O, Opt any](
	ctx context.Context, fn func(context.Context, *I, ...func(*Opt)) (*O, error), params any,
) (any, error) {
	in := new(I)

	if err := decodeSDKInput(reflect.ValueOf(in), params); err != nil {
		return nil, &asl.FailError{ErrCode: errCodeRuntime, Cause: err.Error()}
	}

	out, err := fn(ctx, in)
	if err != nil {
		return nil, err
	}

	return encodeSDKOutput(reflect.ValueOf(out)), nil
}

// shapeWireOutput applies the per-action output conventions of shapeOutput to
// a decoded response.
func shapeWireOutput(svc, action string, out any) any {
	m, ok := out.(map[string]any)
	if !ok {
		return out
	}

	switch {
	case svc == svcBedrockRuntime && action == "invokeModel":
		return invokeModelOutput(m)
	case svc == awsServiceLambda && action == actionInvoke:
		if enc, _ := m["Payload"].(string); enc != "" {
			if raw, err := base64.StdEncoding.DecodeString(enc); err == nil {
				m["Payload"] = string(raw)
			}
		}
	}

	return m
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}

	r := []rune(s)
	r[0] = unicode.ToLower(r[0])

	return string(r)
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
	case svcBedrockRuntime + ".invokeModel":
		return withJSONString(m, "Body", true)
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
