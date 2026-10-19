package lambda

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	credentialRefreshMargin = 5 * time.Minute
	lambdaServicePrincipal  = "lambda.amazonaws.com"
	lambdaTaskRoot          = "/var/task"
	lambdaRuntimeDir        = "/var/runtime"
	latestVersion           = "$LATEST"
	tracingModeActive       = "Active"
	traceRootRandomBytes    = 12
	traceParentRandomBytes  = 8
)

// RoleCredentials are temporary credentials for a function's execution role.
type RoleCredentials struct {
	Expires         time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// RoleCredentialIssuer mints execution-role credentials for a function container.
type RoleCredentialIssuer interface {
	IssueFunctionCredentials(roleArn, sessionName string) (RoleCredentials, error)
}

// SetRoleCredentialIssuer wires STS-backed role credentials into function containers.
func (b *InMemoryBackend) SetRoleCredentialIssuer(i RoleCredentialIssuer) {
	b.mu.Lock("SetRoleCredentialIssuer")
	defer b.mu.Unlock()
	b.credIssuer = i
}

// SetEndpointURL sets the AWS_ENDPOINT_URL injected into function containers.
func (b *InMemoryBackend) SetEndpointURL(u string) {
	b.mu.Lock("SetEndpointURL")
	defer b.mu.Unlock()
	b.endpointURL = u
}

// EndpointURL returns the AWS_ENDPOINT_URL injected into function containers.
func (b *InMemoryBackend) EndpointURL() string {
	b.mu.RLock("EndpointURL")
	defer b.mu.RUnlock()

	return b.endpointURL
}

// ServicePrincipal is the service principal a function's execution role must trust.
func ServicePrincipal() string { return lambdaServicePrincipal }

func isReservedEnvKey(k string) bool {
	switch k {
	case "_HANDLER", "_X_AMZN_TRACE_ID", "AWS_DEFAULT_REGION", "AWS_REGION", "AWS_EXECUTION_ENV",
		"AWS_LAMBDA_FUNCTION_NAME", "AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "AWS_LAMBDA_FUNCTION_VERSION",
		"AWS_LAMBDA_INITIALIZATION_TYPE", "AWS_LAMBDA_LOG_GROUP_NAME", "AWS_LAMBDA_LOG_STREAM_NAME",
		"AWS_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_LAMBDA_RUNTIME_API", "LAMBDA_TASK_ROOT", "LAMBDA_RUNTIME_DIR", "AWS_LAMBDA_MAX_CONCURRENCY",
		"AWS_LAMBDA_METADATA_API", "AWS_LAMBDA_METADATA_TOKEN":
		return true
	}

	return false
}

// buildContainerEnv returns the container environment for fn plus the expiry of
// the injected role credentials (zero when none were injected). Reserved keys in
// the user's Environment are dropped; other user keys override the defaults.
func (b *InMemoryBackend) buildContainerEnv(
	ctx context.Context,
	fn *FunctionConfiguration,
	runtimePort int,
) ([]string, time.Time) {
	b.mu.RLock("buildContainerEnv")
	issuer := b.credIssuer
	endpoint := b.endpointURL
	b.mu.RUnlock()

	version := fn.Version
	if version == "" {
		version = latestVersion
	}

	vars := map[string]string{
		"AWS_LAMBDA_RUNTIME_API":          fmt.Sprintf("%s:%d", b.settings.DockerHost, runtimePort),
		"AWS_DEFAULT_REGION":              b.region,
		"AWS_REGION":                      b.region,
		"AWS_LAMBDA_FUNCTION_NAME":        fn.FunctionName,
		"AWS_LAMBDA_FUNCTION_VERSION":     version,
		"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": strconv.Itoa(fn.MemorySize),
		"AWS_LAMBDA_FUNCTION_TIMEOUT":     strconv.Itoa(fn.Timeout),
		"AWS_LAMBDA_INITIALIZATION_TYPE":  "on-demand",
		"AWS_LAMBDA_LOG_GROUP_NAME":       logGroupFor(fn),
		"AWS_LAMBDA_LOG_STREAM_NAME": time.Now().UTC().Format("2006/01/02") + "/[" + version + "]" +
			strings.ReplaceAll(uuid.NewString(), "-", ""),
		"LAMBDA_TASK_ROOT":   lambdaTaskRoot,
		"LAMBDA_RUNTIME_DIR": lambdaRuntimeDir,
		"TZ":                 ":UTC",
	}

	if fn.Handler != "" {
		vars["AWS_LAMBDA_FUNCTION_HANDLER"] = fn.Handler
		vars["_HANDLER"] = fn.Handler
	}

	if env := executionEnvFor(fn); env != "" {
		vars["AWS_EXECUTION_ENV"] = env
	}

	if endpoint != "" {
		vars["AWS_ENDPOINT_URL"] = endpoint
	}

	expires := b.injectCredentials(ctx, fn, issuer, vars)

	if fn.Environment != nil {
		for k, v := range fn.Environment.Variables {
			if !isReservedEnvKey(k) {
				vars[k] = v
			}
		}
	}

	b.applyDebugEnv(fn, vars)

	env := make([]string, 0, len(vars))
	for k, v := range vars {
		env = append(env, k+"="+v)
	}

	return env, expires
}

func (b *InMemoryBackend) injectCredentials(
	ctx context.Context,
	fn *FunctionConfiguration,
	issuer RoleCredentialIssuer,
	vars map[string]string,
) time.Time {
	if issuer == nil || fn.Role == "" {
		return time.Time{}
	}

	creds, err := issuer.IssueFunctionCredentials(fn.Role, fn.FunctionName)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "lambda: execution role credentials unavailable",
			"function", fn.FunctionName, "role", fn.Role, "error", err)

		return time.Time{}
	}

	vars["AWS_ACCESS_KEY_ID"] = creds.AccessKeyID
	vars["AWS_SECRET_ACCESS_KEY"] = creds.SecretAccessKey
	vars["AWS_SESSION_TOKEN"] = creds.SessionToken

	return creds.Expires
}

func logGroupFor(fn *FunctionConfiguration) string {
	if fn.LoggingConfig != nil && fn.LoggingConfig.LogGroup != "" {
		return fn.LoggingConfig.LogGroup
	}

	return "/aws/lambda/" + fn.FunctionName
}

// executionEnvFor returns AWS_EXECUTION_ENV; OS-only (provided) and image functions have none.
func executionEnvFor(fn *FunctionConfiguration) string {
	if fn.PackageType != PackageTypeZip || fn.Runtime == "" || strings.HasPrefix(fn.Runtime, "provided") {
		return ""
	}

	return "AWS_Lambda_" + fn.Runtime
}

// newTraceHeader returns a sampled X-Ray trace header for an Active-tracing invocation.
func newTraceHeader(now time.Time) string {
	root := make([]byte, traceRootRandomBytes)
	parent := make([]byte, traceParentRandomBytes)
	_, _ = rand.Read(root)
	_, _ = rand.Read(parent)

	return fmt.Sprintf("Root=1-%08x-%s;Parent=%s;Sampled=1",
		now.Unix(), hex.EncodeToString(root), hex.EncodeToString(parent))
}

func tracingActive(fn *FunctionConfiguration) bool {
	return fn.TracingConfig != nil && fn.TracingConfig.Mode == tracingModeActive
}

// recycleExpiringCredentialRuntime stops fn's warm runtime when its injected role
// credentials are about to expire, so the next start mints fresh ones.
func (b *InMemoryBackend) recycleExpiringCredentialRuntime(fn *FunctionConfiguration) {
	b.mu.RLock("recycleExpiringCredentialRuntime")
	rt := b.runtimes[fn.FunctionName]
	b.mu.RUnlock()

	if rt == nil {
		return
	}

	rt.mu.RLock("recycleExpiringCredentialRuntime")
	expiring := rt.started && rt.containerID != "" && !rt.credsExpire.IsZero() &&
		time.Until(rt.credsExpire) < credentialRefreshMargin
	rt.mu.RUnlock()

	if !expiring {
		return
	}

	b.mu.Lock("recycleExpiringCredentialRuntime")

	owned := b.runtimes[fn.FunctionName] == rt
	if owned {
		delete(b.runtimes, fn.FunctionName)
	}

	b.mu.Unlock()

	if !owned {
		return
	}

	ctx, cancel := context.WithTimeout(b.ctx, containerShutdownTimeout)
	defer cancel()

	b.cleanupRuntime(ctx, rt)
}
