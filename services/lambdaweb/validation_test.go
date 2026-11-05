package lambdaweb_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errNoRole = errors.New("role does not exist")
	errNoKey  = errors.New("key is disabled")
)

type fakeTrust struct {
	err       atomic.Pointer[error]
	principal atomic.Pointer[string]
	role      atomic.Pointer[string]
}

func (f *fakeTrust) CheckServiceRoleTrust(principal, roleARN string) error {
	f.principal.Store(&principal)
	f.role.Store(&roleARN)

	if e := f.err.Load(); e != nil {
		return *e
	}

	return nil
}

type fakeKeys struct {
	err atomic.Pointer[error]
}

func (f *fakeKeys) CheckKey(context.Context, string, string) error {
	if e := f.err.Load(); e != nil {
		return *e
	}

	return nil
}

func TestValidation_ExecutionRoleMustBeAssumable(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)
	trust := &fakeTrust{}
	d.backend.SetRoleTrust(trust)

	trust.err.Store(&errNoRole)

	resp := d.get(t)
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Equal(t, "Platform.CannotAssumeRole", resp.Header.Get("Lambda-Web-Error-Type"))
	assert.Equal(t, int32(0), d.launcher.launches.Load(), "no environment starts without a usable role")
	assert.Equal(t, "lambda.amazonaws.com", *trust.principal.Load())
	assert.Equal(t, testRoleARN, *trust.role.Load())

	var none error

	trust.err.Store(&none)
	assert.Equal(t, http.StatusOK, d.get(t).StatusCode, "fixing the trust policy recovers the next request")
}

func TestValidation_KeyCheckedAtRevisionCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
		code string
	}{
		{name: "usable", err: nil},
		{name: "disabled", err: errNoKey, code: "ValidationException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			keys := &fakeKeys{}
			env.backend.SetKeyChecker(keys)

			if tc.err != nil {
				keys.err.Store(&tc.err)
			}

			rc := revisionConfig()
			rc.KmsKeyArn = aws.String("arn:aws:kms:us-east-1:123456789012:key/abc")

			_, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
				FunctionName: aws.String("app"), RevisionConfig: rc, EndpointConfig: endpointConfig(),
			})
			if tc.code == "" {
				require.NoError(t, err)

				return
			}

			assertAPIErrorCode(t, err, tc.code)

			env.createFunctionWithoutKey(t)

			_, err = env.client.CreateWebFunctionRevision(t.Context(), &lwsdk.CreateWebFunctionRevisionInput{
				FunctionName: aws.String("plain"), BuildConfig: rc.BuildConfig, ServiceConfig: rc.ServiceConfig,
				KmsKeyArn: rc.KmsKeyArn,
			})
			assertAPIErrorCode(t, err, tc.code)
		})
	}
}

func (e *testEnv) createFunctionWithoutKey(t *testing.T) {
	t.Helper()

	_, err := e.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("plain"), RevisionConfig: revisionConfig(), EndpointConfig: endpointConfig(),
	})
	require.NoError(t, err)
}

func TestValidation_KeyDisabledAfterCreationFailsLaunch(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	keys := &fakeKeys{}
	env.backend.SetKeyChecker(keys)

	l := &fakeLauncher{}
	env.backend.SetLauncher(l)

	rc := revisionConfig()
	rc.KmsKeyArn = aws.String("arn:aws:kms:us-east-1:123456789012:key/abc")

	created, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("app"), RevisionConfig: rc, EndpointConfig: endpointConfig(),
	})
	require.NoError(t, err)
	env.settle()

	keys.err.Store(&errNoKey)

	d := &dataEnv{testEnv: env, launcher: l, domain: aws.ToString(created.Endpoint.DomainName), http: &http.Client{}}
	resp := d.get(t)

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Equal(t, "Platform.KMSDecryptionError", resp.Header.Get("Lambda-Web-Error-Type"))
	assert.Equal(t, int32(0), l.launches.Load())
}
