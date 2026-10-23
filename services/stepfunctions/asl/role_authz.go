package asl

import (
	"context"
	"errors"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// SetRoleAuthorizer makes direct (non-SDK) service calls run under the execution role's policies.
func (e *Executor) SetRoleAuthorizer(a roleauth.Authorizer) { e.auth = a }

// authorizeRole checks the state machine role may perform action on resource.
func (e *Executor) authorizeRole(errPrefix, action, resource string) error {
	if e.auth == nil {
		return nil
	}

	err := e.auth.AuthorizeRole(roleauth.PrincipalStates, e.execMeta.RoleArn, action, resource)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, roleauth.ErrNotAssumable):
		return &FailError{ErrCode: errCodeStatesPermissions, Cause: err.Error()}
	default:
		return &FailError{ErrCode: errPrefix + ".AccessDeniedException", Cause: err.Error()}
	}
}

// resourceARN expands a bare name into an ARN of the given service and resource type.
func (e *Executor) resourceARN(service, kind, name string) string {
	if strings.HasPrefix(name, "arn:") {
		return name
	}

	region, account := e.regionAndAccount()

	return "arn:aws:" + service + ":" + region + ":" + account + ":" + kind + name
}

const eventsConnectionDenied = "Events.ConnectionResource.AccessDenied"

// authorizeHTTPTask checks the role may invoke the endpoint and read the connection's secret.
func (e *Executor) authorizeHTTPTask(input any) error {
	if e.auth == nil {
		return nil
	}

	if err := e.authorizeRole("States", "states:InvokeHTTPEndpoint", e.execMeta.StateMachineArn); err != nil {
		return asPermissions(err)
	}

	connARN := httpConnectionARN(input)
	if connARN == "" {
		return nil
	}

	region, account := e.regionAndAccount()
	_, name, _ := strings.Cut(connARN, "connection/")
	secretARN := arn.Build("secretsmanager", region, account, "secret:events!connection/"+name)

	for _, check := range [][2]string{
		{"events:RetrieveConnectionCredentials", connARN},
		{"secretsmanager:GetSecretValue", secretARN},
		{"secretsmanager:DescribeSecret", secretARN},
	} {
		if err := e.authorizeRole("Events", check[0], check[1]); err != nil {
			return connectionDenied(err)
		}
	}

	return nil
}

func asPermissions(err error) error {
	if fe, ok := errors.AsType[*FailError](err); ok {
		return &FailError{ErrCode: errCodeStatesPermissions, Cause: fe.Cause}
	}

	return err
}

func connectionDenied(err error) error {
	fe, ok := errors.AsType[*FailError](err)
	if !ok || fe.ErrCode == errCodeStatesPermissions {
		return err
	}

	return &FailError{ErrCode: eventsConnectionDenied, Cause: fe.Cause}
}

// httpConnectionARN returns Authentication or InvocationConfig ConnectionArn from HTTP Task parameters.
func httpConnectionARN(input any) string {
	params, _ := input.(map[string]any)

	for _, key := range []string{"InvocationConfig", "Authentication"} {
		if cfg, ok := params[key].(map[string]any); ok {
			if s, _ := cfg["ConnectionArn"].(string); strings.Contains(s, "connection/") {
				return s
			}
		}
	}

	return ""
}

// getS3Object reads an object under the execution role's s3:GetObject permission.
func (e *Executor) getS3Object(ctx context.Context, bucket, key string) ([]byte, error) {
	if err := e.authorizeRole("S3", "s3:GetObject", arn.BuildS3(bucket+"/"+key)); err != nil {
		return nil, err
	}

	return e.s3.GetObjectBytes(ctx, bucket, key)
}

// putS3Object writes an object under the execution role's s3:PutObject permission.
func (e *Executor) putS3Object(ctx context.Context, bucket, key string, data []byte) error {
	if err := e.authorizeRole("S3", "s3:PutObject", arn.BuildS3(bucket+"/"+key)); err != nil {
		return err
	}

	return e.s3w.PutObjectBytes(ctx, bucket, key, data)
}
