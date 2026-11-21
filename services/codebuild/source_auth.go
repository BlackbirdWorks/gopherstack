package codebuild

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	authTypeCodeConnections = "CODECONNECTIONS"
	authTypeSecretsManager  = "SECRETS_MANAGER"
	connectionAvailable     = "AVAILABLE"
	maxSecretPayload        = 64 * 1024
)

var (
	errAuthConnection = errors.New("source auth connection is unusable")
	errAuthSecret     = errors.New("source auth secret is unusable")
	errAuthScope      = errors.New("source auth secret is out of scope")
)

// ConnectionResolver reports the status of a CodeConnections connection.
type ConnectionResolver interface {
	ConnectionStatus(ctx context.Context, region, connectionArn string) (string, error)
}

// validateSourceAuth checks ProjectSource.auth: the type is required and the resource of a
// CODECONNECTIONS or SECRETS_MANAGER auth is the connection or secret ARN (ImportSourceCredentials token docs).
func validateSourceAuth(field string, a SourceAuth) error {
	if a.Type == "" && a.Resource == "" {
		return nil
	}

	if a.Type == "" {
		return fmt.Errorf("%w: %s.type is required", ErrValidation, field)
	}

	if err := checkEnum(field+".type", cbtypes.SourceAuthType(a.Type)); err != nil {
		return err
	}

	switch a.Type {
	case authTypeCodeConnections:
		if !arn.HasService(a.Resource, "codeconnections") && !arn.HasService(a.Resource, "codestar-connections") {
			return fmt.Errorf("%w: %s.resource must be a connection ARN", ErrValidation, field)
		}
	case authTypeSecretsManager:
		if !arn.HasService(a.Resource, "secretsmanager") {
			return fmt.Errorf("%w: %s.resource must be a Secrets Manager secret ARN", ErrValidation, field)
		}
	}

	return nil
}

func validateSourcesAuth(src *ProjectSource, secondary []ProjectSource) error {
	if src != nil {
		if err := validateSourceAuth("source.auth", src.Auth); err != nil {
			return err
		}
	}

	for i := range secondary {
		if err := validateSourceAuth(fmt.Sprintf("secondarySources[%d].auth", i), secondary[i].Auth); err != nil {
			return err
		}
	}

	return nil
}

type sourceSecret struct {
	ServerType string `json:"ServerType"`
	AuthType   string `json:"AuthType"`
	Token      string `json:"Token"`
	Username   string `json:"Username"`
}

// resolveSourceAuth returns the clone credentials of src: from its Secrets Manager secret, or, for OAUTH and
// no auth, from the imported credentials of its server type. A CODECONNECTIONS connection must be AVAILABLE.
func (r *buildRunner) resolveSourceAuth(ctx context.Context, src *ProjectSource) (string, string, error) {
	auth := src.Auth

	if auth.Type != authTypeSecretsManager && auth.Type != authTypeCodeConnections {
		if token, user, ok := r.b.sourceToken(src.Type); ok {
			return token, user, nil
		}

		imported, ok := r.b.importedAuth(src.Type)
		if !ok {
			return "", "", nil
		}

		auth = imported
	}

	switch auth.Type {
	case authTypeSecretsManager:
		return r.secretSourceCredentials(ctx, src, auth.Resource)
	default:
		return "", "", r.checkConnection(ctx, src, auth.Resource)
	}
}

func (r *buildRunner) secretSourceCredentials(
	ctx context.Context, src *ProjectSource, resource string,
) (string, string, error) {
	if r.svc.Values == nil {
		return "", "", fmt.Errorf("%w: Secrets Manager", errSourceDependency)
	}

	if err := r.sameAccountRegion(resource); err != nil {
		return "", "", fmt.Errorf("%w: %w", errAuthSecret, err)
	}

	raw, err := r.svc.Values.SecretValue(ctx, r.region, resource, "", "")
	if err != nil {
		return "", "", fmt.Errorf("%w: cannot read secret %s: %w", errAuthSecret, resource, err)
	}

	if len(raw) > maxSecretPayload {
		return "", "", fmt.Errorf("%w: secret payload is too large", errAuthSecret)
	}

	var sec sourceSecret
	if jerr := json.Unmarshal([]byte(raw), &sec, jsonx.V1()); jerr != nil {
		return "", "", fmt.Errorf("%w: secret is not the documented JSON object", errAuthSecret)
	}

	if sec.Token == "" {
		return "", "", fmt.Errorf("%w: secret has no Token", errAuthSecret)
	}

	if sec.ServerType != src.Type {
		return "", "", fmt.Errorf("%w: secret ServerType %q does not match source type %s",
			errAuthSecret, sec.ServerType, src.Type)
	}

	if authType := cbtypes.AuthType(sec.AuthType); authType != cbtypes.AuthTypePersonalAccessToken &&
		authType != cbtypes.AuthTypeBasicAuth {
		return "", "", fmt.Errorf("%w: secret AuthType %q must be PERSONAL_ACCESS_TOKEN or BASIC_AUTH",
			errAuthSecret, sec.AuthType)
	}

	return sec.Token, sec.Username, nil
}

func (r *buildRunner) checkConnection(ctx context.Context, src *ProjectSource, resource string) error {
	if r.svc.Connections == nil {
		return fmt.Errorf("%w: CodeConnections", errSourceDependency)
	}

	if err := r.sameAccountRegion(resource); err != nil {
		return fmt.Errorf("%w: %w", errAuthConnection, err)
	}

	status, err := r.svc.Connections.ConnectionStatus(ctx, r.region, resource)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", errAuthConnection, resource, err)
	}

	if status != connectionAvailable {
		return fmt.Errorf("%w: %s is %s, not %s", errAuthConnection, resource, status, connectionAvailable)
	}

	logger.Load(ctx).InfoContext(ctx, "codebuild: would obtain a provider token from the connection",
		"connection", resource, "serverType", src.Type, "cloning", "without a token")

	return nil
}

// sameAccountRegion requires a secret ARN of this project's account and Region (asm-create-secret docs).
func (r *buildRunner) sameAccountRegion(resource string) error {
	parsed, err := awsarn.Parse(resource)
	if err != nil {
		return fmt.Errorf("%w: %q", ErrValidation, resource)
	}

	if parsed.Region != r.region || parsed.AccountID != r.b.accountID {
		return fmt.Errorf("%w: source auth resource must be in account %s and Region %s",
			errAuthScope, r.b.accountID, r.region)
	}

	return nil
}
