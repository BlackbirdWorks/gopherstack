package lambdaweb

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// LogSink delivers execution environment output to CloudWatch Logs.
type LogSink interface {
	EnsureLogGroupAndStream(ctx context.Context, region, group, stream string) error
	PutLogLines(ctx context.Context, region, group, stream string, lines []string) error
}

// Credentials are temporary credentials for an execution role.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// CredentialIssuer mints execution-role credentials for an execution environment.
type CredentialIssuer interface {
	IssueCredentials(roleARN, sessionName string) (Credentials, error)
}

// SetLogSink wires the CloudWatch Logs backend environments write to.
func (b *InMemoryBackend) SetLogSink(s LogSink) {
	b.mu.Lock("SetLogSink")
	defer b.mu.Unlock()

	b.logs = s
}

// SetCredentialIssuer wires STS-backed execution-role credentials into environments.
func (b *InMemoryBackend) SetCredentialIssuer(i CredentialIssuer) {
	b.mu.Lock("SetCredentialIssuer")
	defer b.mu.Unlock()

	b.creds = i
}

// SetEndpointURL sets the AWS_ENDPOINT_URL injected into environments.
func (b *InMemoryBackend) SetEndpointURL(u string) {
	b.mu.Lock("SetEndpointURL")
	defer b.mu.Unlock()

	b.endpointURL = u
}

// environmentServices returns the sibling services a launcher uses.
func (b *InMemoryBackend) environmentServices() (CodeStore, LogSink, CredentialIssuer, string) {
	b.mu.RLock("environmentServices")
	defer b.mu.RUnlock()

	return b.codes, b.logs, b.creds, b.endpointURL
}

// ServicePrincipal is the service principal an execution role's trust policy must allow.
const ServicePrincipal = "lambda.amazonaws.com"

// RoleTrust checks that a service principal may assume a role; the STS backend implements it.
type RoleTrust interface {
	CheckServiceRoleTrust(servicePrincipal, roleArn string) error
}

// KeyChecker validates that a KMS key can protect a revision: it exists, is enabled and is a symmetric
// encryption key.
type KeyChecker interface {
	CheckKey(ctx context.Context, region, keyARN string) error
}

var (
	errCannotAssumeRole = errors.New("lambda could not assume the execution role")
	errKMSDecryption    = errors.New("lambda could not use the KMS key")
)

// SetRoleTrust wires the IAM/STS trust check run when an execution environment starts.
func (b *InMemoryBackend) SetRoleTrust(t RoleTrust) {
	b.mu.Lock("SetRoleTrust")
	defer b.mu.Unlock()

	b.roleTrust = t
}

// SetKeyChecker wires the KMS check run at revision creation and when an execution environment starts.
func (b *InMemoryBackend) SetKeyChecker(k KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = k
}

// SetPrincipalResolver wires the resolver that maps a request's access key to its IAM principal.
func (b *InMemoryBackend) SetPrincipalResolver(r awsmeta.PrincipalResolver) {
	b.mu.Lock("SetPrincipalResolver")
	defer b.mu.Unlock()

	b.principals = r
}

// preflight runs before an environment starts: the execution role must be assumable by Lambda and the revision's
// KMS key usable.
func (b *InMemoryBackend) preflight(rev *Revision) error {
	b.mu.RLock("preflight")
	trust, keys := b.roleTrust, b.keys
	b.mu.RUnlock()

	if trust != nil {
		if err := trust.CheckServiceRoleTrust(ServicePrincipal, rev.ServiceConfig.ExecutionRoleArn); err != nil {
			return fmt.Errorf("%w: %w", errCannotAssumeRole, err)
		}
	}

	if keys != nil && rev.KmsKeyArn != "" {
		if err := keys.CheckKey(context.Background(), rev.Region, rev.KmsKeyArn); err != nil {
			return fmt.Errorf("%w: %w", errKMSDecryption, err)
		}
	}

	if rev.KmsKeyArn != "" {
		if err := roleauth.AuthorizeResource(
			b.authorizer(), roleauth.PrincipalLambda, "kms:Decrypt", rev.KmsKeyArn, rev.ARN,
		); err != nil {
			return fmt.Errorf("%w: %w", errKMSDecryption, err)
		}
	}

	return nil
}

// logsAllowed reports whether the revision's execution role may write the revision's log group; without that
// permission Lambda cannot deliver any log event.
func (b *InMemoryBackend) logsAllowed(rev *Revision) bool {
	auth := b.authorizer()
	if auth == nil {
		return true
	}

	group := arn.Build("logs", rev.Region, rev.AccountID, "log-group:"+logGroupOf(rev)+":*")

	for _, action := range []string{"logs:CreateLogStream", "logs:PutLogEvents"} {
		role := rev.ServiceConfig.ExecutionRoleArn
		if err := roleauth.Authorize(auth, roleauth.PrincipalLambda, role, action, group); err != nil {
			logger.Load(context.Background()).Warn(
				"lambdaweb: execution role cannot write the log group; log events are not delivered",
				"role", rev.ServiceConfig.ExecutionRoleArn, "logGroup", logGroupOf(rev), "action", action,
			)

			return false
		}
	}

	return true
}

// SetRoleAuthorizer enforces the Lambda service principal's and the execution role's permissions (KMS key policy,
// S3 bucket policy, CloudWatch Logs) under IAM enforcement.
func (b *InMemoryBackend) SetRoleAuthorizer(a roleauth.Authorizer) {
	b.mu.Lock("SetRoleAuthorizer")
	defer b.mu.Unlock()

	b.roleAuth = a
}

func (b *InMemoryBackend) authorizer() roleauth.Authorizer {
	b.mu.RLock("authorizer")
	defer b.mu.RUnlock()

	return b.roleAuth
}

// pendingRevisionARN stands for the revision ARN Lambda generates after the checks that carry it as aws:SourceArn.
func (b *InMemoryBackend) pendingRevisionARN(region, fn string) string {
	return revisionARN(region, b.accountID, fn, "pending")
}

// checkKey rejects a revision whose KMS key is missing, disabled, not a symmetric encryption key, or whose key
// policy does not let the Lambda service principal decrypt.
func (b *InMemoryBackend) checkKey(ctx context.Context, region, fn, keyARN string) error {
	if keyARN == "" {
		return nil
	}

	b.mu.RLock("checkKey")
	keys := b.keys
	b.mu.RUnlock()

	if keys != nil {
		if err := keys.CheckKey(ctx, region, keyARN); err != nil {
			return validationError("The KMS key %s cannot be used: %v", keyARN, err)
		}
	}

	if err := roleauth.AuthorizeResource(
		b.authorizer(), roleauth.PrincipalLambda, "kms:Decrypt", keyARN, b.pendingRevisionARN(region, fn),
	); err != nil {
		return accessDeniedError(
			"The key policy of %s does not allow %s to call kms:Decrypt.", keyARN, ServicePrincipal)
	}

	return nil
}
