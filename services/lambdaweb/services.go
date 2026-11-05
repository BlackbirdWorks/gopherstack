package lambdaweb

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
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

	return nil
}

// checkKey rejects a revision whose KMS key is missing, disabled or not a symmetric encryption key.
func (b *InMemoryBackend) checkKey(ctx context.Context, region, keyARN string) error {
	if keyARN == "" {
		return nil
	}

	b.mu.RLock("checkKey")
	keys := b.keys
	b.mu.RUnlock()

	if keys == nil {
		return nil
	}

	if err := keys.CheckKey(ctx, region, keyARN); err != nil {
		return validationError("The KMS key %s cannot be used: %v", keyARN, err)
	}

	return nil
}
