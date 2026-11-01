package lambdaweb

import "context"

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
