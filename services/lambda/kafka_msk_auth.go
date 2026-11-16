package lambda

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	mskaws "github.com/twmb/franz-go/pkg/sasl/aws"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	clientCertAccessType = "CLIENT_CERTIFICATE_TLS_AUTH"
	credentialRefresh    = time.Minute
)

var (
	errNoRoleCredentials = errors.New("no execution role credentials available for function")
	errNoSecretResolver  = errors.New("no secret resolver configured")
	errClientCertSecret  = errors.New("client certificate secret needs certificate and privateKey")
)

// mskIAMMechanism signs AWS_MSK_IAM payloads for region whatever the broker's advertised host is: the emulated
// brokers are reached by IP, which franz-go's own mechanism cannot derive a region from.
type mskIAMMechanism struct {
	inner  sasl.Mechanism
	region string
}

func newMSKIAMMechanism(region string, creds func(context.Context) (mskaws.Auth, error)) sasl.Mechanism {
	return mskIAMMechanism{inner: mskaws.ManagedStreamingIAM(creds), region: region}
}

func (m mskIAMMechanism) Name() string { return m.inner.Name() }

func (m mskIAMMechanism) Authenticate(ctx context.Context, _ string) (sasl.Session, []byte, error) {
	return m.inner.Authenticate(ctx, "b-1.msk.kafka."+m.region+".amazonaws.com:9098")
}

func (b *InMemoryBackend) issueRoleCredentials(functionARN string) (RoleCredentials, error) {
	fn, err := b.GetFunction(functionARN)
	if err != nil {
		return RoleCredentials{}, err
	}

	b.mu.RLock("issueRoleCredentials")
	issuer := b.credIssuer
	b.mu.RUnlock()

	if issuer == nil || fn.Role == "" {
		return RoleCredentials{}, errNoRoleCredentials
	}

	return issuer.IssueFunctionCredentials(fn.Role, fn.FunctionName)
}

// iamCredentials returns the function's execution role credentials, reissued shortly before they expire.
func (p *EventSourcePoller) iamCredentials(functionARN string) func(context.Context) (mskaws.Auth, error) {
	var cached atomic.Pointer[RoleCredentials]

	return func(context.Context) (mskaws.Auth, error) {
		creds := cached.Load()
		if creds == nil || time.Until(creds.Expires) < credentialRefresh {
			fresh, err := p.lambdaBackend.issueRoleCredentials(functionARN)
			if err != nil {
				return mskaws.Auth{}, err
			}

			creds = &fresh
			cached.Store(creds)
		}

		return mskaws.Auth{
			AccessKey: creds.AccessKeyID, SecretKey: creds.SecretAccessKey, SessionToken: creds.SessionToken,
		}, nil
	}
}

// clientCertificate reads the CLIENT_CERTIFICATE_TLS_AUTH secret ({"certificate": PEM, "privateKey": PEM}).
func (p *EventSourcePoller) clientCertificate(
	ctx context.Context, access []SourceAccessConfiguration,
) ([]byte, []byte, error) {
	var uri string

	for _, a := range access {
		if a.Type == clientCertAccessType {
			uri = a.URI
		}
	}

	p.mu.RLock("clientCertificate")
	secrets := p.mqSecrets
	p.mu.RUnlock()

	if secrets == nil || uri == "" {
		return nil, nil, errNoSecretResolver
	}

	raw, err := secrets.SecretString(ctx, uri)
	if err != nil {
		return nil, nil, fmt.Errorf("read client certificate secret: %w", err)
	}

	var doc struct {
		Certificate string `json:"certificate"`
		PrivateKey  string `json:"privateKey"`
	}

	if json.Unmarshal([]byte(raw), &doc, jsonx.V1()) != nil || doc.Certificate == "" || doc.PrivateKey == "" {
		return nil, nil, errClientCertSecret
	}

	return []byte(doc.Certificate), []byte(doc.PrivateKey), nil
}
