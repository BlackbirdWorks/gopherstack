package dmsengine

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Errors reported while opening endpoints.
var (
	ErrUnsupportedEngine = errors.New("unsupported endpoint engine")
	ErrNotConfigured     = errors.New("in-process target not available")
)

// DefaultConnectTimeout bounds each new database connection.
const DefaultConnectTimeout = 10 * time.Second

// EndpointConfig is the engine's view of a DMS endpoint.
type EndpointConfig struct {
	Engine   string
	Server   string
	Database string
	User     string
	Password string
	SSLMode  string
	// CertificatePEM is the endpoint's CertificateArn contents, trusted when SslMode asks for TLS.
	CertificatePEM string
	// SlotName names the PostgreSQL logical replication slot for source endpoints.
	SlotName string
	// S3Settings, KinesisSettings and DynamoDBSettings carry the raw settings JSON objects.
	S3Settings       string
	KinesisSettings  string
	DynamoDBSettings string
	Port             int
	// PollInterval spaces PostgreSQL change polls; zero picks a default.
	PollInterval time.Duration
}

func (c EndpointConfig) addr() string {
	if c.Port == 0 {
		return c.Server
	}

	return fmt.Sprintf("%s:%d", c.Server, c.Port)
}

// ObjectStore writes objects into the emulator's S3.
type ObjectStore interface {
	PutObject(ctx context.Context, bucket, key string, body []byte) error
	CheckBucket(ctx context.Context, bucket string) error
}

// StreamStore writes records into the emulator's Kinesis.
type StreamStore interface {
	PutRecord(ctx context.Context, stream, partitionKey string, data []byte) error
	CheckStream(ctx context.Context, stream string) error
}

// DynamoKey is one key attribute of a target table; Number selects type N over S.
type DynamoKey struct {
	Name   string
	Number bool
}

// DynamoValue is a typed attribute value: N, S or B.
type DynamoValue struct {
	Type string
	S    string
	B    []byte
}

// DynamoStore writes items into the emulator's DynamoDB.
type DynamoStore interface {
	CreateTable(ctx context.Context, name string, keys []DynamoKey) error
	DropTable(ctx context.Context, name string) error
	PutItem(ctx context.Context, table string, item map[string]DynamoValue) error
	DeleteItem(ctx context.Context, table string, key map[string]DynamoValue) error
}

// Stores bundles the in-process services DMS targets can write to.
type Stores struct {
	Objects ObjectStore
	Streams StreamStore
	Dynamo  DynamoStore
}

// Opener builds sources and targets for endpoints.
type Opener interface {
	OpenSource(ctx context.Context, ep EndpointConfig) (Source, error)
	OpenTarget(ctx context.Context, ep EndpointConfig, src Family) (Target, error)
	// ReleaseSlot drops the replication slot a source endpoint created, if any.
	ReleaseSlot(ctx context.Context, ep EndpointConfig) error
}

// SupportedSource reports whether the engine can read from the endpoint engine.
func SupportedSource(engine string) bool {
	f := FamilyOf(engine)

	return f == FamilyMySQL || f == FamilyPostgres
}

// SupportedTarget reports whether the engine can write to the endpoint engine.
func SupportedTarget(engine string) bool {
	return FamilyOf(engine) != FamilyOther
}

type defaultOpener struct{ stores Stores }

// NewOpener returns the production Opener, writing in-process targets through stores.
func NewOpener(stores Stores) Opener { return &defaultOpener{stores: stores} }

func (o *defaultOpener) OpenSource(ctx context.Context, ep EndpointConfig) (Source, error) {
	switch FamilyOf(ep.Engine) {
	case FamilyMySQL:
		return openMySQLSource(ctx, ep)
	case FamilyPostgres:
		return openPGSource(ctx, ep)
	default:
		return nil, fmt.Errorf("%w: %s as a source", ErrUnsupportedEngine, ep.Engine)
	}
}

func (o *defaultOpener) OpenTarget(ctx context.Context, ep EndpointConfig, src Family) (Target, error) {
	switch FamilyOf(ep.Engine) {
	case FamilyMySQL:
		return openSQLTarget(ctx, ep, FamilyMySQL, src)
	case FamilyPostgres:
		return openSQLTarget(ctx, ep, FamilyPostgres, src)
	case FamilyS3:
		return newS3Target(ep, o.stores.Objects)
	case FamilyKinesis:
		return newKinesisTarget(ep, o.stores.Streams)
	case FamilyDynamoDB:
		return newDynamoTarget(o.stores.Dynamo)
	default:
		return nil, fmt.Errorf("%w: %s as a target", ErrUnsupportedEngine, ep.Engine)
	}
}

func (o *defaultOpener) ReleaseSlot(ctx context.Context, ep EndpointConfig) error {
	if FamilyOf(ep.Engine) != FamilyPostgres || ep.SlotName == "" {
		return nil
	}

	src, err := openPGSource(ctx, ep)
	if err != nil {
		return err
	}
	defer src.Close()

	return src.Release(ctx)
}

// TestEndpoint opens the endpoint and verifies it is reachable, returning the driver's own error on failure.
func TestEndpoint(ctx context.Context, o Opener, ep EndpointConfig, source bool) error {
	if source {
		s, err := o.OpenSource(ctx, ep)
		if err != nil {
			return err
		}
		defer s.Close()

		return s.Ping(ctx)
	}

	t, err := o.OpenTarget(ctx, ep, FamilyOther)
	if err != nil {
		return err
	}
	defer t.Close()

	return t.Ping(ctx)
}

func tlsRequired(mode string) bool {
	switch strings.ToLower(mode) {
	case sslRequire, sslVerifyCA, sslVerifyFull:
		return true
	}

	return false
}

const (
	sslRequire    = "require"
	sslVerifyCA   = "verify-ca"
	sslVerifyFull = "verify-full"
)

// ErrCertificate reports an endpoint CertificateArn whose PEM holds no usable certificate.
var ErrCertificate = errors.New("endpoint certificate is not a valid PEM CA bundle")

// tlsConfigFor builds the client TLS settings for the endpoint's SslMode: nil for none, otherwise a verified
// connection trusting the endpoint's CertificatePEM, or the system roots when none was supplied. The server
// certificate must match the endpoint's server name in every TLS mode.
func tlsConfigFor(ep EndpointConfig) (*tls.Config, error) {
	if !tlsRequired(ep.SSLMode) {
		return nil, nil //nolint:nilnil // nil config means plaintext
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: ep.Server}

	if strings.TrimSpace(ep.CertificatePEM) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ep.CertificatePEM)) {
			return nil, ErrCertificate
		}

		cfg.RootCAs = pool
	}

	return cfg, nil
}

// explainTLS adds guidance to a certificate-verification failure.
func explainTLS(ep EndpointConfig, err error) error {
	if err == nil || !tlsRequired(ep.SSLMode) || !strings.Contains(err.Error(), "x509:") {
		return err
	}

	return fmt.Errorf("SslMode %s needs a trusted server certificate: attach a certificate (CertificateArn) "+
		"that signed the server's certificate, or use a publicly trusted one: %w", ep.SSLMode, err)
}
