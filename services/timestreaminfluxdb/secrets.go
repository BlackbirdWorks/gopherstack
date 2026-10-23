package timestreaminfluxdb

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const tokenLen = 64

// SecretStore holds the InfluxDB auth parameter secrets (a Secrets Manager adapter in the app).
type SecretStore interface {
	CreateSecret(region, name, secretString string) (string, error)
	DeleteSecret(region, secretARN string) error
}

// SetSecretStore enables the InfluxAuthParametersSecretArn secrets.
func (b *InMemoryBackend) SetSecretStore(s SecretStore) {
	b.mu.Lock("SetSecretStore")
	defer b.mu.Unlock()

	b.secrets = s
}

func newToken() string { return awsid.NewN("", tokenLen) }

type authSecret struct {
	Username     string `json:"username"`
	Password     string `json:"password"`
	Organization string `json:"organization"`
	Bucket       string `json:"bucket"`
	Token        string `json:"token"`
}

// authSecretLocked stores the initial credentials and returns the secret ARN; empty without a store.
func (b *InMemoryBackend) authSecretLocked(sc scope, id string, a authSecret) (string, error) {
	if b.secrets == nil {
		return "", nil
	}

	body, err := json.Marshal(a) //nolint:gosec // the secret holds the credentials by design
	if err != nil {
		return "", fail(ErrInternalServer, "encode auth secret")
	}

	arn, err := b.secrets.CreateSecret(sc.region, "timestream-influxdb!"+id, string(body))
	if err != nil {
		logger.Load(b.bgOrBackground()).WarnContext(b.bgOrBackground(),
			"timestreaminfluxdb: auth secret creation failed", "resource", id, "error", err)

		return "", fail(ErrInternalServer, "create the InfluxDB auth parameters secret")
	}

	return arn, nil
}

func (b *InMemoryBackend) deleteSecretLocked(region, arn string) {
	if b.secrets == nil || arn == "" {
		return
	}

	if err := b.secrets.DeleteSecret(region, arn); err != nil {
		logger.Load(b.bgOrBackground()).WarnContext(b.bgOrBackground(),
			"timestreaminfluxdb: auth secret deletion failed", "error", err)
	}
}

func (b *InMemoryBackend) deleteAllSecretsLocked() {
	for _, i := range b.instances.All() {
		b.deleteSecretLocked(i.Region, i.SecretARN)
	}

	for _, c := range b.clusters.All() {
		b.deleteSecretLocked(c.Region, c.SecretARN)
	}
}
