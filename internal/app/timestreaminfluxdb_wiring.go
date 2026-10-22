package app

import (
	"path/filepath"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	timestreaminfluxdbbackend "github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
)

// GetTimestreamInfluxDBDataDir keeps engine-backed instance data and backups under the data dir when --persist
// is on (timestreaminfluxdb.DataDirConfig).
func (c *CLI) GetTimestreamInfluxDBDataDir() string {
	if !c.Persist {
		return ""
	}

	return filepath.Join(c.resolvedDataDir(), "timestream-influxdb")
}

// influxAuthSecretAdapter stores InfluxAuthParametersSecretArn secrets in Secrets Manager.
type influxAuthSecretAdapter struct {
	secrets *secretsmanagerbackend.InMemoryBackend
}

func (a influxAuthSecretAdapter) CreateSecret(region, name, secretString string) (string, error) {
	return a.secrets.CreateManagedSecret(region, name, "", secretString)
}

func (a influxAuthSecretAdapter) DeleteSecret(region, secretARN string) error {
	return a.secrets.DeleteManagedSecret(region, secretARN)
}

// wireTimestreamInfluxDBIntegrations gives Timestream for InfluxDB its auth-parameter secrets (Secrets
// Manager) and its log delivery bucket store (S3).
func wireTimestreamInfluxDBIntegrations(byName map[string]service.Registerable) {
	h, ok := byName["TimestreamInfluxDB"].(*timestreaminfluxdbbackend.Handler)
	if !ok {
		return
	}

	if smH, sok := byName["SecretsManager"].(*secretsmanagerbackend.Handler); sok {
		if bk, bok := smH.Backend.(*secretsmanagerbackend.InMemoryBackend); bok {
			h.Backend.SetSecretStore(influxAuthSecretAdapter{secrets: bk})
		}
	}

	if s3H, sok := byName["S3"].(*s3backend.S3Handler); sok {
		if bk, bok := s3H.Backend.(*s3backend.InMemoryBackend); bok {
			h.Backend.SetLogSink(bk)
		}
	}
}
