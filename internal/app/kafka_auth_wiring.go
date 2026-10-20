package app

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	acmpcabackend "github.com/blackbirdworks/gopherstack/services/acmpca"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/kafka"
	"github.com/blackbirdworks/gopherstack/services/kafkaconnect"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const kafkaCAPath = "/_gopherstack/kafka/ca.pem"

// GetKafkaCADir persists the MSK broker CA under the data dir when --persist is on (kafka.EngineCADirConfig).
func (c *CLI) GetKafkaCADir() string {
	if !c.Persist {
		return ""
	}

	return filepath.Join(c.resolvedDataDir(), "kafka-ca")
}

// kafkaScramSource reads AmazonMSK_ SCRAM credentials from the emulator's Secrets Manager.
type kafkaScramSource struct {
	backend *secretsmanagerbackend.InMemoryBackend
}

func (s kafkaScramSource) ResolveScramSecret(ctx context.Context, secretArn string) (kafka.ScramCredential, error) {
	out, err := s.backend.GetSecretValue(ctx, &secretsmanagerbackend.GetSecretValueInput{SecretID: secretArn})
	if err != nil {
		if errors.Is(err, secretsmanagerbackend.ErrSecretNotFound) {
			return kafka.ScramCredential{}, kafka.ErrScramSecretNotFound
		}

		return kafka.ScramCredential{}, err //nolint:wrapcheck // surfaced verbatim as the per-secret error message
	}

	cred, err := kafka.ParseScramSecret(out.SecretString)
	if err != nil {
		return kafka.ScramCredential{}, err //nolint:wrapcheck // sentinel describes the expected shape
	}

	cred.SecretName = out.Name

	return cred, nil
}

// wireKafkaAuth connects MSK client authentication to Secrets Manager (SASL/SCRAM), IAM and STS (AWS_MSK_IAM)
// and serves the broker CA at kafkaCAPath.
func wireKafkaAuth(e *echo.Echo, services []service.Registerable, enforceIAM bool) {
	hs := collectKafkaAuthHandlers(services)
	kafkaH, iamH, stsH, smH, kcH, pcaH := hs.kafka, hs.iam, hs.sts, hs.sm, hs.kc, hs.pca

	if kafkaH == nil {
		return
	}

	bk, ok := kafkaH.Backend.(*kafka.InMemoryBackend)
	if !ok {
		return
	}

	bk.SetIAMAuth(func() kafka.IAMAuthConfig { return kafkaIAMAuthConfig(iamH, stsH, enforceIAM) })

	if smH != nil {
		if smBk, smOk := smH.Backend.(*secretsmanagerbackend.InMemoryBackend); smOk {
			bk.SetScramSecretResolver(kafkaScramSource{backend: smBk})
		}
	}

	if pcaH != nil {
		bk.SetClientCAResolver(func(_ context.Context, caArn string) ([]byte, error) {
			return pcaH.Backend.CertificatePEMForARN(caArn)
		})
	}

	if kcH != nil {
		for _, b := range kcH.RegionBackends() {
			if kb, kbOk := b.(*kafkaconnect.InMemoryBackend); kbOk {
				kb.SetTrustCAPEM(bk.BrokerCAPEM)
			}
		}
	}

	e.GET(kafkaCAPath, func(c *echo.Context) error {
		pem := bk.BrokerCAPEM()
		if pem == nil {
			return c.String(http.StatusNotFound, "MSK docker engine is not enabled")
		}

		return c.Blob(http.StatusOK, "application/x-pem-file", pem)
	})
}

type kafkaAuthHandlers struct {
	kafka *kafka.Handler
	iam   *iambackend.Handler
	sts   *stsbackend.Handler
	sm    *secretsmanagerbackend.Handler
	kc    *kafkaconnect.Handler
	pca   *acmpcabackend.Handler
}

func collectKafkaAuthHandlers(services []service.Registerable) kafkaAuthHandlers {
	var hs kafkaAuthHandlers

	for _, svc := range services {
		switch h := svc.(type) {
		case *kafka.Handler:
			hs.kafka = h
		case *iambackend.Handler:
			hs.iam = h
		case *stsbackend.Handler:
			hs.sts = h
		case *secretsmanagerbackend.Handler:
			hs.sm = h
		case *kafkaconnect.Handler:
			hs.kc = h
		case *acmpcabackend.Handler:
			hs.pca = h
		}
	}

	return hs
}

func kafkaIAMAuthConfig(iamH *iambackend.Handler, stsH *stsbackend.Handler, enforceIAM bool) kafka.IAMAuthConfig {
	cfg := kafka.IAMAuthConfig{Enforce: enforceIAM}

	if iamH != nil {
		if ids, ok := iamH.Backend.(kafka.IAMAuthIdentityProvider); ok {
			cfg.Identities = ids
		}
	}

	if stsH != nil {
		if ss, ok := stsH.Backend.(kafka.IAMAuthSessionProvider); ok {
			cfg.Sessions = ss
		}
	}

	return cfg
}

const scramSourceAccessType = "SASL_SCRAM_512_AUTH"

// kafkaLambdaResolver lets Lambda MSK event source mappings reach plaintext, TLS and SASL/SCRAM clusters.
type kafkaLambdaResolver struct {
	bk *kafka.InMemoryBackend
}

func (r kafkaLambdaResolver) BootstrapServers(clusterARN string) []string {
	return r.bk.BootstrapServers(clusterARN)
}

func (r kafkaLambdaResolver) MSKConnection(
	ctx context.Context, clusterARN string, access []lambdabackend.SourceAccessConfiguration,
) (lambdabackend.MSKConnection, bool) {
	secret := ""

	for _, a := range access {
		if a.Type == scramSourceAccessType {
			secret = a.URI
		}
	}

	ep, ok := r.bk.ClientEndpointFor(ctx, clusterARN, secret)
	if !ok {
		return lambdabackend.MSKConnection{}, false
	}

	return lambdabackend.MSKConnection{
		Brokers: ep.Brokers, RootCAPEM: ep.RootCAPEM, Username: ep.Username, Password: ep.Password,
	}, true
}
