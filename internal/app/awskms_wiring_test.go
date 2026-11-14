package app

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appintegrationsbackend "github.com/blackbirdworks/gopherstack/services/appintegrations"
	codegurureviewerbackend "github.com/blackbirdworks/gopherstack/services/codegurureviewer"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
)

type fakeAWSSecrets struct{}

func (fakeAWSSecrets) CreateManagedSecret(region, name, _, _ string) (string, error) {
	return "arn:aws:secretsmanager:" + region + ":000000000000:secret:" + name, nil
}
func (fakeAWSSecrets) PutManagedSecretValue(string, string, string) error { return nil }
func (fakeAWSSecrets) DeleteManagedSecret(string, string) error           { return nil }

func TestWireAWSKeyLookups(t *testing.T) {
	t.Parallel()

	kb := kmsbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	ai := appintegrationsbackend.NewInMemoryBackend("000000000000", "us-east-1")
	cg := codegurureviewerbackend.NewInMemoryBackend("000000000000", "us-east-1")
	rb := rdsbackend.NewInMemoryBackend("000000000000", "us-east-1")
	rb.SetSecretsStore(fakeAWSSecrets{})

	rh := rdsbackend.NewHandler(rb)

	wireAWSKeyLookups(map[string]service.Registerable{
		"KMS":              kmsbackend.NewHandler(kb),
		"AppIntegrations":  appintegrationsbackend.NewHandler(ai),
		"CodeGuruReviewer": codegurureviewerbackend.NewHandler(cg),
		"RDS":              rh,
	})

	_, err := ai.CreateDataIntegration(t.Context(), &appintegrationsbackend.CreateDataIntegrationInput{
		Name: "d", KmsKey: "arn:aws:kms:us-east-1:000000000000:key/missing", SourceURI: "salesforce://x",
	})
	require.Error(t, err)

	_, err = rb.CreateDBInstance("i", "mysql", "db.t3.micro", "db", "admin", "", 20,
		rdsbackend.DBInstanceOptions{
			ManageMasterUserPassword: true, ManageSet: true, MasterUserSecretKmsKeyID: "missing",
		})
	require.ErrorIs(t, err, rdsbackend.ErrKMSKeyNotAccessible)

	inst, err := rb.CreateDBInstance("j", "mysql", "db.t3.micro", "db", "admin", "", 20,
		rdsbackend.DBInstanceOptions{
			ManageMasterUserPassword: true, ManageSet: true,
		})
	require.NoError(t, err)
	require.Regexp(t, `^arn:aws:kms:us-east-1:\d{12}:key/`, inst.MasterUserSecretKmsKeyID)
}
