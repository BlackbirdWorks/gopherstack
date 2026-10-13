package docdb

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	masterSecretStatusActive = "active"
	masterPasswordBytes      = 16
)

// SecretsStore creates and removes the Secrets Manager secrets behind managed master passwords.
type SecretsStore interface {
	CreateManagedSecret(region, name, kmsKeyID, secretString string) (string, error)
	PutManagedSecretValue(region, secretARN, secretString string) error
	DeleteManagedSecret(region, secretARN string) error
}

// SetSecretsStore wires the Secrets Manager accessor used for ManageMasterUserPassword.
func (b *InMemoryBackend) SetSecretsStore(s SecretsStore) {
	b.mu.Lock("SetSecretsStore")
	defer b.mu.Unlock()

	b.secrets = s
}

func masterSecretBody(user string) (string, error) {
	raw := make([]byte, masterPasswordBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate master password: %w", err)
	}

	body, err := json.Marshal(map[string]string{"username": user, "password": hex.EncodeToString(raw)})
	if err != nil {
		return "", fmt.Errorf("encode master user secret: %w", err)
	}

	return string(body), nil
}

func (b *InMemoryBackend) rotateMasterSecret(c *DBCluster) error {
	if c.MasterUserSecretARN == "" {
		return fmt.Errorf(
			"%w: RotateMasterUserPassword requires a managed master user password", ErrInvalidParameterCombination,
		)
	}

	if b.secrets == nil {
		return nil
	}

	body, err := masterSecretBody(c.MasterUsername)
	if err != nil {
		return err
	}

	if err = b.secrets.PutManagedSecretValue(c.region, c.MasterUserSecretARN, body); err != nil {
		return fmt.Errorf("rotate master user secret: %w", err)
	}

	return nil
}

func (b *InMemoryBackend) provisionMasterSecret(c *DBCluster, kmsKeyID string) error {
	name := "rds!cluster-" + uuid.NewString()
	secretARN := arn.Build("secretsmanager", c.region, b.accountID, "secret:"+name)

	if b.secrets != nil {
		body, err := masterSecretBody(c.MasterUsername)
		if err != nil {
			return err
		}

		if secretARN, err = b.secrets.CreateManagedSecret(c.region, name, kmsKeyID, body); err != nil {
			return fmt.Errorf("create master user secret: %w", err)
		}
	}

	c.MasterUserSecretARN = secretARN
	c.MasterUserSecretStatus = masterSecretStatusActive
	c.MasterUserSecretKmsKeyID = kmsKeyID

	return nil
}

func (b *InMemoryBackend) releaseMasterSecret(c *DBCluster) {
	if b.secrets != nil && c.MasterUserSecretARN != "" {
		_ = b.secrets.DeleteManagedSecret(c.region, c.MasterUserSecretARN)
	}
}

// MasterSecretRequest carries ManageMasterUserPassword and MasterUserSecretKmsKeyId.
type MasterSecretRequest struct {
	MasterUserSecretKmsKeyID string
	ManageMasterUserPassword bool
	ManageSet                bool
	RotateMasterUserPassword bool
}

func parseMasterSecretRequest(vals url.Values) MasterSecretRequest {
	return MasterSecretRequest{
		MasterUserSecretKmsKeyID: vals.Get("MasterUserSecretKmsKeyId"),
		ManageMasterUserPassword: vals.Get("ManageMasterUserPassword") == stringTrue,
		ManageSet:                vals.Get("ManageMasterUserPassword") != "",
		RotateMasterUserPassword: vals.Get("RotateMasterUserPassword") == stringTrue,
	}
}

func (b *InMemoryBackend) createClusterMasterSecret(c *DBCluster, opts *CreateDBClusterOptions, password string) error {
	if opts == nil {
		return nil
	}

	return b.createMasterSecret(c, opts.MasterSecretRequest, password)
}

func (b *InMemoryBackend) createMasterSecret(c *DBCluster, req MasterSecretRequest, password string) error {
	if !req.ManageMasterUserPassword {
		if req.MasterUserSecretKmsKeyID != "" {
			return fmt.Errorf(
				"%w: MasterUserSecretKmsKeyId requires ManageMasterUserPassword", ErrInvalidParameterCombination,
			)
		}

		return nil
	}
	if password != "" {
		return fmt.Errorf(
			"%w: MasterUserPassword can't be specified with ManageMasterUserPassword",
			ErrInvalidParameterCombination,
		)
	}

	return b.provisionMasterSecret(c, req.MasterUserSecretKmsKeyID)
}

// updateMasterSecret applies ModifyDBCluster's secret fields; the KMS key is only settable when enabling management.
func (b *InMemoryBackend) updateMasterSecret(c *DBCluster, req MasterSecretRequest, password string) error {
	if req.RotateMasterUserPassword {
		if err := b.rotateMasterSecret(c); err != nil {
			return err
		}
	}

	managed := c.MasterUserSecretARN != ""
	switch {
	case req.ManageSet && !req.ManageMasterUserPassword:
		if req.MasterUserSecretKmsKeyID != "" {
			return fmt.Errorf(
				"%w: MasterUserSecretKmsKeyId requires ManageMasterUserPassword", ErrInvalidParameterCombination,
			)
		}
		if !managed {
			return nil
		}
		if password == "" {
			return fmt.Errorf(
				"%w: MasterUserPassword is required to stop managing the master user password",
				ErrInvalidParameterCombination,
			)
		}
		b.releaseMasterSecret(c)
		c.MasterUserSecretARN, c.MasterUserSecretStatus, c.MasterUserSecretKmsKeyID = "", "", ""
	case req.ManageMasterUserPassword && !managed:
		return b.createMasterSecret(c, req, password)
	case req.MasterUserSecretKmsKeyID != "":
		return fmt.Errorf(
			"%w: MasterUserSecretKmsKeyId can only be set when turning on ManageMasterUserPassword",
			ErrInvalidParameterCombination,
		)
	}

	return nil
}

type xmlClusterMasterUserSecret struct {
	SecretArn    string `xml:"SecretArn,omitempty"`
	SecretStatus string `xml:"SecretStatus,omitempty"`
	KmsKeyID     string `xml:"KmsKeyId,omitempty"`
}

func toXMLMasterUserSecret(c *DBCluster) *xmlClusterMasterUserSecret {
	if c.MasterUserSecretARN == "" {
		return nil
	}

	return &xmlClusterMasterUserSecret{
		SecretArn:    c.MasterUserSecretARN,
		SecretStatus: c.MasterUserSecretStatus,
		KmsKeyID:     c.MasterUserSecretKmsKeyID,
	}
}
