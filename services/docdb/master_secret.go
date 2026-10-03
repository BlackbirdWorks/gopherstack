package docdb

import (
	"fmt"
	"net/url"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const masterSecretStatusActive = "active"

// MasterSecretRequest carries ManageMasterUserPassword and MasterUserSecretKmsKeyId.
type MasterSecretRequest struct {
	MasterUserSecretKmsKeyID string
	ManageMasterUserPassword bool
	ManageSet                bool
}

func parseMasterSecretRequest(vals url.Values) MasterSecretRequest {
	return MasterSecretRequest{
		MasterUserSecretKmsKeyID: vals.Get("MasterUserSecretKmsKeyId"),
		ManageMasterUserPassword: vals.Get("ManageMasterUserPassword") == stringTrue,
		ManageSet:                vals.Get("ManageMasterUserPassword") != "",
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
	c.MasterUserSecretARN = arn.Build("secretsmanager", c.region, b.accountID, "secret:rds!cluster-"+uuid.NewString())
	c.MasterUserSecretStatus = masterSecretStatusActive
	c.MasterUserSecretKmsKeyID = req.MasterUserSecretKmsKeyID

	return nil
}

// updateMasterSecret applies ModifyDBCluster's secret fields; the KMS key is only settable when enabling management.
func (b *InMemoryBackend) updateMasterSecret(c *DBCluster, req MasterSecretRequest, password string) error {
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
