package rds

import (
	"fmt"
	"net/url"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const masterSecretStatusActive = "active"

// MasterSecret is the RDS-managed master-user secret surfaced as MasterUserSecret.
type MasterSecret struct {
	MasterUserSecretARN      string `json:"masterUserSecretArn,omitempty"`
	MasterUserSecretStatus   string `json:"masterUserSecretStatus,omitempty"`
	MasterUserSecretKmsKeyID string `json:"masterUserSecretKmsKeyId,omitempty"`
}

// MasterSecretRequest carries ManageMasterUserPassword and MasterUserSecretKmsKeyId.
type MasterSecretRequest struct {
	MasterUserSecretKmsKeyID string
	ManageMasterUserPassword bool
	ManageSet                bool
}

func parseMasterSecretRequest(vals url.Values) MasterSecretRequest {
	return MasterSecretRequest{
		MasterUserSecretKmsKeyID: vals.Get("MasterUserSecretKmsKeyId"),
		ManageMasterUserPassword: vals.Get("ManageMasterUserPassword") == formTrue,
		ManageSet:                vals.Get("ManageMasterUserPassword") != "",
	}
}

func (s MasterSecret) managed() bool { return s.MasterUserSecretARN != "" }

func (b *InMemoryBackend) newMasterSecret(kind, kmsKeyID string) MasterSecret {
	return MasterSecret{
		MasterUserSecretARN: arn.Build(
			"secretsmanager", b.region, b.accountID, "secret:rds!"+kind+"-"+uuid.NewString(),
		),
		MasterUserSecretStatus:   masterSecretStatusActive,
		MasterUserSecretKmsKeyID: kmsKeyID,
	}
}

// createMasterSecret validates the create-time combination and provisions the secret.
func (b *InMemoryBackend) createMasterSecret(
	kind string,
	req MasterSecretRequest,
	password string,
) (MasterSecret, error) {
	if !req.ManageMasterUserPassword {
		if req.MasterUserSecretKmsKeyID != "" {
			return MasterSecret{}, fmt.Errorf(
				"%w: MasterUserSecretKmsKeyId requires ManageMasterUserPassword", ErrInvalidParameterCombination,
			)
		}

		return MasterSecret{}, nil
	}
	if password != "" {
		return MasterSecret{}, fmt.Errorf(
			"%w: MasterUserPassword can't be specified with ManageMasterUserPassword", ErrInvalidParameterCombination,
		)
	}

	return b.newMasterSecret(kind, req.MasterUserSecretKmsKeyID), nil
}

// updateMasterSecret applies a Modify* request to the current secret state.
func (b *InMemoryBackend) updateMasterSecret(
	cur MasterSecret, kind string, req MasterSecretRequest, password string,
) (MasterSecret, error) {
	switch {
	case req.ManageSet && !req.ManageMasterUserPassword:
		if req.MasterUserSecretKmsKeyID != "" {
			return cur, fmt.Errorf(
				"%w: MasterUserSecretKmsKeyId requires ManageMasterUserPassword", ErrInvalidParameterCombination,
			)
		}
		if !cur.managed() {
			return cur, nil
		}
		if password == "" {
			return cur, fmt.Errorf(
				"%w: MasterUserPassword is required to stop managing the master user password",
				ErrInvalidParameterCombination,
			)
		}

		return MasterSecret{}, nil
	case req.ManageMasterUserPassword && !cur.managed():
		return b.createMasterSecret(kind, req, password)
	case req.MasterUserSecretKmsKeyID != "":
		if !cur.managed() {
			return cur, fmt.Errorf(
				"%w: MasterUserSecretKmsKeyId requires ManageMasterUserPassword", ErrInvalidParameterCombination,
			)
		}
		cur.MasterUserSecretKmsKeyID = req.MasterUserSecretKmsKeyID

		return cur, nil
	}

	return cur, nil
}

type xmlMasterUserSecret struct {
	SecretArn    string `xml:"SecretArn,omitempty"`
	SecretStatus string `xml:"SecretStatus,omitempty"`
	KmsKeyID     string `xml:"KmsKeyId,omitempty"`
}

func (s MasterSecret) toXML() *xmlMasterUserSecret {
	if !s.managed() {
		return nil
	}

	return &xmlMasterUserSecret{
		SecretArn:    s.MasterUserSecretARN,
		SecretStatus: s.MasterUserSecretStatus,
		KmsKeyID:     s.MasterUserSecretKmsKeyID,
	}
}
