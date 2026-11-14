package rds

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"net/url"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
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
	RotateMasterUserPassword bool
}

func parseMasterSecretRequest(vals url.Values) MasterSecretRequest {
	return MasterSecretRequest{
		MasterUserSecretKmsKeyID: vals.Get("MasterUserSecretKmsKeyId"),
		ManageMasterUserPassword: vals.Get("ManageMasterUserPassword") == formTrue,
		ManageSet:                vals.Get("ManageMasterUserPassword") != "",
		RotateMasterUserPassword: vals.Get("RotateMasterUserPassword") == formTrue,
	}
}

func (s MasterSecret) managed() bool { return s.MasterUserSecretARN != "" }

const managedSecretKeyAlias = "alias/aws/secretsmanager"

// SetKeyChecker resolves master user secret keys against AWS KMS; nil disables the lookup.
func (b *InMemoryBackend) SetKeyChecker(c cmek.KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = c
}

func (b *InMemoryBackend) keyContext() context.Context {
	return awsmeta.WithRegion(context.Background(), b.region)
}

// checkSecretKey requires a MasterUserSecretKmsKeyId to name a usable key (KMSKeyNotAccessibleFault otherwise).
func (b *InMemoryBackend) checkSecretKey(keyID string) error {
	if b.keys == nil || keyID == "" {
		return nil
	}

	if err := b.keys.CheckKey(b.keyContext(), keyID); err != nil {
		return fmt.Errorf("%w: KMS key %s: %w", ErrKMSKeyNotAccessible, keyID, err)
	}

	return nil
}

// surfacedSecretKey is the KmsKeyId reported for the secret: the requested key, else the ARN of the
// aws/secretsmanager managed key (rds-secrets-manager.html shows that ARN in MasterUserSecret).
func (b *InMemoryBackend) surfacedSecretKey(keyID string) string {
	if keyID != "" || b.keys == nil {
		return keyID
	}

	r, ok := b.keys.(cmek.KeyResolver)
	if !ok {
		return ""
	}

	keyARN, err := r.ResolveKeyARN(b.keyContext(), managedSecretKeyAlias)
	if err != nil {
		return ""
	}

	return keyARN
}

// sameSecretKey reports whether two key references name the same key.
func (b *InMemoryBackend) sameSecretKey(a, c string) bool {
	if a == c {
		return true
	}

	r, ok := b.keys.(cmek.KeyResolver)
	if !ok {
		return false
	}

	ctx := b.keyContext()

	aARN, errA := r.ResolveKeyARN(ctx, a)
	cARN, errC := r.ResolveKeyARN(ctx, c)

	return errA == nil && errC == nil && aARN == cARN
}

func (b *InMemoryBackend) newMasterSecret(kind, user, kmsKeyID string) (MasterSecret, error) {
	if err := b.checkSecretKey(kmsKeyID); err != nil {
		return MasterSecret{}, err
	}

	name := "rds!" + kind + "-" + uuid.NewString()
	secretARN := arn.Build("secretsmanager", b.region, b.accountID, "secret:"+name)

	if b.secrets != nil {
		body, err := masterSecretBody(user)
		if err != nil {
			return MasterSecret{}, err
		}

		if secretARN, err = b.secrets.CreateManagedSecret(b.region, name, kmsKeyID, body); err != nil {
			return MasterSecret{}, fmt.Errorf("create master user secret: %w", err)
		}
	}

	return MasterSecret{
		MasterUserSecretARN:      secretARN,
		MasterUserSecretStatus:   masterSecretStatusActive,
		MasterUserSecretKmsKeyID: b.surfacedSecretKey(kmsKeyID),
	}, nil
}

func masterSecretBody(user string) (string, error) {
	raw := make([]byte, masterPasswordBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate master password: %w", err)
	}

	body, err := json.Marshal(map[string]string{"username": user, "password": hex.EncodeToString(raw)}, jsonx.V1())
	if err != nil {
		return "", fmt.Errorf("encode master user secret: %w", err)
	}

	return string(body), nil
}

// rotateMasterSecret writes a fresh password into the managed secret.
func (b *InMemoryBackend) rotateMasterSecret(cur MasterSecret, user string) error {
	if !cur.managed() {
		return fmt.Errorf(
			"%w: RotateMasterUserPassword requires a managed master user password", ErrInvalidParameterCombination,
		)
	}

	if b.secrets == nil {
		return nil
	}

	body, err := masterSecretBody(user)
	if err != nil {
		return err
	}

	if err = b.secrets.PutManagedSecretValue(b.region, cur.MasterUserSecretARN, body); err != nil {
		return fmt.Errorf("rotate master user secret: %w", err)
	}

	return nil
}

// releaseMasterSecret deletes the backing Secrets Manager secret of a managed master password.
func (b *InMemoryBackend) releaseMasterSecret(s MasterSecret) {
	if b.secrets != nil && s.managed() {
		_ = b.secrets.DeleteManagedSecret(b.region, s.MasterUserSecretARN)
	}
}

// createMasterSecret validates the create-time combination and provisions the secret.
func (b *InMemoryBackend) createMasterSecret(
	kind, user string,
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

	return b.newMasterSecret(kind, user, req.MasterUserSecretKmsKeyID)
}

// updateMasterSecret applies a Modify* request to the current secret state.
func (b *InMemoryBackend) updateMasterSecret(
	cur MasterSecret, kind, user string, req MasterSecretRequest, password string,
) (MasterSecret, error) {
	if req.RotateMasterUserPassword {
		if err := b.rotateMasterSecret(cur, user); err != nil {
			return cur, err
		}
	}

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

		b.releaseMasterSecret(cur)

		return MasterSecret{}, nil
	case req.ManageMasterUserPassword && !cur.managed():
		return b.createMasterSecret(kind, user, req, password)
	case req.MasterUserSecretKmsKeyID != "":
		if !cur.managed() {
			return cur, fmt.Errorf(
				"%w: MasterUserSecretKmsKeyId requires ManageMasterUserPassword", ErrInvalidParameterCombination,
			)
		}
		if !b.sameSecretKey(req.MasterUserSecretKmsKeyID, cur.MasterUserSecretKmsKeyID) {
			return cur, fmt.Errorf(
				"%w: the KMS key of a managed master user secret can't be changed", ErrInvalidParameterCombination,
			)
		}

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
