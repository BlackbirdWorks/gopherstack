package kms

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

type keyChecker struct{ b StorageBackend }

// NewKeyChecker returns a cmek.KeyChecker over the backend: a key reference (ID, ARN or alias, including the
// lazily created alias/aws/<service> managed keys) must name an enabled symmetric ENCRYPT_DECRYPT key.
func NewKeyChecker(b StorageBackend) cmek.KeyChecker { return keyChecker{b: b} }

func (k keyChecker) describe(ctx context.Context, keyRef string) (*DescribeKeyOutput, error) {
	if r := awsmeta.Region(ctx); r != "" {
		ctx = WithRegion(ctx, r)
	}

	out, err := k.b.DescribeKey(ctx, &DescribeKeyInput{KeyID: keyRef})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrAliasNotFound) {
			return nil, cmek.ErrNotFound
		}

		return nil, fmt.Errorf("describe key %s: %w", keyRef, err)
	}

	return out, nil
}

// ResolveKeyARN implements cmek.KeyResolver.
func (k keyChecker) ResolveKeyARN(ctx context.Context, keyRef string) (string, error) {
	out, err := k.describe(ctx, keyRef)
	if err != nil {
		return "", err
	}

	return out.KeyMetadata.Arn, nil
}

func (k keyChecker) CheckKey(ctx context.Context, keyRef string) error {
	out, err := k.describe(ctx, keyRef)
	if err != nil {
		return err
	}

	md := out.KeyMetadata

	switch {
	case md.KeyState == KeyStateDisabled:
		return cmek.ErrDisabled
	case md.KeyState != KeyStateEnabled:
		return cmek.ErrState
	case md.KeyUsage != KeyUsageEncryptDecrypt || (md.KeySpec != "" && md.KeySpec != keySpecSymmetric):
		return cmek.ErrPurpose
	}

	return nil
}
