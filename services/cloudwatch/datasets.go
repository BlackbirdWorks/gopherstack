package cloudwatch

import (
	"fmt"
	"regexp"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// kmsKeyArnOnlyRe matches only a fully qualified KMS key ARN, e.g.
// "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab".
// AssociateDatasetKmsKey documents that key IDs, aliases, and alias ARNs are
// NOT accepted here (unlike the more permissive validateKmsKeyID pattern some
// other services in this repo use for fields that DO accept aliases) -- only
// the "key/<uuid>" ARN form qualifies.
var kmsKeyArnOnlyRe = regexp.MustCompile(
	`^arn:aws[a-zA-Z-]*:kms:[a-z0-9-]+:\d{12}:key/[0-9a-fA-F-]{36}$`,
)

// validateDatasetKmsKeyArn checks a KmsKeyArn value against
// AssociateDatasetKmsKey's documented ARN-only shape.
func validateDatasetKmsKeyArn(kmsKeyArn string) error {
	if kmsKeyArn == "" {
		return fmt.Errorf("%w: KmsKeyArn is required", ErrValidation)
	}
	if !kmsKeyArnOnlyRe.MatchString(kmsKeyArn) {
		return ErrInvalidKmsKeyArn
	}

	return nil
}

// resolveDatasetIdentifier normalises a DatasetIdentifier (either the literal
// "default" or the default dataset's fully qualified ARN) to the internal
// dataset ID, or returns ErrDatasetNotFound for anything else. Only the
// default dataset exists in real CloudWatch.
func (b *InMemoryBackend) resolveDatasetIdentifier(identifier string) (string, error) {
	defaultArn := arn.Build("cloudwatch", b.region, b.accountID, "dataset/"+defaultDatasetID)
	if identifier == defaultDatasetID || identifier == defaultArn {
		return defaultDatasetID, nil
	}

	return "", ErrDatasetNotFound
}

// GetDataset returns information about the (only) default dataset, including
// its ARN and any associated customer managed KMS key. The default dataset is
// implicit -- it exists, with no KMS key, even if AssociateDatasetKmsKey has
// never been called.
func (b *InMemoryBackend) GetDataset(identifier string) (Dataset, error) {
	id, err := b.resolveDatasetIdentifier(identifier)
	if err != nil {
		return Dataset{}, err
	}

	b.mu.RLock("GetDataset")
	defer b.mu.RUnlock()

	ds := Dataset{DatasetID: id}
	if got, ok := b.datasets.Get(id); ok {
		ds = *got
	}
	ds.Arn = arn.Build("cloudwatch", b.region, b.accountID, "dataset/"+id)

	return ds, nil
}

// AssociateDatasetKmsKey associates (or replaces) the customer managed KMS
// key used to encrypt data published to the default dataset.
func (b *InMemoryBackend) AssociateDatasetKmsKey(identifier, kmsKeyArn string) error {
	id, err := b.resolveDatasetIdentifier(identifier)
	if err != nil {
		return err
	}
	if validateErr := validateDatasetKmsKeyArn(kmsKeyArn); validateErr != nil {
		return validateErr
	}

	b.mu.Lock("AssociateDatasetKmsKey")
	defer b.mu.Unlock()

	b.datasets.Put(&Dataset{DatasetID: id, KmsKeyArn: kmsKeyArn})

	return nil
}

// DisassociateDatasetKmsKey removes the customer managed KMS key association
// from the default dataset. Fails with ErrDatasetNotFound if the dataset
// currently has no associated KMS key (matches the real doc comment).
func (b *InMemoryBackend) DisassociateDatasetKmsKey(identifier string) error {
	id, err := b.resolveDatasetIdentifier(identifier)
	if err != nil {
		return err
	}

	b.mu.Lock("DisassociateDatasetKmsKey")
	defer b.mu.Unlock()

	ds, ok := b.datasets.Get(id)
	if !ok || ds.KmsKeyArn == "" {
		return ErrDatasetNotFound
	}

	b.datasets.Put(&Dataset{DatasetID: id})

	return nil
}
