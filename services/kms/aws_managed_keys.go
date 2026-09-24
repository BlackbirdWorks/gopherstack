package kms

import (
	"context"
	"fmt"
	"strings"
	"time"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/google/uuid"

	gopherarn "github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// awsManagedKeyAliasPrefix is the only alias-name prefix AWS reserves for its
// own managed keys (developerguide/kms-alias.html: "The aws/ prefix for an
// alias name is reserved for AWS managed keys. You cannot create an alias
// with this prefix.").
const awsManagedKeyAliasPrefix = "alias/aws/"

// awsManagedKeyServices maps the service segment of a well-known
// "alias/aws/<service>" alias to the human-readable service name used in the
// lazily-provisioned key's Description, matching real KMS's per-account,
// per-region lazy creation of AWS managed keys on first reference
// (gopherstack-6u8p4).
//
//nolint:gochecknoglobals // static lookup table, mirrors ipamValidRirs-style tables elsewhere
var awsManagedKeyServices = map[string]string{
	"dynamodb":          "DynamoDB",
	"s3":                "S3",
	"ebs":               "EBS",
	"rds":               "RDS",
	"lambda":            "Lambda",
	"secretsmanager":    "Secrets Manager",
	"ssm":               "Systems Manager",
	"sns":               "SNS",
	"sqs":               "SQS",
	"kinesis":           "Kinesis",
	"logs":              "CloudWatch Logs",
	"backup":            "Backup",
	"elasticfilesystem": "EFS",
	"fsx":               "FSx",
	"redshift":          "Redshift",
}

// awsManagedServiceFromAliasName returns the service segment of aliasName
// (e.g. "dynamodb" for "alias/aws/dynamodb") and true when it names a known
// AWS-managed-key alias.
func awsManagedServiceFromAliasName(aliasName string) (string, bool) {
	svc, ok := strings.CutPrefix(aliasName, awsManagedKeyAliasPrefix)
	if !ok {
		return "", false
	}

	if _, known := awsManagedKeyServices[svc]; !known {
		return "", false
	}

	return svc, true
}

// awsManagedAliasFromKeyID reports whether keyID -- a bare alias name or an
// alias ARN -- names a known AWS-managed-key alias, returning the alias name
// and, for an ARN, its embedded region (empty for a bare alias name, meaning
// "use the request region").
func awsManagedAliasFromKeyID(keyID string) (string, string, bool) {
	name, region := keyID, ""

	if strings.HasPrefix(keyID, "arn:") {
		parsed, err := awsarn.Parse(keyID)
		if err != nil || !strings.HasPrefix(parsed.Resource, "alias/") {
			return "", "", false
		}

		name, region = parsed.Resource, parsed.Region
	} else if !strings.HasPrefix(keyID, "alias/") {
		return "", "", false
	}

	if _, known := awsManagedServiceFromAliasName(name); !known {
		return "", "", false
	}

	return name, region, true
}

// ensureAWSManagedKey lazily provisions the AWS-managed key and alias named
// by keyID (a bare "alias/aws/<service>" name or its alias ARN) the first
// time any operation references it, matching real KMS: "AWS managed keys are
// KMS keys ... created, managed, and used on your behalf by an AWS service
// integrated with AWS KMS" (developerguide/concepts.html), created lazily
// per account/region on first reference. A no-op for any other KeyId shape
// (customer key ID/alias/ARN, or an alias/aws/ name outside the known
// service list, which real KeyId resolution still rejects as NotFound).
//
// The caller MUST invoke this before taking its own lock -- it takes the
// backend write lock itself.
func (b *InMemoryBackend) ensureAWSManagedKey(ctx context.Context, keyID string) error {
	aliasName, arnRegion, ok := awsManagedAliasFromKeyID(keyID)
	if !ok {
		return nil
	}

	svc, _ := awsManagedServiceFromAliasName(aliasName)

	region := arnRegion
	if region == "" {
		region = getRegion(ctx, b.defaultRegion)
	}

	b.mu.Lock("ensureAWSManagedKey")
	defer b.mu.Unlock()

	if b.aliasesStore(region).Has(aliasName) {
		return nil
	}

	newKeyID := uuid.New().String()
	keyARN := gopherarn.Build("kms", region, b.accountID, "key/"+newKeyID)
	now := UnixTimeFloat(time.Now())
	description := fmt.Sprintf(
		"Default key that protects my %s data when no other key is defined", awsManagedKeyServices[svc],
	)

	key := &Key{
		KeyID:         newKeyID,
		Arn:           keyARN,
		Description:   description,
		KeyState:      KeyStateEnabled,
		KeyUsage:      KeyUsageEncryptDecrypt,
		KeySpec:       keySpecSymmetric,
		Origin:        KeyOriginAWSKMS,
		KeyManager:    KeyManagerAWS,
		PrimaryRegion: region,
		CreationDate:  now,
		Enabled:       true,
	}

	km, err := generateKeyMaterial(keySpecSymmetric)
	if err != nil {
		return fmt.Errorf("generating key material for AWS managed key %q: %w", aliasName, err)
	}

	b.keyMaterialsStore(region)[key.KeyID] = km
	b.keysStore(region).Put(key)

	aliasArn := gopherarn.Build("kms", region, b.accountID, aliasName)
	b.aliasesStore(region).Put(&Alias{
		AliasName:       aliasName,
		AliasArn:        aliasArn,
		TargetKeyID:     key.KeyID,
		CreationDate:    now,
		LastUpdatedDate: now,
	})

	return nil
}

// isAWSManagedKey reports whether key is an AWS-managed key (KeyManager ==
// AWS), for enforcing the real restrictions on managed keys: "you cannot
// change any properties of AWS managed keys, rotate them, change their key
// policies, or schedule them for deletion" (developerguide/concepts.html).
func isAWSManagedKey(key *Key) bool {
	return key.KeyManager == KeyManagerAWS
}

// errAWSManagedKeyInvalidState rejects an op that real AWS KMS refuses on an
// AWS-managed key with KMSInvalidStateException -- the only state-shaped
// error ScheduleKeyDeletion, DisableKey, DeleteAlias, and UpdateAlias declare
// (kms@v1.59.0 deserializers.go's per-op error lists all include
// KMSInvalidStateException; none of the four declares
// UnsupportedOperationException).
func errAWSManagedKeyInvalidState(op, arn string) error {
	return fmt.Errorf("%w: %s is an AWS managed key and cannot be used with %s", ErrKeyInvalidState, arn, op)
}

// errAWSManagedKeyUnsupported rejects PutKeyPolicy on an AWS-managed key with
// UnsupportedOperationException, which -- unlike the four ops above --
// PutKeyPolicy's own deserializeOpError does declare.
func errAWSManagedKeyUnsupported(arn string) error {
	return fmt.Errorf(
		"%w: the key policy of AWS managed key %s cannot be changed", ErrUnsupportedParameter, arn,
	)
}
