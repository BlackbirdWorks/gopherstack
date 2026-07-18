package iot

import (
	"fmt"
	"time"
)

// Real AWS IoT only defines two encryption types (see EncryptionType in the
// AWS SDK); there is no third "KMS_BASED" variant.
const (
	encryptionTypeAWSOwned        = "AWS_OWNED_KMS_KEY"
	encryptionTypeCustomerManaged = "CUSTOMER_MANAGED_KMS_KEY"

	configurationStatusHealthy = "HEALTHY"
)

// Generic AWS-style lifecycle status values shared across several final
// stub batch resources (authorizers, topic rule destinations).
const (
	statusActive     = "ACTIVE"
	statusEnabled    = "ENABLED"
	statusInProgress = "IN_PROGRESS"
)

// AccountEncryptionConfiguration represents the account-wide encryption
// configuration for data at rest.
type AccountEncryptionConfiguration struct {
	ConfigurationDetails *EncryptionConfigurationDetails `json:"configurationDetails,omitempty"`
	EncryptionType       string                          `json:"encryptionType"`
	KMSKeyARN            string                          `json:"kmsKeyArn,omitempty"`
	KMSAccessRoleARN     string                          `json:"kmsAccessRoleArn,omitempty"`
	LastModifiedDate     float64                         `json:"lastModifiedDate,omitempty"`
}

// EncryptionConfigurationDetails describes the health of the KMS key/role
// backing a customer-managed encryption configuration.
type EncryptionConfigurationDetails struct {
	ConfigurationStatus string `json:"configurationStatus"`
	ErrorCode           string `json:"errorCode,omitempty"`
	ErrorMessage        string `json:"errorMessage,omitempty"`
}

// UpdateEncryptionConfigurationInput is the input for UpdateEncryptionConfiguration.
type UpdateEncryptionConfigurationInput struct {
	EncryptionType   string `json:"encryptionType"`
	KMSKeyARN        string `json:"kmsKeyArn,omitempty"`
	KMSAccessRoleARN string `json:"kmsAccessRoleArn,omitempty"`
}

// DescribeEncryptionConfiguration returns the account's encryption
// configuration, defaulting to the AWS-owned key when never configured.
func (b *InMemoryBackend) DescribeEncryptionConfiguration() *AccountEncryptionConfiguration {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.accountEncryptionConfig == nil {
		return &AccountEncryptionConfiguration{
			EncryptionType:       encryptionTypeAWSOwned,
			ConfigurationDetails: &EncryptionConfigurationDetails{ConfigurationStatus: configurationStatusHealthy},
		}
	}

	cp := *b.accountEncryptionConfig
	if b.accountEncryptionConfig.ConfigurationDetails != nil {
		details := *b.accountEncryptionConfig.ConfigurationDetails
		cp.ConfigurationDetails = &details
	}

	return &cp
}

// UpdateEncryptionConfiguration validates and mutates the account's
// encryption configuration.
func (b *InMemoryBackend) UpdateEncryptionConfiguration(input *UpdateEncryptionConfigurationInput) error {
	switch input.EncryptionType {
	case encryptionTypeAWSOwned:
	case encryptionTypeCustomerManaged:
		if input.KMSKeyARN == "" {
			return fmt.Errorf("%w: kmsKeyArn is required for encryptionType %q", ErrValidation, input.EncryptionType)
		}

		if input.KMSAccessRoleARN == "" {
			return fmt.Errorf(
				"%w: kmsAccessRoleArn is required for encryptionType %q",
				ErrValidation,
				input.EncryptionType,
			)
		}
	default:
		return fmt.Errorf("%w: invalid encryptionType %q", ErrValidation, input.EncryptionType)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.accountEncryptionConfig = &AccountEncryptionConfiguration{
		EncryptionType:       input.EncryptionType,
		KMSKeyARN:            input.KMSKeyARN,
		KMSAccessRoleARN:     input.KMSAccessRoleARN,
		ConfigurationDetails: &EncryptionConfigurationDetails{ConfigurationStatus: configurationStatusHealthy},
		LastModifiedDate:     float64(time.Now().Unix()),
	}

	return nil
}
