package glue

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateGlueIdentityCenterConfiguration creates the configuration and
// returns it. Real CreateGlueIdentityCenterConfigurationOutput carries
// ApplicationArn -- the ARN of the Identity Center application created for
// this Glue configuration (confirmed against
// awsAwsjson11_deserializeOpDocumentCreateGlueIdentityCenterConfigurationOutput
// in the pinned glue SDK's deserializers.go) -- so this backend must
// generate and track one rather than leaving it unset.
func (b *InMemoryBackend) CreateGlueIdentityCenterConfiguration(instanceARN string) (*IdentityCenterConfig, error) {
	b.mu.Lock("CreateGlueIdentityCenterConfiguration")
	defer b.mu.Unlock()

	appARN := arn.Build("sso", "", b.accountID, fmt.Sprintf("application/apl-%s", uuid.NewString()))

	b.glueIdentityCenterConfig = &IdentityCenterConfig{
		InstanceARN:    instanceARN,
		ApplicationARN: appARN,
		Status:         "ENABLED",
	}

	cp := *b.glueIdentityCenterConfig

	return &cp, nil
}

// GetGlueIdentityCenterConfiguration returns the configuration.
func (b *InMemoryBackend) GetGlueIdentityCenterConfiguration() (*IdentityCenterConfig, error) {
	b.mu.RLock("GetGlueIdentityCenterConfiguration")
	defer b.mu.RUnlock()

	if b.glueIdentityCenterConfig == nil {
		return &IdentityCenterConfig{Status: "DISABLED"}, nil
	}

	cp := *b.glueIdentityCenterConfig

	return &cp, nil
}

// UpdateGlueIdentityCenterConfiguration updates the configuration.
func (b *InMemoryBackend) UpdateGlueIdentityCenterConfiguration(instanceARN string) error {
	b.mu.Lock("UpdateGlueIdentityCenterConfiguration")
	defer b.mu.Unlock()

	if b.glueIdentityCenterConfig == nil {
		b.glueIdentityCenterConfig = &IdentityCenterConfig{}
	}

	b.glueIdentityCenterConfig.InstanceARN = instanceARN

	return nil
}

// DeleteGlueIdentityCenterConfiguration removes the configuration.
func (b *InMemoryBackend) DeleteGlueIdentityCenterConfiguration() error {
	b.mu.Lock("DeleteGlueIdentityCenterConfiguration")
	defer b.mu.Unlock()

	b.glueIdentityCenterConfig = nil

	return nil
}
