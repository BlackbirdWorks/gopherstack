package ssm

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) serviceSettingsStore(region string) *store.Table[ServiceSetting] {
	return getOrCreateTable(b, b.serviceSettings, "serviceSettings", region, serviceSettingKeyFn)
}

// serviceSettingARN builds the real ARN shape for a service setting
// (api_op_UpdateServiceSetting.go doc comment example:
// "arn:aws:ssm:us-east-1:111122223333:servicesetting/ssm/parameter-store/high-throughput-enabled"
// -- settingID already carries its own leading slash).
func serviceSettingARN(region, settingID string) string {
	return arn.Build("ssm", region, defaultAccountID, "servicesetting"+settingID)
}

// GetServiceSetting returns the value for a service setting.
func (b *InMemoryBackend) GetServiceSetting(
	ctx context.Context,
	input *GetServiceSettingInput,
) (*GetServiceSettingOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetServiceSetting")
	defer b.mu.RUnlock()

	if s, exists := b.serviceSettingsStore(region).Get(input.SettingID); exists {
		out := *s
		out.ARN = serviceSettingARN(region, input.SettingID)

		return &GetServiceSettingOutputFull{ServiceSetting: &out}, nil
	}

	return &GetServiceSettingOutputFull{ServiceSetting: &ServiceSetting{
		SettingID:    input.SettingID,
		SettingValue: "",
		Status:       settingStatusDefault,
		ARN:          serviceSettingARN(region, input.SettingID),
	}}, nil
}

// UpdateServiceSetting stores a custom value for a service setting.
func (b *InMemoryBackend) UpdateServiceSetting(
	ctx context.Context,
	input *UpdateServiceSettingInput,
) (*UpdateServiceSettingOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateServiceSetting")
	defer b.mu.Unlock()

	b.serviceSettingsStore(region).Put(&ServiceSetting{
		SettingID:        input.SettingID,
		SettingValue:     input.SettingValue,
		Status:           settingStatusCustomized,
		LastModifiedDate: UnixTimeFloat(time.Now()),
	})

	return &UpdateServiceSettingOutput{}, nil
}

// ResetServiceSetting removes any custom value for a service setting.
func (b *InMemoryBackend) ResetServiceSetting(
	ctx context.Context,
	input *ResetServiceSettingInput,
) (*ResetServiceSettingOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("ResetServiceSetting")
	defer b.mu.Unlock()

	b.serviceSettingsStore(region).Delete(input.SettingID)

	return &ResetServiceSettingOutputFull{ServiceSetting: &ServiceSetting{
		SettingID: input.SettingID,
		Status:    settingStatusDefault,
		ARN:       serviceSettingARN(region, input.SettingID),
	}}, nil
}
