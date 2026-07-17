package ssm

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) serviceSettingsStore(region string) *store.Table[ServiceSetting] {
	return getOrCreateTable(b, b.serviceSettings, "serviceSettings", region, serviceSettingKeyFn)
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
		return &GetServiceSettingOutputFull{ServiceSetting: s}, nil
	}

	return &GetServiceSettingOutputFull{ServiceSetting: &ServiceSetting{
		SettingID:    input.SettingID,
		SettingValue: "",
		Status:       settingStatusDefault,
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
		SettingID:    input.SettingID,
		SettingValue: input.SettingValue,
		Status:       settingStatusCustomized,
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
	}}, nil
}
