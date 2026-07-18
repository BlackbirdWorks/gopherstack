package securityhub

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) hubARN() string {
	return arn.Build("securityhub", b.region, b.accountID, "hub/default")
}

func (b *InMemoryBackend) EnableHub(enableDefaultStandards bool, tags map[string]string) error {
	b.mu.Lock("EnableHub")
	defer b.mu.Unlock()

	if b.hubEnabled {
		return ErrHubAlreadyExists
	}

	b.hubEnabled = true
	now := time.Now().UTC().Format(time.RFC3339)
	b.hub = &Hub{
		HubArn:                  b.hubARN(),
		SubscribedAt:            now,
		AutoEnableControls:      true,
		AutoEnableStandards:     "DEFAULT",
		ControlFindingGenerator: "SECURITY_CONTROL",
	}

	if len(tags) > 0 {
		b.tags[b.hub.HubArn] = tags
	}

	if enableDefaultStandards {
		for i, std := range knownStandards {
			if std.EnabledByDefault {
				b.standardsSeq++
				subArn := fmt.Sprintf(
					"arn:aws:securityhub:%s:%s:subscription/%s/v/1.0.0",
					b.region,
					b.accountID,
					fmt.Sprintf("default-%d", i),
				)
				b.standardsSubscriptions.Put(&StandardsSubscription{
					StandardsSubscriptionArn: subArn,
					StandardsArn:             std.StandardsArn,
					StandardsInput:           map[string]string{},
					StandardsStatus:          "READY",
				})
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) DisableHub() error {
	b.mu.Lock("DisableHub")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return ErrHubNotEnabled
	}

	b.hubEnabled = false
	b.hub = nil

	return nil
}

func (b *InMemoryBackend) DescribeHub() (*Hub, error) {
	b.mu.RLock("DescribeHub")
	defer b.mu.RUnlock()

	if !b.hubEnabled || b.hub == nil {
		return nil, ErrHubNotEnabled
	}

	cp := *b.hub

	return &cp, nil
}

func (b *InMemoryBackend) UpdateHubConfiguration(
	autoEnableControls *bool,
	autoEnableStandards *string,
	controlFindingGenerator *string,
) error {
	b.mu.Lock("UpdateHubConfiguration")
	defer b.mu.Unlock()

	if !b.hubEnabled || b.hub == nil {
		return ErrHubNotEnabled
	}

	if autoEnableControls != nil {
		b.hub.AutoEnableControls = *autoEnableControls
	}

	if autoEnableStandards != nil {
		b.hub.AutoEnableStandards = *autoEnableStandards
	}

	if controlFindingGenerator != nil {
		b.hub.ControlFindingGenerator = *controlFindingGenerator
	}

	return nil
}

func (b *InMemoryBackend) hubV2ARN() string {
	return arn.Build("securityhub", b.region, b.accountID, "hub-v2/default")
}

func (b *InMemoryBackend) EnableSecurityHubV2(tags map[string]string) error {
	b.mu.Lock("EnableSecurityHubV2")
	defer b.mu.Unlock()

	if b.hubV2Enabled {
		return ErrHubAlreadyExists
	}

	now := time.Now().UTC().Format(time.RFC3339)
	b.hubV2Enabled = true
	b.hubV2 = &HubV2{
		HubV2Arn:  b.hubV2ARN(),
		CreatedAt: now,
		UpdatedAt: now,
	}

	if len(tags) > 0 {
		b.tags[b.hubV2.HubV2Arn] = tags
	}

	return nil
}

func (b *InMemoryBackend) DisableSecurityHubV2() error {
	b.mu.Lock("DisableSecurityHubV2")
	defer b.mu.Unlock()

	if !b.hubV2Enabled {
		return ErrHubNotEnabled
	}

	b.hubV2Enabled = false
	b.hubV2 = nil

	return nil
}

func (b *InMemoryBackend) DescribeSecurityHubV2() (*HubV2, error) {
	b.mu.RLock("DescribeSecurityHubV2")
	defer b.mu.RUnlock()

	if !b.hubV2Enabled || b.hubV2 == nil {
		return nil, ErrHubNotEnabled
	}

	cp := *b.hubV2

	return &cp, nil
}
