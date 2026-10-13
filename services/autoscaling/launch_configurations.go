package autoscaling

import (
	"cmp"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateLaunchConfiguration creates a new launch configuration.
func (b *InMemoryBackend) CreateLaunchConfiguration(
	input CreateLaunchConfigurationInput,
) (*LaunchConfiguration, error) {
	b.mu.Lock("CreateLaunchConfiguration")
	defer b.mu.Unlock()

	return b.createLaunchConfigurationLocked(input)
}

func (b *InMemoryBackend) createLaunchConfigurationLocked(
	input CreateLaunchConfigurationInput,
) (*LaunchConfiguration, error) {
	if b.launchConfigurations.Has(input.LaunchConfigurationName) {
		return nil, fmt.Errorf(
			"%w: launch configuration %q already exists",
			ErrLaunchConfigurationAlreadyExists,
			input.LaunchConfigurationName,
		)
	}

	if input.LaunchConfigurationName == "" {
		return nil, fmt.Errorf("%w: LaunchConfigurationName is required", ErrInvalidParameter)
	}

	if err := b.applyInstanceAttributes(&input); err != nil {
		return nil, err
	}

	lc := &LaunchConfiguration{
		LaunchConfigurationName: input.LaunchConfigurationName,
		LaunchConfigurationARN: fmt.Sprintf(
			"arn:aws:autoscaling:%s:%s:launchConfiguration:%s:launchConfigurationName/%s",
			b.region, b.accountID, uuid.NewString(), input.LaunchConfigurationName,
		),
		ImageID:                      input.ImageID,
		InstanceType:                 input.InstanceType,
		KeyName:                      input.KeyName,
		IAMInstanceProfile:           input.IAMInstanceProfile,
		UserData:                     input.UserData,
		KernelID:                     input.KernelID,
		RamdiskID:                    input.RamdiskID,
		SpotPrice:                    input.SpotPrice,
		PlacementTenancy:             input.PlacementTenancy,
		ClassicLinkVPCID:             input.ClassicLinkVPCID,
		SecurityGroups:               input.SecurityGroups,
		ClassicLinkVPCSecurityGroups: input.ClassicLinkVPCSecurityGroups,
		BlockDeviceMappings:          input.BlockDeviceMappings,
		MetadataOptions:              input.MetadataOptions,
		AssociatePublicIPAddress:     input.AssociatePublicIPAddress,
		EbsOptimized:                 input.EbsOptimized,
		InstanceMonitoring:           input.InstanceMonitoring,
		CreatedTime:                  time.Now(),
	}

	b.launchConfigurations.Put(lc)

	cp := *lc

	return &cp, nil
}

// DescribeLaunchConfigurations returns launch configurations, optionally filtered by name.
func (b *InMemoryBackend) DescribeLaunchConfigurations(names []string) ([]LaunchConfiguration, error) {
	b.mu.RLock("DescribeLaunchConfigurations")
	defer b.mu.RUnlock()

	result := describeByNames(b.launchConfigurations, names,
		func(a, c *LaunchConfiguration) bool {
			return a.LaunchConfigurationName < c.LaunchConfigurationName
		})

	return result, nil
}

// DeleteLaunchConfiguration removes a launch configuration by name.
// api_op_DeleteLaunchConfiguration.go: "The launch configuration must not be
// attached to an Auto Scaling group".
func (b *InMemoryBackend) DeleteLaunchConfiguration(name string) error {
	b.mu.Lock("DeleteLaunchConfiguration")
	defer b.mu.Unlock()

	if !b.launchConfigurations.Has(name) {
		return fmt.Errorf("%w: %q", ErrLaunchConfigurationNotFound, name)
	}

	for _, g := range b.groups.All() {
		if g.LaunchConfigurationName == name {
			return fmt.Errorf("%w: launch configuration %q is still attached to Auto Scaling group %q",
				ErrLaunchConfigurationInUse, name, g.AutoScalingGroupName)
		}
	}

	b.launchConfigurations.Delete(name)

	return nil
}

// applyInstanceAttributes fills launch settings the request left unset from
// the EC2 instance named by InstanceId (block device mappings are not derived).
func (b *InMemoryBackend) applyInstanceAttributes(input *CreateLaunchConfigurationInput) error {
	if input.InstanceID == "" {
		return nil
	}

	attrs, ok := b.instanceAttributes(input.InstanceID)
	if !ok {
		return fmt.Errorf("%w: The instance %q does not exist", ErrInvalidParameter, input.InstanceID)
	}

	input.ImageID = cmp.Or(input.ImageID, attrs.ImageID)
	input.InstanceType = cmp.Or(input.InstanceType, attrs.InstanceType)
	input.KeyName = cmp.Or(input.KeyName, attrs.KeyName)
	input.UserData = cmp.Or(input.UserData, attrs.UserData)

	if len(input.SecurityGroups) == 0 {
		input.SecurityGroups = attrs.SecurityGroups
	}

	return nil
}
