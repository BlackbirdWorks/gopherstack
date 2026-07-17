package autoscaling

import (
	"fmt"
	"strings"
)

// DescribeAutoScalingNotificationTypes returns the supported notification types.
func (b *InMemoryBackend) DescribeAutoScalingNotificationTypes() ([]string, error) {
	return []string{
		"autoscaling:EC2_INSTANCE_LAUNCH",
		"autoscaling:EC2_INSTANCE_LAUNCH_ERROR",
		"autoscaling:EC2_INSTANCE_TERMINATE",
		"autoscaling:EC2_INSTANCE_TERMINATE_ERROR",
		"autoscaling:TEST_NOTIFICATION",
		"autoscaling:EC2_INSTANCE_IN_STANDBY",
	}, nil
}

// PutNotificationConfiguration stores or updates a notification configuration.
func (b *InMemoryBackend) PutNotificationConfiguration(groupName, topicARN string, types []string) error {
	b.mu.Lock("PutNotificationConfiguration")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	// Validate SNS ARN format
	if topicARN != "" &&
		!strings.HasPrefix(topicARN, "arn:aws:sns:") &&
		!strings.HasPrefix(topicARN, "arn:aws-cn:sns:") &&
		!strings.HasPrefix(topicARN, "arn:aws-us-gov:sns:") {
		return fmt.Errorf("%w: TopicARN must be a valid SNS ARN", ErrInvalidParameter)
	}

	// Remove existing configs for this group+topic
	existing := b.notificationConfigs[groupName]
	newConfigs := make([]*NotificationConfiguration, 0, len(existing)+len(types))

	for _, c := range existing {
		if c.TopicARN != topicARN {
			newConfigs = append(newConfigs, c)
		}
	}

	for _, t := range types {
		nc := &NotificationConfiguration{
			AutoScalingGroupName: groupName,
			TopicARN:             topicARN,
			NotificationType:     t,
		}
		newConfigs = append(newConfigs, nc)
	}

	b.notificationConfigs[groupName] = newConfigs

	return nil
}

// DeleteNotificationConfiguration removes notification configs for a group+topic.
func (b *InMemoryBackend) DeleteNotificationConfiguration(groupName, topicARN string) error {
	b.mu.Lock("DeleteNotificationConfiguration")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	existing := b.notificationConfigs[groupName]
	newConfigs := make([]*NotificationConfiguration, 0, len(existing))

	for _, c := range existing {
		if c.TopicARN != topicARN {
			newConfigs = append(newConfigs, c)
		}
	}

	b.notificationConfigs[groupName] = newConfigs

	return nil
}

// DescribeNotificationConfigurations returns notification configs for the given groups.
func (b *InMemoryBackend) DescribeNotificationConfigurations(groupNames []string) ([]NotificationConfiguration, error) {
	b.mu.RLock("DescribeNotificationConfigurations")
	defer b.mu.RUnlock()

	var result []NotificationConfiguration

	if len(groupNames) > 0 {
		for _, name := range groupNames {
			for _, c := range b.notificationConfigs[name] {
				result = append(result, *c)
			}
		}
	} else {
		for _, configs := range b.notificationConfigs {
			for _, c := range configs {
				result = append(result, *c)
			}
		}
	}

	return result, nil
}
