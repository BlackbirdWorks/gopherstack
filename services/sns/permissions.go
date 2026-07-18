package sns

import (
	"fmt"
)

// isValidPermissionLabel returns true if the label is non-empty, not longer than
// maxPermissionLabelLen, and contains only alphanumeric characters or hyphens.
func isValidPermissionLabel(label string) bool {
	if label == "" || len(label) > maxPermissionLabelLen {
		return false
	}

	for _, c := range label {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}

	return true
}

// AddPermission adds a permission statement to an SNS topic's access policy.
// Duplicate labels are rejected with ErrPermissionLabelExists.
// Labels must be non-empty, at most 80 characters, and consist only of alphanumeric
// characters or hyphens; invalid labels are rejected with ErrInvalidParameter.
func (b *InMemoryBackend) AddPermission(topicArn, label string, accounts, actions []string) error {
	if !isValidPermissionLabel(label) {
		return fmt.Errorf(
			"%w: Label must be non-empty, max 80 chars, alphanumeric or hyphen",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("AddPermission")
	defer b.mu.Unlock()

	topic, exists := b.topics.Get(topicArn)
	if !exists {
		return ErrTopicNotFound
	}

	if topic.Permissions == nil {
		topic.Permissions = make(map[string]*TopicPermission)
	}

	if _, alreadyExists := topic.Permissions[label]; alreadyExists {
		return ErrPermissionLabelExists
	}

	topic.Permissions[label] = &TopicPermission{
		Label:      label,
		AWSAccount: accounts,
		Actions:    actions,
	}

	return nil
}

// RemovePermission removes a permission statement (identified by label) from an SNS topic.
func (b *InMemoryBackend) RemovePermission(topicArn, label string) error {
	b.mu.Lock("RemovePermission")
	defer b.mu.Unlock()

	topic, exists := b.topics.Get(topicArn)
	if !exists {
		return ErrTopicNotFound
	}

	if topic.Permissions == nil {
		return ErrPermissionLabelNotFound
	}

	if _, labelExists := topic.Permissions[label]; !labelExists {
		return ErrPermissionLabelNotFound
	}

	delete(topic.Permissions, label)

	return nil
}
