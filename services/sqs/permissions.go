package sqs

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// iamPolicyDocument represents the IAM resource policy document stored in the
// queue's Policy attribute. AWS SQS serializes AddPermission calls into a JSON
// policy document of this shape.
type iamPolicyDocument struct {
	Version   string               `json:"Version"`
	Statement []iamPolicyStatement `json:"Statement"`
}

// iamPolicyStatement is a single Allow statement within a queue's resource policy.
type iamPolicyStatement struct {
	Principal map[string][]string `json:"Principal"`
	Resource  string              `json:"Resource"`
	Sid       string              `json:"Sid"`
	Effect    string              `json:"Effect"`
	Action    []string            `json:"Action"`
}

// buildQueueIAMPolicy rebuilds the IAM resource policy JSON for q from its
// current Permissions map and stores it in q.Attributes[attrPolicy].
// Must be called with b.mu held (write).
func buildQueueIAMPolicy(q *Queue) {
	if len(q.Permissions) == 0 {
		delete(q.Attributes, attrPolicy)

		return
	}

	queueARN := q.Attributes[attrQueueArn]

	// Iterate in sorted label order so the output is deterministic.
	labels := collections.SortedKeys(q.Permissions)

	stmts := make([]iamPolicyStatement, 0, len(labels))
	for _, label := range labels {
		entry := q.Permissions[label]

		actions := make([]string, 0, len(entry.Actions))
		for _, a := range entry.Actions {
			switch {
			case strings.HasPrefix(a, "sqs:"):
				actions = append(actions, a)
			case a == "*":
				actions = append(actions, "sqs:*")
			default:
				actions = append(actions, "sqs:"+a)
			}
		}

		principals := make([]string, 0, len(entry.AWSAccountIDs))
		for _, id := range entry.AWSAccountIDs {
			if id == "*" {
				principals = append(principals, "*")
			} else {
				principals = append(principals, "arn:aws:iam::"+id+":root")
			}
		}

		stmts = append(stmts, iamPolicyStatement{
			Sid:    label,
			Effect: "Allow",
			Principal: map[string][]string{
				"AWS": principals,
			},
			Action:   actions,
			Resource: queueARN,
		})
	}

	doc := iamPolicyDocument{
		Version:   "2012-10-17",
		Statement: stmts,
	}

	raw, err := json.Marshal(doc)
	if err != nil {
		return
	}

	q.Attributes[attrPolicy] = string(raw)
}

// AddPermission adds a permission statement to the specified queue.
func (b *InMemoryBackend) AddPermission(input *AddPermissionInput) error {
	if input.Label == "" {
		return ErrInvalidPermissionLabel
	}

	if len(input.Actions) == 0 {
		return ErrInvalidPermissionActions
	}

	if len(input.AWSAccountIDs) == 0 {
		return ErrInvalidPermissionAccountIDs
	}

	b.mu.Lock("AddPermission")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	q.Permissions[input.Label] = &QueuePermissionEntry{
		AWSAccountIDs: slices.Clone(input.AWSAccountIDs),
		Actions:       slices.Clone(input.Actions),
	}

	buildQueueIAMPolicy(q)

	return nil
}

// RemovePermission removes a permission statement from the specified queue.
func (b *InMemoryBackend) RemovePermission(input *RemovePermissionInput) error {
	b.mu.Lock("RemovePermission")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	delete(q.Permissions, input.Label)

	buildQueueIAMPolicy(q)

	return nil
}
