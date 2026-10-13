package main

import (
	"context"
	"encoding/json"
	"strings"

	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const logsPolicyPageSize = 50

type logsPolicyBackend interface {
	DescribeResourcePolicies(
		policyScope, resourceArn, nextToken string,
		limit int,
	) ([]cwlogsbackend.ResourcePolicy, string)
}

// logsPolicyAdapter serves CloudWatch Logs resource policies (PutResourcePolicy) to the IAM resource-policy evaluator.
type logsPolicyAdapter struct {
	backend logsPolicyBackend
}

// GetResourcePolicy merges every account-scoped policy plus the group's own resource-scoped policy.
func (a *logsPolicyAdapter) GetResourcePolicy(_ context.Context, resourceARN string) (string, error) {
	if !strings.HasPrefix(resourceARN, "arn:aws:logs:") {
		return "", nil
	}

	var (
		statements []json.RawMessage
		token      string
	)

	for {
		policies, next := a.backend.DescribeResourcePolicies("", "", token, logsPolicyPageSize)
		for _, p := range policies {
			statements = append(statements, policyStatements(p.PolicyDocument)...)
		}

		if next == "" {
			break
		}

		token = next
	}

	scoped, _ := a.backend.DescribeResourcePolicies("", strings.TrimSuffix(resourceARN, ":*"), "", 1)
	for _, p := range scoped {
		statements = append(statements, policyStatements(p.PolicyDocument)...)
	}

	if len(statements) == 0 {
		return "", nil
	}

	doc, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": statements})
	if err != nil {
		return "", err
	}

	return string(doc), nil
}

func policyStatements(doc string) []json.RawMessage {
	var parsed struct {
		Statement json.RawMessage `json:"Statement"`
	}

	if json.Unmarshal([]byte(doc), &parsed) != nil || len(parsed.Statement) == 0 {
		return nil
	}

	var list []json.RawMessage
	if json.Unmarshal(parsed.Statement, &list) == nil {
		return list
	}

	return []json.RawMessage{parsed.Statement}
}
