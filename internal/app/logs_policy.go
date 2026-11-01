package app

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
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
		statements []jsontext.Value
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

	doc, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": statements}, jsonx.V1())
	if err != nil {
		return "", err
	}

	return string(doc), nil
}

func policyStatements(doc string) []jsontext.Value {
	var parsed struct {
		Statement jsontext.Value `json:"Statement"`
	}

	if json.Unmarshal([]byte(doc), &parsed, jsonx.V1()) != nil || len(parsed.Statement) == 0 {
		return nil
	}

	var list []jsontext.Value
	if json.Unmarshal(parsed.Statement, &list, jsonx.V1()) == nil {
		return list
	}

	return []jsontext.Value{parsed.Statement}
}
