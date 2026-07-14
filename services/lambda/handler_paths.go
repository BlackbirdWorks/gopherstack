package lambda

import "strings"

// lambdaPathPrefix is the path prefix for Lambda REST API v1 endpoints.
const lambdaPathPrefix = "/2015-03-31/functions"

// lambda2017PathPrefix is the API date prefix used by the AWS SDK v2 for
// reserved concurrency operations (PutFunctionConcurrency, DeleteFunctionConcurrency).
const lambda2017PathPrefix = "/2017-10-31/functions"

// lambda2019PathPrefix is the API date prefix used by the AWS SDK v2 for
// provisioned concurrency and GetFunctionConcurrency operations.
const lambda2019PathPrefix = "/2019-09-30/functions"

// lambda2020PathPrefix is the path prefix for Lambda REST API v2 endpoints (e.g. code signing configs).
const lambda2020PathPrefix = "/2020-06-30/functions"

// lambda2021PathPrefix is the path prefix for Lambda function URL config endpoints (SDK "Url" casing).
const lambda2021PathPrefix = "/2021-10-31/functions"

// lambda2021RuntimeMgmtPathPrefix is the path prefix for runtime management config endpoints.
const lambda2021RuntimeMgmtPathPrefix = "/2021-07-20/functions"

// lambda2024RecursionPathPrefix is the path prefix for function recursion config endpoints.
const lambda2024RecursionPathPrefix = "/2024-08-28/functions"

// lambda2023ScalingPathPrefix is the path prefix for function scaling config endpoints.
const lambda2023ScalingPathPrefix = "/2023-10-26/functions"

// lambda2014AsyncPathPrefix is the path prefix for the legacy InvokeAsync endpoint.
const lambda2014AsyncPathPrefix = "/2014-11-13/functions"

// lambda2021StreamingPathPrefix is the path prefix for InvokeWithResponseStream.
const lambda2021StreamingPathPrefix = "/2021-11-15/functions"

// lambdaFunctionPrefixes holds all the date-versioned /functions path prefixes that
// Gopherstack normalises to lambdaPathPrefix for route matching.
//
//nolint:gochecknoglobals // intentional package-level prefix table
var lambdaFunctionPrefixes = []string{
	lambdaPathPrefix,
	lambda2017PathPrefix,
	lambda2019PathPrefix,
	lambda2020PathPrefix,
	lambda2014AsyncPathPrefix,
	lambda2021StreamingPathPrefix,
}

// esmPathPrefix is the path prefix for Lambda event source mapping endpoints.
const esmPathPrefix = "/2015-03-31/event-source-mappings"

// lambdaTagsPathPrefix is the path prefix for Lambda resource tag endpoints.
const lambdaTagsPathPrefix = "/2015-03-31/tags"

// lambdaLayersPathPrefix is the path prefix for Lambda Layers endpoints.
// The Lambda Layers API uses the 2018-10-31 date version.
const lambdaLayersPathPrefix = "/2018-10-31/layers"

// lambdaCodeSigningPathPrefix is the path prefix for Lambda code signing config endpoints.
const lambdaCodeSigningPathPrefix = "/2020-04-22/code-signing-configs"

// lambdaCapacityPathPrefix is the path prefix for Lambda capacity provider endpoints.
const lambdaCapacityPathPrefix = "/2025-11-30/capacity-providers"

// lambdaDurableExecPathPrefix is the path prefix for Lambda durable execution endpoints.
const lambdaDurableExecPathPrefix = "/2025-12-01/durable-executions"

// lambdaAccountSettingsPath is the exact path for the GetAccountSettings endpoint.
const lambdaAccountSettingsPath = "/2016-08-19/account-settings"

// lambdaLayersByArnPath is the path prefix for GetLayerVersionByArn (query-param based).
const lambdaLayersByArnPath = "/2018-10-31/layers-by-arn"

func isEmptyRest(rest string) bool { return rest == "" }

func hasSuffixCode(rest string) bool { return strings.HasSuffix(rest, "/code") }

func hasSuffixConfiguration(rest string) bool { return strings.HasSuffix(rest, "/configuration") }

func hasSuffixInvocations(rest string) bool { return strings.HasSuffix(rest, "/invocations") }

func hasSuffixURL(rest string) bool { return strings.HasSuffix(rest, "/url") }

func hasSuffixConcurrency(rest string) bool { return strings.HasSuffix(rest, "/concurrency") }

func hasSuffixProvisionedConcurrency(rest string) bool {
	return strings.HasSuffix(rest, "/provisioned-concurrency")
}

func hasSuffixEventInvokeConfig(rest string) bool {
	return strings.HasSuffix(rest, "/event-invoke-config")
}

func hasSuffixEventInvokeConfigs(rest string) bool {
	return strings.HasSuffix(rest, "/event-invoke-configs")
}

func hasSuffixCodeSigningConfig(rest string) bool {
	return strings.HasSuffix(rest, "/code-signing-config")
}

func hasSuffixPolicy(rest string) bool { return strings.HasSuffix(rest, "/policy") }

// hasPolicyStatementSuffix reports whether rest is a RemovePermission path of
// the form "/{name}/policy/{statementId}" — the real wire format, where the
// SDK embeds StatementId as a URI path segment (never a query parameter).
func hasPolicyStatementSuffix(rest string) bool {
	trimmed := strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(trimmed, "/", 3) //nolint:mnd // name, "policy", statementId

	return len(parts) == 3 && parts[1] == "policy" && parts[2] != ""
}

// extractNameAndPolicyStatement extracts the function name and StatementId
// from a RemovePermission path of the form "/{name}/policy/{statementId}".
func extractNameAndPolicyStatement(rest string) (string, string) {
	trimmed := strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(trimmed, "/", 3) //nolint:mnd // name, "policy", statementId

	var name, statementID string

	if len(parts) >= 1 {
		name = parts[0]
	}

	if len(parts) >= 3 { //nolint:mnd // parts: name, "policy", statementId
		statementID = parts[2]
	}

	return name, statementID
}

func hasSuffixVersions(rest string) bool { return strings.HasSuffix(rest, "/versions") }

func hasSuffixInvokeAsync(rest string) bool { return strings.HasSuffix(rest, "/invoke-async/") }

func hasSuffixResponseStream(rest string) bool {
	return strings.HasSuffix(rest, "/response-streaming-invocations")
}

func hasSuffixAliasPath(rest string) bool {
	trimmed := strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(trimmed, "/", 3) //nolint:mnd // split into name + "aliases" + optional alias name

	return len(parts) >= 2 && parts[1] == "aliases"
}

// lambdaPathPrefixes holds all path prefixes handled by the Lambda service.
//
//nolint:gochecknoglobals // intentional package-level prefix table
var lambdaPathPrefixes = []string{
	lambdaPathPrefix,
	lambda2017PathPrefix,
	lambda2019PathPrefix,
	lambda2020PathPrefix,
	lambda2021PathPrefix,
	lambda2021RuntimeMgmtPathPrefix,
	lambda2023ScalingPathPrefix,
	lambda2024RecursionPathPrefix,
	lambda2014AsyncPathPrefix,
	lambda2021StreamingPathPrefix,
	esmPathPrefix,
	lambdaTagsPathPrefix,
	lambdaLayersPathPrefix,
	lambdaCodeSigningPathPrefix,
	lambdaCapacityPathPrefix,
	lambdaDurableExecPathPrefix,
}

// isLambdaPath returns true when the given path belongs to the Lambda service.
func isLambdaPath(path string) bool {
	if path == lambdaAccountSettingsPath || path == lambdaLayersByArnPath {
		return true
	}

	for _, prefix := range lambdaPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

// normalizeFunctionPath strips the date-versioned /functions prefix from path and
// returns the remainder (including the leading slash before the function name).
// It falls back to stripping lambdaPathPrefix when no other prefix matches.
func normalizeFunctionPath(path string) string {
	for _, prefix := range lambdaFunctionPrefixes {
		if after, ok := strings.CutPrefix(path, prefix); ok {
			return after
		}
	}

	return strings.TrimPrefix(path, lambdaPathPrefix)
}

// isNameOnly returns true when rest is a single path segment (/{name} with no sub-paths).
func isNameOnly(rest string) bool {
	trimmed := strings.TrimPrefix(rest, "/")

	return trimmed != "" && !strings.Contains(trimmed, "/")
}

// nameFromRest strips the leading slash from a single-segment path like /{name}.
func nameFromRest(rest string) string {
	return strings.TrimPrefix(rest, "/")
}
