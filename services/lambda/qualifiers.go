package lambda

import (
	mrand "math/rand/v2"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// versionLatest is the sentinel qualifier for the live function configuration.
const versionLatest = "$LATEST"

// globalRand is used for non-security random choices (e.g. weighted alias routing).
//
//nolint:gochecknoglobals // intentional package-level RNG for weighted routing
var globalRand = mrand.New(mrand.NewPCG(0, 1)) //nolint:gosec // non-security use

// versionInList reports whether target matches any version in the list.
func versionInList(versions []*FunctionVersion, target string) bool {
	for _, v := range versions {
		if v.Version == target {
			return true
		}
	}

	return false
}

// extractFunctionName parses an ARN and returns the function name.
func extractFunctionName(name string) string {
	if strings.Contains(name, ":function:") {
		parts := strings.Split(name, ":")
		for i, p := range parts {
			if p == "function" && i+1 < len(parts) {
				return parts[i+1]
			}
		}
	}

	return name
}

// functionNameAndQualifierFromARN splits a full/partial Lambda function ARN, or
// the bare "name:qualifier" shorthand AWS also accepts, into the bare function
// name and an optional version/alias qualifier.
//
// Examples:
//
//	arn:aws:lambda:us-east-1:000000000000:function:my-func       -> ("my-func", "")
//	arn:aws:lambda:us-east-1:000000000000:function:my-func:PROD  -> ("my-func", "PROD")
//	my-func:PROD                                                 -> ("my-func", "PROD")
//	my-func                                                      -> ("my-func", "")
//
// Function names never contain a colon (AWS restricts them to
// [a-zA-Z0-9-_]+), so any colon in the input unambiguously marks an ARN
// boundary or an appended qualifier — never returns a false split.
func functionNameAndQualifierFromARN(name string) (string, string) {
	parts := strings.Split(name, ":")
	for i, p := range parts {
		if p == "function" && i+1 < len(parts) {
			fnName := parts[i+1]
			qualifier := ""
			if i+2 < len(parts) {
				qualifier = parts[i+2]
			}

			return fnName, qualifier
		}
	}

	// Not an ARN with a "function:" segment. Support the bare "name:qualifier"
	// shorthand (e.g. AddPermission/RemovePermission's "my-function:v1" format).
	if before, after, ok := strings.Cut(name, ":"); ok && before != "" && after != "" {
		return before, after
	}

	return name, ""
}

// resolveQualifier resolves a function name with an optional qualifier to a FunctionConfiguration.
// Qualifier may be a version number, alias name, or "$LATEST" (default when empty).
// Returns the resolved function config.
func (b *InMemoryBackend) resolveQualifier(name, qualifier string) (*FunctionConfiguration, error) {
	name = extractFunctionName(name)
	if qualifier == "" || qualifier == versionLatest {
		return b.GetFunction(name)
	}

	// Check if qualifier is an alias; if so, resolve to the target version string.
	// Hold a single RLock for both the alias lookup and the version search to avoid
	// TOCTOU races with concurrent alias/version updates.
	var fn *FunctionConfiguration

	func() {
		b.mu.RLock("resolveQualifier")
		defer b.mu.RUnlock()

		if alias, ok := b.aliases.Get(aliasKey(name, qualifier)); ok {
			qualifier = selectAliasVersion(alias)
		}

		// Now qualifier is a version number. Find the version snapshot.
		if vMap := b.versionIndex[name]; vMap != nil {
			if v, ok := vMap[qualifier]; ok {
				fn = versionToFn(v)
			}
		}
	}()

	if fn != nil {
		return fn, nil
	}

	// If it's "$LATEST" after alias resolution, fall through to live config.
	if qualifier == versionLatest {
		return b.GetFunction(name)
	}

	return nil, ErrVersionNotFound
}

// selectAliasVersion picks the target version for an alias invocation, respecting weighted
// routing when RoutingConfig.AdditionalVersionWeights is set.
//
// AWS routing: AdditionalVersionWeights maps a secondary version to a weight (0–1).
// A random float in [0,1) < secondaryWeight routes to the secondary version; otherwise
// the primary alias.FunctionVersion is used.
func selectAliasVersion(alias *FunctionAlias) string {
	if alias.RoutingConfig == nil || len(alias.RoutingConfig.AdditionalVersionWeights) == 0 {
		return alias.FunctionVersion
	}

	// Accumulate weights; the first bucket whose cumulative weight exceeds a random
	// value [0,1) wins. If no bucket wins (total weight < 1), the primary version is used.
	r := globalRand.Float64()
	var cumulative float64

	for version, weight := range alias.RoutingConfig.AdditionalVersionWeights {
		cumulative += weight

		if r < cumulative {
			return version
		}
	}

	return alias.FunctionVersion
}

// buildVersionARN constructs a Lambda function version ARN.
func buildVersionARN(region, accountID, functionName, version string) string {
	return arn.Build("lambda", region, accountID, "function:"+functionName+":"+version)
}

// buildAliasARN constructs a Lambda function alias ARN.
func buildAliasARN(region, accountID, functionName, aliasName string) string {
	return arn.Build("lambda", region, accountID, "function:"+functionName+":"+aliasName)
}

// --- AddPermission / resource-based policy ---

// permissionMapKey returns the b.permissions map key for a function+qualifier
// pair. Real Lambda resource policies are scoped per version/alias when
// AddPermission is called with a Qualifier — the unqualified function-wide
// policy and each qualified policy are entirely independent documents.
func permissionMapKey(functionName, qualifier string) string {
	if qualifier == "" {
		return functionName
	}

	return functionName + ":" + qualifier
}

// qualifierExistsLocked reports whether qualifier resolves to a known alias
// or published version of name. Caller must hold b.mu (read or write).
func (b *InMemoryBackend) qualifierExistsLocked(name, qualifier string) bool {
	if qualifier == "" || qualifier == versionLatest {
		return true
	}

	if _, ok := b.aliases.Get(aliasKey(name, qualifier)); ok {
		return true
	}

	if vMap := b.versionIndex[name]; vMap != nil {
		if _, ok := vMap[qualifier]; ok {
			return true
		}
	}

	return false
}

// resolvePermissionTarget normalizes the FunctionName/Qualifier pair used by
// AddPermission, RemovePermission, and GetPolicy. functionName may be a bare
// name, a full/partial ARN, or the "name:qualifier" shorthand; qualifier is
// the separate query-string Qualifier (may be empty). An explicit Qualifier
// query parameter wins over one embedded in functionName.
func resolvePermissionTarget(functionName, qualifier string) (string, string) {
	name, arnQualifier := functionNameAndQualifierFromARN(functionName)
	if qualifier == "" {
		qualifier = arnQualifier
	}

	return name, qualifier
}
