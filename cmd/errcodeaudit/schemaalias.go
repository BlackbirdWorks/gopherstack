package main

import (
	"os"
	"regexp"
)

var queryAliasRe = regexp.MustCompile(`AWSQueryError\{\s*ErrorCode:\s*"([^"]+)"`)

// parseSchemaQueryAliases reads the awsQuery wire codes that schema-based
// SDKs declare via AWSQueryError{ErrorCode: ...}; they differ from type names.
func parseSchemaQueryAliases(schemasPath string) (map[string]bool, error) {
	data, err := os.ReadFile(schemasPath)
	if os.IsNotExist(err) {
		return map[string]bool{}, nil
	}

	if err != nil {
		return nil, err
	}

	out := map[string]bool{}

	for _, m := range queryAliasRe.FindAllSubmatch(data, -1) {
		out[string(m[1])] = true
	}

	return out, nil
}
