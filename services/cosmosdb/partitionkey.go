package cosmosdb

import (
	"encoding/json"
	"fmt"
	"strings"
)

// maxPartitionKeyPaths is the hierarchical (MultiHash) partition key depth limit.
const maxPartitionKeyPaths = 3

// normalizePartitionKeyPaths validates and returns the container's partition key paths.
func normalizePartitionKeyPaths(spec ContainerSpec) ([]string, error) {
	paths := spec.PartitionKeyPaths
	if len(paths) == 0 {
		paths = []string{spec.PartitionKeyPath}
	}

	if len(paths) > maxPartitionKeyPaths {
		return nil, ErrInvalidPartitionKeyPath
	}

	seen := make(map[string]struct{}, len(paths))

	for _, p := range paths {
		if len(p) < 2 || !strings.HasPrefix(p, "/") {
			return nil, ErrInvalidPartitionKeyPath
		}

		if _, dup := seen[p]; dup {
			return nil, ErrInvalidPartitionKeyPath
		}

		seen[p] = struct{}{}
	}

	return append([]string(nil), paths...), nil
}

// paths returns the container's partition key paths (always at least one).
func (c *storedContainer) paths() []string {
	if len(c.PartitionKeyPaths) > 0 {
		return c.PartitionKeyPaths
	}

	return []string{c.PartitionKeyPath}
}

// partitionKeyForBody derives the canonical partition key JSON of body: a scalar for one path, a JSON array of
// scalars for hierarchical keys. Absent components are null.
func partitionKeyForBody(body map[string]any, paths []string) (string, error) {
	if len(paths) == 1 {
		v, _ := extractPartitionKeyValue(body, paths[0])

		return canonicalPartitionKeyJSON(v)
	}

	vals := make([]any, len(paths))

	for i, p := range paths {
		v, _ := extractPartitionKeyValue(body, p)
		if _, err := canonicalPartitionKeyJSON(v); err != nil {
			return "", err
		}

		vals[i] = v
	}

	data, err := json.Marshal(vals)
	if err != nil {
		return "", fmt.Errorf("cosmosdb: encode partition key: %w", err)
	}

	return string(data), nil
}

// bodyDeclaresPartitionKey reports whether body carries a value at any of paths.
func bodyDeclaresPartitionKey(body map[string]any, paths []string) bool {
	for _, p := range paths {
		if _, ok := extractPartitionKeyValue(body, p); ok {
			return true
		}
	}

	return false
}

func partitionKeyArity(pkJSON string) int {
	if !strings.HasPrefix(pkJSON, "[") {
		return 1
	}

	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(pkJSON), &arr); err != nil {
		return 0
	}

	return len(arr)
}

// checkPartitionKey rejects a point-operation key whose component count differs from the container's.
func (c *storedContainer) checkPartitionKey(pkJSON string) error {
	if got, want := partitionKeyArity(pkJSON), len(c.paths()); got != want {
		return fmt.Errorf("%w: partition key has %d component(s), container defines %d",
			ErrInvalidDocument, got, want)
	}

	return nil
}

// partitionKeyMatches reports whether a document's key falls under filter: an exact match, or for hierarchical
// keys a leading-components prefix.
func partitionKeyMatches(docPK, filter string) bool {
	if docPK == filter {
		return true
	}

	if !strings.HasPrefix(docPK, "[") {
		return false
	}

	// A one-component filter is a scalar; a longer one is an array. Both prefix-match a hierarchical key.
	if !strings.HasPrefix(filter, "[") {
		return strings.HasPrefix(docPK, "["+filter+",")
	}

	return strings.HasPrefix(docPK, strings.TrimSuffix(filter, "]")+",")
}
