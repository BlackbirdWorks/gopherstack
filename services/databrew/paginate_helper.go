package databrew

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func paginateKeys(keys []string, maxResults int, nextToken string) ([]string, string) {
	if maxResults <= 0 {
		maxResults = 100
	}
	startIdx := 0
	if nextToken != "" {
		startIdx = len(keys)
		for i, k := range keys {
			if k > nextToken {
				startIdx = i

				break
			}
		}
	}
	endIdx := startIdx + maxResults
	endIdx = min(endIdx, len(keys))
	var next string
	if endIdx < len(keys) {
		next = keys[endIdx-1]
	}
	if startIdx < len(keys) {
		return keys[startIdx:endIdx], next
	}

	return nil, ""
}

const (
	maxListResults  = 100
	pageTokenPrefix = "dbt-"
)

func listParams(maxResultsStr, nextToken string) (int, string, error) {
	maxResults := 0
	if maxResultsStr != "" {
		n, err := strconv.Atoi(maxResultsStr)
		if err != nil || n < 1 || n > maxListResults {
			return 0, "", fmt.Errorf("%w: MaxResults must be between 1 and %d", errInvalidRequest, maxListResults)
		}
		maxResults = n
	}

	if nextToken == "" {
		return maxResults, "", nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(nextToken)
	if err != nil || !strings.HasPrefix(string(raw), pageTokenPrefix) {
		return 0, "", fmt.Errorf("%w: Invalid NextToken", errInvalidRequest)
	}

	return maxResults, strings.TrimPrefix(string(raw), pageTokenPrefix), nil
}

func marshalPage(key string, items any, next string) ([]byte, error) {
	out := map[string]any{key: items}
	if next != "" {
		out[nextTokenKey] = base64.RawURLEncoding.EncodeToString([]byte(pageTokenPrefix + next))
	}

	return json.Marshal(out)
}
