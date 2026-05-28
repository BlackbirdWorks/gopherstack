package databrew

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
