package azureblob

// Exported wrappers for internal functions used in blackbox tests.

// ParseRange exposes parseRange for external tests.
func ParseRange(header string, size int64) (start, end int64, ok bool) {
	return parseRange(header, size)
}

// SplitPath exposes splitPath for external tests.
func SplitPath(p string) (account, container, blob string) {
	return splitPath(p)
}
