package azureblob

// Exported wrappers for internal functions used in blackbox tests.

// ParseRange exposes parseRange for external tests.
func ParseRange(header string, size int64) (int64, int64, bool) {
	return parseRange(header, size)
}

// SplitPath exposes splitPath for external tests.
func SplitPath(p string) (string, string, string) {
	return splitPath(p)
}
