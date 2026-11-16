package azuresearch

// CrackBlob exposes document cracking to the external tests.
func CrackBlob(name, contentType string, data []byte, cfg map[string]any) []map[string]any {
	return crackBlob(BlobEntry{Name: name, ContentType: contentType}, data, "acct", "docs", cfg)
}
