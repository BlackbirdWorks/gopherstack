package glue

// resolveCatalogID returns catalogID if the caller supplied one, otherwise
// the backend's own account ID. Every Create*Input.CatalogId in this service
// defaults to the caller's account ID when omitted (e.g. glue@v1.157.0
// api_op_CreateDatabase.go: "If none is provided, the Amazon Web Services
// account ID is used by default").
func (b *InMemoryBackend) resolveCatalogID(catalogID string) string {
	if catalogID == "" {
		return b.accountID
	}

	return catalogID
}

// catalogIDMismatch reports whether a caller-supplied catalog ID (empty
// means "unspecified", matching any catalog) conflicts with a stored
// resource's catalog ID. A real client naming a Data Catalog the resource
// doesn't belong to gets EntityNotFoundException, since the resource has no
// existence in that catalog's namespace.
func catalogIDMismatch(requested, stored string) bool {
	return requested != "" && requested != stored
}
