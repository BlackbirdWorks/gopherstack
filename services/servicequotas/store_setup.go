package servicequotas

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func requestKey(v *Request) string               { return v.ID }
func appliedValueKey(v *AppliedValue) string     { return v.Key }
func templateItemKey(v *TemplateItem) string     { return v.Key }
func associationKey(v *Association) string       { return v.Account }
func reportKey(v *Report) string                 { return v.ID }
func autoManagementKey(v *AutoManagement) string { return v.Key }
func resourceTagsKey(v *ResourceTags) string     { return v.ARN }

func registerAllTables(b *InMemoryBackend) {
	b.requests = store.Register(b.registry, "requests", store.New(requestKey))
	b.applied = store.Register(b.registry, "applied", store.New(appliedValueKey))
	b.templates = store.Register(b.registry, "templates", store.New(templateItemKey))
	b.associations = store.Register(b.registry, "associations", store.New(associationKey))
	b.reports = store.Register(b.registry, "reports", store.New(reportKey))
	b.autoMgmt = store.Register(b.registry, "autoManagement", store.New(autoManagementKey))
	b.resourceTags = store.Register(b.registry, "resourceTags", store.New(resourceTagsKey))
}
