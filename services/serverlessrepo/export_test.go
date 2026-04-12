package serverlessrepo

// ApplicationCount returns the number of applications in the backend (test helper).
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return len(b.applications)
}

// VersionCount returns the number of versions for a given application in the backend (test helper).
func VersionCount(b *InMemoryBackend, appName string) int {
	b.mu.RLock("VersionCount")
	defer b.mu.RUnlock()

	return len(b.appVersions[appName])
}

// TemplateCount returns the number of CloudFormation templates for a given application in the backend (test helper).
func TemplateCount(b *InMemoryBackend, appName string) int {
	b.mu.RLock("TemplateCount")
	defer b.mu.RUnlock()

	return len(b.cfTemplates[appName])
}

// ChangeSetCount returns the number of CloudFormation change sets for a given application in the backend (test helper).
func ChangeSetCount(b *InMemoryBackend, appName string) int {
	b.mu.RLock("ChangeSetCount")
	defer b.mu.RUnlock()

	return len(b.cfChangeSets[appName])
}

// PolicyStatementCount returns the number of policy statements for a given application in the backend (test helper).
func PolicyStatementCount(b *InMemoryBackend, appName string) int {
	b.mu.RLock("PolicyStatementCount")
	defer b.mu.RUnlock()

	return len(b.appPolicies[appName])
}

// HandlerOpsLen returns the number of supported operations in the handler (test helper).
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
