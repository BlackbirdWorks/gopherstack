package serverlessrepo

import "time"

// ApplicationCount returns the number of applications in the backend (test helper).
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return b.applications.Len()
}

// VersionCount returns the number of versions for a given application in the backend (test helper).
func VersionCount(b *InMemoryBackend, appName string) int {
	b.mu.RLock("VersionCount")
	defer b.mu.RUnlock()

	return len(b.appVersionsByApp.Get(appName))
}

// TemplateCount returns the number of CloudFormation templates for a given application in the backend (test helper).
func TemplateCount(b *InMemoryBackend, appName string) int {
	b.mu.RLock("TemplateCount")
	defer b.mu.RUnlock()

	return len(b.cfTemplatesByApp.Get(appName))
}

// ChangeSetCount returns the number of CloudFormation change sets for a given application in the backend (test helper).
func ChangeSetCount(b *InMemoryBackend, appName string) int {
	b.mu.RLock("ChangeSetCount")
	defer b.mu.RUnlock()

	return len(b.cfChangeSetsByApp.Get(appName))
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

// AddExpiredTemplateInternal creates a CloudFormation template whose expiration time
// is set to the past so that GetCloudFormationTemplate returns EXPIRED status.
func AddExpiredTemplateInternal(b *InMemoryBackend, appName, semanticVersion string) *CloudFormationTemplate {
	b.mu.Lock("AddExpiredTemplateInternal")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(appName)
	if !ok {
		return nil
	}

	now := time.Now()
	templateID := appName + "-expired"
	t := &CloudFormationTemplate{
		ApplicationID:   app.ApplicationID,
		TemplateID:      templateID,
		AppName:         appName,
		SemanticVersion: semanticVersion,
		Status:          templateStatusActive,
		CreationTime:    now.Add(-2 * time.Hour),
		ExpirationTime:  now.Add(-1 * time.Hour),
		TemplateURL: "https://s3.amazonaws.com/serverlessrepo-templates/" +
			appName + "/" + templateID + ".template",
	}
	b.cfTemplates.Put(t)

	cp := *t

	return &cp
}
