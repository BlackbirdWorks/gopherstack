package codebuild

func (b *InMemoryBackend) buildWebhookURL(projectName string) string {
	return "https://codebuild." + b.region + ".amazonaws.com/webhooks/" + projectName
}

// CreateWebhook creates a webhook for a CodeBuild project.
//
// Real AWS surfaces the created webhook back on the project itself (the
// Project.Webhook field returned by BatchGetProjects/GetProject), so the new
// webhook is mirrored onto the project record here as well as stored in the
// webhooks table.
func (b *InMemoryBackend) CreateWebhook(
	projectName, branchFilter, buildType string, filterGroups [][]WebhookFilter,
) (*Webhook, error) {
	b.mu.Lock("CreateWebhook")
	defer b.mu.Unlock()

	p, ok := b.projects.Get(projectName)
	if !ok {
		return nil, ErrNotFound
	}

	if b.webhooks.Has(projectName) {
		return nil, ErrAlreadyExists
	}

	w := &Webhook{
		ProjectName:  projectName,
		URL:          b.buildWebhookURL(projectName),
		PayloadURL:   b.buildWebhookURL(projectName),
		BranchFilter: branchFilter,
		BuildType:    buildType,
		FilterGroups: filterGroups,
	}
	b.webhooks.Put(w)

	out := *w
	projCopy := out
	p.Webhook = &projCopy

	return &out, nil
}

// DeleteWebhook removes the webhook for a project.
func (b *InMemoryBackend) DeleteWebhook(projectName string) error {
	b.mu.Lock("DeleteWebhook")
	defer b.mu.Unlock()

	if !b.webhooks.Delete(projectName) {
		return ErrNotFound
	}

	if p, ok := b.projects.Get(projectName); ok {
		p.Webhook = nil
	}

	return nil
}

// UpdateWebhook updates the branchFilter and buildType of an existing webhook.
func (b *InMemoryBackend) UpdateWebhook(
	projectName, branchFilter, buildType string, filterGroups [][]WebhookFilter,
) (*Webhook, error) {
	b.mu.Lock("UpdateWebhook")
	defer b.mu.Unlock()

	w, ok := b.webhooks.Get(projectName)
	if !ok {
		return nil, ErrNotFound
	}

	w.BranchFilter = branchFilter
	w.BuildType = buildType

	if filterGroups != nil {
		w.FilterGroups = filterGroups
	}

	out := *w

	if p, projOK := b.projects.Get(projectName); projOK {
		projCopy := out
		p.Webhook = &projCopy
	}

	return &out, nil
}
