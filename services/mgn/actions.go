package mgn

import "github.com/blackbirdworks/gopherstack/pkgs/page"

// PutSourceServerAction/ListSourceServerActions/RemoveSourceServerAction and
// PutTemplateAction/ListTemplateActions/RemoveTemplateAction are two structurally
// near-identical, distinct, post-launch custom SSM-document action families. This
// repo has no SSM document execution engine, so these ops track STATE only
// (documents listed, ordered, active/inactive) -- never actually invoking any
// SSM document.

// PutSourceServerActionInput mirrors PutSourceServerActionInput.
type PutSourceServerActionInput struct {
	ExternalParameters    map[string]string
	Parameters            map[string][]ssmParameter
	SourceServerID        string
	ActionID              string
	ActionName            string
	Category              string
	Description           string
	DocumentIdentifier    string
	DocumentVersion       string
	Order                 int32
	TimeoutSeconds        int32
	Active                bool
	MustSucceedForCutover bool
}

// PutSourceServerAction creates or replaces a post-launch action on a
// SourceServer.
func (b *InMemoryBackend) PutSourceServerAction(in PutSourceServerActionInput) (*SourceServerActionDocument, error) {
	b.mu.Lock("PutSourceServerAction")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if in.ActionID == "" || in.ActionName == "" || in.DocumentIdentifier == "" {
		return nil, validationError("actionID, actionName, and documentIdentifier are required")
	}

	if _, ok := b.resolveSourceServerLocked(in.SourceServerID); !ok {
		return nil, notFoundError(resourceSourceServer, in.SourceServerID)
	}

	doc := &SourceServerActionDocument{
		SourceServerID:        in.SourceServerID,
		ActionID:              in.ActionID,
		ActionName:            in.ActionName,
		Active:                in.Active,
		Category:              in.Category,
		Description:           in.Description,
		DocumentIdentifier:    in.DocumentIdentifier,
		DocumentVersion:       in.DocumentVersion,
		ExternalParameters:    in.ExternalParameters,
		MustSucceedForCutover: in.MustSucceedForCutover,
		Order:                 in.Order,
		Parameters:            in.Parameters,
		TimeoutSeconds:        in.TimeoutSeconds,
	}
	b.sourceServerActions.Put(doc)

	return doc.clone(), nil
}

// ListSourceServerActions returns a page of post-launch actions for
// sourceServerID matching actionIDs (SourceServerActionsRequestFilters.ActionIDs
// -- empty means unfiltered).
func (b *InMemoryBackend) ListSourceServerActions(
	sourceServerID string,
	actionIDs []string,
	token string,
	limit int,
) (page.Page[*SourceServerActionDocument], error) {
	b.mu.RLock("ListSourceServerActions")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*SourceServerActionDocument]{}, err
	}

	if _, ok := b.resolveSourceServerLocked(sourceServerID); !ok {
		return page.Page[*SourceServerActionDocument]{}, notFoundError(resourceSourceServer, sourceServerID)
	}

	all := b.sourceServerActionsByServer.Get(sourceServerID)
	items := make([]*SourceServerActionDocument, 0, len(all))

	for _, a := range all {
		if len(actionIDs) == 0 || containsStr(actionIDs, a.ActionID) {
			items = append(items, a)
		}
	}

	cloned := make([]*SourceServerActionDocument, len(items))

	for i, a := range items {
		cloned[i] = a.clone()
	}

	return page.New(cloned, token, limit, defaultPageLimit), nil
}

// RemoveSourceServerAction removes a post-launch action from a SourceServer.
func (b *InMemoryBackend) RemoveSourceServerAction(sourceServerID, actionID string) error {
	b.mu.Lock("RemoveSourceServerAction")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	key := actionKey(sourceServerID, actionID)
	if !b.sourceServerActions.Has(key) {
		return notFoundError(resourceAction, actionID)
	}

	b.sourceServerActions.Delete(key)

	return nil
}

// PutTemplateActionInput mirrors PutTemplateActionInput.
type PutTemplateActionInput struct {
	ExternalParameters            map[string]string
	Parameters                    map[string][]ssmParameter
	LaunchConfigurationTemplateID string
	ActionID                      string
	ActionName                    string
	Category                      string
	Description                   string
	DocumentIdentifier            string
	DocumentVersion               string
	OperatingSystem               string
	Order                         int32
	TimeoutSeconds                int32
	Active                        bool
	MustSucceedForCutover         bool
}

// PutTemplateAction creates or replaces a post-launch action on a
// LaunchConfigurationTemplate.
func (b *InMemoryBackend) PutTemplateAction(in PutTemplateActionInput) (*TemplateActionDocument, error) {
	b.mu.Lock("PutTemplateAction")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if in.ActionID == "" || in.ActionName == "" || in.DocumentIdentifier == "" {
		return nil, validationError("actionID, actionName, and documentIdentifier are required")
	}

	if !b.launchTemplates.Has(in.LaunchConfigurationTemplateID) {
		return nil, notFoundError(resourceLaunchTemplate, in.LaunchConfigurationTemplateID)
	}

	doc := &TemplateActionDocument{
		LaunchConfigurationTemplateID: in.LaunchConfigurationTemplateID,
		ActionID:                      in.ActionID,
		ActionName:                    in.ActionName,
		Active:                        in.Active,
		Category:                      in.Category,
		Description:                   in.Description,
		DocumentIdentifier:            in.DocumentIdentifier,
		DocumentVersion:               in.DocumentVersion,
		ExternalParameters:            in.ExternalParameters,
		MustSucceedForCutover:         in.MustSucceedForCutover,
		OperatingSystem:               in.OperatingSystem,
		Order:                         in.Order,
		Parameters:                    in.Parameters,
		TimeoutSeconds:                in.TimeoutSeconds,
	}
	b.templateActions.Put(doc)

	return doc.clone(), nil
}

// ListTemplateActions returns a page of post-launch actions for
// launchConfigurationTemplateID matching actionIDs
// (TemplateActionsRequestFilters.ActionIDs -- empty means unfiltered).
func (b *InMemoryBackend) ListTemplateActions(
	launchConfigurationTemplateID string,
	actionIDs []string,
	token string,
	limit int,
) (page.Page[*TemplateActionDocument], error) {
	b.mu.RLock("ListTemplateActions")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*TemplateActionDocument]{}, err
	}

	if !b.launchTemplates.Has(launchConfigurationTemplateID) {
		return page.Page[*TemplateActionDocument]{}, notFoundError(
			resourceLaunchTemplate,
			launchConfigurationTemplateID,
		)
	}

	all := b.templateActionsByTemplate.Get(launchConfigurationTemplateID)
	items := make([]*TemplateActionDocument, 0, len(all))

	for _, a := range all {
		if len(actionIDs) == 0 || containsStr(actionIDs, a.ActionID) {
			items = append(items, a)
		}
	}

	cloned := make([]*TemplateActionDocument, len(items))

	for i, a := range items {
		cloned[i] = a.clone()
	}

	return page.New(cloned, token, limit, defaultPageLimit), nil
}

// RemoveTemplateAction removes a post-launch action from a
// LaunchConfigurationTemplate.
func (b *InMemoryBackend) RemoveTemplateAction(launchConfigurationTemplateID, actionID string) error {
	b.mu.Lock("RemoveTemplateAction")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	key := actionKey(launchConfigurationTemplateID, actionID)
	if !b.templateActions.Has(key) {
		return notFoundError(resourceAction, actionID)
	}

	b.templateActions.Delete(key)

	return nil
}
