package codecommit

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// CreateApprovalRuleTemplate creates a new approval rule template.
func (b *InMemoryBackend) CreateApprovalRuleTemplate(name, description, content string) (*ApprovalRuleTemplate, error) {
	b.mu.Lock("CreateApprovalRuleTemplate")
	defer b.mu.Unlock()

	if b.approvalRuleTemplates.Has(name) {
		return nil, fmt.Errorf(
			"%w: approval rule template %s already exists",
			ErrApprovalRuleTemplateAlreadyExists,
			name,
		)
	}

	templateID := uuid.NewString()
	templateARN := arn.Build("codecommit", b.region, b.accountID, "approval-rule-template/"+name)
	now := time.Now().UTC()
	hash := sha256.Sum256([]byte(content))
	t := &ApprovalRuleTemplate{
		ApprovalRuleTemplateID:          templateID,
		ApprovalRuleTemplateName:        name,
		ApprovalRuleTemplateARN:         templateARN,
		ApprovalRuleTemplateContent:     content,
		ApprovalRuleTemplateDescription: description,
		CreationDate:                    now,
		LastModifiedDate:                now,
		RuleContentSha256:               hex.EncodeToString(hash[:]),
	}
	b.approvalRuleTemplates.Put(t)
	cp := *t

	return &cp, nil
}

// AssociateApprovalRuleTemplateWithRepository associates an approval rule template with a repository.
func (b *InMemoryBackend) AssociateApprovalRuleTemplateWithRepository(templateName, repositoryName string) error {
	b.mu.Lock("AssociateApprovalRuleTemplateWithRepository")
	defer b.mu.Unlock()

	if !b.approvalRuleTemplates.Has(templateName) {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, templateName)
	}

	if !b.repositories.Has(repositoryName) {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	if b.repoTemplateAssoc[repositoryName] == nil {
		b.repoTemplateAssoc[repositoryName] = make(map[string]struct{})
	}
	b.repoTemplateAssoc[repositoryName][templateName] = struct{}{}

	return nil
}

// DisassociateApprovalRuleTemplateFromRepository removes an approval rule template association from a repository.
func (b *InMemoryBackend) DisassociateApprovalRuleTemplateFromRepository(templateName, repositoryName string) error {
	b.mu.Lock("DisassociateApprovalRuleTemplateFromRepository")
	defer b.mu.Unlock()

	if !b.approvalRuleTemplates.Has(templateName) {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, templateName)
	}

	if !b.repositories.Has(repositoryName) {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	if assoc, ok := b.repoTemplateAssoc[repositoryName]; ok {
		delete(assoc, templateName)
	}

	return nil
}

// BatchAssociateApprovalRuleTemplateWithRepositories associates an approval rule template with multiple repositories.
// Returns lists of associated and failed repository names.
func (b *InMemoryBackend) BatchAssociateApprovalRuleTemplateWithRepositories(
	templateName string,
	repositoryNames []string,
) ([]string, []BatchAssociationError) {
	b.mu.Lock("BatchAssociateApprovalRuleTemplateWithRepositories")
	defer b.mu.Unlock()

	var associated []string
	var errors []BatchAssociationError

	if !b.approvalRuleTemplates.Has(templateName) {
		for _, name := range repositoryNames {
			errors = append(errors, BatchAssociationError{
				RepositoryName: name,
				ErrorCode:      errApprovalRuleTemplateNotExist,
				ErrorMessage:   fmt.Sprintf("approval rule template %s not found", templateName),
			})
		}

		return associated, errors
	}

	for _, name := range repositoryNames {
		if !b.repositories.Has(name) {
			errors = append(errors, BatchAssociationError{
				RepositoryName: name,
				ErrorCode:      errRepoDoesNotExist,
				ErrorMessage:   fmt.Sprintf("repository %s not found", name),
			})

			continue
		}

		if b.repoTemplateAssoc[name] == nil {
			b.repoTemplateAssoc[name] = make(map[string]struct{})
		}
		b.repoTemplateAssoc[name][templateName] = struct{}{}
		associated = append(associated, name)
	}

	return associated, errors
}

// BatchDisassociateApprovalRuleTemplateFromRepositories removes associations between
// a template and multiple repositories.
func (b *InMemoryBackend) BatchDisassociateApprovalRuleTemplateFromRepositories(
	templateName string,
	repositoryNames []string,
) ([]string, []BatchAssociationError) {
	b.mu.Lock("BatchDisassociateApprovalRuleTemplateFromRepositories")
	defer b.mu.Unlock()

	var disassociated []string
	var errors []BatchAssociationError

	if !b.approvalRuleTemplates.Has(templateName) {
		for _, name := range repositoryNames {
			errors = append(errors, BatchAssociationError{
				RepositoryName: name,
				ErrorCode:      errApprovalRuleTemplateNotExist,
				ErrorMessage:   fmt.Sprintf("approval rule template %s not found", templateName),
			})
		}

		return disassociated, errors
	}

	for _, name := range repositoryNames {
		if !b.repositories.Has(name) {
			errors = append(errors, BatchAssociationError{
				RepositoryName: name,
				ErrorCode:      errRepoDoesNotExist,
				ErrorMessage:   fmt.Sprintf("repository %s not found", name),
			})

			continue
		}

		if assoc, ok := b.repoTemplateAssoc[name]; ok {
			delete(assoc, templateName)
		}
		disassociated = append(disassociated, name)
	}

	return disassociated, errors
}

// DeleteApprovalRuleTemplate deletes an approval rule template by name,
// returning its ID. The real DeleteApprovalRuleTemplateOutput echoes
// ApprovalRuleTemplateId as a required field
// (api_op_DeleteApprovalRuleTemplate.go:38).
func (b *InMemoryBackend) DeleteApprovalRuleTemplate(name string) (string, error) {
	b.mu.Lock("DeleteApprovalRuleTemplate")
	defer b.mu.Unlock()

	t, ok := b.approvalRuleTemplates.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, name)
	}
	b.approvalRuleTemplates.Delete(name)

	return t.ApprovalRuleTemplateID, nil
}

// GetApprovalRuleTemplate retrieves an approval rule template by name.
func (b *InMemoryBackend) GetApprovalRuleTemplate(name string) (*ApprovalRuleTemplate, error) {
	b.mu.RLock("GetApprovalRuleTemplate")
	defer b.mu.RUnlock()

	t, ok := b.approvalRuleTemplates.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, name)
	}
	cp := *t

	return &cp, nil
}

// ListApprovalRuleTemplates returns all approval rule templates.
func (b *InMemoryBackend) ListApprovalRuleTemplates() []*ApprovalRuleTemplate {
	b.mu.RLock("ListApprovalRuleTemplates")
	defer b.mu.RUnlock()

	snap := b.approvalRuleTemplates.Snapshot()
	list := make([]*ApprovalRuleTemplate, 0, len(snap))

	for _, t := range snap {
		cp := *t
		list = append(list, &cp)
	}

	return list
}

// UpdateApprovalRuleTemplateContent updates the content of an approval rule template.
func (b *InMemoryBackend) UpdateApprovalRuleTemplateContent(name, content string) error {
	b.mu.Lock("UpdateApprovalRuleTemplateContent")
	defer b.mu.Unlock()

	t, ok := b.approvalRuleTemplates.Get(name)
	if !ok {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, name)
	}
	t.ApprovalRuleTemplateContent = content
	t.LastModifiedDate = time.Now().UTC()

	return nil
}

// UpdateApprovalRuleTemplateDescription updates the description of an approval rule template.
func (b *InMemoryBackend) UpdateApprovalRuleTemplateDescription(name, desc string) error {
	b.mu.Lock("UpdateApprovalRuleTemplateDescription")
	defer b.mu.Unlock()

	t, ok := b.approvalRuleTemplates.Get(name)
	if !ok {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, name)
	}
	t.ApprovalRuleTemplateDescription = desc
	t.LastModifiedDate = time.Now().UTC()

	return nil
}

// UpdateApprovalRuleTemplateName renames an approval rule template.
func (b *InMemoryBackend) UpdateApprovalRuleTemplateName(oldName, newName string) error {
	b.mu.Lock("UpdateApprovalRuleTemplateName")
	defer b.mu.Unlock()

	t, ok := b.approvalRuleTemplates.Get(oldName)
	if !ok {
		return fmt.Errorf("%w: approval rule template %s not found", ErrApprovalRuleTemplateNotFound, oldName)
	}
	if b.approvalRuleTemplates.Has(newName) {
		return fmt.Errorf("%w: approval rule template %s already exists", ErrApprovalRuleTemplateAlreadyExists, newName)
	}
	t.ApprovalRuleTemplateName = newName
	t.LastModifiedDate = time.Now().UTC()
	b.approvalRuleTemplates.Delete(oldName)
	b.approvalRuleTemplates.Put(t)

	// Update repoTemplateAssoc
	for _, assoc := range b.repoTemplateAssoc {
		if _, hasOld := assoc[oldName]; hasOld {
			delete(assoc, oldName)
			assoc[newName] = struct{}{}
		}
	}

	return nil
}

// ListAssociatedApprovalRuleTemplatesForRepository returns template names associated with a repository.
func (b *InMemoryBackend) ListAssociatedApprovalRuleTemplatesForRepository(repoName string) ([]string, error) {
	b.mu.RLock("ListAssociatedApprovalRuleTemplatesForRepository")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	assoc := b.repoTemplateAssoc[repoName]
	names := collections.SortedKeys(assoc)

	return names, nil
}

// ListRepositoriesForApprovalRuleTemplate returns repository names that have a given template associated.
func (b *InMemoryBackend) ListRepositoriesForApprovalRuleTemplate(templateName string) ([]string, error) {
	b.mu.RLock("ListRepositoriesForApprovalRuleTemplate")
	defer b.mu.RUnlock()

	if !b.approvalRuleTemplates.Has(templateName) {
		return nil, fmt.Errorf(
			"%w: approval rule template %s not found",
			ErrApprovalRuleTemplateNotFound,
			templateName,
		)
	}

	var repos []string
	for repoName, assoc := range b.repoTemplateAssoc {
		if _, ok := assoc[templateName]; ok {
			repos = append(repos, repoName)
		}
	}
	sort.Strings(repos)

	return repos, nil
}
