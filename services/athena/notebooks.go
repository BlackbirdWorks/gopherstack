package athena

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// notebookNameKey returns the composite key for notebook name uniqueness.
func notebookNameKey(workGroup, name string) string {
	return workGroup + "/" + name
}

// CreateNotebook creates a new Athena notebook and returns its ID.
//
// The real CreateNotebookInput carries only Name/WorkGroup/ClientRequestToken
// -- no Tags field (unlike CreateWorkGroup/CreateDataCatalog/
// CreateCapacityReservation, which all accept Tags at creation time). A
// notebook can still be tagged after creation via TagResource against its ARN.
func (b *InMemoryBackend) CreateNotebook(workGroup, name string) (string, error) {
	switch {
	case workGroup == "":
		return "", fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	case name == "":
		return "", fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock("CreateNotebook")
	defer b.mu.Unlock()

	nameKey := notebookNameKey(workGroup, name)
	if len(b.notebooksByName.Get(nameKey)) > 0 {
		return "", fmt.Errorf(
			"%w: notebook %q already exists in workgroup %q",
			ErrAlreadyExists,
			name,
			workGroup,
		)
	}

	id := randomID()
	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.notebooks.Put(&Notebook{
		NotebookID:       id,
		Name:             name,
		WorkGroup:        workGroup,
		Type:             "IPYNB",
		CreationTime:     now,
		LastModifiedTime: now,
		Content:          "",
	})

	return id, nil
}

// CreatePresignedNotebookURL generates a presigned notebook URL plus the
// AuthToken/AuthTokenExpirationTime pair the real CreatePresignedNotebookUrl
// response carries alongside it.
func (b *InMemoryBackend) CreatePresignedNotebookURL(sessionID string) (string, string, float64, error) {
	url := fmt.Sprintf(
		"https://athena.%s.amazonaws.com/notebooks/presigned/%s",
		b.region,
		sessionID,
	)
	authToken, authTokenExpiration := newSessionAuthToken()

	return url, authToken, authTokenExpiration, nil
}

// DeleteNotebook removes a notebook by its ID.
func (b *InMemoryBackend) DeleteNotebook(notebookID string) error {
	b.mu.Lock("DeleteNotebook")
	defer b.mu.Unlock()

	if !b.notebooks.Has(notebookID) {
		return fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	b.notebooks.Delete(notebookID)

	return nil
}

// ExportNotebook returns the notebook metadata and content for the given notebook ID.
func (b *InMemoryBackend) ExportNotebook(notebookID string) (NotebookMetadata, string, error) {
	b.mu.RLock("ExportNotebook")
	defer b.mu.RUnlock()

	nb, ok := b.notebooks.Get(notebookID)
	if !ok {
		return NotebookMetadata{}, "", fmt.Errorf(
			"%w: notebook %q not found",
			ErrNotFound,
			notebookID,
		)
	}

	meta := NotebookMetadata{
		NotebookID:       nb.NotebookID,
		Name:             nb.Name,
		WorkGroup:        nb.WorkGroup,
		Type:             nb.Type,
		CreationTime:     nb.CreationTime,
		LastModifiedTime: nb.LastModifiedTime,
	}

	return meta, nb.Content, nil
}

// GetNotebookMetadata returns the metadata for a notebook by ID.
func (b *InMemoryBackend) GetNotebookMetadata(notebookID string) (*NotebookMetadata, error) {
	b.mu.RLock("GetNotebookMetadata")
	defer b.mu.RUnlock()

	nb, ok := b.notebooks.Get(notebookID)
	if !ok {
		return nil, fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	return &NotebookMetadata{
		NotebookID:       nb.NotebookID,
		Name:             nb.Name,
		WorkGroup:        nb.WorkGroup,
		Type:             nb.Type,
		CreationTime:     nb.CreationTime,
		LastModifiedTime: nb.LastModifiedTime,
	}, nil
}

// ListNotebookMetadata lists all notebooks (optionally filtered by workgroup and name).
func (b *InMemoryBackend) ListNotebookMetadata(workGroup, namePrefix string) ([]NotebookMetadata, error) {
	b.mu.RLock("ListNotebookMetadata")
	defer b.mu.RUnlock()

	out := make([]NotebookMetadata, 0, b.notebooks.Len())

	for _, nb := range b.notebooks.All() {
		if workGroup != "" && nb.WorkGroup != workGroup {
			continue
		}

		if namePrefix != "" && !strings.HasPrefix(nb.Name, namePrefix) {
			continue
		}

		out = append(out, NotebookMetadata{
			NotebookID:       nb.NotebookID,
			Name:             nb.Name,
			WorkGroup:        nb.WorkGroup,
			Type:             nb.Type,
			CreationTime:     nb.CreationTime,
			LastModifiedTime: nb.LastModifiedTime,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// ImportNotebook creates a new notebook from inline payload.
func (b *InMemoryBackend) ImportNotebook(workGroup, name, payload, notebookType string) (string, error) {
	switch {
	case workGroup == "":
		return "", fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	case name == "":
		return "", fmt.Errorf("%w: Name is required", ErrValidation)
	case payload == "":
		return "", fmt.Errorf("%w: Payload is required", ErrValidation)
	}

	if notebookType == "" {
		notebookType = "IPYNB"
	}

	b.mu.Lock("ImportNotebook")
	defer b.mu.Unlock()

	nameKey := notebookNameKey(workGroup, name)
	if len(b.notebooksByName.Get(nameKey)) > 0 {
		return "", fmt.Errorf("%w: notebook %q already exists in workgroup %q", ErrAlreadyExists, name, workGroup)
	}

	id := randomID()
	now := nowSeconds()
	b.notebooks.Put(&Notebook{
		NotebookID:       id,
		Name:             name,
		WorkGroup:        workGroup,
		Type:             notebookType,
		Content:          payload,
		CreationTime:     now,
		LastModifiedTime: now,
	})

	return id, nil
}

// UpdateNotebook replaces the payload of an existing notebook.
func (b *InMemoryBackend) UpdateNotebook(notebookID, payload, notebookType, sessionID string) error {
	if notebookID == "" {
		return fmt.Errorf("%w: NotebookId is required", ErrValidation)
	}

	if payload == "" {
		return fmt.Errorf("%w: Payload is required", ErrValidation)
	}

	b.mu.Lock("UpdateNotebook")
	defer b.mu.Unlock()

	nb, ok := b.notebooks.Get(notebookID)
	if !ok {
		return fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	if sessionID != "" {
		if !b.sessions.Has(sessionID) {
			return fmt.Errorf("%w: session %q not found", ErrNotFound, sessionID)
		}
	}

	nb.Content = payload

	if notebookType != "" {
		nb.Type = notebookType
	}

	nb.LastModifiedTime = nowSeconds()

	return nil
}

// UpdateNotebookMetadata renames a notebook.
func (b *InMemoryBackend) UpdateNotebookMetadata(notebookID, newName string) error {
	if notebookID == "" {
		return fmt.Errorf("%w: NotebookId is required", ErrValidation)
	}

	if newName == "" {
		return fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock("UpdateNotebookMetadata")
	defer b.mu.Unlock()

	nb, ok := b.notebooks.Get(notebookID)
	if !ok {
		return fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	if nb.Name == newName {
		return nil
	}

	newKey := notebookNameKey(nb.WorkGroup, newName)
	if len(b.notebooksByName.Get(newKey)) > 0 {
		return fmt.Errorf("%w: notebook %q already exists in workgroup %q", ErrAlreadyExists, newName, nb.WorkGroup)
	}

	// Renaming changes the notebooksByName secondary index's group key, so the
	// entry must be removed under its OLD key before Name is mutated: index
	// removal reads the value's CURRENT fields (see pkgs/store's Index doc),
	// so removing after mutating would look up the wrong (already-new) key
	// and leak the stale index entry.
	b.notebooks.Delete(notebookID)
	nb.Name = newName
	nb.LastModifiedTime = nowSeconds()
	b.notebooks.Put(nb)

	return nil
}
