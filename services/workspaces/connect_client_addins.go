package workspaces

// CreateConnectClientAddIn creates a new Connect client add-in.
func (b *InMemoryBackend) CreateConnectClientAddIn(name, resourceID, url string) (string, error) {
	b.mu.Lock("CreateConnectClientAddIn")
	defer b.mu.Unlock()

	id := b.nextID("wscai-")
	b.connectAddIns.Put(&storedConnectAddIn{
		AddInID:    id,
		Name:       name,
		ResourceID: resourceID,
		URL:        url,
	})

	return id, nil
}

// DeleteConnectClientAddIn removes a Connect client add-in.
func (b *InMemoryBackend) DeleteConnectClientAddIn(addInID, _ /*resourceId*/ string) error {
	b.mu.Lock("DeleteConnectClientAddIn")
	defer b.mu.Unlock()

	if !b.connectAddIns.Has(addInID) {
		return errAddInNotFound
	}

	b.connectAddIns.Delete(addInID)

	return nil
}

// DescribeConnectClientAddIns returns add-ins for a resource.
func (b *InMemoryBackend) DescribeConnectClientAddIns(
	resourceID string, _ int32, _ string,
) ([]*storedConnectAddIn, string, error) {
	b.mu.RLock("DescribeConnectClientAddIns")
	defer b.mu.RUnlock()

	var result []*storedConnectAddIn

	for _, a := range b.connectAddIns.All() {
		if a.ResourceID != resourceID {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedConnectAddIn{}
	}

	return result, "", nil
}

// UpdateConnectClientAddIn updates a Connect client add-in.
func (b *InMemoryBackend) UpdateConnectClientAddIn(
	addInID, _ /*resourceId*/, name, url string,
) error {
	b.mu.Lock("UpdateConnectClientAddIn")
	defer b.mu.Unlock()

	a, ok := b.connectAddIns.Get(addInID)
	if !ok {
		return errAddInNotFound
	}

	if name != "" {
		a.Name = name
	}

	if url != "" {
		a.URL = url
	}

	return nil
}
