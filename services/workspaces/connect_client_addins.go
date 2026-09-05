package workspaces

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// connectClientAddInsPageSize is this backend's default page size for
// DescribeConnectClientAddIns; real AWS doesn't document an exact default,
// so this is chosen generously (larger than any realistic per-directory
// add-in count) so pagination only activates when a caller explicitly
// requests a smaller MaxResults.
const connectClientAddInsPageSize = 100

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
	resourceID string, maxResults int32, nextToken string,
) ([]*storedConnectAddIn, string, error) {
	b.mu.RLock("DescribeConnectClientAddIns")
	defer b.mu.RUnlock()

	all := b.connectAddIns.All()

	sort.Slice(all, func(i, j int) bool { return all[i].AddInID < all[j].AddInID })

	result := make([]*storedConnectAddIn, 0, len(all))

	for _, a := range all {
		if a.ResourceID != resourceID {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	pg := page.New(result, nextToken, int(maxResults), connectClientAddInsPageSize)

	return pg.Data, pg.Next, nil
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
