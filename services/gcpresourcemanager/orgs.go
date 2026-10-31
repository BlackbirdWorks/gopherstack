package gcpresourcemanager

import (
	"strings"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) orgFields(f string) ([]string, bool) {
	switch f {
	case "domain", "displayname":
		return []string{b.org.Domain}, true
	case "directorycustomerid", "owner.directorycustomerid":
		return []string{"C" + b.org.ID}, true
	case "state":
		return []string{stateActive}, true
	default:
		return nil, false
	}
}

// GetOrganization returns the emulated organization.
func (b *InMemoryBackend) GetOrganization(name string) (*resourcemanagerpb.Organization, error) {
	b.mu.RLock("GetOrganization")
	defer b.mu.RUnlock()

	if id, ok := splitName(name, kindOrganizations); !ok || id != b.org.ID {
		return nil, errDenied("resourcemanager.organizations.get", name)
	}

	return b.org.view(), nil
}

// SearchOrganizations returns the organization when it matches the query (domain or directoryCustomerId).
func (b *InMemoryBackend) SearchOrganizations(in *resourcemanagerpb.SearchOrganizationsRequest) (
	*resourcemanagerpb.SearchOrganizationsResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	q, err := parseQuery(in.GetQuery(), false)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("SearchOrganizations")
	ok, err := q.match(b.orgFields)
	view := b.org.view()
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	var all []*resourcemanagerpb.Organization
	if ok {
		all = append(all, view)
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &resourcemanagerpb.SearchOrganizationsResponse{Organizations: pg.Data, NextPageToken: pg.Next}, nil
}

// OrganizationName is the resource name of the emulated organization.
func (b *InMemoryBackend) OrganizationName() string {
	b.mu.RLock("OrganizationName")
	defer b.mu.RUnlock()

	return strings.Join([]string{kindOrganizations, b.org.ID}, "/")
}
