package cloudtrail

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateDashboard creates a new CloudTrail dashboard.
func (b *InMemoryBackend) CreateDashboard(name, dashType string, kv map[string]string) (*Dashboard, error) {
	b.mu.Lock("CreateDashboard")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	if matches := b.dashboardsByName.Get(name); len(matches) > 0 {
		return nil, fmt.Errorf("%w: dashboard %s already exists", ErrAlreadyExists, name)
	}

	b.dashboardCounter++
	id := fmt.Sprintf("dashboard-%06d", b.dashboardCounter)
	dashARN := arn.Build("cloudtrail", b.region, b.accountID, "dashboard/"+id)
	t := tags.New("cloudtrail.dashboard." + id + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	d := &Dashboard{
		DashboardID:  id,
		DashboardARN: dashARN,
		Name:         name,
		Type:         dashType,
		Status:       "CREATED",
		Tags:         t,
	}
	b.dashboards.Put(d)

	cp := *d

	return &cp, nil
}

// DeleteDashboard deletes a dashboard by ID or ARN.
func (b *InMemoryBackend) DeleteDashboard(dashboardIDOrARN string) error {
	b.mu.Lock("DeleteDashboard")
	defer b.mu.Unlock()

	d := b.findDashboardLocked(dashboardIDOrARN)
	if d == nil {
		return fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashboardIDOrARN)
	}

	d.Tags.Close()
	b.dashboards.Delete(d.DashboardID)

	return nil
}

// GetDashboard returns a dashboard by ID or ARN.
func (b *InMemoryBackend) GetDashboard(dashIDOrARN string) (*Dashboard, error) {
	b.mu.RLock("GetDashboard")
	defer b.mu.RUnlock()

	d := b.findDashboardLocked(dashIDOrARN)
	if d == nil {
		return nil, fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashIDOrARN)
	}
	cp := *d

	return &cp, nil
}

// UpdateDashboard updates an existing dashboard.
func (b *InMemoryBackend) UpdateDashboard(dashIDOrARN string, name string) (*Dashboard, error) {
	b.mu.Lock("UpdateDashboard")
	defer b.mu.Unlock()

	d := b.findDashboardLocked(dashIDOrARN)
	if d == nil {
		return nil, fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashIDOrARN)
	}
	if name != "" && name != d.Name {
		// d.Name is an indexed field (dashboardsByName): delete before mutating
		// so the old index entry is removed using the pre-mutation value, then
		// re-Put to rebuild every index (byARN, byName) under the new state.
		b.dashboards.Delete(d.DashboardID)
		d.Name = name
		b.dashboards.Put(d)
	}
	cp := *d

	return &cp, nil
}

// ListDashboards returns all dashboards.
func (b *InMemoryBackend) ListDashboards() []*Dashboard {
	b.mu.RLock("ListDashboards")
	defer b.mu.RUnlock()

	all := b.dashboards.All()
	list := make([]*Dashboard, 0, len(all))
	for _, d := range all {
		cp := *d
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].DashboardARN < list[j].DashboardARN })

	return list
}

// StartDashboardRefresh triggers a refresh of a dashboard (sets status to REFRESHING).
func (b *InMemoryBackend) StartDashboardRefresh(dashIDOrARN string) (*Dashboard, error) {
	b.mu.Lock("StartDashboardRefresh")
	defer b.mu.Unlock()

	d := b.findDashboardLocked(dashIDOrARN)
	if d == nil {
		return nil, fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashIDOrARN)
	}
	d.Status = "REFRESHING"
	cp := *d

	return &cp, nil
}
