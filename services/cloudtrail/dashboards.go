package cloudtrail

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateDashboard creates a new CloudTrail dashboard.
func (b *InMemoryBackend) CreateDashboard(
	name, dashType string,
	kv map[string]string,
	widgets []Widget,
	refreshSchedule *RefreshSchedule,
	terminationProtected bool,
) (*Dashboard, error) {
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
	now := time.Now().UTC()
	d := &Dashboard{
		DashboardID:                  id,
		DashboardARN:                 dashARN,
		Name:                         name,
		Type:                         dashType,
		Status:                       "CREATED",
		Tags:                         t,
		Widgets:                      widgets,
		RefreshSchedule:              refreshSchedule,
		TerminationProtectionEnabled: terminationProtected,
		CreatedTimestamp:             now,
		UpdatedTimestamp:             now,
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

// UpdateDashboard updates an existing dashboard's refresh schedule, widgets,
// and/or termination protection. Real UpdateDashboardInput has no Name field
// (dashboards cannot be renamed) -- a previous version of this backend
// accepted a rename-via-Name parameter that does not exist on the real API;
// it has been removed.
func (b *InMemoryBackend) UpdateDashboard(
	dashIDOrARN string,
	widgets []Widget,
	refreshSchedule *RefreshSchedule,
	terminationProtected *bool,
) (*Dashboard, error) {
	b.mu.Lock("UpdateDashboard")
	defer b.mu.Unlock()

	d := b.findDashboardLocked(dashIDOrARN)
	if d == nil {
		return nil, fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashIDOrARN)
	}
	if widgets != nil {
		d.Widgets = widgets
	}
	if refreshSchedule != nil {
		d.RefreshSchedule = refreshSchedule
	}
	if terminationProtected != nil {
		d.TerminationProtectionEnabled = *terminationProtected
	}
	d.Status = "UPDATED"
	d.UpdatedTimestamp = time.Now().UTC()
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

// StartDashboardRefresh triggers a refresh of a dashboard, recording a new
// LastRefreshId. "REFRESHING" is not a valid DashboardStatus (real values are
// CREATING/CREATED/UPDATING/UPDATED/DELETING only) -- a previous version of
// this backend set it as the dashboard's Status, which real
// StartDashboardRefreshOutput has no Status field on anyway (it returns only
// RefreshId; see handleStartDashboardRefresh).
func (b *InMemoryBackend) StartDashboardRefresh(dashIDOrARN string) (*Dashboard, error) {
	b.mu.Lock("StartDashboardRefresh")
	defer b.mu.Unlock()

	d := b.findDashboardLocked(dashIDOrARN)
	if d == nil {
		return nil, fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashIDOrARN)
	}
	b.dashboardCounter++
	d.LastRefreshID = fmt.Sprintf("refresh-%06d", b.dashboardCounter)
	cp := *d

	return &cp, nil
}
