package cloudwatch

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// PutDashboard creates or updates a CloudWatch dashboard by name.
func (b *InMemoryBackend) PutDashboard(name, body string) error {
	if name == "" {
		return ErrDashboardNameRequired
	}

	b.mu.Lock("PutDashboard")
	defer b.mu.Unlock()

	b.dashboards.Put(&dashboardRecord{
		Name:         name,
		Body:         body,
		LastModified: time.Now().UTC(),
	})

	return nil
}

// GetDashboard returns the dashboard entry and body for the given name.
func (b *InMemoryBackend) GetDashboard(name string) (DashboardEntry, string, error) {
	b.mu.RLock("GetDashboard")
	defer b.mu.RUnlock()

	rec, ok := b.dashboards.Get(name)
	if !ok {
		return DashboardEntry{}, "", fmt.Errorf("%w: %s", ErrDashboardNotFound, name)
	}

	return b.toDashboardEntry(rec), rec.Body, nil
}

// ListDashboards returns a page of dashboard entries optionally filtered by name prefix.
func (b *InMemoryBackend) ListDashboards(
	prefix, nextToken string,
) (page.Page[DashboardEntry], error) {
	b.mu.RLock("ListDashboards")
	defer b.mu.RUnlock()

	var result []DashboardEntry

	for _, rec := range b.dashboards.All() {
		if prefix != "" && !strings.HasPrefix(rec.Name, prefix) {
			continue
		}

		result = append(result, b.toDashboardEntry(rec))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DashboardName < result[j].DashboardName
	})

	return page.New(result, nextToken, 0, cwDefaultListDashboardsLimit), nil
}

// DeleteDashboards removes the named dashboards. Names that do not exist are silently ignored.
func (b *InMemoryBackend) DeleteDashboards(names []string) error {
	b.mu.Lock("DeleteDashboards")
	defer b.mu.Unlock()

	for _, name := range names {
		b.dashboards.Delete(name)
	}

	return nil
}

// toDashboardEntry converts a dashboardRecord to a DashboardEntry.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) toDashboardEntry(rec *dashboardRecord) DashboardEntry {
	return DashboardEntry{
		DashboardName: rec.Name,
		DashboardArn:  arn.Build("cloudwatch", b.region, b.accountID, "dashboard/"+rec.Name),
		LastModified:  rec.LastModified,
		Size:          int64(len(rec.Body)),
	}
}

// GetDashboardARNs returns the ARNs for the given dashboard names.
// Used by the HTTP handler to clean up tag entries on delete.
func (b *InMemoryBackend) GetDashboardARNs(names []string) []string {
	b.mu.RLock("GetDashboardARNs")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(names))
	for _, name := range names {
		if b.dashboards.Has(name) {
			arns = append(arns, arn.Build("cloudwatch", b.region, b.accountID, "dashboard/"+name))
		}
	}

	return arns
}
