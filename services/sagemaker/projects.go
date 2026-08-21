package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrProjectNotFound is returned when a project does not exist.
var ErrProjectNotFound = awserr.New("ValidationException", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// Project
// ---------------------------------------------------------------------------

// Project represents a SageMaker project. ServiceCatalogProvisioningDetails
// and TemplateProviders are stored as opaque json.RawMessage passthrough of
// whatever a client sent at Create (same convention as ai_workload_configs.go):
// this backend never simulates actual Service Catalog provisioning, so only
// the client-supplied identity fields round-trip, not server-derived state.
type Project struct {
	CreationTime                      time.Time         `json:"CreationTime"`
	LastModifiedTime                  time.Time         `json:"LastModifiedTime"`
	Tags                              map[string]string `json:"Tags,omitempty"`
	ProjectName                       string            `json:"ProjectName"`
	ProjectArn                        string            `json:"ProjectArn"`
	ProjectID                         string            `json:"ProjectId"`
	ProjectStatus                     string            `json:"ProjectStatus"`
	ProjectDescription                string            `json:"ProjectDescription,omitempty"`
	ServiceCatalogProvisioningDetails json.RawMessage   `json:"ServiceCatalogProvisioningDetails,omitempty"`
	TemplateProviders                 json.RawMessage   `json:"TemplateProviders,omitempty"`
}

func cloneProject(p *Project) *Project {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	cp.ServiceCatalogProvisioningDetails = append(json.RawMessage(nil), p.ServiceCatalogProvisioningDetails...)
	cp.TemplateProviders = append(json.RawMessage(nil), p.TemplateProviders...)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 string — read by
// persistence.go's snapshot path, not by the handler (handleDescribeProject
// builds its own response map so Tags, which has no wire counterpart on
// DescribeProjectOutput, never leaks onto the wire).
func (p *Project) MarshalJSON() ([]byte, error) {
	type alias Project

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(p),
		CreationTime:     epochSeconds(p.CreationTime),
		LastModifiedTime: epochSeconds(p.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [Project.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (p *Project) UnmarshalJSON(data []byte) error {
	type alias Project

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(p)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	p.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	p.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateProjectOptions holds the parameters for creating a project.
type CreateProjectOptions struct {
	Tags                              map[string]string
	Name                              string
	Description                       string
	ServiceCatalogProvisioningDetails json.RawMessage
	TemplateProviders                 json.RawMessage
}

// CreateProject creates a SageMaker project.
func (b *InMemoryBackend) CreateProject(ctx context.Context, opts CreateProjectOptions) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", ErrValidation)
	}

	if _, ok := b.projectsStore(region).Get(opts.Name); ok {
		return nil, fmt.Errorf("%w: project %q already exists", ErrValidation, opts.Name)
	}

	projectARN := arn.Build("sagemaker", region, b.accountID, "project/"+opts.Name)
	now := time.Now()

	p := &Project{
		ProjectName:                       opts.Name,
		ProjectArn:                        projectARN,
		ProjectID:                         generateID(),
		ProjectStatus:                     "CreateCompleted",
		ProjectDescription:                opts.Description,
		Tags:                              mergeTags(nil, opts.Tags),
		ServiceCatalogProvisioningDetails: opts.ServiceCatalogProvisioningDetails,
		TemplateProviders:                 opts.TemplateProviders,
		CreationTime:                      now,
		LastModifiedTime:                  now,
	}
	b.projectsStore(region).Put(p)

	return cloneProject(p), nil
}

// DescribeProject returns a project by name.
func (b *InMemoryBackend) DescribeProject(ctx context.Context, name string) (*Project, error) {
	b.mu.RLock("DescribeProject")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.projectsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: project %q not found", ErrProjectNotFound, name)
	}

	return cloneProject(p), nil
}

// DeleteProject removes a project by name.
func (b *InMemoryBackend) DeleteProject(ctx context.Context, name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.projectsStore(region).Get(name); !ok {
		return fmt.Errorf("%w: project %q not found", ErrProjectNotFound, name)
	}

	store := b.projectsStore(region)
	store.Delete(name)

	return nil
}

// ListProjectsFilter narrows and orders the results of ListProjects
// (api_op_ListProjects.go:30-58). The op's own doc states both real defaults
// explicitly: SortBy is CreationTime, SortOrder is Ascending.
type ListProjectsFilter struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	NameContains       string
	SortBy             string
	SortOrder          string
	MaxResults         int32
}

func projectMatchesFilter(p *Project, filter ListProjectsFilter) bool {
	if filter.NameContains != "" &&
		!strings.Contains(strings.ToLower(p.ProjectName), strings.ToLower(filter.NameContains)) {
		return false
	}

	if filter.CreationTimeAfter != nil && !p.CreationTime.After(*filter.CreationTimeAfter) {
		return false
	}

	if filter.CreationTimeBefore != nil && !p.CreationTime.Before(*filter.CreationTimeBefore) {
		return false
	}

	return true
}

// lessProject orders a before b by sortBy (Name/default CreationTime, tie-broken by name).
func lessProject(a, b *Project, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		return a.ProjectName < b.ProjectName
	default:
		if a.CreationTime.Equal(b.CreationTime) {
			return a.ProjectName < b.ProjectName
		}

		return a.CreationTime.Before(b.CreationTime)
	}
}

// ListProjects returns projects matching filter, sorted by filter.SortBy
// (default CreationTime) / filter.SortOrder (default Ascending).
func (b *InMemoryBackend) ListProjects(
	ctx context.Context,
	nextToken string,
	filter ListProjectsFilter,
) ([]*Project, string) {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*Project, 0, b.projectsStoreRO(region).Len())

	for _, p := range b.projectsStoreRO(region).All() {
		if projectMatchesFilter(p, filter) {
			list = append(list, cloneProject(p))
		}
	}

	desc := strings.EqualFold(filter.SortOrder, "Descending")
	sort.Slice(list, func(i, k int) bool {
		less := lessProject(list[i], list[k], filter.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}

// ---------------------------------------------------------------------------
// UpdateProject
// ---------------------------------------------------------------------------

// UpdateProject updates a project's description and merges in new tags.
// ServiceCatalogProvisioningUpdateDetails/TemplateProvidersToUpdate
// (api_op_UpdateProject.go) are disclosed not modeled: applying either for
// real requires simulating an actual Service Catalog provisioned-product
// update against the ServiceCatalogProvisioningDetails this backend already
// stores as opaque passthrough, which this pass did not build — the handler
// does not decode either field, so nothing is silently accepted and dropped.
func (b *InMemoryBackend) UpdateProject(
	ctx context.Context,
	name, description string,
	tags map[string]string,
) (*Project, error) {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.projectsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: project %q not found", ErrProjectNotFound, name)
	}

	if description != "" {
		p.ProjectDescription = description
	}

	if len(tags) > 0 {
		p.Tags = mergeTags(p.Tags, tags)
	}

	p.LastModifiedTime = time.Now()

	return cloneProject(p), nil
}
