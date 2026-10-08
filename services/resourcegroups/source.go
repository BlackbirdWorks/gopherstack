package resourcegroups

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// Query error codes, per types.QueryErrorCode.
const (
	queryErrStackInactive   = "CLOUDFORMATION_STACK_INACTIVE"
	queryErrStackNotExist   = "CLOUDFORMATION_STACK_NOT_EXISTING"
	resourceQueryTagFilters = "TAG_FILTERS_1_0"
	taggedPageSize          = 100
)

// TaggedResource is a resource ARN with its tags, as seen by the tag service.
type TaggedResource struct {
	Tags map[string]string
	ARN  string
}

// StackResource is one resource of a CloudFormation stack.
type StackResource struct {
	Type       string
	PhysicalID string
}

// StackState is a CloudFormation stack's status and resources.
type StackState struct {
	Status    string
	Resources []StackResource
}

// ResourceSource supplies the live resource view that ResourceQuery groups are evaluated against.
type ResourceSource interface {
	TaggedResources(ctx context.Context) []TaggedResource
	// Stack returns false when no stack matches the identifier.
	Stack(identifier string) (*StackState, bool)
}

type handlerGetters interface {
	GetResourceGroupsTaggingHandler() service.Registerable
	GetCloudFormationHandler() service.Registerable
}

// serviceSource resolves the tagging and CloudFormation backends lazily, so
// provider init order does not matter.
type serviceSource struct {
	cfg handlerGetters
}

func (s serviceSource) TaggedResources(ctx context.Context) []TaggedResource {
	h, ok := s.cfg.GetResourceGroupsTaggingHandler().(*resourcegroupstaggingapi.Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil
	}

	var (
		out   []TaggedResource
		token string
	)

	pageSize := int32(taggedPageSize)

	for {
		res, err := h.Backend.GetResources(ctx, &resourcegroupstaggingapi.GetResourcesInput{
			ResourcesPerPage: &pageSize,
			PaginationToken:  token,
		})
		if err != nil || res == nil {
			return out
		}

		for _, m := range res.ResourceTagMappingList {
			tags := make(map[string]string, len(m.Tags))
			for _, t := range m.Tags {
				tags[t.Key] = t.Value
			}

			out = append(out, TaggedResource{ARN: m.ResourceARN, Tags: tags})
		}

		if res.PaginationToken == nil || *res.PaginationToken == "" {
			return out
		}

		token = *res.PaginationToken
	}
}

func (s serviceSource) Stack(identifier string) (*StackState, bool) {
	h, ok := s.cfg.GetCloudFormationHandler().(*cloudformation.Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	stack, err := h.Backend.DescribeStack(identifier)
	if err != nil || stack == nil {
		return nil, false
	}

	resources, err := h.Backend.DescribeStackResources(stack.StackID)
	if err != nil {
		return nil, false
	}

	st := &StackState{Status: stack.StackStatus, Resources: make([]StackResource, 0, len(resources))}
	for i := range resources {
		st.Resources = append(st.Resources, StackResource{Type: resources[i].Type, PhysicalID: resources[i].PhysicalID})
	}

	return st, true
}

// SetResourceSource wires the live resource view used to evaluate ResourceQuery groups and SearchResources.
func (b *InMemoryBackend) SetResourceSource(s ResourceSource) {
	b.mu.Lock("SetResourceSource")
	defer b.mu.Unlock()

	b.source = s
}

func (b *InMemoryBackend) resourceSource() ResourceSource {
	b.mu.RLock("resourceSource")
	defer b.mu.RUnlock()

	return b.source
}

type cfnQuery struct {
	StackIdentifier     string   `json:"StackIdentifier"`
	ResourceTypeFilters []string `json:"ResourceTypeFilters"`
}

// stackInactive reports statuses that render a stack unusable as a query source.
func stackInactive(status string) bool {
	switch status {
	case "CREATE_FAILED", "ROLLBACK_IN_PROGRESS", "ROLLBACK_COMPLETE", "ROLLBACK_FAILED",
		"DELETE_IN_PROGRESS", "DELETE_FAILED":
		return true
	}

	return false
}

func matchesTagFilters(tags map[string]string, filters []tagFilter) bool {
	for _, f := range filters {
		v, ok := tags[f.Key]
		if !ok || (len(f.Values) > 0 && !slices.Contains(f.Values, v)) {
			return false
		}
	}

	return true
}

func typeAllowed(want map[string]bool, resType string) bool {
	return len(want) == 0 || want[resType]
}

func evalTagFilterQuery(ctx context.Context, src ResourceSource, queryJSON string) []ResourceIdentifier {
	var q tagFilterQuery
	if err := json.Unmarshal([]byte(queryJSON), &q); err != nil {
		return nil
	}

	want := parseResourceTypeFilters(queryJSON)
	out := make([]ResourceIdentifier, 0)

	for _, r := range src.TaggedResources(ctx) {
		resType := resourceTypeFromARN(r.ARN)
		if !typeAllowed(want, resType) || !matchesTagFilters(r.Tags, q.TagFilters) {
			continue
		}

		out = append(out, ResourceIdentifier{ResourceArn: r.ARN, ResourceType: resType})
	}

	return out
}

func evalStackQuery(
	ctx context.Context, src ResourceSource, queryJSON string,
) ([]ResourceIdentifier, []queryErrorWire) {
	var q cfnQuery
	if err := json.Unmarshal([]byte(queryJSON), &q); err != nil {
		return nil, nil
	}

	stack, ok := src.Stack(q.StackIdentifier)
	if !ok || stack.Status == "DELETE_COMPLETE" {
		return nil, []queryErrorWire{{
			ErrorCode: queryErrStackNotExist,
			Message:   "CloudFormation stack " + q.StackIdentifier + " does not exist",
		}}
	}

	if stackInactive(stack.Status) {
		return nil, []queryErrorWire{{
			ErrorCode: queryErrStackInactive,
			Message:   "CloudFormation stack " + q.StackIdentifier + " is in status " + stack.Status,
		}}
	}

	want := parseResourceTypeFilters(queryJSON)
	tagged := src.TaggedResources(ctx)
	out := make([]ResourceIdentifier, 0, len(stack.Resources))

	for _, r := range stack.Resources {
		if !typeAllowed(want, r.Type) {
			continue
		}

		if arn := stackResourceARN(r, tagged); arn != "" {
			out = append(out, ResourceIdentifier{ResourceArn: arn, ResourceType: r.Type})
		}
	}

	return out, nil
}

// stackResourceARN returns the ARN of a stack resource: its physical ID when
// that is an ARN, else the tagged resource of the same type whose ARN ends in it.
func stackResourceARN(r StackResource, tagged []TaggedResource) string {
	if strings.HasPrefix(r.PhysicalID, "arn:") {
		return r.PhysicalID
	}

	for _, t := range tagged {
		if resourceTypeFromARN(t.ARN) != r.Type {
			continue
		}

		if strings.HasSuffix(t.ARN, ":"+r.PhysicalID) || strings.HasSuffix(t.ARN, "/"+r.PhysicalID) {
			return t.ARN
		}
	}

	return ""
}

// evaluateQuery returns the live members of a ResourceQuery, or handled=false
// when no resource source is wired.
func (b *InMemoryBackend) evaluateQuery(
	ctx context.Context, q *ResourceQuery,
) ([]ResourceIdentifier, []queryErrorWire, bool) {
	src := b.resourceSource()
	if src == nil || q == nil {
		return nil, nil, false
	}

	if q.Type == resourceQueryTagFilters {
		return evalTagFilterQuery(ctx, src, q.Query), nil, true
	}

	ids, errs := evalStackQuery(ctx, src, q.Query)

	return ids, errs, true
}
