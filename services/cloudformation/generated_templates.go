package cloudformation

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/google/uuid"
)

func (b *InMemoryBackend) CreateGeneratedTemplate(
	name string,
	resourceIDs []string,
) (*GeneratedTemplate, error) {
	b.mu.Lock("CreateGeneratedTemplate")
	defer b.mu.Unlock()
	// Build a template body from the given resource IDs. Each resource ID is
	// treated as "Type/LogicalID"; if it doesn't have that form we synthesize a
	// generic resource.
	templateBody := b.buildGeneratedTemplateBody(resourceIDs)
	gt := &GeneratedTemplate{
		GeneratedTemplateID:   uuid.New().String(),
		GeneratedTemplateName: name,
		Status:                statusComplete,
		TemplateBody:          templateBody,
	}
	b.generatedTemplates.Put(gt)

	return gt, nil
}

type cfnResource struct {
	Properties map[string]any `json:"Properties"`
	Type       string         `json:"Type"`
}

type cfnTemplate struct {
	Resources                map[string]cfnResource `json:"Resources"`
	AWSTemplateFormatVersion string                 `json:"AWSTemplateFormatVersion"`
	Description              string                 `json:"Description"`
}

// parseResourceIDs converts a slice of "Type/LogicalID" strings into a cfnResource map.
func parseResourceIDs(resourceIDs []string) map[string]cfnResource {
	resources := make(map[string]cfnResource)
	for i, rid := range resourceIDs {
		resType := cfnTypeWaitConditionHandle
		logicalID := fmt.Sprintf("GeneratedResource%d", i+1)
		if idx := strings.Index(rid, "/"); idx > 0 {
			resType = rid[:idx]
			rawLogical := rid[idx+1:]
			sanitised := strings.Map(func(r rune) rune {
				if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
					return r
				}

				return -1
			}, rawLogical)
			if sanitised != "" {
				logicalID = sanitised
			}
		}
		resources[logicalID] = cfnResource{
			Type:       resType,
			Properties: map[string]any{},
		}
	}

	return resources
}

// marshalGeneratedTemplate marshals a cfnResource map into a CloudFormation template JSON string.
func marshalGeneratedTemplate(resources map[string]cfnResource) string {
	body, err := json.Marshal(cfnTemplate{
		AWSTemplateFormatVersion: "2010-09-09",
		Description:              "Generated template",
		Resources:                resources,
	})
	if err != nil {
		return `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`
	}

	return string(body)
}

// buildGeneratedTemplateBody synthesises a minimal CloudFormation template
// from a list of resource identifiers (Type/LogicalID pairs).
// Caller must hold the lock.
func (b *InMemoryBackend) buildGeneratedTemplateBody(resourceIDs []string) string {
	resources := parseResourceIDs(resourceIDs)
	if len(resources) == 0 {
		// Build from existing stack resources as a convenience.
		for _, stack := range b.stacks.All() {
			if stack.StackStatus == statusDeleteComplete {
				continue
			}
			for _, res := range b.resources[stack.StackID] {
				resources[res.LogicalID] = cfnResource{
					Type:       res.Type,
					Properties: map[string]any{},
				}
			}
		}
	}
	if len(resources) == 0 {
		return `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`
	}

	return marshalGeneratedTemplate(resources)
}

func (b *InMemoryBackend) UpdateGeneratedTemplate(id, name string) error {
	b.mu.Lock("UpdateGeneratedTemplate")
	defer b.mu.Unlock()
	gt, ok := b.generatedTemplates.Get(id)
	if !ok {
		return ErrGeneratedTemplateNotFound
	}
	if name != "" {
		gt.GeneratedTemplateName = name
	}

	return nil
}

func (b *InMemoryBackend) DeleteGeneratedTemplate(id string) error {
	b.mu.Lock("DeleteGeneratedTemplate")
	defer b.mu.Unlock()
	if !b.generatedTemplates.Has(id) {
		return ErrGeneratedTemplateNotFound
	}
	b.generatedTemplates.Delete(id)

	return nil
}

func (b *InMemoryBackend) DescribeGeneratedTemplate(id string) (*GeneratedTemplate, error) {
	b.mu.RLock("DescribeGeneratedTemplate")
	defer b.mu.RUnlock()
	gt, ok := b.generatedTemplates.Get(id)
	if !ok {
		return nil, ErrGeneratedTemplateNotFound
	}

	return gt, nil
}

func (b *InMemoryBackend) GetGeneratedTemplate(id string) (string, error) {
	b.mu.RLock("GetGeneratedTemplate")
	defer b.mu.RUnlock()
	gt, ok := b.generatedTemplates.Get(id)
	if !ok {
		return "", ErrGeneratedTemplateNotFound
	}

	return gt.TemplateBody, nil
}

func (b *InMemoryBackend) ListGeneratedTemplates(
	nextToken string,
) (page.Page[GeneratedTemplate], error) {
	b.mu.RLock("ListGeneratedTemplates")
	defer b.mu.RUnlock()
	result := make([]GeneratedTemplate, 0, b.generatedTemplates.Len())
	for _, gt := range b.generatedTemplates.All() {
		result = append(result, *gt)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GeneratedTemplateName < result[j].GeneratedTemplateName
	})

	return page.New(result, nextToken, 0, cfnDefaultPageSize), nil
}

func (b *InMemoryBackend) StartResourceScan() (string, error) {
	b.mu.Lock("StartResourceScan")
	defer b.mu.Unlock()
	scanID := uuid.New().String()
	b.resourceScans.Put(&ResourceScan{
		ResourceScanID:      scanID,
		Status:              statusComplete,
		PercentageCompleted: resourceScanCompletePercent,
	})
	// Populate scan items from existing active stacks.
	items := make([]ScannedResource, 0)
	for _, stack := range b.stacks.All() {
		if stack.StackStatus == statusDeleteComplete {
			continue
		}
		for _, res := range b.resources[stack.StackID] {
			items = append(items, ScannedResource{
				ResourceType:       res.Type,
				ResourceIdentifier: map[string]string{"Id": res.PhysicalID},
				ManagedByStack:     true,
				StackID:            stack.StackID,
			})
		}
	}
	// If no managed resources were found, include a synthetic placeholder so that
	// callers always see at least one resource (matches legacy mock behaviour).
	if len(items) == 0 {
		items = []ScannedResource{
			{
				ResourceType:       resTypeS3Bucket,
				ResourceIdentifier: map[string]string{"Id": "example-bucket"},
				ManagedByStack:     false,
			},
		}
	}
	b.resourceScanItems[scanID] = items

	return scanID, nil
}

func (b *InMemoryBackend) DescribeResourceScan(scanID string) (*ResourceScan, error) {
	b.mu.RLock("DescribeResourceScan")
	defer b.mu.RUnlock()
	rs, ok := b.resourceScans.Get(scanID)
	if !ok {
		return nil, ErrResourceScanNotFound
	}

	return rs, nil
}

func (b *InMemoryBackend) ListResourceScans(nextToken string) (page.Page[ResourceScan], error) {
	b.mu.RLock("ListResourceScans")
	defer b.mu.RUnlock()
	result := make([]ResourceScan, 0, b.resourceScans.Len())
	for _, rs := range b.resourceScans.All() {
		result = append(result, *rs)
	}

	return page.New(result, nextToken, 0, cfnDefaultPageSize), nil
}

func (b *InMemoryBackend) ListResourceScanResources(scanID, _ string) ([]ScannedResource, error) {
	b.mu.RLock("ListResourceScanResources")
	defer b.mu.RUnlock()
	if !b.resourceScans.Has(scanID) {
		return nil, ErrResourceScanNotFound
	}
	items := b.resourceScanItems[scanID]
	out := make([]ScannedResource, len(items))
	copy(out, items)

	return out, nil
}

func (b *InMemoryBackend) ListResourceScanRelatedResources(
	scanID string,
	_ []string,
) ([]string, error) {
	b.mu.RLock("ListResourceScanRelatedResources")
	defer b.mu.RUnlock()
	if !b.resourceScans.Has(scanID) {
		return nil, ErrResourceScanNotFound
	}

	return []string{}, nil
}
