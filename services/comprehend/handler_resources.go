package comprehend

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func resourceSpecs() map[string]resourceSpec {
	return map[string]resourceSpec{
		"DocumentClassifier": {
			resourceType: resourceTypeDocClassifier,
			nameField:    "DocumentClassifierName",
			arnField:     fieldDocumentClassifierARN,
			objectField:  "DocumentClassifierProperties",
			listField:    "DocumentClassifierPropertiesList",
		},
		"DocumentClassifierVersion": {
			resourceType: resourceTypeDocClassifierVersion,
			nameField:    fieldDocumentClassifierARN,
			arnField:     fieldDocumentClassifierARN,
			objectField:  "DocumentClassifierProperties",
			listField:    "DocumentClassifierPropertiesList",
		},
		"EntityRecognizer": {
			resourceType: resourceTypeEntityRecognizer,
			nameField:    "RecognizerName",
			arnField:     fieldEntityRecognizerARN,
			objectField:  "EntityRecognizerProperties",
			listField:    "EntityRecognizerPropertiesList",
		},
		"EntityRecognizerVersion": {
			resourceType: resourceTypeEntityRecognizerVer,
			nameField:    fieldEntityRecognizerARN,
			arnField:     fieldEntityRecognizerARN,
			objectField:  "EntityRecognizerProperties",
			listField:    "EntityRecognizerPropertiesList",
		},
		"Endpoint": {
			resourceType: resourceTypeEndpoint,
			nameField:    "EndpointName",
			arnField:     fieldEndpointARN,
			objectField:  "EndpointProperties",
			listField:    "EndpointPropertiesList",
		},
		"Flywheel": {
			resourceType: resourceTypeFlywheel,
			nameField:    "FlywheelName",
			arnField:     fieldFlywheelARN,
			objectField:  "FlywheelProperties",
			// ListFlywheelsOutput wraps items as FlywheelSummaryList (FlywheelSummary
			// shape), unlike every other List*Output here which reuses the Properties
			// name -- DescribeFlywheelOutput/UpdateFlywheelOutput still use
			// FlywheelProperties (objectField above), only the List response differs.
			listField: "FlywheelSummaryList",
		},
		"Dataset": {
			resourceType: resourceTypeDataset,
			nameField:    "DatasetName",
			arnField:     fieldDatasetARN,
			objectField:  "DatasetProperties",
			listField:    "DatasetPropertiesList",
		},
	}
}

func (h *Handler) createResource(spec resourceSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		resource, err := h.Backend.CreateResource(
			spec.resourceType, stringValue(input, spec.nameField, ""), stringValue(input, "VersionName", ""),
			input, inputTags(input),
		)
		if err != nil {
			return nil, err
		}

		return map[string]any{spec.arnField: resource.Arn}, nil
	}
}

func (h *Handler) describeResource(spec resourceSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		resource, err := h.Backend.GetResource(stringValue(input, spec.arnField, ""), spec.resourceType)
		if err != nil {
			return nil, err
		}

		return map[string]any{spec.objectField: resourceMap(resource, spec)}, nil
	}
}

func (h *Handler) listResources(spec resourceSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		resources := h.Backend.ListResources(spec.resourceType)
		items := make([]map[string]any, 0, len(resources))
		for _, resource := range resources {
			items = append(items, resourceMap(resource, spec))
		}

		tok, maxResults := paginationParams(input)
		page, nextTok := comprehendPaginate(items, tok, maxResults)
		out := map[string]any{spec.listField: page}
		if nextTok != "" {
			out["NextToken"] = nextTok
		}

		return out, nil
	}
}

func (h *Handler) updateResource(spec resourceSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		_, err := h.Backend.UpdateResource(stringValue(input, spec.arnField, ""), spec.resourceType, input)

		return map[string]any{}, err
	}
}

func (h *Handler) deleteResource(spec resourceSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		err := h.Backend.DeleteResource(stringValue(input, spec.arnField, ""), spec.resourceType)

		return map[string]any{}, err
	}
}

// resourceMap renders a Resource as its AWS wire-shape Properties object.
// The timestamp field names are NOT uniform across resource types in the
// real API: DocumentClassifier(Version) and EntityRecognizer(Version)
// properties use SubmitTime/EndTime, Endpoint and Flywheel properties use
// CreationTime/LastModifiedTime, and Dataset properties use
// CreationTime/EndTime. Emitting the wrong field name means the real SDK's
// client-side unmarshal leaves that field nil, so callers of, e.g.,
// DescribeEndpoint always saw a nil CreationTime/LastModifiedTime before
// this switch existed.
func resourceMap(resource *Resource, spec resourceSpec) map[string]any {
	out := cloneMap(resource.Configuration)
	out[spec.arnField] = resource.Arn
	out["Status"] = resource.Status
	switch resource.Type {
	case resourceTypeEndpoint, resourceTypeFlywheel:
		out["CreationTime"] = awstime.Epoch(resource.CreatedAt)
		out["LastModifiedTime"] = awstime.Epoch(resource.UpdatedAt)
	case resourceTypeDataset:
		out["CreationTime"] = awstime.Epoch(resource.CreatedAt)
		out["EndTime"] = awstime.Epoch(resource.UpdatedAt)
	default:
		out["SubmitTime"] = awstime.Epoch(resource.CreatedAt)
		out["EndTime"] = awstime.Epoch(resource.UpdatedAt)
	}
	if resource.VersionName != "" {
		out["VersionName"] = resource.VersionName
	}

	return out
}

func (h *Handler) importModel(input map[string]any) (map[string]any, error) {
	// ImportModel creates a resource modeled after SourceModelArn (required):
	// the imported model is a DocumentClassifier or an EntityRecognizer
	// depending on which kind of model SourceModelArn identifies, so the
	// resource type must be derived from the ARN rather than hardcoded.
	sourceArn := stringValue(input, "SourceModelArn", "")
	resourceType := resourceTypeDocClassifier
	if strings.Contains(sourceArn, resourceTypeEntityRecognizer) {
		resourceType = resourceTypeEntityRecognizer
	}
	name := stringValue(input, "ModelName", "")
	if name == "" {
		name = modelNameFromArn(sourceArn)
	}
	resource, err := h.Backend.CreateResource(
		resourceType,
		name,
		stringValue(input, "VersionName", ""),
		input,
		inputTags(input),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ModelArn": resource.Arn,
	}, nil
}

// modelNameFromArn extracts the resource name segment from a Comprehend
// model ARN (e.g. ".../document-classifier/my-model" or
// ".../entity-recognizer/my-model/version/v1" both yield "my-model"), used
// as a fallback when ImportModel's optional ModelName is omitted.
const minArnNameSegments = 2

func modelNameFromArn(sourceArn string) string {
	parts := strings.Split(sourceArn, "/")
	if len(parts) < minArnNameSegments {
		return ""
	}

	return parts[1]
}

func (h *Handler) listDocumentClassifierSummaries(input map[string]any) (map[string]any, error) {
	resources := h.Backend.ListResources(resourceTypeDocClassifier)
	items := make([]map[string]any, 0, len(resources))
	for _, resource := range resources {
		items = append(items, map[string]any{
			"DocumentClassifierName": resource.Name,
			"NumberOfVersions":       1,
			"LatestVersionCreatedAt": awstime.Epoch(resource.CreatedAt),
			"LatestVersionName":      resource.VersionName,
			"LatestVersionStatus":    resource.Status,
		})
	}

	tok, maxResults := paginationParams(input)
	page, nextTok := comprehendPaginate(items, tok, maxResults)
	out := map[string]any{"DocumentClassifierSummariesList": page}
	if nextTok != "" {
		out["NextToken"] = nextTok
	}

	return out, nil
}

func (h *Handler) listEntityRecognizerSummaries(input map[string]any) (map[string]any, error) {
	resources := h.Backend.ListResources(resourceTypeEntityRecognizer)
	items := make([]map[string]any, 0, len(resources))
	for _, resource := range resources {
		items = append(items, map[string]any{
			"RecognizerName":         resource.Name,
			"NumberOfVersions":       1,
			"LatestVersionCreatedAt": awstime.Epoch(resource.CreatedAt),
			"LatestVersionName":      resource.VersionName,
			"LatestVersionStatus":    resource.Status,
		})
	}

	tok, maxResults := paginationParams(input)
	page, nextTok := comprehendPaginate(items, tok, maxResults)
	out := map[string]any{"EntityRecognizerSummariesList": page}
	if nextTok != "" {
		out["NextToken"] = nextTok
	}

	return out, nil
}

func (h *Handler) stopTrainingDocumentClassifier(input map[string]any) (map[string]any, error) {
	err := h.Backend.StopTrainingResource(stringValue(input, fieldDocumentClassifierARN, ""), resourceTypeDocClassifier)

	return map[string]any{}, err
}

func (h *Handler) stopTrainingEntityRecognizer(input map[string]any) (map[string]any, error) {
	err := h.Backend.StopTrainingResource(
		stringValue(input, fieldEntityRecognizerARN, ""),
		resourceTypeEntityRecognizer,
	)

	return map[string]any{}, err
}
