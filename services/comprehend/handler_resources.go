package comprehend

import (
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// resourceSpecs enumerates the 5 real Comprehend resource families. There
// is deliberately no "DocumentClassifierVersion"/"EntityRecognizerVersion"
// entry: the real API has no CreateDocumentClassifierVersion/
// DescribeDocumentClassifierVersion/ListDocumentClassifierVersions/
// DeleteDocumentClassifierVersion operations (confirmed against the full
// api_op_*.go listing in aws-sdk-go-v2/service/comprehend -- no such files
// exist, and CreateDocumentClassifierInput/CreateEntityRecognizerInput both
// already carry an optional VersionName field). A new version is created by
// calling CreateDocumentClassifier/CreateEntityRecognizer again with the
// SAME name and a new VersionName; createResource below already threads
// VersionName through generically for every spec, so the base
// "DocumentClassifier"/"EntityRecognizer" entries handle versioning without
// a separate resource type. A prior pass invented these 8 extra operation
// names (Create/Describe/List/Delete x2 families); they never existed on
// the real SDK client and have been removed.
func resourceSpecs() map[string]resourceSpec {
	return map[string]resourceSpec{
		"DocumentClassifier": {
			resourceType: resourceTypeDocClassifier,
			nameField:    "DocumentClassifierName",
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
			// Real Comprehend has no DeleteDataset operation at all -- datasets
			// are immutable once created (confirmed against
			// aws-sdk-go-v2/service/comprehend.Client: no Client.DeleteDataset,
			// unlike DocumentClassifier/EntityRecognizer/Endpoint/Flywheel,
			// which are all real Delete ops).
			noDelete: true,
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
		filter, _ := input["Filter"].(map[string]any)
		items := make([]map[string]any, 0, len(resources))
		for _, resource := range resources {
			if !matchesResourceFilter(resource, filter, spec.resourceType) {
				continue
			}
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
	if isTrainingResourceType(resource.Type) && resource.Status == statusTrained {
		// TrainingStartTime/TrainingEndTime and ClassifierMetadata/
		// RecognizerMetadata only exist on the real DocumentClassifierProperties/
		// EntityRecognizerProperties shapes once training has actually
		// completed (real AWS doc: ClassifierMetadata "Information about the
		// document classifier, including the number of documents used for
		// training... and an accuracy rating" -- meaningless before TRAINED).
		out["TrainingStartTime"] = awstime.Epoch(resource.TrainingStartTime)
		out["TrainingEndTime"] = awstime.Epoch(resource.TrainingEndTime)
		if resource.Type == resourceTypeDocClassifier {
			out["ClassifierMetadata"] = classifierMetadata()
		} else {
			out["RecognizerMetadata"] = recognizerMetadata(resource)
		}
	}

	return out
}

// Deterministic synthetic training-metrics constants. Real NLP accuracy
// figures aren't computed by this emulator (no real training happens --
// see initialResourceStatus's fast-forward-to-TRAINED note in store.go);
// these mirror the shape and a plausible fixed value for every field the
// real ClassifierMetadata/EntityRecognizerMetadata carry, the same
// deterministic-synthetic-result approach detectSentiment/detectEntities
// use for word-list based mock detection.
const (
	syntheticNumberOfLabels           = 2
	syntheticNumberOfTestDocuments    = 200
	syntheticNumberOfTrainedDocuments = 800
	syntheticAccuracy                 = 0.97
	syntheticF1Score                  = 0.95
	syntheticHammingLoss              = 0.03
	syntheticPrecision                = 0.96
	syntheticRecall                   = 0.94

	fieldEvaluationMetrics = "EvaluationMetrics"
	fieldF1Score           = "F1Score"
	fieldPrecision         = "Precision"
	fieldRecall            = "Recall"
)

// recognizerEvaluationMetrics is the 3-field EvaluationMetrics shape shared
// by EntityRecognizerMetadata itself and each of its per-type
// EntityRecognizerMetadataEntityTypesListItem entries.
func recognizerEvaluationMetrics() map[string]any {
	return map[string]any{
		fieldF1Score:   syntheticF1Score,
		fieldPrecision: syntheticPrecision,
		fieldRecall:    syntheticRecall,
	}
}

func classifierMetadata() map[string]any {
	return map[string]any{
		"NumberOfLabels":           syntheticNumberOfLabels,
		"NumberOfTestDocuments":    syntheticNumberOfTestDocuments,
		"NumberOfTrainedDocuments": syntheticNumberOfTrainedDocuments,
		fieldEvaluationMetrics: map[string]any{
			"Accuracy":       syntheticAccuracy,
			fieldF1Score:     syntheticF1Score,
			"HammingLoss":    syntheticHammingLoss,
			"MicroF1Score":   syntheticF1Score,
			"MicroPrecision": syntheticPrecision,
			"MicroRecall":    syntheticRecall,
			fieldPrecision:   syntheticPrecision,
			fieldRecall:      syntheticRecall,
		},
	}
}

// recognizerEntityTypes builds the EntityTypes list of an
// EntityRecognizerMetadata from the InputDataConfig.EntityTypes the resource
// was created with, so the returned types actually match what the caller
// configured rather than a hardcoded placeholder list.
func recognizerEntityTypes(resource *Resource) []map[string]any {
	entityTypes := make([]map[string]any, 0)
	inputConfig, ok := resource.Configuration["InputDataConfig"].(map[string]any)
	if !ok {
		return entityTypes
	}
	rawTypes, ok := inputConfig["EntityTypes"].([]any)
	if !ok {
		return entityTypes
	}
	for _, rawType := range rawTypes {
		entry, entryOK := rawType.(map[string]any)
		if !entryOK {
			continue
		}
		entityTypes = append(entityTypes, map[string]any{
			"Type":                  stringValue(entry, "Type", ""),
			"NumberOfTrainMentions": syntheticNumberOfTrainedDocuments,
			fieldEvaluationMetrics:  recognizerEvaluationMetrics(),
		})
	}

	return entityTypes
}

func recognizerMetadata(resource *Resource) map[string]any {
	return map[string]any{
		"EntityTypes":              recognizerEntityTypes(resource),
		"NumberOfTestDocuments":    syntheticNumberOfTestDocuments,
		"NumberOfTrainedDocuments": syntheticNumberOfTrainedDocuments,
		fieldEvaluationMetrics:     recognizerEvaluationMetrics(),
	}
}

// matchesResourceFilter reports whether resource satisfies a List* request's
// optional Filter object. Filter shapes are NOT uniform across resource
// families in the real API (DocumentClassifierFilter/EntityRecognizerFilter
// key on name+SubmitTime*, EndpointFilter keys on ModelArn+CreationTime*,
// FlywheelFilter/DatasetFilter key on CreationTime* only, and DatasetFilter
// additionally has DatasetType) -- field-diffed against each Filter type in
// aws-sdk-go-v2/service/comprehend/types. A nil/empty filter matches
// everything.
func matchesResourceFilter(resource *Resource, filter map[string]any, resourceType string) bool {
	if filter == nil {
		return true
	}
	if status, ok := filter["Status"].(string); ok && status != "" && resource.Status != status {
		return false
	}
	if !matchesResourceFilterIdentity(resource, filter, resourceType) {
		return false
	}

	return matchesResourceFilterTimeWindow(resource, filter, resourceType)
}

// matchesResourceFilterIdentity checks the one identity-ish field each
// resource family's real Filter type carries beyond Status/time-window
// (DocumentClassifierName/RecognizerName/ModelArn/DatasetType -- Flywheel
// has none of these).
func matchesResourceFilterIdentity(resource *Resource, filter map[string]any, resourceType string) bool {
	switch resourceType {
	case resourceTypeDocClassifier:
		name, ok := filter["DocumentClassifierName"].(string)

		return !ok || name == "" || resource.Name == name
	case resourceTypeEntityRecognizer:
		name, ok := filter["RecognizerName"].(string)

		return !ok || name == "" || resource.Name == name
	case resourceTypeEndpoint:
		modelArn, ok := filter["ModelArn"].(string)

		return !ok || modelArn == "" || resource.ModelArn == modelArn
	case resourceTypeDataset:
		datasetType, ok := filter["DatasetType"].(string)
		if !ok || datasetType == "" {
			return true
		}
		cfgType, _ := resource.Configuration["DatasetType"].(string)

		return cfgType == datasetType
	default:
		return true
	}
}

// matchesResourceFilterTimeWindow checks the SubmitTimeBefore/SubmitTimeAfter
// (classifier/recognizer families) or CreationTimeBefore/CreationTimeAfter
// (endpoint/flywheel/dataset families) window every real Filter type carries.
func matchesResourceFilterTimeWindow(resource *Resource, filter map[string]any, resourceType string) bool {
	beforeKey, afterKey := "SubmitTimeBefore", "SubmitTimeAfter"
	if resourceType == resourceTypeEndpoint || resourceType == resourceTypeFlywheel ||
		resourceType == resourceTypeDataset {
		beforeKey, afterKey = "CreationTimeBefore", "CreationTimeAfter"
	}
	if before, ok := filterTime(filter[beforeKey]); ok && !resource.CreatedAt.Before(before) {
		return false
	}
	if after, ok := filterTime(filter[afterKey]); ok && !resource.CreatedAt.After(after) {
		return false
	}

	return true
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

// resourceSummaries groups resources by Name into one summary row per
// distinct name, aggregating NumberOfVersions and picking the most recently
// created resource as the "latest version" -- matching
// ListDocumentClassifierSummaries/ListEntityRecognizerSummaries' real
// semantics of grouping every version created under the same
// DocumentClassifierName/RecognizerName (via repeated Create* calls with a
// new VersionName -- see resourceSpecs' doc comment) into a single summary,
// rather than emitting one row per stored resource.
func resourceSummaries(resources []*Resource, nameField string) []map[string]any {
	type group struct {
		latest *Resource
		count  int
	}
	groups := make(map[string]*group, len(resources))
	names := make([]string, 0, len(resources))
	for _, resource := range resources {
		g, ok := groups[resource.Name]
		if !ok {
			g = &group{}
			groups[resource.Name] = g
			names = append(names, resource.Name)
		}
		g.count++
		if g.latest == nil || resource.CreatedAt.After(g.latest.CreatedAt) {
			g.latest = resource
		}
	}
	sort.Strings(names)

	items := make([]map[string]any, 0, len(names))
	for _, name := range names {
		g := groups[name]
		items = append(items, map[string]any{
			nameField:                name,
			"NumberOfVersions":       g.count,
			"LatestVersionCreatedAt": awstime.Epoch(g.latest.CreatedAt),
			"LatestVersionName":      g.latest.VersionName,
			"LatestVersionStatus":    g.latest.Status,
		})
	}

	return items
}

func (h *Handler) listDocumentClassifierSummaries(input map[string]any) (map[string]any, error) {
	items := resourceSummaries(h.Backend.ListResources(resourceTypeDocClassifier), "DocumentClassifierName")

	tok, maxResults := paginationParams(input)
	page, nextTok := comprehendPaginate(items, tok, maxResults)
	out := map[string]any{"DocumentClassifierSummariesList": page}
	if nextTok != "" {
		out["NextToken"] = nextTok
	}

	return out, nil
}

func (h *Handler) listEntityRecognizerSummaries(input map[string]any) (map[string]any, error) {
	items := resourceSummaries(h.Backend.ListResources(resourceTypeEntityRecognizer), "RecognizerName")

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
