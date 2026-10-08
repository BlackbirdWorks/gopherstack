package comprehend

import "time"

const (
	flywheelModelVersionPrefix = "flywheel-generated-"
	modelTypeDocClassifier     = "DOCUMENT_CLASSIFIER"
	modelTypeEntityRecognizer  = "ENTITY_RECOGNIZER"
)

func flywheelModelVersion(iterationID string) string {
	return flywheelModelVersionPrefix + iterationID
}

// createFlywheelModelLocked materialises the model an iteration trains, as a
// real classifier/recognizer resource owned by the flywheel. Caller holds b.mu.
func (b *InMemoryBackend) createFlywheelModelLocked(iteration *FlywheelIteration) {
	flywheel, ok := b.resources.Get(iteration.FlywheelArn)
	if !ok {
		return
	}

	var resourceType string

	switch stringValue(flywheel.Configuration, "ModelType", "") {
	case modelTypeDocClassifier:
		resourceType = resourceTypeDocClassifier
	case modelTypeEntityRecognizer:
		resourceType = resourceTypeEntityRecognizer
	default:
		return
	}

	version := flywheelModelVersion(iteration.FlywheelIterationID)
	modelArn := b.resourceARN(resourceType, flywheel.Name, version)

	if b.resources.Has(modelArn) {
		return
	}

	now := time.Now().UTC()
	b.resources.Put(&Resource{
		CreatedAt:         now,
		UpdatedAt:         now,
		TrainingStartTime: iteration.CreationTime,
		TrainingEndTime:   now,
		Name:              flywheel.Name,
		Arn:               modelArn,
		Type:              resourceType,
		Status:            statusTrained,
		VersionName:       version,
		FlywheelArn:       flywheel.Arn,
		Configuration:     map[string]any{fieldFlywheelARN: flywheel.Arn},
	})
	b.tags[modelArn] = map[string]string{}
}

// IterationModelArn returns the ARN of the model trained by iteration, or ""
// while the iteration has not yet produced one.
func (b *InMemoryBackend) IterationModelArn(iteration *FlywheelIteration) string {
	b.mu.RLock("IterationModelArn")
	defer b.mu.RUnlock()

	flywheel, ok := b.resources.Get(iteration.FlywheelArn)
	if !ok {
		return ""
	}

	for _, resourceType := range []string{resourceTypeDocClassifier, resourceTypeEntityRecognizer} {
		modelArn := b.resourceARN(resourceType, flywheel.Name, flywheelModelVersion(iteration.FlywheelIterationID))
		if b.resources.Has(modelArn) {
			return modelArn
		}
	}

	return ""
}

func flywheelModelMetrics() map[string]any {
	return map[string]any{
		"AverageAccuracy":  syntheticAccuracy,
		"AverageF1Score":   syntheticF1Score,
		"AveragePrecision": syntheticPrecision,
		"AverageRecall":    syntheticRecall,
	}
}
