package forecast

const (
	keyDatasetGroupArn       = "DatasetGroupArn"
	keyIsAutoPredictor       = "IsAutoPredictor"
	keyReferencePredictorArn = "ReferencePredictorArn"
	keyReferenceSummary      = "ReferencePredictorSummary"
	keyCreatedUsingAuto      = "CreatedUsingAutoPredictor"

	referenceStateActive  = "Active"
	referenceStateDeleted = "Deleted"
)

// predictorDatasetGroup returns the DatasetGroupArn nested under
// InputDataConfig (CreatePredictor) or DataConfig (CreateAutoPredictor).
func predictorDatasetGroup(data map[string]any) string {
	for _, parent := range predictorDatasetGroupParents {
		if config, ok := data[parent].(map[string]any); ok {
			if value := stringValue(config[keyDatasetGroupArn]); value != "" {
				return value
			}
		}
	}

	return ""
}

// recordLineageLocked stores the fields PredictorSummary/ForecastSummary
// declare that no Create input carries directly. Must hold b.mu.
func (b *InMemoryBackend) recordLineageLocked(kind resourceKind, action string, data map[string]any) {
	switch kind {
	case kindPredictor:
		data[keyIsAutoPredictor] = action == "CreateAutoPredictor"
		if dsg := predictorDatasetGroup(data); dsg != "" {
			data[keyDatasetGroupArn] = dsg
		}
	case kindForecast:
		predictor, ok := b.lookupLocked(kindPredictor, stringValue(data[fieldPredictorArn]))
		if !ok {
			return
		}

		if dsg := stringValue(predictor.Data[keyDatasetGroupArn]); dsg != "" {
			data[keyDatasetGroupArn] = dsg
		}

		if auto, isBool := predictor.Data[keyIsAutoPredictor].(bool); isBool {
			data[keyCreatedUsingAuto] = auto
		}
	default:
	}
}

// enrichLocked rewrites a predictor clone's ReferencePredictorArn into the
// wire-shaped ReferencePredictorSummary. Must hold b.mu.
func (b *InMemoryBackend) enrichLocked(r *Resource) {
	if r.Kind != kindPredictor {
		return
	}

	refArn := stringValue(r.Data[keyReferencePredictorArn])
	delete(r.Data, keyReferencePredictorArn)

	if refArn == "" {
		return
	}

	state := referenceStateDeleted
	if _, ok := b.arnIndex[refArn]; ok {
		state = referenceStateActive
	}

	r.Data[keyReferenceSummary] = map[string]any{"Arn": refArn, "State": state}
}

// trimDescribeOnly removes lineage members the named Describe output does
// not declare (forecast@v1.44.4 api_op_Describe{Predictor,AutoPredictor,Forecast}.go).
func trimDescribeOnly(action string, output map[string]any) {
	switch action {
	case "DescribePredictor":
		delete(output, keyDatasetGroupArn)
		delete(output, keyReferenceSummary)
	case "DescribeAutoPredictor":
		delete(output, keyDatasetGroupArn)
		delete(output, keyIsAutoPredictor)
	case "DescribeForecast":
		delete(output, keyCreatedUsingAuto)
	}
}
