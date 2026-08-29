package comprehend

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

func (h *Handler) startIteration(input map[string]any) (map[string]any, error) {
	iteration, err := h.Backend.StartFlywheelIteration(stringValue(input, fieldFlywheelARN, ""))
	if err != nil {
		return nil, err
	}

	return map[string]any{"FlywheelIterationId": iteration.FlywheelIterationID}, nil
}

func (h *Handler) getIteration(input map[string]any) (map[string]any, error) {
	iteration, err := h.Backend.GetFlywheelIteration(stringValue(input, "FlywheelIterationId", ""))
	if err != nil {
		return nil, err
	}

	return map[string]any{"FlywheelIterationProperties": iterationMap(iteration)}, nil
}

func (h *Handler) listIterations(input map[string]any) (map[string]any, error) {
	iterations := h.Backend.ListFlywheelIterations(stringValue(input, fieldFlywheelARN, ""))
	items := make([]map[string]any, 0, len(iterations))
	for _, iteration := range iterations {
		items = append(items, iterationMap(iteration))
	}

	tok, maxResults := paginationParams(input)
	page, nextTok := comprehendPaginate(items, tok, maxResults)
	out := map[string]any{"FlywheelIterationPropertiesList": page}
	if nextTok != "" {
		out["NextToken"] = nextTok
	}

	return out, nil
}

// iterationMap renders a FlywheelIteration as its real wire-shape object.
// The status field's wire key is "Status", not "FlywheelIterationStatus" --
// confirmed against awsAwsjson11_deserializeDocumentFlywheelIterationProperties
// (aws-sdk-go-v2/service/comprehend@v1.43.4 deserializers.go:16022), whose
// switch has no "FlywheelIterationStatus" case at all.
func iterationMap(iteration *FlywheelIteration) map[string]any {
	return map[string]any{
		fieldFlywheelARN:      iteration.FlywheelArn,
		"FlywheelIterationId": iteration.FlywheelIterationID,
		"Status":              iteration.FlywheelIterationStatus,
		"CreationTime":        awstime.Epoch(iteration.CreationTime),
		"EndTime":             awstime.Epoch(iteration.EndTime),
		"Message":             iteration.Message,
	}
}
