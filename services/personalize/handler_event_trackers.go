package personalize

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// --- EventTracker ---

func (h *Handler) createEventTracker(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	tags := extractTags(input)

	et, err := h.Backend.CreateEventTracker(name, datasetGroupArn, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"eventTrackerArn": et.EventTrackerArn,
		"trackingId":      et.TrackingID,
	}, nil
}

func (h *Handler) describeEventTracker(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["eventTrackerArn"].(string)

	et, err := h.Backend.DescribeEventTracker(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"eventTracker": eventTrackerToMap(et)}, nil
}

func (h *Handler) deleteEventTracker(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["eventTrackerArn"].(string)

	return map[string]any{}, h.Backend.DeleteEventTracker(nameOrArn)
}

func (h *Handler) listEventTrackers(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListEventTrackers(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, et := range list {
		summaries = append(summaries, eventTrackerToMap(et))
	}

	result := map[string]any{"eventTrackers": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func eventTrackerToMap(et *EventTracker) map[string]any {
	return map[string]any{
		"eventTrackerArn":      et.EventTrackerArn,
		keyName:                et.Name,
		keyDatasetGroupArn:     et.DatasetGroupArn,
		"trackingId":           et.TrackingID,
		keyStatus:              et.Status,
		keyCreationDateTime:    awstime.Epoch(et.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(et.LastUpdatedDateTime),
	}
}
