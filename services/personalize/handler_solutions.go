package personalize

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// --- Solution ---

func (h *Handler) createSolution(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	recipeArn, _ := input["recipeArn"].(string)
	eventType, _ := input["eventType"].(string)
	performAutoML, _ := input["performAutoML"].(bool)
	performHPO, _ := input["performHPO"].(bool)
	// performAutoTraining defaults to true when omitted (the real API
	// automatically creates new solution versions every 7 days unless told
	// otherwise); performIncrementalUpdate defaults to false.
	performAutoTraining := boolFieldDefault(input, "performAutoTraining", true)
	performIncrementalUpdate, _ := input["performIncrementalUpdate"].(bool)
	solutionConfig, _ := input["solutionConfig"].(map[string]any)
	tags := extractTags(input)

	sol, err := h.Backend.CreateSolution(
		name, datasetGroupArn, recipeArn, eventType,
		performAutoML, performHPO, performAutoTraining, performIncrementalUpdate,
		solutionConfig,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySolutionArn: sol.SolutionArn}, nil
}

func (h *Handler) describeSolution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["solutionArn"].(string)

	sol, err := h.Backend.DescribeSolution(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"solution": solutionToMap(sol)}, nil
}

func (h *Handler) updateSolution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["solutionArn"].(string)

	// The real UpdateSolutionInput only carries performAutoTraining and
	// performIncrementalUpdate (both optional *bool) -- performAutoML/
	// performHPO are creation-only and are not accepted here. A nil pointer
	// means "not specified in the request", leaving the current value alone.
	var performAutoTraining, performIncrementalUpdate *bool
	if v, ok := input["performAutoTraining"].(bool); ok {
		performAutoTraining = &v
	}
	if v, ok := input["performIncrementalUpdate"].(bool); ok {
		performIncrementalUpdate = &v
	}

	sol, err := h.Backend.UpdateSolution(nameOrArn, performAutoTraining, performIncrementalUpdate)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySolutionArn: sol.SolutionArn}, nil
}

func (h *Handler) deleteSolution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["solutionArn"].(string)

	return map[string]any{}, h.Backend.DeleteSolution(nameOrArn)
}

func (h *Handler) listSolutions(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListSolutions(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, sol := range list {
		summaries = append(summaries, solutionToMap(sol))
	}

	result := map[string]any{"solutions": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- SolutionVersion ---

func (h *Handler) createSolutionVersion(input map[string]any) (map[string]any, error) {
	solutionArn, _ := input["solutionArn"].(string)
	trainingMode, _ := input["trainingMode"].(string)
	tags := extractTags(input)

	sv, err := h.Backend.CreateSolutionVersion(solutionArn, trainingMode, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keySolutionVersionArn: sv.SolutionVersionArn}, nil
}

func (h *Handler) describeSolutionVersion(input map[string]any) (map[string]any, error) {
	svArn, _ := input["solutionVersionArn"].(string)

	sv, err := h.Backend.DescribeSolutionVersion(svArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"solutionVersion": solutionVersionToMap(sv)}, nil
}

func (h *Handler) listSolutionVersions(input map[string]any) (map[string]any, error) {
	solutionArn, _ := input["solutionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListSolutionVersions(solutionArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, sv := range list {
		summaries = append(summaries, solutionVersionToMap(sv))
	}

	result := map[string]any{"solutionVersions": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func (h *Handler) stopSolutionVersionCreation(input map[string]any) (map[string]any, error) {
	svArn, _ := input["solutionVersionArn"].(string)

	return map[string]any{}, h.Backend.StopSolutionVersionCreation(svArn)
}

func (h *Handler) getSolutionMetrics(input map[string]any) (map[string]any, error) {
	svArn, _ := input["solutionVersionArn"].(string)

	return h.Backend.GetSolutionMetrics(svArn)
}

func solutionToMap(sol *Solution) map[string]any {
	m := map[string]any{
		keySolutionArn:             sol.SolutionArn,
		keyName:                    sol.Name,
		keyDatasetGroupArn:         sol.DatasetGroupArn,
		keyRecipeArn:               sol.RecipeArn,
		"eventType":                sol.EventType,
		"performAutoML":            sol.PerformAutoML,
		"performHPO":               sol.PerformHPO,
		"performAutoTraining":      sol.PerformAutoTraining,
		"performIncrementalUpdate": sol.PerformIncrementalUpdate,
		keyStatus:                  sol.Status,
		keyCreationDateTime:        awstime.Epoch(sol.CreationDateTime),
		keyLastUpdatedDateTime:     awstime.Epoch(sol.LastUpdatedDateTime),
	}
	if sol.SolutionConfig != nil {
		m["solutionConfig"] = sol.SolutionConfig
	}
	if sol.AutoMLResult != nil {
		m["autoMLResult"] = sol.AutoMLResult
	}
	// latestSolutionUpdate is only returned once the solution has had at
	// least one UpdateSolution call -- matches the real API.
	if sol.LatestSolutionUpdate != nil {
		m["latestSolutionUpdate"] = sol.LatestSolutionUpdate
	}

	return m
}

func solutionVersionToMap(sv *SolutionVersion) map[string]any {
	m := map[string]any{
		keySolutionVersionArn:  sv.SolutionVersionArn,
		keySolutionArn:         sv.SolutionArn,
		keyStatus:              sv.Status,
		"trainingMode":         sv.TrainingMode,
		"trainingHours":        sv.TrainingHours,
		keyCreationDateTime:    awstime.Epoch(sv.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(sv.LastUpdatedDateTime),
	}
	if sv.SolutionConfig != nil {
		m["solutionConfig"] = sv.SolutionConfig
	}

	return m
}
