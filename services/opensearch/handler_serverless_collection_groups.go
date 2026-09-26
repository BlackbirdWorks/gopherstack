package opensearch

// Real AOSS collection-group JSON-RPC handlers (opensearchserverless@v1.34.4
// api_op_{Create,Update,Delete,List,BatchGet}CollectionGroup.go,
// serializers.go/deserializers.go for every field key cited inline below).

func decodeCollectionGroupCapacityLimitsJR(raw any) *CollectionGroupCapacityLimits {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}

	cl := &CollectionGroupCapacityLimits{}
	cl.MaxIndexingCapacityInOCU = float64PtrJR(m["maxIndexingCapacityInOCU"])
	cl.MaxSearchCapacityInOCU = float64PtrJR(m["maxSearchCapacityInOCU"])
	cl.MinIndexingCapacityInOCU = float64PtrJR(m["minIndexingCapacityInOCU"])
	cl.MinSearchCapacityInOCU = float64PtrJR(m["minSearchCapacityInOCU"])

	return cl
}

func float64PtrJR(raw any) *float64 {
	f, ok := raw.(float64)
	if !ok {
		return nil
	}

	return &f
}

// collectionGroupSummaryJR mirrors types.CollectionGroupSummary
// (types.go:295-320): arn, capacityLimits, createdDate, generation, id, name,
// numberOfCollections. numberOfCollections is the real, live count of
// collections whose CollectionGroupName matches this group (see
// CountServerlessCollectionsInGroup), not a fabricated constant.
func collectionGroupSummaryJR(cg *ServerlessCollectionGroup, numberOfCollections int) map[string]any {
	return map[string]any{
		jsonKeyAppArn:           cg.Arn,
		"id":                    cg.ID,
		jsonKeyAppName:          cg.Name,
		jsonKeyCreatedDateJR:    cg.CreatedDate,
		"generation":            cg.Generation,
		jsonKeyCapacityLimitsJR: cg.CapacityLimits,
		"numberOfCollections":   numberOfCollections,
	}
}

// collectionGroupDetailJR mirrors types.CollectionGroupDetail
// (types.go:234-273): everything collectionGroupSummaryJR has, plus
// description, standbyReplicas and tags. currentCapacity is omitted -- no
// live autoscaling loop exists to report one.
func collectionGroupDetailJR(cg *ServerlessCollectionGroup, numberOfCollections int) map[string]any {
	m := collectionGroupSummaryJR(cg, numberOfCollections)
	m["standbyReplicas"] = cg.StandbyReplicas

	if cg.Description != "" {
		m["description"] = cg.Description
	}

	if tags := cloneCollectionGroupTags(cg); len(tags) > 0 {
		m["tags"] = tagMapToListJR(tags)
	}

	return m
}

// createCollectionGroupDetailJR mirrors types.CreateCollectionGroupDetail
// (types.go:395-425): same fields as collectionGroupDetailJR minus
// numberOfCollections (a group has none the instant it's created).
func createCollectionGroupDetailJR(cg *ServerlessCollectionGroup) map[string]any {
	m := collectionGroupDetailJR(cg, 0)
	delete(m, "numberOfCollections")

	return m
}

// updateCollectionGroupDetailJR mirrors types.UpdateCollectionGroupDetail
// (types.go:957-982): arn, capacityLimits, createdDate, description,
// generation, id, lastModifiedDate, name -- no standbyReplicas, tags or
// numberOfCollections.
func updateCollectionGroupDetailJR(cg *ServerlessCollectionGroup) map[string]any {
	m := map[string]any{
		jsonKeyAppArn:             cg.Arn,
		"id":                      cg.ID,
		jsonKeyAppName:            cg.Name,
		jsonKeyCreatedDateJR:      cg.CreatedDate,
		jsonKeyLastModifiedDateJR: cg.LastModifiedDate,
		"generation":              cg.Generation,
		jsonKeyCapacityLimitsJR:   cg.CapacityLimits,
	}

	if cg.Description != "" {
		m["description"] = cg.Description
	}

	return m
}

func (h *Handler) jrCreateCollectionGroup(input map[string]any) (map[string]any, error) {
	name, _ := input[jsonKeyAppName].(string)
	standbyReplicas, _ := input["standbyReplicas"].(string)
	desc, _ := input["description"].(string)
	generation, _ := input["generation"].(string)
	capacityLimits := decodeCollectionGroupCapacityLimitsJR(input[jsonKeyCapacityLimitsJR])
	tags := tagListToMapJR(input["tags"])

	cg, err := h.Backend.CreateServerlessCollectionGroup(name, standbyReplicas, desc, generation, capacityLimits, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"createCollectionGroupDetail": createCollectionGroupDetailJR(cg)}, nil
}

func (h *Handler) jrUpdateCollectionGroup(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	desc, _ := input["description"].(string)
	capacityLimits := decodeCollectionGroupCapacityLimitsJR(input[jsonKeyCapacityLimitsJR])

	cg, err := h.Backend.UpdateServerlessCollectionGroup(id, desc, capacityLimits)
	if err != nil {
		return nil, err
	}

	return map[string]any{"updateCollectionGroupDetail": updateCollectionGroupDetailJR(cg)}, nil
}

func (h *Handler) jrDeleteCollectionGroup(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)

	if err := h.Backend.DeleteServerlessCollectionGroup(id); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) jrListCollectionGroups(_ map[string]any) (map[string]any, error) {
	groups := h.Backend.ListServerlessCollectionGroups()
	summaries := make([]map[string]any, 0, len(groups))

	for _, cg := range groups {
		count := h.Backend.CountServerlessCollectionsInGroup(cg.Name)
		summaries = append(summaries, collectionGroupSummaryJR(cg, count))
	}

	return map[string]any{"collectionGroupSummaries": summaries}, nil
}

func (h *Handler) jrBatchGetCollectionGroup(input map[string]any) (map[string]any, error) {
	ids := strSliceJR(input, "ids")
	names := strSliceJR(input, "names")

	found, errs := h.Backend.BatchGetServerlessCollectionGroups(ids, names)

	details := make([]map[string]any, 0, len(found))
	for _, cg := range found {
		count := h.Backend.CountServerlessCollectionsInGroup(cg.Name)
		details = append(details, collectionGroupDetailJR(cg, count))
	}

	errDetails := make([]map[string]any, 0, len(errs))

	for _, e := range errs {
		entry := map[string]any{jsonKeyErrorCodeJR: e.ErrorCode, jsonKeyErrorMessageJR: e.ErrorMessage}
		if e.ID != "" {
			entry["id"] = e.ID
		}

		if e.Name != "" {
			entry[jsonKeyAppName] = e.Name
		}

		errDetails = append(errDetails, entry)
	}

	return map[string]any{
		"collectionGroupDetails":      details,
		"collectionGroupErrorDetails": errDetails,
	}, nil
}
