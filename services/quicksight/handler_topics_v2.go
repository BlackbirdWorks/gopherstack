package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// TopicV2-only JSON keys; keys shared with V1 (keyTopicID, keyName, ...) are
// reused from handler_topics.go/handler.go -- see topics_v2.go's doc comment.
const (
	keyDataSetRelations      = "DataSetRelations"
	keyCustomInstructions    = "CustomInstructions"
	keyCustomInstructionsStr = "CustomInstructionsString"
	keyPublishOption         = "PublishOption"
	keyTopicSummaryListV2    = "TopicSummaryList"
)

func isTopicV2Op(op string) bool {
	switch op {
	case opCreateTopicV2, opDescribeTopicV2, opUpdateTopicV2, opDeleteTopicV2,
		opListTopicsV2, opSearchTopicsV2, opDescribeTopicPermsV2, opUpdateTopicPermsV2:
		return true
	}

	return false
}

// dispatchTopicV2 routes the eight TopicV2 ops. The two permissions ops route
// straight to the V1 handlers (byte-identical wire shape, same
// storedTopic.Permissions -- see topics_v2.go); the rest get their own
// handlers since their JSON envelopes genuinely differ from V1's.
func (h *Handler) dispatchTopicV2(c *echo.Context, op string) error {
	switch op {
	case opCreateTopicV2:
		return h.handleCreateTopicV2(c)
	case opDescribeTopicV2:
		return h.handleDescribeTopicV2(c)
	case opUpdateTopicV2:
		return h.handleUpdateTopicV2(c)
	case opDeleteTopicV2:
		return h.handleDeleteTopicV2(c)
	case opListTopicsV2:
		return h.handleListTopicsV2(c)
	case opSearchTopicsV2:
		return h.handleSearchTopicsV2(c)
	case opDescribeTopicPermsV2:
		return h.handleDescribeTopicPermissions(c)
	case opUpdateTopicPermsV2:
		return h.handleUpdateTopicPermissions(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

// mapSliceField extracts a []map[string]any field from body, returning nil
// (not empty) when absent so "omitted" vs "explicitly empty" stays
// distinguishable, matching strField/mapField's convention.
func mapSliceField(body map[string]any, key string) []map[string]any {
	raw, ok := body[key].([]any)
	if !ok {
		return nil
	}

	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, isMap := item.(map[string]any); isMap {
			out = append(out, m)
		}
	}

	return out
}

// topicV2FieldsFromBody reads the TopicV2Details fields from a
// Create/UpdateTopicV2 request body's nested "Topic" object.
func topicV2FieldsFromBody(
	body map[string]any,
) (string, string, []map[string]any, []map[string]any) {
	topic, _ := body[keyTopic].(map[string]any)
	if topic == nil {
		topic = body
	}

	name := strField(topic, keyName)
	description := strField(topic, keyDescription)
	dataSets := mapSliceField(topic, keyDataSets)
	dataSetRelations := mapSliceField(topic, keyDataSetRelations)

	return name, description, dataSets, dataSetRelations
}

// customInstructionsFromBody reads the top-level CustomInstructions object's
// CustomInstructionsString member (types.CustomInstructions, per serializers.go).
func customInstructionsFromBody(body map[string]any) string {
	ci, _ := body[keyCustomInstructions].(map[string]any)

	return strField(ci, keyCustomInstructionsStr)
}

func (h *Handler) handleCreateTopicV2(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	topicID := strField(body, keyTopicID)
	name, description, dataSets, dataSetRelations := topicV2FieldsFromBody(body)
	customInstructions := customInstructionsFromBody(body)

	t, err := h.Backend.CreateTopicV2(
		accountID, topicID, name, description, customInstructions,
		dataSets, dataSetRelations, tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrTopicAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       t.Arn,
		keyTopicID:   t.TopicID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeTopicV2(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	t, err := h.Backend.DescribeTopic(accountID, topicID)
	if err != nil {
		return httpErr(c, err)
	}

	resp := map[string]any{
		keyArn:       t.Arn,
		keyTopicID:   t.TopicID,
		keyTopic:     topicV2ToMap(t),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if t.CustomInstructions != "" {
		resp[keyCustomInstructions] = map[string]any{keyCustomInstructionsStr: t.CustomInstructions}
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateTopicV2(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	name, description, dataSets, dataSetRelations := topicV2FieldsFromBody(body)
	customInstructions := customInstructionsFromBody(body)
	publishOption := strField(body, keyPublishOption)

	t, err := h.Backend.UpdateTopicV2(
		accountID, topicID, name, description, customInstructions, publishOption,
		dataSets, dataSetRelations,
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       t.Arn,
		keyTopicID:   t.TopicID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// DeleteTopicV2Output carries an Arn field (api_op_DeleteTopicV2.go), unlike
// V1, so this handler describes the topic first to capture its Arn.
func (h *Handler) handleDeleteTopicV2(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	t, err := h.Backend.DescribeTopic(accountID, topicID)
	if err != nil {
		return httpErr(c, err)
	}

	if delErr := h.Backend.DeleteTopic(accountID, topicID); delErr != nil {
		return httpErr(c, delErr)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       t.Arn,
		keyTopicID:   topicID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListTopicsV2(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	topics, next, err := h.Backend.ListTopics(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, topicSummaryListV2Response(topics, next))
}

// Unlike ListTopicsV2 (MaxResults/NextToken as query params, per
// awsRestjson1_serializeOpHttpBindingsListTopicsV2Input), SearchTopicsV2Input
// carries Filters/MaxResults/NextToken in the JSON body (per
// awsRestjson1_serializeOpDocumentSearchTopicsV2Input).
func (h *Handler) handleSearchTopicsV2(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	topics, next, err := h.Backend.SearchTopics(
		accountID, folderFiltersFromBody(body), intField(body, "MaxResults"), strField(body, "NextToken"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, topicSummaryListV2Response(topics, next))
}

// topicV2ToMap builds the TopicV2Details wire shape (per
// awsRestjson1_deserializeDocumentTopicV2Details): leaner than V1's
// topicToMap (no UserExperienceVersion/ConfigOptions), and DataSets here
// means TopicV2DataSetReference, not V1's DatasetMetadata.
func topicV2ToMap(t *Topic) map[string]any {
	return map[string]any{
		keyName:             t.Name,
		keyDescription:      t.Description,
		keyDataSets:         t.DataSetsV2,
		keyDataSetRelations: t.DataSetRelations,
	}
}

// topicV2SummaryToMap builds a TopicV2Summary entry (Arn/Name/TopicId only;
// unlike V1's TopicSummary it carries no UserExperienceVersion).
func topicV2SummaryToMap(t *Topic) map[string]any {
	return map[string]any{
		keyArn:     t.Arn,
		keyName:    t.Name,
		keyTopicID: t.TopicID,
	}
}

// topicSummaryListV2Response builds the shared ListTopicsV2Output/
// SearchTopicsV2Output envelope: both use the TopicSummaryList key, distinct
// from V1 ListTopics' TopicsSummaries key.
func topicSummaryListV2Response(topics []*Topic, next string) map[string]any {
	items := make([]map[string]any, 0, len(topics))
	for _, t := range topics {
		items = append(items, topicV2SummaryToMap(t))
	}

	resp := map[string]any{
		keyTopicSummaryListV2: items,
		keyRequestID:          reqIDPlaceholder,
		keyStatus:             http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return resp
}

// classifyTopicV2Paths routes /accounts/{id}/topicsV2/... paths: same segment
// shape as classifyTopicPaths (V1), one level shallower since TopicV2 has no
// refresh/refresh-schedule/reviewed-answer sub-resources.
func classifyTopicV2Paths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsAccountRes:
		return classifyTopicV2Root(method, segs)
	case nSegsAccountResID:
		return classifyTopicV2ByID(method, segs)
	case nSegsSubRes:
		return classifyTopicV2SubRes(method, segs)
	}

	return opUnknown, ""
}

func classifyTopicV2Root(method string, segs []string) (string, string) {
	accountID := seg(segs, segAccountID)
	switch method {
	case http.MethodPost:
		return opCreateTopicV2, accountID
	case http.MethodGet:
		return opListTopicsV2, accountID
	}

	return opUnknown, ""
}

func classifyTopicV2ByID(method string, segs []string) (string, string) {
	id := seg(segs, segResID)
	switch method {
	case http.MethodGet:
		return opDescribeTopicV2, id
	case http.MethodPut:
		return opUpdateTopicV2, id
	case http.MethodDelete:
		return opDeleteTopicV2, id
	}

	return opUnknown, ""
}

func classifyTopicV2SubRes(method string, segs []string) (string, string) {
	id := seg(segs, segResID)
	if seg(segs, segSubRes) != pathSegPermissions {
		return opUnknown, ""
	}

	switch method {
	case http.MethodGet:
		return opDescribeTopicPermsV2, id
	case http.MethodPut:
		return opUpdateTopicPermsV2, id
	}

	return opUnknown, ""
}
