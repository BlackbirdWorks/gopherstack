package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by TopicV2 operations. Topic/permissions keys
// shared with V1 (keyTopicID, keyTopicArn, keyPermissions, keyName,
// keyDescription, keyDataSets, ...) are reused from handler_topics.go/
// handler.go -- see topics_v2.go's doc comment for why these two families
// share one wire vocabulary.
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

// dispatchTopicV2 routes the eight TopicV2 ops. DescribeTopicPermissionsV2
// and UpdateTopicPermissionsV2 are routed straight to the existing V1
// handlers (handleDescribeTopicPermissions/handleUpdateTopicPermissions):
// their wire response shape (Permissions/RequestId/Status/TopicArn/TopicId)
// is byte-identical to the V1 ops' and both read/write the same
// storedTopic.Permissions -- see topics_v2.go's doc comment. The remaining
// six ops have their own handlers below because their JSON envelopes
// genuinely differ from V1's (TopicV2Details' leaner shape, TopicSummaryList
// vs TopicsSummaries, a top-level CustomInstructions on Describe, ...).
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

// mapSliceField extracts a []map[string]any array field from body, mirroring
// topicFieldsFromBody's nil-preserving convention: an absent/wrong-typed key
// returns nil (not an empty slice), so "omitted" and "explicitly empty" stay
// distinguishable the same way strField/mapField already keep them for
// scalar/object fields.
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
// CustomInstructionsString member (types.CustomInstructions in aws-sdk-go-v2 --
// a single-required-field struct, confirmed against serializers.go).
func customInstructionsFromBody(body map[string]any) string {
	ci, _ := body[keyCustomInstructions].(map[string]any)

	return strField(ci, keyCustomInstructionsStr)
}

// ---- CreateTopicV2 ----

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

// ---- DescribeTopicV2 ----

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

// ---- UpdateTopicV2 ----

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

// ---- DeleteTopicV2 ----
//
// Unlike handleDeleteTopic (V1), DeleteTopicV2Output carries an Arn field
// (confirmed against api_op_DeleteTopicV2.go), so this handler describes the
// topic first to capture its Arn before the record is gone.
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

// ---- ListTopicsV2 ----

func (h *Handler) handleListTopicsV2(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	topics, next, err := h.Backend.ListTopics(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, topicSummaryListV2Response(topics, next))
}

// ---- SearchTopicsV2 ----
//
// Unlike ListTopicsV2 (MaxResults/NextToken as "max-results"/"next-token"
// query params, confirmed against awsRestjson1_serializeOpHttpBindingsListTopicsV2Input),
// SearchTopicsV2Input puts Filters/MaxResults/NextToken in the JSON body
// (confirmed against awsRestjson1_serializeOpDocumentSearchTopicsV2Input --
// its HTTP-bindings function only binds AwsAccountId).
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

// ---- shared helpers ----

// topicV2ToMap builds the TopicV2Details wire shape (Name/Description/
// DataSets/DataSetRelations -- confirmed against
// awsRestjson1_deserializeDocumentTopicV2Details). Note this is a leaner
// shape than V1's topicToMap: no UserExperienceVersion/ConfigOptions, and
// DataSets here means TopicV2DataSetReference (DataSetArn/DataSetName), not
// V1's DatasetMetadata.
func topicV2ToMap(t *Topic) map[string]any {
	return map[string]any{
		keyName:             t.Name,
		keyDescription:      t.Description,
		keyDataSets:         t.DataSetsV2,
		keyDataSetRelations: t.DataSetRelations,
	}
}

// topicV2SummaryToMap builds a TopicV2Summary entry (Arn/Name/TopicId only --
// confirmed against types.TopicV2Summary, which unlike V1's TopicSummary
// carries no UserExperienceVersion).
func topicV2SummaryToMap(t *Topic) map[string]any {
	return map[string]any{
		keyArn:     t.Arn,
		keyName:    t.Name,
		keyTopicID: t.TopicID,
	}
}

// topicSummaryListV2Response builds the shared ListTopicsV2Output/
// SearchTopicsV2Output envelope: both use the TopicSummaryList key
// (confirmed against both ops' deserializers), distinct from V1 ListTopics'
// TopicsSummaries key.
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

// ---- path classification ----
//
// classifyTopicV2Paths routes /accounts/{id}/topicsV2/... paths -- the same
// segment shape as classifyTopicPaths (V1), one level shallower since
// TopicV2 has no refresh/refresh-schedule/reviewed-answer sub-resources.
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
