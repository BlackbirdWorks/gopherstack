package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by topic operations.
const (
	keyTopicID          = "TopicId"
	keyTopic            = "Topic"
	keyTopicArn         = "TopicArn"
	keyTopicsSummaries  = "TopicsSummaries"
	keyDataSets         = "DataSets"
	keyRefreshDetails   = "RefreshDetails"
	keyRefreshSchedule  = "RefreshSchedule"
	keyRefreshSchedules = "RefreshSchedules"
	keyDatasetID        = "DatasetId"
	keyDatasetArn       = "DatasetArn"
	keyIsEnabled        = "IsEnabled"
	keyRefreshType      = "RefreshType"
	keyAnswers          = "Answers"
	keyAnswerIDs        = "AnswerIds"
	keySucceededAnswer  = "SucceededAnswer"
	keyInvalidAnswers   = "InvalidAnswers"
	keyAnswerID         = "AnswerId"
	keyQuestion         = "Question"
	keyMode             = "Mode"
	keyPrimaryVisual    = "PrimaryVisual"
	keyTemplateField    = "Template"
	keyUserExpVersion   = "UserExperienceVersion"
)

func isTopicOp(op string) bool {
	switch op {
	case opCreateTopic, opDescribeTopic, opUpdateTopic, opDeleteTopic, opListTopics,
		opDescribeTopicPerms, opUpdateTopicPerms, opDescribeTopicRefresh,
		opCreateTopicRefreshSchedule, opDescribeTopicRefreshSchedule,
		opUpdateTopicRefreshSchedule, opDeleteTopicRefreshSchedule, opListTopicRefreshSchedules,
		opBatchCreateTopicAnswers, opBatchDeleteTopicAnswers, opListTopicReviewedAnswers:
		return true
	}

	return false
}

// dispatchTopic handles the core Topic CRUD/list/permissions/refresh-status ops.
// Refresh-schedule and reviewed-answer sub-resource ops are delegated to
// dispatchTopicSchedulesAndAnswers to keep this switch's complexity in budget.
func (h *Handler) dispatchTopic(c *echo.Context, op string) error {
	switch op {
	case opCreateTopic:
		return h.handleCreateTopic(c)
	case opDescribeTopic:
		return h.handleDescribeTopic(c)
	case opUpdateTopic:
		return h.handleUpdateTopic(c)
	case opDeleteTopic:
		return h.handleDeleteTopic(c)
	case opListTopics:
		return h.handleListTopics(c)
	case opDescribeTopicPerms:
		return h.handleDescribeTopicPermissions(c)
	case opUpdateTopicPerms:
		return h.handleUpdateTopicPermissions(c)
	case opDescribeTopicRefresh:
		return h.handleDescribeTopicRefresh(c)
	}

	return h.dispatchTopicSchedulesAndAnswers(c, op)
}

func (h *Handler) dispatchTopicSchedulesAndAnswers(c *echo.Context, op string) error {
	switch op {
	case opCreateTopicRefreshSchedule:
		return h.handleCreateTopicRefreshSchedule(c)
	case opDescribeTopicRefreshSchedule:
		return h.handleDescribeTopicRefreshSchedule(c)
	case opUpdateTopicRefreshSchedule:
		return h.handleUpdateTopicRefreshSchedule(c)
	case opDeleteTopicRefreshSchedule:
		return h.handleDeleteTopicRefreshSchedule(c)
	case opListTopicRefreshSchedules:
		return h.handleListTopicRefreshSchedules(c)
	case opBatchCreateTopicAnswers:
		return h.handleBatchCreateTopicReviewedAnswer(c)
	case opBatchDeleteTopicAnswers:
		return h.handleBatchDeleteTopicReviewedAnswer(c)
	case opListTopicReviewedAnswers:
		return h.handleListTopicReviewedAnswers(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

// ---- Topics ----

func topicFieldsFromBody(body map[string]any) (string, string, string, []map[string]any) {
	topic, _ := body[keyTopic].(map[string]any)
	if topic == nil {
		topic = body
	}

	name := strField(topic, keyName)
	description := strField(topic, keyDescription)
	uxVersion := strField(topic, keyUserExpVersion)

	var dataSets []map[string]any
	if raw, ok := topic[keyDataSets].([]any); ok {
		dataSets = make([]map[string]any, 0, len(raw))
		for _, item := range raw {
			if m, isMap := item.(map[string]any); isMap {
				dataSets = append(dataSets, m)
			}
		}
	}

	return name, description, uxVersion, dataSets
}

func (h *Handler) handleCreateTopic(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	topicID := strField(body, keyTopicID)
	name, description, uxVersion, dataSets := topicFieldsFromBody(body)

	t, err := h.Backend.CreateTopic(
		accountID, topicID, name, description, uxVersion, dataSets,
		permissionsField(body, keyPermissions), tagsFromBody(body),
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

func (h *Handler) handleDescribeTopic(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	t, err := h.Backend.DescribeTopic(accountID, topicID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       t.Arn,
		keyTopicID:   t.TopicID,
		keyTopic:     topicToMap(t),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateTopic(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	name, description, uxVersion, dataSets := topicFieldsFromBody(body)

	t, err := h.Backend.UpdateTopic(accountID, topicID, name, description, uxVersion, dataSets)
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

// DeleteTopicOutput carries an Arn field (api_op_DeleteTopic.go), so this
// handler describes the topic first to capture its Arn — same pattern as
// DeleteTopicV2.
func (h *Handler) handleDeleteTopic(c *echo.Context) error {
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

func (h *Handler) handleListTopics(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	topics, next, err := h.Backend.ListTopics(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(topics))
	for _, t := range topics {
		items = append(items, map[string]any{
			keyArn:             t.Arn,
			keyTopicID:         t.TopicID,
			keyName:            t.Name,
			keyCreatedTime:     t.CreatedTime.Unix(),
			keyLastUpdatedTime: t.LastUpdatedTime.Unix(),
		})
	}

	resp := map[string]any{
		keyTopicsSummaries: items,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- Topic permissions ----

func (h *Handler) handleDescribeTopicPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	t, perms, err := h.Backend.DescribeTopicPermissions(accountID, topicID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:     topicID,
		keyTopicArn:    t.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleUpdateTopicPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	t, perms, err := h.Backend.UpdateTopicPermissions(
		accountID, topicID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:     topicID,
		keyTopicArn:    t.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

// ---- Topic refresh ----

func (h *Handler) handleDescribeTopicRefresh(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)
	refreshID := seg(segs, segSubResID)

	rd, err := h.Backend.DescribeTopicRefresh(accountID, topicID, refreshID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID: topicID,
		keyRefreshDetails: map[string]any{
			"RefreshId":     rd.RefreshID,
			"RefreshStatus": rd.RefreshStatus,
		},
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- Topic refresh schedules ----

func scheduleFieldsFromBody(body map[string]any) (string, string, string, map[string]any) {
	datasetID := strField(body, keyDatasetID)
	datasetArn := strField(body, keyDatasetArn)

	sched, _ := body[keyRefreshSchedule].(map[string]any)
	if sched == nil {
		sched = body
	}
	refreshType := strField(sched, keyRefreshType)
	scheduleConfig := mapField(sched, "ScheduleFrequency")

	return datasetID, datasetArn, refreshType, scheduleConfig
}

func (h *Handler) handleCreateTopicRefreshSchedule(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	datasetID, datasetArn, refreshType, scheduleConfig := scheduleFieldsFromBody(body)
	isEnabled, _ := body[keyIsEnabled].(bool)

	s, err := h.Backend.CreateTopicRefreshSchedule(
		accountID, topicID, datasetID, datasetArn, refreshType, isEnabled, scheduleConfig,
	)
	if err != nil {
		if errors.Is(err, ErrTopicRefreshScheduleAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:    topicID,
		keyDatasetID:  s.DatasetID,
		keyDatasetArn: s.DatasetArn,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleDescribeTopicRefreshSchedule(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)
	datasetID := seg(segs, segSubResID)

	s, err := h.Backend.DescribeTopicRefreshSchedule(accountID, topicID, datasetID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:         topicID,
		keyDatasetID:       s.DatasetID,
		keyDatasetArn:      s.DatasetArn,
		keyRefreshSchedule: topicRefreshScheduleToMap(s),
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleUpdateTopicRefreshSchedule(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)
	datasetID := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	_, _, refreshType, scheduleConfig := scheduleFieldsFromBody(body)

	var isEnabled *bool
	if v, present := body[keyIsEnabled]; present {
		b, _ := v.(bool)
		isEnabled = &b
	}

	s, err := h.Backend.UpdateTopicRefreshSchedule(
		accountID,
		topicID,
		datasetID,
		refreshType,
		isEnabled,
		scheduleConfig,
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:    topicID,
		keyDatasetID:  s.DatasetID,
		keyDatasetArn: s.DatasetArn,
		keyRequestID:  reqIDPlaceholder,
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleDeleteTopicRefreshSchedule(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)
	datasetID := seg(segs, segSubResID)

	if err := h.Backend.DeleteTopicRefreshSchedule(accountID, topicID, datasetID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:   topicID,
		keyDatasetID: datasetID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListTopicRefreshSchedules(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	schedules, err := h.Backend.ListTopicRefreshSchedules(accountID, topicID)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(schedules))
	for _, s := range schedules {
		items = append(items, map[string]any{
			keyDatasetID:       s.DatasetID,
			keyDatasetArn:      s.DatasetArn,
			keyRefreshSchedule: topicRefreshScheduleToMap(s),
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:          topicID,
		keyRefreshSchedules: items,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

// ---- Topic reviewed answers ----

func answersFromBody(body map[string]any, key string) []map[string]any {
	raw, _ := body[key].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}

	return out
}

func stringsFromBody(body map[string]any, key string) []string {
	raw, _ := body[key].([]any)
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func (h *Handler) handleBatchCreateTopicReviewedAnswer(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	succeeded, failed, err := h.Backend.BatchCreateTopicReviewedAnswer(
		accountID,
		topicID,
		answersFromBody(body, keyAnswers),
	)
	if err != nil {
		return httpErr(c, err)
	}

	succeededItems := make([]map[string]any, 0, len(succeeded))
	for _, a := range succeeded {
		succeededItems = append(succeededItems, map[string]any{
			keyAnswerID:   a.AnswerID,
			keyDatasetArn: a.DatasetArn,
			keyQuestion:   a.Question,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:         topicID,
		keySucceededAnswer: succeededItems,
		keyInvalidAnswers:  answerErrorsToMaps(failed),
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleBatchDeleteTopicReviewedAnswer(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	succeeded, failed, err := h.Backend.BatchDeleteTopicReviewedAnswer(
		accountID,
		topicID,
		stringsFromBody(body, keyAnswerIDs),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:         topicID,
		keySucceededAnswer: succeeded,
		keyInvalidAnswers:  answerErrorsToMaps(failed),
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleListTopicReviewedAnswers(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	topicID := seg(segs, segResID)

	answers, err := h.Backend.ListTopicReviewedAnswers(accountID, topicID)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(answers))
	for _, a := range answers {
		items = append(items, map[string]any{
			keyAnswerID:      a.AnswerID,
			keyDatasetArn:    a.DatasetArn,
			keyQuestion:      a.Question,
			keyMode:          a.Mode,
			keyPrimaryVisual: a.PrimaryVisual,
			keyTemplateField: a.Template,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTopicID:   topicID,
		keyAnswers:   items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- shared helpers ----

func topicToMap(t *Topic) map[string]any {
	return map[string]any{
		keyTopicID:         t.TopicID,
		keyArn:             t.Arn,
		keyName:            t.Name,
		keyDescription:     t.Description,
		keyUserExpVersion:  t.UserExperienceVersion,
		keyDataSets:        t.DataSets,
		keyCreatedTime:     t.CreatedTime.Unix(),
		keyLastUpdatedTime: t.LastUpdatedTime.Unix(),
	}
}

// topicSummaryToMap builds a real TopicSummary entry (Arn, Name, TopicId,
// UserExperienceVersion only — unlike the full Topic map returned by
// DescribeTopic, TopicSummary carries no Description/DataSets/timestamps).
func topicSummaryToMap(t *Topic) map[string]any {
	return map[string]any{
		keyArn:            t.Arn,
		keyName:           t.Name,
		keyTopicID:        t.TopicID,
		keyUserExpVersion: t.UserExperienceVersion,
	}
}

func topicRefreshScheduleToMap(s *TopicRefreshSchedule) map[string]any {
	m := map[string]any{
		"ScheduleId":   s.DatasetID,
		keyIsEnabled:   s.IsEnabled,
		keyRefreshType: s.RefreshType,
	}
	if s.ScheduleConfig != nil {
		m["ScheduleFrequency"] = s.ScheduleConfig
	}

	return m
}

func answerErrorsToMaps(errs []TopicAnswerError) []map[string]any {
	out := make([]map[string]any, 0, len(errs))
	for _, e := range errs {
		out = append(out, map[string]any{
			keyAnswerID: e.AnswerID,
			"Error":     map[string]any{"Message": e.Message},
		})
	}

	return out
}

// classifyTopicPaths routes /accounts/{id}/topics/... paths.
func classifyTopicPaths(method string, segs []string, n int) (string, string) {
	switch n {
	case nSegsAccountRes:
		return classifyTopicRoot(method, segs)
	case nSegsAccountResID:
		return classifyTopicByID(method, segs)
	case nSegsSubRes:
		return classifyTopicSubRes(method, segs)
	case nSegsSubResID:
		return classifyTopicSubResID(method, segs)
	}

	return opUnknown, ""
}

func classifyTopicRoot(method string, segs []string) (string, string) {
	accountID := seg(segs, segAccountID)
	switch method {
	case http.MethodPost:
		return opCreateTopic, accountID
	case http.MethodGet:
		return opListTopics, accountID
	}

	return opUnknown, ""
}

func classifyTopicByID(method string, segs []string) (string, string) {
	id := seg(segs, segResID)
	switch method {
	case http.MethodGet:
		return opDescribeTopic, id
	case http.MethodPut:
		return opUpdateTopic, id
	case http.MethodDelete:
		return opDeleteTopic, id
	}

	return opUnknown, ""
}

func classifyTopicSubRes(method string, segs []string) (string, string) {
	id := seg(segs, segResID)
	sub := seg(segs, segSubRes)

	switch sub {
	case pathSegPermissions:
		switch method {
		case http.MethodGet:
			return opDescribeTopicPerms, id
		case http.MethodPut:
			return opUpdateTopicPerms, id
		}
	case pathSegSchedules:
		if method == http.MethodPost {
			return opCreateTopicRefreshSchedule, id
		}
		if method == http.MethodGet {
			return opListTopicRefreshSchedules, id
		}
	case pathSegReviewedAnswers:
		if method == http.MethodGet {
			return opListTopicReviewedAnswers, id
		}
	case pathSegBatchCreateReviewed:
		if method == http.MethodPost {
			return opBatchCreateTopicAnswers, id
		}
	case pathSegBatchDeleteReviewed:
		if method == http.MethodPost {
			return opBatchDeleteTopicAnswers, id
		}
	}

	return opUnknown, ""
}

func classifyTopicSubResID(method string, segs []string) (string, string) {
	id := seg(segs, segResID)
	sub := seg(segs, segSubRes)

	switch sub {
	case pathSegSchedules:
		switch method {
		case http.MethodGet:
			return opDescribeTopicRefreshSchedule, id
		case http.MethodPut:
			return opUpdateTopicRefreshSchedule, id
		case http.MethodDelete:
			return opDeleteTopicRefreshSchedule, id
		}
	case pathSegRefresh:
		if method == http.MethodGet {
			return opDescribeTopicRefresh, id
		}
	}

	return opUnknown, ""
}

// ---- Predict QA ----

// qaResultToMap converts a QAResult into its PredictQAResultsOutput wire
// representation. Real AWS nests the generated-answer fields under a
// GeneratedAnswer object; the DASHBOARD_VISUAL result type (not produced by
// this emulator, since it cannot render a visual for a natural-language
// query) is intentionally not modeled here.
func qaResultToMap(r *QAResult) map[string]any {
	m := map[string]any{"ResultType": r.ResultType}

	if r.ResultType == qaResultTypeGeneratedAnswer {
		m["GeneratedAnswer"] = map[string]any{
			"AnswerId":     r.AnswerID,
			"AnswerStatus": r.AnswerStatus,
			"QuestionId":   r.QuestionID,
			"QuestionText": r.QuestionText,
			"TopicId":      r.TopicID,
			"TopicName":    r.TopicName,
		}
	}

	return m
}

// handlePredictQAResults answers a natural-language query, grounding the
// answer in the account's real Topics rather than fabricating a response.
func (h *Handler) handlePredictQAResults(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	result, err := h.Backend.PredictQAResults(accountID, strField(body, "QueryText"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PrimaryResult": qaResultToMap(result),
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

// handleSearchTopics searches the account's topics. Real SearchTopicsOutput
// returns the matches under TopicSummaryList (distinct from ListTopics'
// TopicsSummaries key). Unlike ListTopics (MaxResults/NextToken as query
// params), SearchTopicsInput carries Filters/MaxResults/NextToken in the
// JSON body (per awsRestjson1_serializeOpDocumentSearchTopicsInput) —
// same as SearchTopicsV2.
func (h *Handler) handleSearchTopics(c *echo.Context) error {
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

	items := make([]map[string]any, 0, len(topics))
	for _, t := range topics {
		items = append(items, topicSummaryToMap(t))
	}

	resp := map[string]any{
		"TopicSummaryList": items,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}
