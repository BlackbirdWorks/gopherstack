package iot

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// resolveTopicRuleDestinationOps resolves the topic-rule-destination op
// family.
//
// The real wire shape (iot@v1.77.4 serializers.go) uses "/destinations",
// not the "/rule-destinations" (hyphenated) path gopherstack previously used
// for every op in this family -- found unreachable by gopherstack-n1mb's
// route table. UpdateTopicRuleDestination's real path is additionally bare
// PATCH /destinations (the ARN travels in the JSON body, which
// handleUpdateTopicRuleDestination already reads correctly). The old
// "/rule-destinations" shapes are kept too as non-canonical routes wired
// for this package's own tests.
func resolveTopicRuleDestinationOps(path, method string) string {
	if op := resolveTopicRuleDestinationCanonicalOps(path, method); op != unknownOperation {
		return op
	}

	return resolveTopicRuleDestinationLegacyOps(path, method)
}

func resolveTopicRuleDestinationCanonicalOps(path, method string) string {
	switch {
	case path == pathDestinations && method == http.MethodPost:

		return opCreateTopicRuleDestination
	case path == pathDestinations && method == http.MethodGet:

		return opListTopicRuleDestinations
	case path == pathDestinations && method == http.MethodPatch:

		return opUpdateTopicRuleDestination
	case strings.HasPrefix(path, "/destinations/") && method == http.MethodGet:

		return opGetTopicRuleDestination
	case strings.HasPrefix(path, "/destinations/") && method == http.MethodDelete:

		return opDeleteTopicRuleDestination
	}

	return unknownOperation
}

func resolveTopicRuleDestinationLegacyOps(path, method string) string {
	switch {
	case path == pathRuleDestinations && method == http.MethodPost:

		return opCreateTopicRuleDestination
	case path == pathRuleDestinations && method == http.MethodGet:

		return opListTopicRuleDestinations
	case strings.HasPrefix(path, "/rule-destinations") && method == http.MethodGet:

		return opGetTopicRuleDestination
	case strings.HasPrefix(path, "/rule-destinations") && method == http.MethodPatch:

		return opUpdateTopicRuleDestination
	case strings.HasPrefix(path, "/rule-destinations") && method == http.MethodDelete:

		return opDeleteTopicRuleDestination
	}

	return unknownOperation
}

func (h *Handler) dispatchRuleOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateTopicRule:

		return true, h.handleCreateTopicRule(c)
	case opGetTopicRule:

		return true, h.handleGetTopicRule(c)
	case opDeleteTopicRule:

		return true, h.handleDeleteTopicRule(c)
	case opDisableTopicRule:

		return true, h.handleDisableTopicRule(c)
	case opEnableTopicRule:

		return true, h.handleEnableTopicRule(c)
	case opReplaceTopicRule:

		return true, h.handleReplaceTopicRule(c)
	case opListTopicRules:

		return true, h.handleListTopicRules(c)
	}

	return false, nil
}

func (h *Handler) dispatchTopicRuleDestinationOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateTopicRuleDestination:

		return true, h.handleCreateTopicRuleDestination(c)
	case opGetTopicRuleDestination:

		return true, h.handleGetTopicRuleDestination(c)
	case opListTopicRuleDestinations:

		return true, h.handleListTopicRuleDestinations(c)
	case opUpdateTopicRuleDestination:

		return true, h.handleUpdateTopicRuleDestination(c)
	case opDeleteTopicRuleDestination:

		return true, h.handleDeleteTopicRuleDestination(c)
	}

	return false, nil
}

func (h *Handler) handleCreateTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	rawBody, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	// Accept both wrapped {"topicRulePayload":{...}} and flat {...} formats.
	var wrapped struct {
		TopicRulePayload *TopicRulePayload `json:"topicRulePayload"`
	}
	if jsonErr := json.Unmarshal(rawBody, &wrapped); jsonErr != nil && !errors.Is(jsonErr, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, jsonErr.Error()})
	}

	payload := wrapped.TopicRulePayload
	if payload == nil {
		var flat TopicRulePayload
		if jsonErr := json.Unmarshal(rawBody, &flat); jsonErr == nil && flat.SQL != "" {
			payload = &flat
		}
	}
	if payload == nil {
		payload = &TopicRulePayload{}
	}

	if createErr := h.Backend.CreateTopicRule(&CreateTopicRuleInput{
		RuleName:         ruleName,
		TopicRulePayload: payload,
		Tags:             parseTaggingHeader(c.Request().Header.Get("X-Amz-Tagging")),
	}); createErr != nil {
		return h.handleError(c, createErr)
	}

	return c.NoContent(http.StatusOK)
}

// parseTaggingHeader decodes CreateTopicRuleInput.Tags's wire form -- a
// query-string-encoded "key1=value1&key2=value2" string carried in the
// X-Amz-Tagging header, not the JSON body (verified: iot@v1.77.4
// api_op_CreateTopicRule.go:55, serializers.go:5083-5086
// awsRestjson1_serializeOpHttpBindingsCreateTopicRuleInput).
func parseTaggingHeader(raw string) map[string]string {
	if raw == "" {
		return nil
	}

	values, err := url.ParseQuery(raw)
	if err != nil {
		return nil
	}

	out := make(map[string]string, len(values))
	for k, v := range values {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}

	return out
}

func (h *Handler) handleGetTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	r, err := h.Backend.GetTopicRule(ruleName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ruleArn": r.ARN,
		"rule": map[string]any{
			"ruleName":         r.RuleName,
			"sql":              r.SQL,
			"awsIotSqlVersion": r.AWSIoTSQLVersion,
			keyDescription:     r.Description,
			"actions":          r.Actions,
			"ruleDisabled":     !r.Enabled,
			keyCreatedAt:       awstime.Epoch(r.CreatedAt),
		},
	})
}

func (h *Handler) handleDeleteTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	if err := h.Backend.DeleteTopicRule(ruleName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTopicRules(c *echo.Context) error {
	rules := h.Backend.ListTopicRules()

	out := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		// TopicRuleListItem (iot@v1.77.4 deserializers.go's
		// awsRestjson1_deserializeDocumentTopicRuleListItem) has topicPattern,
		// not sql -- a different shape from the full TopicRule GetTopicRule
		// returns. Every real client's TopicPattern decoded empty before this
		// fix.
		parsed, _ := ParseRuleSQL(r.SQL)

		out = append(out, map[string]any{
			"ruleName":     r.RuleName,
			"ruleArn":      r.ARN,
			"topicPattern": parsed.TopicPattern,
			"ruleDisabled": !r.Enabled,
			keyCreatedAt:   awstime.Epoch(r.CreatedAt),
		})
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(out, pageSize, start)

	resp := map[string]any{"rules": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDisableTopicRule(c *echo.Context) error {
	// Path: /rules/{ruleName}/disable
	after := strings.TrimPrefix(c.Request().URL.Path, "/rules/")
	ruleName := strings.TrimSuffix(after, "/disable")

	if err := h.Backend.DisableTopicRule(ruleName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleEnableTopicRule(c *echo.Context) error {
	// Path: /rules/{ruleName}/enable
	after := strings.TrimPrefix(c.Request().URL.Path, "/rules/")
	ruleName := strings.TrimSuffix(after, "/enable")

	if err := h.Backend.EnableTopicRule(ruleName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleReplaceTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	var body struct {
		TopicRulePayload *TopicRulePayload `json:"topicRulePayload"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	payload := body.TopicRulePayload
	if payload == nil {
		payload = &TopicRulePayload{}
	}

	if err := h.Backend.ReplaceTopicRule(&ReplaceTopicRuleInput{
		RuleName:         ruleName,
		TopicRulePayload: payload,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCreateTopicRuleDestination(c *echo.Context) error {
	var body struct {
		DestinationConfiguration *TopicRuleDestinationConfiguration `json:"destinationConfiguration"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}
	dest, err := h.Backend.CreateTopicRuleDestination(&CreateTopicRuleDestinationInput{
		DestinationConfiguration: body.DestinationConfiguration,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"topicRuleDestination": map[string]any{keyArn: dest.ARN, keyStatus: dest.Status},
	})
}

// topicRuleDestinationARNFromPath extracts the ARN from either the real
// "/destinations/{arn+}" path or the non-canonical "/rule-destinations/{arn}"
// path this package's own tests still use.
func topicRuleDestinationARNFromPath(path string) string {
	if arn, ok := strings.CutPrefix(path, "/destinations/"); ok {
		return arn
	}

	return strings.TrimPrefix(path, "/rule-destinations/")
}

func (h *Handler) handleGetTopicRuleDestination(c *echo.Context) error {
	arn := topicRuleDestinationARNFromPath(c.Request().URL.Path)
	dest, err := h.Backend.GetTopicRuleDestination(arn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"topicRuleDestination": map[string]any{keyArn: dest.ARN, keyStatus: dest.Status},
	})
}

func (h *Handler) handleListTopicRuleDestinations(c *echo.Context) error {
	dests := h.Backend.ListTopicRuleDestinations()
	out := make([]map[string]any, 0, len(dests))
	for _, d := range dests {
		out = append(out, map[string]any{keyArn: d.ARN, keyStatus: d.Status})
	}

	return c.JSON(http.StatusOK, map[string]any{"destinationSummaries": out})
}

func (h *Handler) handleUpdateTopicRuleDestination(c *echo.Context) error {
	var body struct {
		ARN    string `json:"arn"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}
	if err := h.Backend.UpdateTopicRuleDestination(&UpdateTopicRuleDestinationInput{
		ARN:    body.ARN,
		Status: body.Status,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteTopicRuleDestination(c *echo.Context) error {
	arn := topicRuleDestinationARNFromPath(c.Request().URL.Path)
	if err := h.Backend.DeleteTopicRuleDestination(arn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleConfirmTopicRuleDestination(c *echo.Context) error {
	token := strings.TrimPrefix(c.Request().URL.Path, pathConfirmDestination+"/")

	if err := h.Backend.ConfirmTopicRuleDestination(token); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{})
}
