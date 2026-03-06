package sns

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	snsVersion       = "Version=2010-03-31"
	snsContentType   = "application/x-www-form-urlencoded"
	snsMatchPriority = 80
	unknownOperation = "Unknown"
)

// Handler is the Echo HTTP handler for SNS operations.
type snsActionFn func(c *echo.Context) error

type Handler struct {
	actions map[string]snsActionFn
	Backend StorageBackend
	// DefaultRegion is the fallback region used when region cannot be extracted from the request.
	DefaultRegion string
}

// NewHandler creates a new SNS Handler with the given backend and logger.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.actions = h.buildActions()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SNS"
}

// GetSupportedOperations returns the list of supported SNS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateTopic",
		"DeleteTopic",
		"ListTopics",
		"GetTopicAttributes",
		"SetTopicAttributes",
		"Subscribe",
		"ConfirmSubscription",
		"Unsubscribe",
		"ListSubscriptions",
		"ListSubscriptionsByTopic",
		"Publish",
		"PublishBatch",
		"GetSubscriptionAttributes",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "sns" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this SNS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches SNS requests by Content-Type and body version.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		ct := c.Request().Header.Get("Content-Type")
		if !strings.Contains(ct, snsContentType) {
			return false
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return false
		}

		return strings.Contains(string(body), snsVersion)
	}
}

// MatchPriority returns the routing priority for the SNS handler.
func (h *Handler) MatchPriority() int {
	return snsMatchPriority
}

// ExtractOperation extracts the SNS action from the request form.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return unknownOperation
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return unknownOperation
	}

	action := vals.Get("Action")
	if action == "" {
		return unknownOperation
	}

	return action
}

// ExtractResource extracts the primary resource (TopicArn or Name) from the request form.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	if arn := vals.Get("TopicArn"); arn != "" {
		return arn
	}

	return vals.Get("Name")
}

// Handler returns the Echo HandlerFunc for SNS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		if err := c.Request().ParseForm(); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameter", err.Error())
		}

		action := c.Request().FormValue("Action")
		log.DebugContext(ctx, "SNS request", "action", action)

		return h.dispatch(c, action)
	}
}

// dispatch routes the action to the appropriate handler method.
func (h *Handler) dispatch(c *echo.Context, action string) error {
	fn, ok := h.actions[action]
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "InvalidAction",
			fmt.Sprintf("Action %s is not valid for this endpoint", action))
	}

	return fn(c)
}

// buildActions constructs the action dispatch table.
func (h *Handler) buildActions() map[string]snsActionFn {
	return map[string]snsActionFn{
		"CreateTopic":               h.handleCreateTopic,
		"DeleteTopic":               h.handleDeleteTopic,
		"ListTopics":                h.handleListTopics,
		"GetTopicAttributes":        h.handleGetTopicAttributes,
		"SetTopicAttributes":        h.handleSetTopicAttributes,
		"Subscribe":                 h.handleSubscribe,
		"ConfirmSubscription":       h.handleConfirmSubscription,
		"Unsubscribe":               h.handleUnsubscribe,
		"ListSubscriptions":         h.handleListSubscriptions,
		"ListSubscriptionsByTopic":  h.handleListSubscriptionsByTopic,
		"Publish":                   h.handlePublish,
		"PublishBatch":              h.handlePublishBatch,
		"GetSubscriptionAttributes": h.handleGetSubscriptionAttributes,
		"ListTagsForResource":       h.handleListTagsForResource,
		"TagResource":               h.handleTagResource,
		"UntagResource":             h.handleUntagResource,
	}
}

func (h *Handler) handleCreateTopic(c *echo.Context) error {
	name := c.Request().FormValue("Name")
	if name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "Name is required")
	}

	attrs := extractFormAttributes(c)

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)
	topic, err := h.Backend.CreateTopicInRegion(name, region, attrs)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, CreateTopicResponse{
		CreateTopicResult: CreateTopicResult{TopicArn: topic.TopicArn},
		ResponseMetadata:  ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleDeleteTopic(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	if err := h.Backend.DeleteTopic(topicArn); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, DeleteTopicResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListTopics(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")

	topics, token, err := h.Backend.ListTopics(nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	members := make([]XMLTopic, len(topics))
	for i, t := range topics {
		members[i] = XMLTopic{TopicArn: t.TopicArn}
	}

	return h.writeXML(c, ListTopicsResponse{
		ListTopicsResult: ListTopicsResult{Topics: members, NextToken: token},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetTopicAttributes(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	attrs, err := h.Backend.GetTopicAttributes(topicArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	entries := attrsToEntries(attrs)

	return h.writeXML(c, GetTopicAttributesResponse{
		GetTopicAttributesResult: GetTopicAttributesResult{Attributes: entries},
		ResponseMetadata:         ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleSetTopicAttributes(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	attrName := c.Request().FormValue("AttributeName")
	attrValue := c.Request().FormValue("AttributeValue")

	if topicArn == "" || attrName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn and AttributeName are required")
	}

	if err := h.Backend.SetTopicAttributes(topicArn, attrName, attrValue); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetTopicAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleSubscribe(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	protocol := c.Request().FormValue("Protocol")
	endpoint := c.Request().FormValue("Endpoint")

	if topicArn == "" || protocol == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn and Protocol are required")
	}

	validProtocols := map[string]bool{
		"email": true, "email-json": true, "http": true, "https": true,
		"sqs": true, "lambda": true, "sms": true,
	}
	if !validProtocols[protocol] {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter",
			fmt.Sprintf("Invalid parameter: Protocol Reason: %s is not a valid protocol", protocol))
	}

	filterPolicy := extractFilterPolicy(c.Request().Form)

	sub, err := h.Backend.Subscribe(topicArn, protocol, endpoint, filterPolicy)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	subArn := sub.SubscriptionArn
	if sub.PendingConfirmation {
		subArn = "PendingConfirmation"
	}

	return h.writeXML(c, SubscribeResponse{
		SubscribeResult:  SubscribeResult{SubscriptionArn: subArn},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleUnsubscribe(c *echo.Context) error {
	subscriptionArn := c.Request().FormValue("SubscriptionArn")
	if subscriptionArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required")
	}

	if err := h.Backend.Unsubscribe(subscriptionArn); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, UnsubscribeResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleConfirmSubscription(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	token := c.Request().FormValue("Token")

	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	if token == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "Token is required")
	}

	sub, err := h.Backend.ConfirmSubscription(topicArn, token)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ConfirmSubscriptionResponse{
		ConfirmSubscriptionResult: ConfirmSubscriptionResult{SubscriptionArn: sub.SubscriptionArn},
		ResponseMetadata:          ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListSubscriptions(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")

	subs, token, err := h.Backend.ListSubscriptions(nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ListSubscriptionsResponse{
		ListSubscriptionsResult: ListSubscriptionsResult{
			Subscriptions: toXMLSubscriptions(subs),
			NextToken:     token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListSubscriptionsByTopic(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	nextToken := c.Request().FormValue("NextToken")

	subs, token, err := h.Backend.ListSubscriptionsByTopic(topicArn, nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ListSubscriptionsByTopicResponse{
		ListSubscriptionsByTopicResult: ListSubscriptionsByTopicResult{
			Subscriptions: toXMLSubscriptions(subs),
			NextToken:     token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handlePublish(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	message := c.Request().FormValue("Message")
	subject := c.Request().FormValue("Subject")
	messageStructure := c.Request().FormValue("MessageStructure")

	if topicArn == "" || message == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn and Message are required")
	}

	attrs := extractMessageAttributes(c.Request().Form)

	messageID, err := h.Backend.Publish(topicArn, message, subject, messageStructure, attrs)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, PublishResponse{
		PublishResult:    PublishResult{MessageID: messageID},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handlePublishBatch(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	if topicArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
	}

	entries := extractBatchEntries(c.Request().Form)

	if len(entries) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PublishBatchRequestEntries is required")
	}

	successful := make([]XMLPublishBatchSuccessEntry, 0, len(entries))
	failed := make([]XMLPublishBatchFailEntry, 0)

	for _, entry := range entries {
		msgID, err := h.Backend.Publish(topicArn, entry.message, entry.subject, "", nil)
		if err != nil {
			failed = append(failed, XMLPublishBatchFailEntry{
				ID:          entry.id,
				Code:        errorCode(err),
				Message:     err.Error(),
				SenderFault: true,
			})

			continue
		}

		successful = append(successful, XMLPublishBatchSuccessEntry{MessageID: msgID, ID: entry.id})
	}

	return h.writeXML(c, PublishBatchResponse{
		PublishBatchResult: PublishBatchResult{Successful: successful, Failed: failed},
		ResponseMetadata:   ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetSubscriptionAttributes(c *echo.Context) error {
	subscriptionArn := c.Request().FormValue("SubscriptionArn")
	if subscriptionArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required")
	}

	attrs, err := h.Backend.GetSubscriptionAttributes(subscriptionArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	entries := attrsToEntries(attrs)

	return h.writeXML(c, GetSubscriptionAttributesResponse{
		GetSubscriptionAttributesResult: GetSubscriptionAttributesResult{Attributes: entries},
		ResponseMetadata:                ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	tags := h.Backend.GetTopicTags(resourceArn)
	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	return h.writeXML(c, snsListTagsResponse{
		Result:           snsListTagsResult{Tags: tagList},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// parseSNSTagsFromForm reads Tags.member.N.Key/Value pairs from the form.
func parseSNSTagsFromForm(c *echo.Context) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := c.Request().FormValue(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			return tags
		}
		tags[k] = c.Request().FormValue(fmt.Sprintf("Tags.member.%d.Value", i))
	}
}

// parseSNSTagKeysFromForm reads TagKeys.member.N values from the form.
func parseSNSTagKeysFromForm(c *echo.Context) []string {
	var keys []string
	for i := 1; ; i++ {
		k := c.Request().FormValue(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			return keys
		}
		keys = append(keys, k)
	}
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	kv := parseSNSTagsFromForm(c)
	h.Backend.SetTopicTags(resourceArn, svcTags.FromMap("sns."+resourceArn+".tags.input", kv))

	return h.writeXML(
		c,
		snsEmptyResponse{
			XMLName: xml.Name{Space: "https://sns.amazonaws.com/doc/2010-03-31/", Local: "TagResourceResponse"},
		},
	)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	keys := parseSNSTagKeysFromForm(c)
	h.Backend.RemoveTopicTags(resourceArn, keys)

	return h.writeXML(
		c,
		snsEmptyResponse{
			XMLName: xml.Name{Space: "https://sns.amazonaws.com/doc/2010-03-31/", Local: "UntagResourceResponse"},
		},
	)
}

// writeXML marshals v to XML and writes an HTTP 200 OK response.
func (h *Handler) writeXML(c *echo.Context, v any) error {
	httputils.WriteXML(c.Request().Context(), c.Response(), http.StatusOK, v)

	return nil
}

// writeError writes an XML error response.
func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	errResp := ErrorResponse{
		Error:     Error{Type: "Sender", Code: code, Message: message},
		RequestID: uuid.New().String(),
	}

	httputils.WriteXML(c.Request().Context(), c.Response(), status, errResp)

	return nil
}

// handleBackendError maps a backend error to an XML error response.
func (h *Handler) handleBackendError(c *echo.Context, err error) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	code := errorCode(err)
	status := http.StatusBadRequest

	switch {
	case errors.Is(err, ErrTopicNotFound), errors.Is(err, ErrSubscriptionNotFound):
		log.WarnContext(ctx, "SNS resource not found", "error", err)
	case errors.Is(err, ErrTopicAlreadyExists):
		log.WarnContext(ctx, "SNS topic already exists", "error", err)
	case errors.Is(err, ErrInvalidParameter):
		log.WarnContext(ctx, "SNS invalid parameter", "error", err)
	default:
		status = http.StatusInternalServerError
		log.ErrorContext(ctx, "SNS internal error", "error", err)
	}

	return h.writeError(c, status, code, err.Error())
}

// errorCode returns the SNS error code string for the given error.
func errorCode(err error) string {
	switch {
	case errors.Is(err, ErrTopicNotFound), errors.Is(err, ErrSubscriptionNotFound):
		return "NotFound"
	case errors.Is(err, ErrTopicAlreadyExists):
		return "TopicAlreadyExists"
	case errors.Is(err, ErrInvalidParameter):
		return "InvalidParameter"
	default:
		return "InternalError"
	}
}

// attrsToEntries converts a string map to sorted XMLAttributeEntry slice.
func attrsToEntries(attrs map[string]string) []XMLAttributeEntry {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	entries := make([]XMLAttributeEntry, len(keys))
	for i, k := range keys {
		entries[i] = XMLAttributeEntry{Key: k, Value: attrs[k]}
	}

	return entries
}

// toXMLSubscriptions converts Subscription slice to XMLSubscription slice.
func toXMLSubscriptions(subs []Subscription) []XMLSubscription {
	result := make([]XMLSubscription, len(subs))
	for i, s := range subs {
		result[i] = XMLSubscription{
			TopicArn:        s.TopicArn,
			Protocol:        s.Protocol,
			SubscriptionArn: s.SubscriptionArn,
			Owner:           s.Owner,
			Endpoint:        s.Endpoint,
		}
	}

	return result
}

// extractFormAttributes reads Attributes.entry.N.key/value pairs from the form.
func extractFormAttributes(c *echo.Context) map[string]string {
	attrs := make(map[string]string)

	for i := 1; ; i++ {
		key := c.Request().FormValue(fmt.Sprintf("Attributes.entry.%d.key", i))
		if key == "" {
			return attrs
		}

		val := c.Request().FormValue(fmt.Sprintf("Attributes.entry.%d.value", i))
		attrs[key] = val
	}
}

// extractFilterPolicy reads the FilterPolicy attribute from form Attributes entries.
func extractFilterPolicy(form url.Values) string {
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("Attributes.entry.%d.key", i))
		if key == "" {
			return ""
		}

		if key == "FilterPolicy" {
			return form.Get(fmt.Sprintf("Attributes.entry.%d.value", i))
		}
	}
}

// extractMessageAttributes reads MessageAttributes.entry.N.Name/Value pairs from the form.
func extractMessageAttributes(form url.Values) map[string]MessageAttribute {
	attrs := make(map[string]MessageAttribute)

	for i := 1; ; i++ {
		name := form.Get(fmt.Sprintf("MessageAttributes.entry.%d.Name", i))
		if name == "" {
			return attrs
		}

		attrs[name] = MessageAttribute{
			DataType:    form.Get(fmt.Sprintf("MessageAttributes.entry.%d.Value.DataType", i)),
			StringValue: form.Get(fmt.Sprintf("MessageAttributes.entry.%d.Value.StringValue", i)),
		}
	}
}

// batchEntry holds a single parsed PublishBatch entry.
type batchEntry struct {
	id      string
	message string
	subject string
}

// snsListTagsResult is the inner result element for ListTagsForResource.
type snsListTagsResult struct {
	XMLName xml.Name     `xml:"ListTagsForResourceResult"`
	Tags    []svcTags.KV `xml:"Tags>Tag"`
}

// snsListTagsResponse is the XML response for ListTagsForResource.
type snsListTagsResponse struct {
	XMLName          xml.Name         `xml:"https://sns.amazonaws.com/doc/2010-03-31/ ListTagsForResourceResponse"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	Result           snsListTagsResult
}

// snsEmptyResponse is the XML response for tag mutation operations (TagResource, UntagResource).
// The XMLName field is set dynamically per action.
type snsEmptyResponse struct {
	XMLName xml.Name `xml:""`
}

// extractBatchEntries reads PublishBatchRequestEntries.member.N entries from the form.
func extractBatchEntries(form url.Values) []batchEntry {
	entries := make([]batchEntry, 0)

	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Id", i))
		if id == "" {
			return entries
		}

		entries = append(entries, batchEntry{
			id:      id,
			message: form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Message", i)),
			subject: form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Subject", i)),
		})
	}
}
