package sns

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

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

	// Apply Tags.member.N.Key/Value pairs supplied at topic creation time.
	tags := parseSNSTagsFromForm(c)
	if len(tags) > 0 {
		h.Backend.SetTopicTags(topic.TopicArn, svcTags.FromMap("sns."+topic.TopicArn+".tags", tags))
	}

	return h.writeXML(c, CreateTopicResponse{
		CreateTopicResult: CreateTopicResult{TopicArn: topic.TopicArn},
		ResponseMetadata:  ResponseMetadata{RequestID: uuid.NewString()},
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

	// Remove the FIFO sequence-number counter so the sync.Map does not leak
	// entries for high-churn topic workloads.
	h.fifoSeqNums.Delete(topicArn)

	return h.writeXML(c, DeleteTopicResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleListTopics(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")
	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	topics, token, err := h.Backend.ListTopicsInRegion(region, nextToken)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	members := make([]XMLTopic, len(topics))
	for i, t := range topics {
		members[i] = XMLTopic{TopicArn: t.TopicArn}
	}

	return h.writeXML(c, ListTopicsResponse{
		ListTopicsResult: ListTopicsResult{Topics: members, NextToken: token},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
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
		ResponseMetadata:         ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleSetTopicAttributes(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	attrName := c.Request().FormValue("AttributeName")
	attrValue := c.Request().FormValue("AttributeValue")

	if topicArn == "" || attrName == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"TopicArn and AttributeName are required",
		)
	}

	if err := h.Backend.SetTopicAttributes(topicArn, attrName, attrValue); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetTopicAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleGetDataProtectionPolicy(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	if resourceArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "ResourceArn is required")
	}

	policy, err := h.Backend.GetDataProtectionPolicy(resourceArn)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetDataProtectionPolicyResponse{
		GetDataProtectionPolicyResult: GetDataProtectionPolicyResult{DataProtectionPolicy: policy},
		ResponseMetadata:              ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handlePutDataProtectionPolicy(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	policy := c.Request().FormValue("DataProtectionPolicy")

	if resourceArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "ResourceArn is required")
	}

	if err := h.Backend.PutDataProtectionPolicy(resourceArn, policy); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, PutDataProtectionPolicyResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}
