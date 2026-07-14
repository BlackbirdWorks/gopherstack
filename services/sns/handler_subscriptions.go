package sns

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleSubscribe(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	protocol := c.Request().FormValue("Protocol")
	endpoint := c.Request().FormValue("Endpoint")

	if topicArn == "" || protocol == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"TopicArn and Protocol are required",
		)
	}

	validProtocols := map[string]bool{
		protocolEmail: true, protocolEmailJSON: true, protocolHTTP: true, protocolHTTPS: true,
		protocolSQS: true, protocolLambda: true, protocolSMS: true, protocolApplication: true, protocolFirehose: true,
	}
	if !validProtocols[protocol] {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter",
			fmt.Sprintf("Invalid parameter: Protocol Reason: %s is not a valid protocol", protocol))
	}

	// AWS requires SubscriptionRoleArn for Firehose delivery stream subscriptions.
	if protocol == "firehose" {
		attrs := extractFormAttributes(c)
		if attrs[attrSubscriptionRoleArn] == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameter",
				"SubscriptionRoleArn is required for Firehose subscriptions")
		}
	}

	filterPolicy := extractFilterPolicy(c.Request().Form)

	sub, err := h.Backend.Subscribe(topicArn, protocol, endpoint, filterPolicy)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	// Apply subscription attributes passed at subscribe time (e.g. RawMessageDelivery, RedrivePolicy).
	attrs := extractFormAttributes(c)
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	for k, v := range attrs {
		if k == attrFilterPolicy {
			continue // already handled by Subscribe
		}

		if setErr := h.Backend.SetSubscriptionAttributes(sub.SubscriptionArn, k, v); setErr != nil {
			log.WarnContext(ctx, "failed to set subscription attribute", "attr", k, "error", setErr)
		}
	}

	// AWS: when ReturnSubscriptionArn is true, always return the real ARN
	// even for pending http/https subscriptions.
	returnArn := strings.EqualFold(c.Request().FormValue("ReturnSubscriptionArn"), "true")
	subArn := sub.SubscriptionArn
	if sub.PendingConfirmation && !returnArn {
		subArn = pendingConfirmationARN
	}

	return h.writeXML(c, SubscribeResponse{
		SubscribeResult:  SubscribeResult{SubscriptionArn: subArn},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleUnsubscribe(c *echo.Context) error {
	subscriptionArn := c.Request().FormValue("SubscriptionArn")
	if subscriptionArn == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"SubscriptionArn is required",
		)
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

func (h *Handler) handleGetSubscriptionAttributes(c *echo.Context) error {
	subscriptionArn := c.Request().FormValue("SubscriptionArn")
	if subscriptionArn == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"SubscriptionArn is required",
		)
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

func (h *Handler) handleSetSubscriptionAttributes(c *echo.Context) error {
	subscriptionArn := c.Request().FormValue("SubscriptionArn")
	attrName := c.Request().FormValue("AttributeName")
	attrValue := c.Request().FormValue("AttributeValue")

	if subscriptionArn == "" || attrName == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"SubscriptionArn and AttributeName are required",
		)
	}

	if err := h.Backend.SetSubscriptionAttributes(subscriptionArn, attrName, attrValue); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetSubscriptionAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}
