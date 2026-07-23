package eks

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// dispatchSubscriptionOps handles EKS Anywhere subscription CRUD operations.
func (h *Handler) dispatchSubscriptionOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateEksAnywhereSubscription:
		return true, h.handleCreateEksAnywhereSubscription(c, body)
	case opDeleteEksAnywhereSubscription:
		return true, h.handleDeleteEksAnywhereSubscription(c, route.clusterName)
	case opDescribeEksAnywhereSubscription:
		return true, h.handleDescribeEksAnywhereSubscription(c, route.clusterName)
	case opListEksAnywhereSubscriptions:
		return true, h.handleListEksAnywhereSubscriptions(c)
	case opUpdateEksAnywhereSubscription:
		return true, h.handleUpdateEksAnywhereSubscription(c, route.clusterName, body)
	}

	return false, nil
}

// parseSubscriptionEKSPath returns the route for /eks-anywhere-subscriptions[/{id}].
// UpdateEksAnywhereSubscription is POST to the same leaf path as
// Describe/Delete (not PUT) -- verified against the SDK serializer.
func parseSubscriptionEKSPath(method, path string) (eksRoute, bool) {
	if path == pathSubscriptions {
		if method == http.MethodPost {
			return eksRoute{operation: opCreateEksAnywhereSubscription}, true
		}
		if method == http.MethodGet {
			return eksRoute{operation: opListEksAnywhereSubscriptions}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	if after, ok := strings.CutPrefix(path, pathSubscriptions+"/"); ok {
		switch method {
		case http.MethodGet:
			return eksRoute{operation: opDescribeEksAnywhereSubscription, clusterName: after}, true
		case http.MethodDelete:
			return eksRoute{operation: opDeleteEksAnywhereSubscription, clusterName: after}, true
		case http.MethodPost:
			return eksRoute{operation: opUpdateEksAnywhereSubscription, clusterName: after}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	return eksRoute{}, false
}

func subscriptionToJSON(sub *AnywhereSubscription) map[string]any {
	m := map[string]any{
		"id":              sub.ID,
		"arn":             sub.ARN,
		keyName:           sub.Name,
		keyStatusField:    sub.Status,
		"licenseType":     sub.LicenseType,
		"licenseQuantity": sub.LicenseQuantity,
		keyCreatedAt:      sub.CreatedAt.Unix(),
		"autoRenew":       sub.AutoRenew,
		"effectiveDate":   sub.EffectiveDate.Unix(),
		"expirationDate":  sub.ExpirationDate.Unix(),
	}

	if sub.Term != nil {
		m["term"] = map[string]any{
			"unit":     sub.Term.Unit,
			"duration": sub.Term.Duration,
		}
	}

	return m
}

type subscriptionTermJSON struct {
	Unit     string `json:"unit"`
	Duration int32  `json:"duration"`
}

type createEksAnywhereSubscriptionBody struct {
	Tags            map[string]string     `json:"tags"`
	Term            *subscriptionTermJSON `json:"term"`
	Name            string                `json:"name"`
	LicenseType     string                `json:"licenseType"`
	LicenseQuantity int32                 `json:"licenseQuantity"`
	AutoRenew       bool                  `json:"autoRenew"`
}

// The only term durations (in months) real AWS accepts for EKS Anywhere
// subscriptions -- verified against
// aws-sdk-go-v2/service/eks/types.EksAnywhereSubscriptionTerm's doc comment.
const (
	eksAnywhereTermUnitMonths = "MONTHS"
	eksAnywhereTermDuration12 = 12
	eksAnywhereTermDuration36 = 36
)

var (
	errSubscriptionTermRequired = errors.New("term is required")
	errSubscriptionTermUnit     = errors.New("term.unit must be MONTHS")
	errSubscriptionTermDuration = errors.New("term.duration must be 12 or 36")
)

func validateSubscriptionTerm(t *subscriptionTermJSON) error {
	if t == nil {
		return errSubscriptionTermRequired
	}

	if t.Unit != eksAnywhereTermUnitMonths {
		return errSubscriptionTermUnit
	}

	if t.Duration != eksAnywhereTermDuration12 && t.Duration != eksAnywhereTermDuration36 {
		return errSubscriptionTermDuration
	}

	return nil
}

func (h *Handler) handleCreateEksAnywhereSubscription(c *echo.Context, body []byte) error {
	var in createEksAnywhereSubscriptionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "name is required"))
	}

	if err := validateSubscriptionTerm(in.Term); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
	}

	term := SubscriptionTerm{Unit: in.Term.Unit, Duration: in.Term.Duration}

	sub, err := h.Backend.CreateEksAnywhereSubscription(
		in.Name, term, in.AutoRenew, in.LicenseQuantity, in.LicenseType, in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySubscription: subscriptionToJSON(sub),
	})
}

func (h *Handler) handleDeleteEksAnywhereSubscription(c *echo.Context, id string) error {
	sub, err := h.Backend.DeleteEksAnywhereSubscription(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySubscription: subscriptionToJSON(sub),
	})
}

func (h *Handler) handleDescribeEksAnywhereSubscription(c *echo.Context, id string) error {
	sub, err := h.Backend.DescribeEksAnywhereSubscription(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySubscription: subscriptionToJSON(sub),
	})
}

func (h *Handler) handleListEksAnywhereSubscriptions(c *echo.Context) error {
	subs := h.Backend.ListEksAnywhereSubscriptions()

	result := make([]map[string]any, len(subs))
	for i, sub := range subs {
		result[i] = subscriptionToJSON(sub)
	}

	maxResults, nextToken := eksPaginationParams(c)
	p := page.New(result, nextToken, maxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("subscriptions", p))
}

type updateSubscriptionBody struct {
	LicenseQuantity *int32 `json:"licenseQuantity,omitempty"`
	LicenseType     string `json:"licenseType"`
}

func (h *Handler) handleUpdateEksAnywhereSubscription(c *echo.Context, id string, body []byte) error {
	var in updateSubscriptionBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	sub, err := h.Backend.UpdateEksAnywhereSubscription(id, in.LicenseQuantity, in.LicenseType)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySubscription: subscriptionToJSON(sub),
	})
}
