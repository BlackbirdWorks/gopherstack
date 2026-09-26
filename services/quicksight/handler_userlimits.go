package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func userLimitsQueriesFromBody(body map[string]any) []UserLimitsQuery {
	raw, _ := body["users"].([]any)
	if len(raw) == 0 {
		return nil
	}

	out := make([]UserLimitsQuery, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, UserLimitsQuery{
			Namespace: strField(m, "namespace"),
			UserName:  strField(m, "userName"),
		})
	}

	return out
}

func effectiveLimitToMap(l EffectiveLimit) map[string]any {
	return map[string]any{
		"limitUnit":    l.LimitUnit,
		"limitValue":   l.LimitValue,
		"profileId":    l.ProfileID,
		"resourceType": l.ResourceType,
		"source":       l.Source,
	}
}

func userLimitsToMap(u UserLimits) map[string]any {
	limits := make([]map[string]any, 0, len(u.EffectiveLimits))
	for _, l := range u.EffectiveLimits {
		limits = append(limits, effectiveLimitToMap(l))
	}

	return map[string]any{
		"effectiveLimits": limits,
		"namespace":       u.Namespace,
		"userName":        u.UserName,
	}
}

func userLimitsErrorToMap(e UserLimitsError) map[string]any {
	m := map[string]any{
		"errorCode": e.ErrorCode,
		"message":   e.Message,
	}

	if e.Namespace != "" {
		m["namespace"] = e.Namespace
	}

	if e.UserName != "" {
		m["userName"] = e.UserName
	}

	if e.UserArn != "" {
		m["userArn"] = e.UserArn
	}

	return m
}

// handleBatchDescribeUserLimits handles POST
// /governance/limits/accounts/{accountId}/user-limits. Real
// BatchDescribeUserLimitsOutput has only Errors/UserLimits (no RequestId
// member, unlike most other QuickSight ops -- api_op_BatchDescribeUserLimits.go),
// so the response omits the RequestId/Status filler this package's other
// handlers add.
func (h *Handler) handleBatchDescribeUserLimits(c *echo.Context) error {
	accountID := limitsProfileAccountIDFromCtx(c)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	userLimits, errs := h.Backend.BatchDescribeUserLimits(
		accountID, userLimitsQueriesFromBody(body), strSliceField(body, "resourceTypes"),
	)

	userLimitsMaps := make([]map[string]any, 0, len(userLimits))
	for _, u := range userLimits {
		userLimitsMaps = append(userLimitsMaps, userLimitsToMap(u))
	}

	errMaps := make([]map[string]any, 0, len(errs))
	for _, e := range errs {
		errMaps = append(errMaps, userLimitsErrorToMap(e))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"errors":     errMaps,
		"userLimits": userLimitsMaps,
	})
}
