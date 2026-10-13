package mediatailor

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxResourceName = 255
	maxURLLength    = 25000
)

func checkPagingValues(maxResults, nextToken string) error {
	if maxResults != "" {
		n, err := strconv.Atoi(maxResults)
		if err != nil || n < 1 {
			return fmt.Errorf("%w: MaxResults must be at least 1", ErrInvalidParameter)
		}
	}

	if page.ValidateToken(nextToken) != nil {
		return fmt.Errorf("%w: invalid NextToken", ErrInvalidParameter)
	}

	return nil
}

func checkPaging(c *echo.Context) error {
	q := c.Request().URL.Query()

	return checkPagingValues(firstNonEmpty(q.Get("MaxResults"), q.Get("maxResults")),
		firstNonEmpty(q.Get("NextToken"), q.Get("nextToken")))
}

func checkBodyPaging(body map[string]any) error {
	maxResults := ""
	if f, ok := body["MaxResults"].(float64); ok {
		maxResults = strconv.Itoa(int(f))
	}

	token, _ := body["NextToken"].(string)

	return checkPagingValues(maxResults, token)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

// validateResourceName rejects names that cannot be embedded in a URL path or ARN.
func validateResourceName(kind, name string) error {
	if len(name) > maxResourceName {
		return fmt.Errorf("%w: %s must be at most %d characters", ErrInvalidParameter, kind, maxResourceName)
	}

	if strings.ContainsFunc(name, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) || r == '/' }) {
		return fmt.Errorf("%w: %s %q contains invalid characters", ErrInvalidParameter, kind, name)
	}

	return nil
}

func validateURLLength(field, v string) error {
	if len(v) > maxURLLength {
		return fmt.Errorf("%w: %s must be at most %d characters", ErrInvalidParameter, field, maxURLLength)
	}

	return nil
}

func validAvailSuppressionMode(v string) bool {
	return v == "OFF" || v == "BEHIND_LIVE_EDGE" || v == "AFTER_LIVE_EDGE"
}

func validFillPolicy(v string) bool { return v == "FULL_AVAIL_ONLY" || v == "PARTIAL_AVAIL" }

func validatePlaybackConfigurationURLs(adsURL, videoURL string, extra map[string]any) error {
	if err := validateURLLength("AdDecisionServerUrl", adsURL); err != nil {
		return err
	}

	if err := validateURLLength("VideoContentSourceUrl", videoURL); err != nil {
		return err
	}

	supp, _ := extra["AvailSuppression"].(map[string]any)
	if mode, _ := supp["Mode"].(string); mode != "" && !validAvailSuppressionMode(mode) {
		return fmt.Errorf("%w: AvailSuppression.Mode value %q is not valid", ErrInvalidParameter, mode)
	}

	if fill, _ := supp["FillPolicy"].(string); fill != "" && !validFillPolicy(fill) {
		return fmt.Errorf("%w: AvailSuppression.FillPolicy value %q is not valid", ErrInvalidParameter, fill)
	}

	return nil
}
