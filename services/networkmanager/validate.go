package networkmanager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	minMaxResults    = 1
	maxMaxResults    = 500
	maxNextTokenLen  = 2048
	maxIDListQuery   = 10
	maxDescriptorLen = 256
	maxShortFieldLen = 128
)

func idListQueryParams() []string {
	return []string{
		"globalNetworkIds", "siteIds", "deviceIds", "linkIds", "customerGatewayArns",
		"transitGatewayArns", "transitGatewayConnectPeerArns",
	}
}

func longBodyFields() map[string]int {
	return map[string]int{
		"Description": maxDescriptorLen, "Address": maxDescriptorLen, "Latitude": maxDescriptorLen,
		"Longitude": maxDescriptorLen, "Model": maxShortFieldLen, "Vendor": maxShortFieldLen,
		"SerialNumber": maxShortFieldLen, "Provider": maxShortFieldLen,
	}
}

func checkQueryParams(q url.Values) error {
	if raw := q.Get("maxResults"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < minMaxResults || n > maxMaxResults {
			return validationError(fmt.Sprintf("maxResults must be between %d and %d", minMaxResults, maxMaxResults))
		}
	}

	if tok := q.Get("nextToken"); tok != "" {
		if len(tok) > maxNextTokenLen || page.ValidateToken(tok) != nil {
			return validationError("nextToken is not valid")
		}
	}

	for _, name := range idListQueryParams() {
		if len(q[name]) > maxIDListQuery {
			return validationError(fmt.Sprintf("%s accepts at most %d values", name, maxIDListQuery))
		}
	}

	return nil
}

func checkStringLengths(m map[string]any) error {
	for key, limit := range longBodyFields() {
		if s, ok := m[key].(string); ok && utf8.RuneCountInString(s) > limit {
			return validationError(fmt.Sprintf("%s must be at most %d characters", key, limit))
		}
	}

	if loc, ok := m["Location"].(map[string]any); ok {
		return checkStringLengths(loc)
	}

	return nil
}

// validateRequest enforces the documented paging, ID-list and string-length bounds that the
// SDK does not check client-side.
func validateRequest(r *http.Request, body []byte) error {
	if err := checkQueryParams(r.URL.Query()); err != nil {
		return err
	}

	m, ok := decodeObject(body)
	if !ok {
		return nil
	}

	return checkStringLengths(m)
}

func decodeObject(body []byte) (map[string]any, bool) {
	if len(body) == 0 || body[0] != '{' {
		return nil, false
	}

	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, false
	}

	return m, true
}
