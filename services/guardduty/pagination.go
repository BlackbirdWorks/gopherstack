package guardduty

import (
	"encoding/base64"
	"net/url"
	"strconv"
)

// paginationParamsFromQuery parses a raw HTTP query string's maxResults and
// nextToken parameters, the real HTTP query bindings shared by every
// GuardDuty REST-JSON List* op whose MaxResults/NextToken are query-bound
// rather than body-bound (verified per-op against the pinned SDK's
// awsRestjson1_serializeOpHttpBindings<Op>Input encoder.SetQuery calls, not
// assumed from a sibling). An unparseable or absent maxResults yields 0
// (caller applies its own default via resolvePageSize).
func paginationParamsFromQuery(query string) (int32, string) {
	values, err := url.ParseQuery(query)
	if err != nil {
		return 0, ""
	}

	var maxResults int32
	if n, convErr := strconv.ParseInt(values.Get("maxResults"), 10, 32); convErr == nil {
		maxResults = int32(n)
	}

	return maxResults, values.Get("nextToken")
}

// decodeToken decodes a base64 pagination token into an integer offset. An
// empty token is treated as offset 0. Mirrors services/sns's decodeToken
// (this package can't import that unexported helper directly).
func decodeToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, err
	}

	offset, err := strconv.Atoi(string(decoded))
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// encodeToken encodes an integer offset as a base64 pagination token.
func encodeToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// paginate returns a page of items starting at offset (size items, or fewer
// once the slice is exhausted) plus the token to fetch the next page, or an
// empty token once there is nothing left.
func paginate[T any](items []T, offset, size int) ([]T, string) {
	if offset >= len(items) {
		return []T{}, ""
	}

	end := offset + size
	nextToken := ""

	if end < len(items) {
		nextToken = encodeToken(end)
	} else {
		end = len(items)
	}

	return items[offset:end], nextToken
}

// standardPageSize is the MaxResults cap every paginated List/Describe op in
// this package currently documents (verified per-op against
// aws-sdk-go-v2/service/guardduty@v1.85.4's api_op_*.go doc comments, not
// assumed): ListFindings/ListFilters/ListIPSets/ListThreatIntelSets/
// ListThreatEntitySets/ListTrustedEntitySets/ListMembers/ListInvitations/
// DescribeMalwareScans/ListMalwareScans all state "default 50, max 50";
// ListPublishingDestinations/ListOrganizationAdminAccounts state "max 50"
// without restating a default (the AWS API reference confirms the same
// ceiling for both); ListInvestigations states "default 50" with no
// explicit max, so 50 is used as the cap here too for consistency.
// ListMalwareProtectionPlans is the one exception (100-per-page, no
// MaxResults on the wire at all) and bypasses this helper entirely.
const standardPageSize = 50

// resolvePageSize returns the effective page size given a caller-requested
// size. If requested is <= 0 or exceeds standardPageSize, standardPageSize
// is used.
func resolvePageSize(requested int) int {
	if requested <= 0 || requested > standardPageSize {
		return standardPageSize
	}

	return requested
}
