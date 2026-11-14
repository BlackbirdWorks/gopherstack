package azurearm

import (
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const (
	msiActionAssociated = "listAssociatedResources"
	msiDefaultIdent     = "default"
	msiIdentitiesSeg    = "identities"
	msiAssocSegs        = 9
	msiAssocTypeIdx     = 6
	msiScopeTailSegs    = 4
	msiSubDisplayName   = "gopherstack"
	msiFilterAndSep     = " and "
	assocSubField       = "subscriptionId"
	assocContains       = "contains"
	assocStartsWith     = "startswith"
	assocEq             = "eq"
)

// msiAssociatedVersions are the api-versions whose spec defines listAssociatedResources; later ones removed it.
func msiAssociatedVersions() []string { return []string{"2021-09-30-preview", "2022-01-31-preview"} }

//nolint:gochecknoglobals // static route table, read-only after init
var msiScopeRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isMSIDefaultScope(segs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSystemIdentity(c, segs) },
	},
}

func isMSIDefaultScope(segs []string) bool {
	n := len(segs)
	if n <= msiScopeTailSegs {
		return false
	}

	tail := segs[n-msiScopeTailSegs:]

	return strings.EqualFold(tail[0], providersSegment) &&
		strings.EqualFold(tail[1], namespaceMicrosoftManagedIdentity) &&
		strings.EqualFold(tail[2], msiIdentitiesSeg) &&
		strings.EqualFold(tail[3], msiDefaultIdent)
}

func isMSIAssociatedAction(segs []string, r *http.Request) bool {
	return r.Method == http.MethodPost && len(segs) == msiAssocSegs &&
		isNamespaceScope(segs, namespaceMicrosoftManagedIdentity) &&
		segAt(segs, msiAssocTypeIdx, msiIdentitiesType) && segAt(segs, msiAssocSegs-1, msiActionAssociated)
}

func (h *Handler) handleSystemIdentity(c *echo.Context, segs []string) error {
	if !h.checkNSAPIVersion(
		c,
		namespaceMicrosoftManagedIdentity,
		msiAPIVersions(),
		"Microsoft.ManagedIdentity/identities",
	) {
		return nil
	}

	scope, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-msiScopeTailSegs]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	entry, ok := h.Registry.systemIdentity(scope.ARMID())
	if !ok {
		return h.writeAPIError(c, ErrResourceNotFound)
	}

	id := entry.id + "/" + msiIdentitiesSeg + "/" + msiDefaultIdent
	client := uuid.NewSHA1(uuid.NameSpaceURL, []byte(entry.ident.PrincipalID)).String()
	secretURL := h.baseURLFor(c.Request()) + id + "/credentials/v2/identities?tid=" +
		url.QueryEscape(entry.ident.TenantID) + "&arpid=" + url.QueryEscape(entry.ident.PrincipalID) + "&said=" + client

	return h.writeJSON(c, http.StatusOK, map[string]any{
		netFieldID: id, fieldName: scope.LeafName(), fieldType: "ResourceProvider", fieldLocation: entry.location,
		fieldProperties: map[string]any{
			netFieldTenantID: entry.ident.TenantID, "principalId": entry.ident.PrincipalID, "clientId": client,
			"clientSecretUrl": secretURL,
		},
	})
}

func (h *Handler) handleListAssociated(c *echo.Context, segs []string) error {
	label := "Microsoft.ManagedIdentity/" + msiIdentitiesType
	if !h.checkNSAPIVersion(c, namespaceMicrosoftManagedIdentity, msiAPIVersions(), label) {
		return nil
	}

	if v := c.QueryParam(nsAPIVersionKey); !containsFold(msiAssociatedVersions(), v) {
		return h.writeError(c, http.StatusBadRequest, "InvalidApiVersionParameter",
			"The api-version '"+v+"' is not supported for listAssociatedResources. The supported versions are '"+
				strings.Join(msiAssociatedVersions(), ",")+"'.")
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if _, err = h.Registry.Get(c.Request().Context(), id); err != nil {
		return h.writeAPIError(c, err)
	}

	rows := make([]map[string]any, 0)

	for _, e := range h.Registry.associatedResources(id.ARMID()) {
		if rid, perr := ParseGenericResourcePath(e.id); perr == nil {
			rows = append(rows, associatedRow(rid))
		}
	}

	rows, err = filterAssociated(rows, c.QueryParam("$filter"))
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, msiCodeBad, err.Error())
	}

	if err = orderAssociated(rows, c.QueryParam("$orderby")); err != nil {
		return h.writeError(c, http.StatusBadRequest, msiCodeBad, err.Error())
	}

	return h.writeJSON(c, http.StatusOK, h.pageAssociated(c, rows))
}

func associatedRow(rid ResourceID) map[string]any {
	typ := strings.Join(append([]string{rid.Namespace}, rid.Types...), "/")

	return map[string]any{
		netFieldID: rid.ARMID(), fieldName: rid.LeafName(), fieldType: strings.ToLower(typ),
		"resourceGroup": rid.ResourceGroup, assocSubField: rid.SubscriptionID,
		"subscriptionDisplayName": msiSubDisplayName,
	}
}

var errAssocFilter = netErr(http.StatusBadRequest, msiCodeBad, "The $filter expression is not supported.")

func assocFields() []string {
	return []string{netFieldID, fieldName, fieldType, "resourceGroup", assocSubField, "subscriptionDisplayName"}
}

// filterAssociated keeps rows matching "and"-joined contains(f,'v'), startswith(f,'v') and f eq 'v' clauses.
func filterAssociated(rows []map[string]any, expr string) ([]map[string]any, error) {
	if strings.TrimSpace(expr) == "" {
		return rows, nil
	}

	var preds []func(map[string]any) bool

	for _, clause := range splitFold(expr, msiFilterAndSep) {
		p, err := assocPredicate(strings.TrimSpace(clause))
		if err != nil {
			return nil, err
		}

		preds = append(preds, p)
	}

	out := rows[:0]

	for _, r := range rows {
		if !slices.ContainsFunc(preds, func(p func(map[string]any) bool) bool { return !p(r) }) {
			out = append(out, r)
		}
	}

	return out, nil
}

func splitFold(s, sep string) []string {
	var parts []string

	for {
		i := strings.Index(strings.ToLower(s), sep)
		if i < 0 {
			return append(parts, s)
		}

		parts = append(parts, s[:i])
		s = s[i+len(sep):]
	}
}

func assocPredicate(clause string) (func(map[string]any) bool, error) {
	lower := strings.ToLower(clause)

	for _, fn := range []string{assocContains, assocStartsWith} {
		if inner, ok := strings.CutPrefix(lower, fn+"("); ok && strings.HasSuffix(inner, ")") {
			field, value, found := strings.Cut(strings.TrimSuffix(clause[len(fn)+1:], ")"), ",")
			if found {
				return assocMatch(fn, strings.TrimSpace(field), strings.TrimSpace(value))
			}
		}
	}

	field, rest, ok := strings.Cut(clause, " ")
	if op, value, found := strings.Cut(strings.TrimSpace(rest), " "); ok && found && strings.EqualFold(op, assocEq) {
		return assocMatch(assocEq, field, strings.TrimSpace(value))
	}

	return nil, errAssocFilter
}

func assocMatch(op, field, quoted string) (func(map[string]any) bool, error) {
	field = canonicalAssocField(field)

	val, ok := strings.CutPrefix(quoted, "'")
	if field == "" || !ok || !strings.HasSuffix(val, "'") {
		return nil, errAssocFilter
	}

	val = strings.ToLower(strings.ReplaceAll(strings.TrimSuffix(val, "'"), "''", "'"))

	return func(r map[string]any) bool {
		got := strings.ToLower(strOf(r, field))

		switch op {
		case assocContains:
			return strings.Contains(got, val)
		case assocStartsWith:
			return strings.HasPrefix(got, val)
		}

		return got == val
	}, nil
}

func canonicalAssocField(f string) string {
	for _, c := range assocFields() {
		if strings.EqualFold(c, f) {
			return c
		}
	}

	return ""
}

func orderAssociated(rows []map[string]any, expr string) error {
	if strings.TrimSpace(expr) == "" {
		expr = fieldName
	}

	field, dir, hasDir := strings.Cut(strings.TrimSpace(expr), " ")
	field = canonicalAssocField(field)
	dir = strings.ToLower(strings.TrimSpace(dir))

	if field == "" || hasDir && dir != "asc" && dir != "desc" {
		return errAssocFilter
	}

	desc := dir == "desc"

	slices.SortStableFunc(rows, func(a, b map[string]any) int {
		c := strings.Compare(strings.ToLower(strOf(a, field)), strings.ToLower(strOf(b, field)))
		if c == 0 {
			c = strings.Compare(strOf(a, netFieldID), strOf(b, netFieldID))
		}

		if desc {
			return -c
		}

		return c
	})

	return nil
}

// pageAssociated applies $skip/$skiptoken and $top; the result carries totalCount and nextLink when more remain.
func (h *Handler) pageAssociated(c *echo.Context, rows []map[string]any) map[string]any {
	total := len(rows)

	start, _ := strconv.Atoi(c.QueryParam("$skiptoken"))
	if start <= 0 {
		start, _ = strconv.Atoi(c.QueryParam("$skip"))
	}

	start = min(max(start, 0), total)
	end := total

	top, err := strconv.Atoi(c.QueryParam("$top"))
	if err == nil && top > 0 {
		end = min(start+top, total)
	}

	out := map[string]any{"totalCount": total, fieldValue: rows[start:end]}

	if end < total {
		q := url.Values{}
		q.Set(nsAPIVersionKey, c.QueryParam(nsAPIVersionKey))
		q.Set("$skiptoken", strconv.Itoa(end))

		if top > 0 {
			q.Set("$top", strconv.Itoa(top))
		}

		out["nextLink"] = h.baseURLFor(c.Request()) + c.Request().URL.Path + "?" + q.Encode()
	}

	return out
}
