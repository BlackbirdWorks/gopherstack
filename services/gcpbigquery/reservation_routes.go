package gcpbigquery

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	minResSegs  = 4
	resKindIdx  = 4
	resIDIdx    = 5
	resSubIdx   = 6
	resSubIDIdx = 7
)

// reservationRoute maps /v1/projects/{p}/locations/{l}/... to an action; act is "<resource>.<method>".
func reservationRoute(method string, segs []string) route {
	rt := route{op: unknownOperation}

	if len(segs) < minResSegs || segs[0] != segProjects || strings.Contains(segs[1], ":") {
		return rt
	}

	rt.project = segs[1]

	loc, verb, _ := strings.Cut(segs[3], ":")
	if segs[2] != segLocations {
		return rt
	}

	rt.loc = loc

	if len(segs) == minResSegs {
		switch verb {
		case "searchAssignments", "searchAllAssignments":
			rt.act, rt.op = "search", opReservation
		}

		return rt
	}

	kind, kverb, _ := strings.Cut(segs[resKindIdx], ":")
	id1, v1, _ := strings.Cut(optSeg(segs, resIDIdx), ":")
	verb = firstNonEmpty(kverb, v1)
	sub, v2, _ := strings.Cut(optSeg(segs, resSubIdx), ":")
	id2, v3, _ := strings.Cut(optSeg(segs, resSubIDIdx), ":")
	verb = firstNonEmpty(verb, v2, v3)

	rt.id1, rt.id2 = id1, id2
	rt.act = reservationAction(method, kind, sub, verb, id1 != "", id2 != "")

	if rt.act != "" {
		rt.op = opReservation
	}

	return rt
}

func optSeg(segs []string, i int) string {
	if i < len(segs) {
		return segs[i]
	}

	return ""
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}

	return ""
}

func reservationAction(method, kind, sub, verb string, hasID, hasSubID bool) string {
	listCreate := map[string]string{http.MethodGet: "list", http.MethodPost: "create"}
	item := map[string]string{http.MethodGet: "get", http.MethodPatch: "patch", http.MethodDelete: "delete"}

	if kind == segReservations && sub == segAssignments && hasID {
		return assignmentAction(method, verb, hasSubID)
	}

	if kind == segBIReservation && !hasID && sub == "" {
		return map[string]string{http.MethodGet: "bi.get", http.MethodPatch: "bi.patch"}[method]
	}

	if a := commitmentVerb(kind, method, verb, hasID); a != "" {
		return a
	}

	res := map[string]string{segReservations: "reservation", segCommitments: "commitment"}[kind]
	if res == "" || sub != "" || verb != "" {
		return ""
	}

	m := listCreate
	if hasID {
		m = item
	}

	if a, ok := m[method]; ok {
		return res + "." + a
	}

	return ""
}

func assignmentAction(method, verb string, hasSubID bool) string {
	switch {
	case !hasSubID && method == http.MethodPost:
		return "assignment.create"
	case !hasSubID && method == http.MethodGet:
		return "assignment.list"
	case hasSubID && verb == "move" && method == http.MethodPost:
		return "assignment.move"
	case hasSubID && verb == "" && method == http.MethodDelete:
		return "assignment.delete"
	case hasSubID && verb == "" && method == http.MethodPatch:
		return "assignment.patch"
	}

	return ""
}

func (h *Handler) reservationOp(c *echo.Context, rt route) error {
	var body map[string]any

	if m := c.Request().Method; m == http.MethodPost || m == http.MethodPatch {
		var err error
		if body, err = decodeBody(c); err != nil {
			return err
		}
	}

	out, err := h.reservationCall(c, rt, body)
	if err != nil {
		return err
	}

	if out == nil {
		return c.JSON(http.StatusOK, map[string]any{})
	}

	return c.JSON(http.StatusOK, out)
}

func listBody(key string, data []map[string]any, next string) map[string]any {
	out := map[string]any{}
	if len(data) > 0 {
		out[key] = data
	}

	if next != "" {
		out["nextPageToken"] = next
	}

	return out
}

func resListOptions(c *echo.Context) ListOptions {
	return ListOptions{PageToken: c.QueryParam("pageToken"), Max: queryInt(c, "pageSize")}
}

func (h *Handler) reservationCall(c *echo.Context, rt route, body map[string]any) (any, error) {
	b, o, mask := h.Backend, resListOptions(c), c.QueryParam("updateMask")
	p, l := rt.project, rt.loc

	switch rt.act {
	case "reservation.create":
		return b.CreateReservation(p, l, c.QueryParam("reservationId"), body)
	case "reservation.get":
		return b.GetReservation(p, l, rt.id1)
	case "reservation.list":
		pg := b.ListReservations(p, l, o)

		return listBody("reservations", pg.Data, pg.Next), nil
	case "reservation.patch":
		return b.UpdateReservation(p, l, rt.id1, mask, body)
	case "reservation.delete":
		return nil, b.DeleteReservation(p, l, rt.id1)
	case "commitment.create":
		return b.CreateCapacityCommitment(p, l, c.QueryParam("capacityCommitmentId"), body)
	case "commitment.get":
		return b.GetCapacityCommitment(p, l, rt.id1)
	case "commitment.list":
		pg := b.ListCapacityCommitments(p, l, o)

		return listBody("capacityCommitments", pg.Data, pg.Next), nil
	case "commitment.patch":
		return b.UpdateCapacityCommitment(p, l, rt.id1, mask, body)
	case "commitment.delete":
		return nil, b.DeleteCapacityCommitment(p, l, rt.id1)
	case "commitment.merge":
		return b.MergeCapacityCommitments(p, l, body)
	case "commitment.split":
		return b.SplitCapacityCommitment(p, l, rt.id1, body)
	}

	return h.assignmentCall(c, rt, body, o, mask)
}

func (h *Handler) assignmentCall(
	c *echo.Context, rt route, body map[string]any, o ListOptions, mask string,
) (any, error) {
	b, p, l := h.Backend, rt.project, rt.loc

	switch rt.act {
	case "assignment.create":
		return b.CreateAssignment(p, l, rt.id1, c.QueryParam("assignmentId"), body)
	case "assignment.list":
		pg, err := b.ListAssignments(p, l, rt.id1, o)
		if err != nil {
			return nil, err
		}

		return listBody("assignments", pg.Data, pg.Next), nil
	case "assignment.delete":
		return nil, b.DeleteAssignment(p, l, rt.id1, rt.id2)
	case "assignment.patch":
		return b.UpdateAssignment(p, l, rt.id1, rt.id2, mask, body)
	case "assignment.move":
		return b.MoveAssignment(p, l, rt.id1, rt.id2, body)
	case "search":
		pg, err := b.SearchAssignments(p, l, c.QueryParam("query"), o)
		if err != nil {
			return nil, err
		}

		return listBody("assignments", pg.Data, pg.Next), nil
	case "bi.get":
		return b.GetBiReservation(p, l), nil
	case "bi.patch":
		return b.UpdateBiReservation(p, l, mask, body)
	}

	return nil, errNotFound("Not found: %s", rt.act)
}

func commitmentVerb(kind, method, verb string, hasID bool) string {
	if kind == segCommitments && method == http.MethodPost && verb != "" && hasID == (verb == "split") {
		return "commitment." + verb
	}

	return ""
}
