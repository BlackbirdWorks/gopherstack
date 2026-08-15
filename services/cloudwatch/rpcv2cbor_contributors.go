package cloudwatch

import (
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

func (h *Handler) cborDescribeAlarmContributors(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	if alarmName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}
	nextToken := cborStr(input, "NextToken")

	p, err := h.Backend.DescribeAlarmContributors(alarmName, nextToken)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	contributors := make(cbor.List, 0, len(p.Data))
	for _, contrib := range p.Data {
		attrs := make(cbor.Map, len(contrib.ContributorAttributes))
		for k, v := range contrib.ContributorAttributes {
			attrs[k] = cbor.String(v)
		}
		m := cbor.Map{
			"ContributorId":         cbor.String(contrib.ContributorID),
			"ContributorAttributes": attrs,
			"StateReason":           cbor.String(contrib.StateReason),
		}
		if !contrib.StateTransitionedTimestamp.IsZero() {
			m["StateTransitionedTimestamp"] = cborFromTime(contrib.StateTransitionedTimestamp)
		}
		contributors = append(contributors, m)
	}

	out := cbor.Map{
		"AlarmContributors": contributors,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}
