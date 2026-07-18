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
		keys := make(cbor.List, 0, len(contrib.Keys))
		for _, k := range contrib.Keys {
			keys = append(keys, cbor.String(k))
		}
		contributors = append(contributors, cbor.Map{
			"Keys":  keys,
			statSum: cbor.Float64(contrib.Sum),
		})
	}

	out := cbor.Map{
		"Contributors": contributors,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}
