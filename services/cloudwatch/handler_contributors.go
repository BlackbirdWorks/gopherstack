package cloudwatch

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// contributorAttributeXML is one entry of ContributorAttributes, serialized
// using the standard AWS query-protocol map shape (key/value entry pairs) --
// cloudwatch@v1.66.3 has no XML/query serializer for this op to verify
// against directly (see rpcv2cbor_contributors.go's doc comment: this SDK
// version speaks rpc-v2 CBOR exclusively), so this mirrors the same
// key/value convention every other AWS query-protocol map field uses.
type contributorAttributeXML struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

// contributorXML is a single DescribeAlarmContributors entry. Real field
// names verified against cloudwatch@v1.66.3 types/types.go:15
// (types.AlarmContributor): ContributorId, ContributorAttributes,
// StateReason, StateTransitionedTimestamp -- see AlarmContributor's doc
// comment in models.go for why this isn't Keys/Sum.
type contributorXML struct {
	ContributorID              string                    `xml:"ContributorId"`
	StateReason                string                    `xml:"StateReason,omitempty"`
	StateTransitionedTimestamp string                    `xml:"StateTransitionedTimestamp,omitempty"`
	ContributorAttributes      []contributorAttributeXML `xml:"ContributorAttributes>entry"`
}

func (h *Handler) handleDescribeAlarmContributors(form url.Values, c *echo.Context) error {
	alarmName := form.Get("AlarmName")
	if alarmName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}

	nextToken := form.Get("NextToken")

	p, err := h.Backend.DescribeAlarmContributors(alarmName, nextToken)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	members := make([]contributorXML, 0, len(p.Data))
	for _, contrib := range p.Data {
		attrs := make([]contributorAttributeXML, 0, len(contrib.ContributorAttributes))
		for k, v := range contrib.ContributorAttributes {
			attrs = append(attrs, contributorAttributeXML{Key: k, Value: v})
		}
		x := contributorXML{
			ContributorID:         contrib.ContributorID,
			ContributorAttributes: attrs,
			StateReason:           contrib.StateReason,
		}
		if !contrib.StateTransitionedTimestamp.IsZero() {
			x.StateTransitionedTimestamp = contrib.StateTransitionedTimestamp.UTC().Format(time.RFC3339)
		}
		members = append(members, x)
	}

	type descResult struct {
		NextToken    string           `xml:"NextToken,omitempty"`
		Contributors []contributorXML `xml:"AlarmContributors>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeAlarmContributorsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeAlarmContributorsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    descResult{Contributors: members, NextToken: p.Next},
	})
}
