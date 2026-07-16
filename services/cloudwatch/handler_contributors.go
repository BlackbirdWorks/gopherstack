package cloudwatch

import (
	"encoding/xml"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

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

	type contributorXML struct {
		Keys []string `xml:"Keys>member"`
		Sum  float64  `xml:"Sum"`
	}
	members := make([]contributorXML, 0, len(p.Data))
	for _, contrib := range p.Data {
		members = append(members, contributorXML(contrib))
	}

	type descResult struct {
		NextToken    string           `xml:"NextToken,omitempty"`
		Contributors []contributorXML `xml:"Contributors>member"`
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
