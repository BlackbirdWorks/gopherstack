package ses

import (
	"encoding/xml"
	"net/url"
	"time"
)

func (h *Handler) handleGetAccountSendingEnabled(reqID string) any {
	return &getAccountSendingEnabledResponse{
		Xmlns:     sesXMLNS,
		Result:    getAccountSendingEnabledResult{Enabled: h.Backend.GetAccountSendingEnabled()},
		RequestID: reqID,
	}
}

func (h *Handler) handleGetSendQuota(reqID string) any {
	q := h.Backend.GetSendQuota()

	return &getSendQuotaResponse{
		Xmlns:     sesXMLNS,
		Result:    getSendQuotaResult(q),
		RequestID: reqID,
	}
}

func (h *Handler) handleGetSendStatistics(reqID string) any {
	points := h.Backend.GetSendStatistics()

	members := make([]xmlSendDataPoint, 0, len(points))

	for _, p := range points {
		members = append(members, xmlSendDataPoint{
			Timestamp:        p.Timestamp.UTC().Format(time.RFC3339),
			DeliveryAttempts: p.DeliveryAttempts,
			Bounces:          p.Bounces,
			Complaints:       p.Complaints,
			Rejects:          p.Rejects,
		})
	}

	return &getSendStatisticsResponse{
		Xmlns: sesXMLNS,
		Result: getSendStatisticsResult{
			SendDataPoints: xmlSendDataPointList{Members: members},
		},
		RequestID: reqID,
	}
}

type getAccountSendingEnabledResult struct {
	Enabled bool `xml:"Enabled"`
}

type getAccountSendingEnabledResponse struct {
	XMLName   xml.Name                       `xml:"GetAccountSendingEnabledResponse"`
	Xmlns     string                         `xml:"xmlns,attr"`
	RequestID string                         `xml:"ResponseMetadata>RequestId"`
	Result    getAccountSendingEnabledResult `xml:"GetAccountSendingEnabledResult"`
}

type getSendQuotaResult struct {
	Max24HourSend   float64 `xml:"Max24HourSend"`
	MaxSendRate     float64 `xml:"MaxSendRate"`
	SentLast24Hours float64 `xml:"SentLast24Hours"`
}

type getSendQuotaResponse struct {
	XMLName   xml.Name           `xml:"GetSendQuotaResponse"`
	Xmlns     string             `xml:"xmlns,attr"`
	RequestID string             `xml:"ResponseMetadata>RequestId"`
	Result    getSendQuotaResult `xml:"GetSendQuotaResult"`
}

type xmlSendDataPoint struct {
	Timestamp        string  `xml:"Timestamp"`
	DeliveryAttempts float64 `xml:"DeliveryAttempts"`
	Bounces          float64 `xml:"Bounces"`
	Complaints       float64 `xml:"Complaints"`
	Rejects          float64 `xml:"Rejects"`
}

type xmlSendDataPointList struct {
	Members []xmlSendDataPoint `xml:"member"`
}

type getSendStatisticsResult struct {
	SendDataPoints xmlSendDataPointList `xml:"SendDataPoints"`
}

type getSendStatisticsResponse struct {
	XMLName   xml.Name                `xml:"GetSendStatisticsResponse"`
	Xmlns     string                  `xml:"xmlns,attr"`
	RequestID string                  `xml:"ResponseMetadata>RequestId"`
	Result    getSendStatisticsResult `xml:"GetSendStatisticsResult"`
}

func (h *Handler) handleUpdateAccountSendingEnabled(vals url.Values, reqID string) any {
	enabled := vals.Get("Enabled") == boolTrue
	h.Backend.UpdateAccountSendingEnabled(enabled)

	return newEmptyResponse("UpdateAccountSendingEnabled", reqID)
}
