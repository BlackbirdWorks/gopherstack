package lightsail

import "context"

// alarmContactOps returns the dispatch table for family W (8 ops).
func (h *Handler) alarmContactOps() map[string]opFunc {
	return map[string]opFunc{
		"PutAlarm":                      h.handlePutAlarm,
		"DeleteAlarm":                   h.handleDeleteAlarm,
		"GetAlarms":                     h.handleGetAlarms,
		"TestAlarm":                     h.handleTestAlarm,
		"CreateContactMethod":           h.handleCreateContactMethod,
		"DeleteContactMethod":           h.handleDeleteContactMethod,
		"GetContactMethods":             h.handleGetContactMethods,
		"SendContactMethodVerification": h.handleSendContactMethodVerification,
	}
}

type monitoredResourceInfoWire struct {
	Arn          string `json:"arn,omitempty"`
	Name         string `json:"name,omitempty"`
	ResourceType string `json:"resourceType,omitempty"`
}

type alarmWire struct {
	CreatedAt             *float64                   `json:"createdAt,omitempty"`
	MonitoredResourceInfo *monitoredResourceInfoWire `json:"monitoredResourceInfo,omitempty"`
	Location              *resourceLocationWire      `json:"location,omitempty"`
	Unit                  string                     `json:"unit,omitempty"`
	SupportCode           string                     `json:"supportCode,omitempty"`
	Arn                   string                     `json:"arn,omitempty"`
	MetricName            string                     `json:"metricName,omitempty"`
	ComparisonOperator    string                     `json:"comparisonOperator,omitempty"`
	Name                  string                     `json:"name,omitempty"`
	TreatMissingData      string                     `json:"treatMissingData,omitempty"`
	ResourceType          string                     `json:"resourceType,omitempty"`
	State                 string                     `json:"state,omitempty"`
	Statistic             string                     `json:"statistic,omitempty"`
	ContactProtocols      []string                   `json:"contactProtocols,omitempty"`
	NotificationTriggers  []string                   `json:"notificationTriggers,omitempty"`
	Threshold             float64                    `json:"threshold,omitempty"`
	EvaluationPeriods     int32                      `json:"evaluationPeriods,omitempty"`
	Period                int32                      `json:"period,omitempty"`
	DatapointsToAlarm     int32                      `json:"datapointsToAlarm,omitempty"`
	NotificationEnabled   bool                       `json:"notificationEnabled,omitempty"`
}

func alarmToWire(a *Alarm) alarmWire {
	return alarmWire{
		Arn: a.Arn, ComparisonOperator: a.ComparisonOperator, ContactProtocols: a.ContactProtocols,
		CreatedAt: epochPtr(a.CreatedAt), DatapointsToAlarm: a.DatapointsToAlarm,
		EvaluationPeriods: a.EvaluationPeriods,
		Location:          locationToWire(a.Location), MetricName: a.MetricName,
		MonitoredResourceInfo: &monitoredResourceInfoWire{Arn: a.MonitoredResourceArn, Name: a.MonitoredResourceName},
		Name:                  a.Name, NotificationEnabled: a.NotificationEnabled,
		NotificationTriggers: a.NotificationTriggers,
		ResourceType:         ResourceTypeAlarm, State: a.State, Statistic: a.Statistic, SupportCode: a.SupportCode,
		Threshold: a.Threshold, TreatMissingData: a.TreatMissingData, Unit: a.Unit,
	}
}

type putAlarmRequest struct {
	AlarmName             string    `json:"alarmName"`
	ComparisonOperator    string    `json:"comparisonOperator"`
	MetricName            string    `json:"metricName"`
	MonitoredResourceName string    `json:"monitoredResourceName"`
	TreatMissingData      string    `json:"treatMissingData,omitempty"`
	ContactProtocols      []string  `json:"contactProtocols,omitempty"`
	NotificationTriggers  []string  `json:"notificationTriggers,omitempty"`
	Tags                  []tagWire `json:"tags,omitempty"`
	Threshold             float64   `json:"threshold"`
	DatapointsToAlarm     int32     `json:"datapointsToAlarm,omitempty"`
	EvaluationPeriods     int32     `json:"evaluationPeriods"`
	NotificationEnabled   bool      `json:"notificationEnabled,omitempty"`
}

func (h *Handler) handlePutAlarm(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[putAlarmRequest](body)
	if err != nil {
		return nil, err
	}

	ops, putErr := h.Backend.PutAlarm(
		req.AlarmName, req.ComparisonOperator, req.MetricName, req.MonitoredResourceName, "Average", "None",
		req.TreatMissingData, req.Threshold, req.EvaluationPeriods, req.DatapointsToAlarm,
		req.ContactProtocols, req.NotificationTriggers, req.NotificationEnabled, tagsFromWire(req.Tags),
	)
	if putErr != nil {
		return nil, putErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type alarmNameRequest struct {
	AlarmName string `json:"alarmName"`
}

func (h *Handler) handleDeleteAlarm(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[alarmNameRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteAlarm(req.AlarmName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type getAlarmsRequest struct {
	AlarmName             string `json:"alarmName,omitempty"`
	MonitoredResourceName string `json:"monitoredResourceName,omitempty"`
	PageToken             string `json:"pageToken,omitempty"`
}

type alarmsListResponse struct {
	NextPageToken string      `json:"nextPageToken,omitempty"`
	Alarms        []alarmWire `json:"alarms,omitempty"`
}

func (h *Handler) handleGetAlarms(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getAlarmsRequest](body)
	if err != nil {
		return nil, err
	}

	alarms, getErr := h.Backend.GetAlarms(req.AlarmName, req.MonitoredResourceName)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]alarmWire, len(alarms))
	for i, a := range alarms {
		out[i] = alarmToWire(a)
	}

	return marshalResponse(alarmsListResponse{Alarms: out})
}

type testAlarmRequest struct {
	AlarmName string `json:"alarmName"`
	State     string `json:"state"`
}

func (h *Handler) handleTestAlarm(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[testAlarmRequest](body)
	if err != nil {
		return nil, err
	}

	ops, testErr := h.Backend.TestAlarm(req.AlarmName, req.State)
	if testErr != nil {
		return nil, testErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type contactMethodWire struct {
	Arn             string                `json:"arn,omitempty"`
	ContactEndpoint string                `json:"contactEndpoint,omitempty"`
	CreatedAt       *float64              `json:"createdAt,omitempty"`
	Location        *resourceLocationWire `json:"location,omitempty"`
	Name            string                `json:"name,omitempty"`
	Protocol        string                `json:"protocol,omitempty"`
	ResourceType    string                `json:"resourceType,omitempty"`
	Status          string                `json:"status,omitempty"`
	SupportCode     string                `json:"supportCode,omitempty"`
	Tags            []tagWire             `json:"tags,omitempty"`
}

func contactMethodToWire(cm *ContactMethod) contactMethodWire {
	return contactMethodWire{
		Arn:             cm.Arn,
		ContactEndpoint: cm.ContactEndpoint,
		CreatedAt:       epochPtr(cm.CreatedAt),
		Location:        locationToWire(cm.Location),
		Name:            cm.Protocol,
		Protocol:        cm.Protocol,
		ResourceType:    ResourceTypeContactMethod,
		Status:          cm.Status,
		SupportCode:     cm.SupportCode,
		Tags:            mapFromTags(cm.Tags),
	}
}

type createContactMethodRequest struct {
	ContactEndpoint string    `json:"contactEndpoint"`
	Protocol        string    `json:"protocol"`
	Tags            []tagWire `json:"tags,omitempty"`
}

func (h *Handler) handleCreateContactMethod(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createContactMethodRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateContactMethod(req.Protocol, req.ContactEndpoint, tagsFromWire(req.Tags))
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type contactMethodProtocolRequest struct {
	Protocol string `json:"protocol"`
}

func (h *Handler) handleDeleteContactMethod(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[contactMethodProtocolRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteContactMethod(req.Protocol)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type getContactMethodsRequest struct {
	Protocols []string `json:"protocols,omitempty"`
}

type contactMethodsListResponse struct {
	ContactMethods []contactMethodWire `json:"contactMethods,omitempty"`
}

func (h *Handler) handleGetContactMethods(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getContactMethodsRequest](body)
	if err != nil {
		return nil, err
	}

	cms, getErr := h.Backend.GetContactMethods(req.Protocols)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]contactMethodWire, len(cms))
	for i, cm := range cms {
		out[i] = contactMethodToWire(cm)
	}

	return marshalResponse(contactMethodsListResponse{ContactMethods: out})
}

func (h *Handler) handleSendContactMethodVerification(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[struct {
		Protocol string `json:"protocol"`
	}](body)
	if err != nil {
		return nil, err
	}

	ops, sendErr := h.Backend.SendContactMethodVerification(req.Protocol)
	if sendErr != nil {
		return nil, sendErr
	}

	return marshalResponse(opsEnvelope(ops))
}
