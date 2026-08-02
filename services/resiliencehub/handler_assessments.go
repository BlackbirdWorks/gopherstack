package resiliencehub

import (
	"context"
	"net/http"
)

func (h *Handler) handleStartAppAssessment(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startAppAssessmentRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.StartAppAssessment(&req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(assessmentEnvelope{Assessment: toAppAssessmentWire(a)})
}

func (h *Handler) handleDescribeAppAssessment(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req assessmentArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.DescribeAppAssessment(req.AssessmentArn)
	if err != nil {
		return nil, err
	}

	return marshalResponse(assessmentEnvelope{Assessment: toAppAssessmentWire(a)})
}

func (h *Handler) handleDeleteAppAssessment(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req assessmentArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	status, err := h.Backend.DeleteAppAssessment(req.AssessmentArn)
	if err != nil {
		return nil, err
	}

	return marshalResponse(deleteAppAssessmentResponse{AssessmentArn: req.AssessmentArn, AssessmentStatus: status})
}

func (h *Handler) handleListAppAssessments(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	f := listAssessmentsFilter{
		appArn: q.Get("appArn"), assessmentName: q.Get("assessmentName"),
		complianceStatus: q.Get("complianceStatus"), invoker: q.Get("invoker"),
		statuses: q["assessmentStatus"], reverseOrder: q.Get("reverseOrder") == "true",
	}

	p := h.Backend.ListAppAssessments(f, q.Get("nextToken"), queryMaxResults(q))

	resp := listAppAssessmentsResponse{
		AssessmentSummaries: make([]appAssessmentSummaryWire, 0, len(p.Data)),
		NextToken:           p.Next,
	}
	for _, a := range p.Data {
		resp.AssessmentSummaries = append(resp.AssessmentSummaries, toAppAssessmentSummaryWire(a))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleListAppAssessmentComplianceDrifts(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req assessmentArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListAppAssessmentComplianceDrifts(req.AssessmentArn); err != nil {
		return nil, err
	}

	return marshalResponse(listAppAssessmentComplianceDriftsResponse{ComplianceDrifts: []complianceDriftWire{}})
}

func (h *Handler) handleListAppAssessmentResourceDrifts(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req assessmentArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListAppAssessmentResourceDrifts(req.AssessmentArn); err != nil {
		return nil, err
	}

	return marshalResponse(listAppAssessmentResourceDriftsResponse{ResourceDrifts: []resourceDriftWire{}})
}

func (h *Handler) handleListAppComponentCompliances(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req assessmentArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	asmt, components, err := h.Backend.ListAppComponentCompliances(req.AssessmentArn)
	if err != nil {
		return nil, err
	}

	resp := listAppComponentCompliancesResponse{
		ComponentCompliances: make([]appComponentComplianceWire, 0, len(components)),
	}

	for _, c := range components {
		resp.ComponentCompliances = append(resp.ComponentCompliances, appComponentComplianceWire{
			AppComponentName: c.Name,
			Compliance:       toComplianceMapWire(asmt.Compliance),
			Status:           asmt.ComplianceStatus,
			ResiliencyScore:  &resiliencyScoreWire{Score: scorePlaceholder},
		})
	}

	return marshalResponse(resp)
}
