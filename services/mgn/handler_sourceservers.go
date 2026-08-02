package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleDescribeSourceServers(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req describeSourceServersRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	f := DescribeSourceServersFilters{}
	if req.Filters != nil {
		f = DescribeSourceServersFilters{
			ApplicationIDs:   req.Filters.ApplicationIDs,
			IsArchived:       req.Filters.IsArchived,
			LifeCycleStates:  req.Filters.LifeCycleStates,
			ReplicationTypes: req.Filters.ReplicationTypes,
			SourceServerIDs:  req.Filters.SourceServerIDs,
		}
	}

	pg, err := h.Backend.DescribeSourceServers(f, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]sourceServerWire, len(pg.Data))
	for i, s := range pg.Data {
		items[i] = toSourceServerWire(s)
	}

	return marshalResponse(describeSourceServersResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleUpdateSourceServer(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req updateSourceServerRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	var action *SourceServerConnectorAction
	if req.ConnectorAction != nil {
		action = &SourceServerConnectorAction{
			ConnectorArn:         req.ConnectorAction.ConnectorArn,
			CredentialsSecretArn: req.ConnectorAction.CredentialsSecretArn,
		}
	}

	s, err := h.Backend.UpdateSourceServer(req.SourceServerID, action)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handleUpdateSourceServerReplicationType(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req updateSourceServerReplicationTypeRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.UpdateSourceServerReplicationType(req.SourceServerID, req.ReplicationType)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handleDeleteSourceServer(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req sourceServerIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteSourceServer(req.SourceServerID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleChangeServerLifeCycleState(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req changeServerLifeCycleStateRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if req.LifeCycle == nil {
		return nil, validationError("lifeCycle is required")
	}

	s, err := h.Backend.ChangeServerLifeCycleState(req.SourceServerID, req.LifeCycle.State)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handleDisconnectFromService(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req sourceServerIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.DisconnectFromService(req.SourceServerID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handleFinalizeCutover(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req sourceServerIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.FinalizeCutover(req.SourceServerID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handleMarkAsArchived(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req sourceServerIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.MarkAsArchived(req.SourceServerID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handleStartReplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req sourceServerIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.StartReplication(req.SourceServerID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleStopReplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req sourceServerIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.StopReplication(req.SourceServerID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handlePauseReplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req sourceServerIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.PauseReplication(req.SourceServerID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handleResumeReplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req sourceServerIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.ResumeReplication(req.SourceServerID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handleRetryDataReplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req sourceServerIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.RetryDataReplication(req.SourceServerID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerWire(s))
}

func (h *Handler) handleStartTest(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startBatchRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	j, err := h.Backend.StartTest(req.SourceServerIDs, req.Tags)
	if err != nil {
		return nil, err
	}

	jw := toJobWire(j)

	return marshalResponse(jobEnvelope{Job: &jw})
}

func (h *Handler) handleStartCutover(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startBatchRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	j, err := h.Backend.StartCutover(req.SourceServerIDs, req.Tags)
	if err != nil {
		return nil, err
	}

	jw := toJobWire(j)

	return marshalResponse(jobEnvelope{Job: &jw})
}

func (h *Handler) handleTerminateTargetInstances(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startBatchRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	j, err := h.Backend.TerminateTargetInstances(req.SourceServerIDs, req.Tags)
	if err != nil {
		return nil, err
	}

	jw := toJobWire(j)

	return marshalResponse(jobEnvelope{Job: &jw})
}
