package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateWave(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createWaveRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	wave, err := h.Backend.CreateWave(req.Name, req.Description, req.Tags)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toWaveWire(wave))
}

func (h *Handler) handleUpdateWave(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req updateWaveRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	wave, err := h.Backend.UpdateWave(req.WaveID, req.Name, req.Description)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toWaveWire(wave))
}

func (h *Handler) handleDeleteWave(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req waveIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteWave(req.WaveID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleListWaves(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listWavesRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	f := ListWavesFilters{}
	if req.Filters != nil {
		f = ListWavesFilters{WaveIDs: req.Filters.WaveIDs, IsArchived: req.Filters.IsArchived}
	}

	pg, err := h.Backend.ListWaves(f, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]waveWire, len(pg.Data))
	for i, w := range pg.Data {
		items[i] = toWaveWire(w)
	}

	return marshalResponse(listWavesResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleArchiveWave(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req waveIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	wave, err := h.Backend.ArchiveWave(req.WaveID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toWaveWire(wave))
}

func (h *Handler) handleUnarchiveWave(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req waveIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	wave, err := h.Backend.UnarchiveWave(req.WaveID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toWaveWire(wave))
}

func (h *Handler) handleAssociateApplications(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req associateApplicationsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.AssociateApplications(req.WaveID, req.ApplicationIDs); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleDisassociateApplications(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req associateApplicationsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DisassociateApplications(req.WaveID, req.ApplicationIDs); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}
