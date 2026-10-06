package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

type sequenceStoreS3AccessConfig struct {
	AccessLogLocation string `json:"accessLogLocation"`
}

func (h *Handler) handleCreateSequenceStore(c *echo.Context) error {
	var req struct {
		Tags                   map[string]string            `json:"tags"`
		SseConfig              map[string]any               `json:"sseConfig"`
		S3AccessConfig         *sequenceStoreS3AccessConfig `json:"s3AccessConfig"`
		Name                   string                       `json:"name"`
		Description            string                       `json:"description"`
		ETagAlgorithmFamily    string                       `json:"eTagAlgorithmFamily"`
		FallbackLocation       string                       `json:"fallbackLocation"`
		ClientToken            string                       `json:"clientToken"`
		PropagatedSetLevelTags []string                     `json:"propagatedSetLevelTags"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	in := CreateSequenceStoreInput{
		Name:                   req.Name,
		Description:            req.Description,
		ETagAlgorithmFamily:    req.ETagAlgorithmFamily,
		FallbackLocation:       req.FallbackLocation,
		SseConfig:              req.SseConfig,
		PropagatedSetLevelTags: req.PropagatedSetLevelTags,
		Tags:                   req.Tags,
	}
	if req.S3AccessConfig != nil {
		in.AccessLogLocation = req.S3AccessConfig.AccessLogLocation
	}

	ss, err := idemCreate(
		h.idem, opCreateSequenceStore, req.ClientToken, idemFingerprint(in),
		func(s *SequenceStore) string { return s.ID }, h.Backend.GetSequenceStore,
		func() (*SequenceStore, error) { return h.Backend.CreateSequenceStore(in) },
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, newSequenceStoreWire(ss, false))
}

func (h *Handler) handleDeleteSequenceStore(c *echo.Context, id string) error {
	if err := h.Backend.DeleteSequenceStore(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": id})
}

func (h *Handler) handleGetSequenceStore(c *echo.Context, id string) error {
	ss, err := h.Backend.GetSequenceStore(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, newSequenceStoreWire(ss, true))
}

func (h *Handler) handleListSequenceStores(c *echo.Context) error {
	var req struct {
		Filter *SequenceStoreFilter `json:"filter"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	maxResults, nextToken := listQueryParams(c)

	stores, next, err := h.Backend.ListSequenceStores(req.Filter, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	items := make([]sequenceStoreWire, 0, len(stores))
	for _, ss := range stores {
		items = append(items, newSequenceStoreWire(ss, true))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"sequenceStores": items,
		keyNextToken:     next,
	})
}

func (h *Handler) handleUpdateSequenceStore(c *echo.Context, id string) error {
	var req struct {
		FallbackLocation       *string                      `json:"fallbackLocation"`
		S3AccessConfig         *sequenceStoreS3AccessConfig `json:"s3AccessConfig"`
		Name                   *string                      `json:"name"`
		Description            *string                      `json:"description"`
		ClientToken            string                       `json:"clientToken"`
		PropagatedSetLevelTags []string                     `json:"propagatedSetLevelTags"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	in := UpdateSequenceStoreInput{
		Name:                   req.Name,
		Description:            req.Description,
		FallbackLocation:       req.FallbackLocation,
		PropagatedSetLevelTags: req.PropagatedSetLevelTags,
	}
	if req.S3AccessConfig != nil {
		in.AccessLogLocation = &req.S3AccessConfig.AccessLogLocation
	}

	ss, err := h.Backend.UpdateSequenceStore(id, in)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, newSequenceStoreWire(ss, true))
}
