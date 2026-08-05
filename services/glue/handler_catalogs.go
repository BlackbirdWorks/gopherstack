package glue

import (
	"context"
)

// createCatalogInput holds input for CreateCatalog.
type createCatalogInput struct {
	CatalogInput struct {
		Parameters  map[string]string `json:"Parameters,omitzero"`
		Name        string            `json:"Name,omitempty"`
		Description string            `json:"Description,omitempty"`
	} `json:"CatalogInput"`
	CatalogID string `json:"CatalogId"`
}

func (h *Handler) handleCreateCatalog(
	_ context.Context,
	in *createCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreateCatalog(
		in.CatalogID,
		in.CatalogInput.Name,
		in.CatalogInput.Description,
		in.CatalogInput.Parameters,
	)
}

// deleteCatalogInput holds input for DeleteCatalog.
type deleteCatalogInput struct {
	CatalogID string `json:"CatalogId"`
}

func (h *Handler) handleDeleteCatalog(
	_ context.Context,
	in *deleteCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteCatalog(in.CatalogID)
}

// getCatalogInput holds input for GetCatalog.
type getCatalogInput struct {
	CatalogID string `json:"CatalogId"`
}

// getCatalogOutput holds the result for GetCatalog.
type getCatalogOutput struct {
	Catalog *CatalogEntry `json:"Catalog"`
}

func (h *Handler) handleGetCatalog(
	_ context.Context,
	in *getCatalogInput,
) (*getCatalogOutput, error) {
	c, err := h.Backend.GetCatalog(in.CatalogID)
	if err != nil {
		return nil, err
	}

	return &getCatalogOutput{Catalog: c}, nil
}

// getCatalogImportStatusInput holds input for GetCatalogImportStatus.
type getCatalogImportStatusInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
}

// getCatalogImportStatusOutput holds the result for GetCatalogImportStatus.
type getCatalogImportStatusOutput struct {
	ImportStatus *CatalogImportStatus `json:"ImportStatus"`
}

func (h *Handler) handleGetCatalogImportStatus(
	_ context.Context,
	in *getCatalogImportStatusInput,
) (*getCatalogImportStatusOutput, error) {
	status := h.Backend.GetCatalogImportStatus(in.CatalogID)

	return &getCatalogImportStatusOutput{ImportStatus: status}, nil
}

// getCatalogsInput holds input for GetCatalogs.
type getCatalogsInput struct{}

// getCatalogsOutput holds the result for GetCatalogs.
type getCatalogsOutput struct {
	CatalogList []*CatalogEntry `json:"CatalogList"`
}

func (h *Handler) handleGetCatalogs(
	_ context.Context,
	_ *getCatalogsInput,
) (*getCatalogsOutput, error) {
	catalogs := h.Backend.GetCatalogs()
	if catalogs == nil {
		catalogs = []*CatalogEntry{}
	}

	return &getCatalogsOutput{CatalogList: catalogs}, nil
}

// getDataCatalogEncryptionSettingsInput holds input for GetDataCatalogEncryptionSettings.
type getDataCatalogEncryptionSettingsInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
}

// getDataCatalogEncryptionSettingsOutput holds the result for GetDataCatalogEncryptionSettings.
type getDataCatalogEncryptionSettingsOutput struct {
	DataCatalogEncryptionSettings *DataCatalogEncryptionSettings `json:"DataCatalogEncryptionSettings"`
}

func (h *Handler) handleGetDataCatalogEncryptionSettings(
	_ context.Context,
	in *getDataCatalogEncryptionSettingsInput,
) (*getDataCatalogEncryptionSettingsOutput, error) {
	s, err := h.Backend.GetDataCatalogEncryptionSettings(in.CatalogID)
	if err != nil {
		return nil, err
	}

	return &getDataCatalogEncryptionSettingsOutput{DataCatalogEncryptionSettings: s}, nil
}

// getDataCatalogExportConfigurationInput holds input for
// GetDataCatalogExportConfiguration -- the real op takes no input fields at
// all (see api_op_GetDataCatalogExportConfiguration.go).
type getDataCatalogExportConfigurationInput struct{}

func (h *Handler) handleGetDataCatalogExportConfiguration(
	_ context.Context,
	_ *getDataCatalogExportConfigurationInput,
) (*DataCatalogExportConfiguration, error) {
	return h.Backend.GetDataCatalogExportConfiguration()
}

// putDataCatalogExportConfigurationInput holds input for
// PutDataCatalogExportConfiguration. ClientToken is an idempotency token the
// SDK client auto-fills; accepted on the wire but not needed for an
// in-memory backend with no retry-dedup window to honor.
type putDataCatalogExportConfigurationInput struct {
	EncryptionConfiguration *ExportEncryptionConfiguration `json:"EncryptionConfiguration,omitempty"`
	ExportSetting           string                         `json:"ExportSetting"`
	ClientToken             string                         `json:"ClientToken,omitempty"`
}

func (h *Handler) handlePutDataCatalogExportConfiguration(
	_ context.Context,
	in *putDataCatalogExportConfigurationInput,
) (*DataCatalogExportConfiguration, error) {
	return h.Backend.PutDataCatalogExportConfiguration(DataCatalogExportConfiguration{
		ExportSetting:           in.ExportSetting,
		EncryptionConfiguration: in.EncryptionConfiguration,
	})
}

// importCatalogToGlueInput holds input for ImportCatalogToGlue.
type importCatalogToGlueInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
}

func (h *Handler) handleImportCatalogToGlue(
	_ context.Context,
	in *importCatalogToGlueInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.ImportCatalogToGlue(in.CatalogID)
}

// putDataCatalogEncryptionSettingsInput holds input for PutDataCatalogEncryptionSettings.
type putDataCatalogEncryptionSettingsInput struct {
	DataCatalogEncryptionSettings DataCatalogEncryptionSettings `json:"DataCatalogEncryptionSettings"`
	CatalogID                     string                        `json:"CatalogId,omitempty"`
}

func (h *Handler) handlePutDataCatalogEncryptionSettings(
	_ context.Context,
	in *putDataCatalogEncryptionSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutDataCatalogEncryptionSettings(
		in.CatalogID,
		in.DataCatalogEncryptionSettings,
	)
}

// updateCatalogInput holds input for UpdateCatalog.
type updateCatalogInput struct {
	CatalogInput struct {
		Parameters  map[string]string `json:"Parameters,omitzero"`
		Description string            `json:"Description,omitempty"`
	} `json:"CatalogInput"`
	CatalogID string `json:"CatalogId"`
}

func (h *Handler) handleUpdateCatalog(
	_ context.Context,
	in *updateCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateCatalog(
		in.CatalogID,
		in.CatalogInput.Description,
		in.CatalogInput.Parameters,
	)
}
