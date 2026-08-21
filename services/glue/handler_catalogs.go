package glue

import (
	"context"
)

// createCatalogInput holds input for CreateCatalog. Name is required and, per
// the real serializer (serializers.go's
// awsAwsjson11_serializeOpDocumentCreateCatalogInput), sits at the top level
// of the request body, a sibling of CatalogInput. The real
// CreateCatalogInput (api_op_CreateCatalog.go) has no CatalogId member at
// all -- unlike Get/Update/Delete/ImportCatalogToGlue, a catalog is created
// and addressed purely by Name, so Name doubles as this backend's storage
// key. A prior version of this struct instead expected a fictional top-level
// CatalogId and read Name from a nonexistent CatalogInput.Name, so a real
// client's Name was silently dropped: every created catalog was stored under
// the empty-string key with an empty Name, and any later GetCatalog(CatalogId:
// <the name the client used>) would 404.
type createCatalogInput struct {
	CatalogInput struct {
		Parameters  map[string]string `json:"Parameters,omitzero"`
		Description string            `json:"Description,omitempty"`
	} `json:"CatalogInput"`
	Name string `json:"Name"`
}

func (h *Handler) handleCreateCatalog(
	_ context.Context,
	in *createCatalogInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreateCatalog(
		in.Name,
		in.Name,
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

// defaultGetCatalogsLimit is used when GetCatalogsInput.MaxResults is unset.
const defaultGetCatalogsLimit = 100

// getCatalogsInput holds input for GetCatalogs.
//
// IncludeRoot, ParentCatalogId and Recursive describe navigating a catalog
// hierarchy (the account's root catalog plus nested federated catalogs).
// CatalogEntry (see catalogs.go) has no ParentCatalogId of its own and this
// backend's b.catalogs table is a flat namespace with no root-catalog
// concept, so there is no honest hierarchy to filter or recurse over; these
// three members are accepted on the wire and otherwise inert. HasDatabases is
// real: Database.CatalogID (databases.go) links a database to its owning
// catalog, so "does this catalog contain any database" is answerable from
// backend state.
type getCatalogsInput struct {
	HasDatabases    *bool  `json:"HasDatabases,omitempty"`
	ParentCatalogID string `json:"ParentCatalogId,omitempty"`
	NextToken       string `json:"NextToken,omitempty"`
	MaxResults      int32  `json:"MaxResults,omitempty"`
	IncludeRoot     bool   `json:"IncludeRoot,omitempty"`
	Recursive       bool   `json:"Recursive,omitempty"`
}

// getCatalogsOutput holds the result for GetCatalogs.
type getCatalogsOutput struct {
	NextToken   string          `json:"NextToken,omitempty"`
	CatalogList []*CatalogEntry `json:"CatalogList"`
}

func (h *Handler) handleGetCatalogs(
	_ context.Context,
	in *getCatalogsInput,
) (*getCatalogsOutput, error) {
	catalogs := h.Backend.GetCatalogs()

	if in.HasDatabases != nil {
		databases := h.Backend.GetDatabases()
		withDatabases := make(map[string]bool, len(catalogs))

		for _, db := range databases {
			withDatabases[db.CatalogID] = true
		}

		filtered := make([]*CatalogEntry, 0, len(catalogs))

		for _, c := range catalogs {
			if withDatabases[c.CatalogID] == *in.HasDatabases {
				filtered = append(filtered, c)
			}
		}

		catalogs = filtered
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetCatalogsLimit
	}

	page, next := paginateSlice(catalogs, in.NextToken, limit)
	if page == nil {
		page = []*CatalogEntry{}
	}

	return &getCatalogsOutput{CatalogList: page, NextToken: next}, nil
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
