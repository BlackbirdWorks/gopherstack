package glue

import (
	"context"
	"fmt"
	"maps"
)

type batchDeleteConnectionInput struct {
	CatalogID          string   `json:"CatalogId,omitempty"`
	ConnectionNameList []string `json:"ConnectionNameList"`
}

type batchDeleteConnectionOutput struct {
	Errors    map[string]ErrorDetail `json:"Errors"`
	Succeeded []string               `json:"Succeeded"`
}

func (h *Handler) handleBatchDeleteConnection(
	_ context.Context,
	in *batchDeleteConnectionInput,
) (*batchDeleteConnectionOutput, error) {
	names := in.ConnectionNameList
	errs := make(map[string]ErrorDetail, len(names))

	if in.CatalogID != "" {
		var scoped []string

		for _, name := range names {
			c, err := h.Backend.GetConnection(name)
			if err == nil && catalogIDMismatch(in.CatalogID, c.CatalogID) {
				errs[name] = ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: "connection not found: " + name,
				}

				continue
			}

			scoped = append(scoped, name)
		}

		names = scoped
	}

	succeeded, batchErrs := h.Backend.BatchDeleteConnection(names)
	maps.Copy(errs, batchErrs)

	return &batchDeleteConnectionOutput{Succeeded: succeeded, Errors: errs}, nil
}

type createConnectionInput struct {
	Tags            map[string]string `json:"Tags,omitempty"`
	CatalogID       string            `json:"CatalogId,omitempty"`
	ConnectionInput connectionInput   `json:"ConnectionInput"`
}

type connectionInput struct {
	ConnectionProperties           map[string]string               `json:"ConnectionProperties,omitempty"`
	PhysicalConnectionRequirements *PhysicalConnectionRequirements `json:"PhysicalConnectionRequirements,omitempty"`
	Name                           string                          `json:"Name"`
	ConnectionType                 string                          `json:"ConnectionType,omitempty"`
	Description                    string                          `json:"Description,omitempty"`
	MatchCriteria                  []string                        `json:"MatchCriteria,omitempty"`
}

type createConnectionOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateConnection(
	_ context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	c, err := h.Backend.CreateConnectionWithOptions(
		in.ConnectionInput.Name,
		in.ConnectionInput.ConnectionType,
		in.ConnectionInput.ConnectionProperties,
		in.Tags,
		ConnectionOptions{
			Description:                    in.ConnectionInput.Description,
			MatchCriteria:                  in.ConnectionInput.MatchCriteria,
			PhysicalConnectionRequirements: in.ConnectionInput.PhysicalConnectionRequirements,
			CatalogID:                      in.CatalogID,
		},
	)
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{Name: c.Name}, nil
}

type getConnectionInput struct {
	Name      string `json:"Name"`
	CatalogID string `json:"CatalogId,omitempty"`
}

type getConnectionOutput struct {
	Connection *connectionWire `json:"Connection"`
}

func (h *Handler) handleGetConnection(
	_ context.Context,
	in *getConnectionInput,
) (*getConnectionOutput, error) {
	c, err := h.Backend.GetConnection(in.Name)
	if err != nil {
		return nil, err
	}

	if catalogIDMismatch(in.CatalogID, c.CatalogID) {
		return nil, ErrNotFound
	}

	return &getConnectionOutput{Connection: toConnectionWire(c)}, nil
}

// defaultGetConnectionsLimit is used when GetConnectionsInput.MaxResults is unset.
const defaultGetConnectionsLimit = 100

// getConnectionsFilter mirrors aws-sdk-go-v2/service/glue/types.GetConnectionsFilter.
// ConnectionSchemaVersion is not modeled: this backend's Connection (models.go)
// has no schema-version field, so there is nothing honest to filter on -- it is
// accepted on the wire and otherwise inert.
type getConnectionsFilter struct {
	ConnectionType          string   `json:"ConnectionType,omitempty"`
	MatchCriteria           []string `json:"MatchCriteria,omitempty"`
	ConnectionSchemaVersion int32    `json:"ConnectionSchemaVersion,omitempty"`
}

// getConnectionsInput holds input for GetConnections.
type getConnectionsInput struct {
	CatalogID    string               `json:"CatalogId,omitempty"`
	NextToken    string               `json:"NextToken,omitempty"`
	Filter       getConnectionsFilter `json:"Filter,omitzero"`
	MaxResults   int32                `json:"MaxResults,omitempty"`
	HidePassword bool                 `json:"HidePassword,omitempty"`
}

type getConnectionsOutput struct {
	NextToken      string            `json:"NextToken,omitempty"`
	ConnectionList []*connectionWire `json:"ConnectionList"`
}

func (h *Handler) handleGetConnections(
	_ context.Context,
	in *getConnectionsInput,
) (*getConnectionsOutput, error) {
	conns := filterConnections(h.Backend.GetConnections(), in)

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetConnectionsLimit
	}

	page, next := paginateSlice(conns, in.NextToken, limit)

	if in.HidePassword {
		page = redactConnectionPasswords(page)
	}

	return &getConnectionsOutput{ConnectionList: toConnectionWireList(page), NextToken: next}, nil
}

// filterConnections applies in.Filter/in.CatalogID to conns. Split out of
// handleGetConnections to keep that function's cognitive complexity under
// the gocognit limit.
func filterConnections(conns []*Connection, in *getConnectionsInput) []*Connection {
	if in.Filter.ConnectionType == "" && len(in.Filter.MatchCriteria) == 0 && in.CatalogID == "" {
		return conns
	}

	filtered := make([]*Connection, 0, len(conns))

	for _, c := range conns {
		if in.Filter.ConnectionType != "" && c.ConnectionType != in.Filter.ConnectionType {
			continue
		}

		if len(in.Filter.MatchCriteria) > 0 && !matchesAllCriteria(c.MatchCriteria, in.Filter.MatchCriteria) {
			continue
		}

		if in.CatalogID != "" && c.CatalogID != in.CatalogID {
			continue
		}

		filtered = append(filtered, c)
	}

	return filtered
}

// redactConnectionPasswords returns page with each connection's PASSWORD
// property removed, copying rather than mutating the originals.
func redactConnectionPasswords(page []*Connection) []*Connection {
	out := make([]*Connection, len(page))

	for i, c := range page {
		cp := *c
		if cp.ConnectionProperties != nil {
			props := maps.Clone(cp.ConnectionProperties)
			delete(props, "PASSWORD")
			cp.ConnectionProperties = props
		}

		out[i] = &cp
	}

	return out
}

// matchesAllCriteria reports whether every entry in want is present in have,
// mirroring GetConnectionsFilter.MatchCriteria's "must match" semantics.
func matchesAllCriteria(have, want []string) bool {
	set := make(map[string]bool, len(have))
	for _, c := range have {
		set[c] = true
	}

	for _, w := range want {
		if !set[w] {
			return false
		}
	}

	return true
}

type deleteConnectionInput struct {
	ConnectionName string `json:"ConnectionName"`
	CatalogID      string `json:"CatalogId,omitempty"`
}

func (h *Handler) handleDeleteConnection(
	_ context.Context,
	in *deleteConnectionInput,
) (*emptyOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetConnection(in.ConnectionName)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	if err := h.Backend.DeleteConnection(in.ConnectionName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// adHocTestConnectionInput holds an inline (not-yet-created) connection to test.
type adHocTestConnectionInput struct {
	ConnectionProperties map[string]string `json:"ConnectionProperties,omitempty"`
	ConnectionType       string            `json:"ConnectionType,omitempty"`
}

// testConnectionInput holds input for TestConnection.
type testConnectionInput struct {
	TestConnectionInput *adHocTestConnectionInput `json:"TestConnectionInput,omitempty"`
	CatalogID           string                    `json:"CatalogId,omitempty"`
	ConnectionName      string                    `json:"ConnectionName,omitempty"`
}

// handleTestConnection validates the named connection exists, or that an
// ad-hoc connection description is well-formed, and returns success. Real AWS
// returns an empty 200 response on success; the actual dial happens
// asynchronously and can't be modeled here.
func (h *Handler) handleTestConnection(_ context.Context, in *testConnectionInput) (*emptyOutput, error) {
	hasName := in.ConnectionName != ""
	hasAdHoc := in.TestConnectionInput != nil

	switch {
	case hasName && hasAdHoc:
		return nil, fmt.Errorf(
			"%w: specify either ConnectionName or TestConnectionInput, not both", ErrValidation,
		)
	case hasName:
		if _, err := h.Backend.GetConnection(in.ConnectionName); err != nil {
			return nil, err
		}
	case hasAdHoc:
		if in.TestConnectionInput.ConnectionType == "" || len(in.TestConnectionInput.ConnectionProperties) == 0 {
			return nil, fmt.Errorf(
				"%w: TestConnectionInput requires ConnectionType and ConnectionProperties", ErrValidation,
			)
		}
	default:
		return nil, fmt.Errorf("%w: must specify ConnectionName or TestConnectionInput", ErrValidation)
	}

	return &emptyOutput{}, nil
}

// updateConnectionInput holds input for UpdateConnection.
type updateConnectionInput struct {
	Name            string          `json:"Name"`
	CatalogID       string          `json:"CatalogId,omitempty"`
	ConnectionInput connectionInput `json:"ConnectionInput"`
}

func (h *Handler) handleUpdateConnection(
	_ context.Context,
	in *updateConnectionInput,
) (*emptyOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetConnection(in.Name)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	if err := h.Backend.UpdateConnectionWithOptions(
		in.Name,
		in.ConnectionInput.ConnectionType,
		in.ConnectionInput.ConnectionProperties,
		ConnectionOptions{
			Description:                    in.ConnectionInput.Description,
			MatchCriteria:                  in.ConnectionInput.MatchCriteria,
			PhysicalConnectionRequirements: in.ConnectionInput.PhysicalConnectionRequirements,
		},
	); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
