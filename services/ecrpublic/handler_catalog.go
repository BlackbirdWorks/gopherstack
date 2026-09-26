package ecrpublic

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) buildCatalogOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetRepositoryCatalogData": service.WrapOp(h.handleGetRepositoryCatalogData),
		"PutRepositoryCatalogData": service.WrapOp(h.handlePutRepositoryCatalogData),
		"DescribeRegistries":       service.WrapOp(h.handleDescribeRegistries),
		"GetRegistryCatalogData":   service.WrapOp(h.handleGetRegistryCatalogData),
		"PutRegistryCatalogData":   service.WrapOp(h.handlePutRegistryCatalogData),
	}
}

type getRepositoryCatalogDataInput struct {
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName"`
}

type getRepositoryCatalogDataOutput struct {
	CatalogData CatalogDataWire `json:"catalogData"`
}

func (h *Handler) handleGetRepositoryCatalogData(
	_ context.Context, in *getRepositoryCatalogDataInput,
) (*getRepositoryCatalogDataOutput, error) {
	cd, err := h.Backend.GetRepositoryCatalogData(in.RegistryID, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return &getRepositoryCatalogDataOutput{CatalogData: toCatalogDataWire(cd)}, nil
}

type putRepositoryCatalogDataInput struct {
	CatalogData    *CatalogDataInputWire `json:"catalogData,omitempty"`
	RegistryID     string                `json:"registryId,omitempty"`
	RepositoryName string                `json:"repositoryName"`
}

type putRepositoryCatalogDataOutput struct {
	CatalogData CatalogDataWire `json:"catalogData"`
}

func (h *Handler) handlePutRepositoryCatalogData(
	_ context.Context, in *putRepositoryCatalogDataInput,
) (*putRepositoryCatalogDataOutput, error) {
	cd, err := h.Backend.PutRepositoryCatalogData(
		in.RegistryID, in.RepositoryName, toCatalogDataInput(in.CatalogData),
	)
	if err != nil {
		return nil, err
	}

	return &putRepositoryCatalogDataOutput{CatalogData: toCatalogDataWire(cd)}, nil
}

type describeRegistriesInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int32  `json:"maxResults,omitempty"`
}

type describeRegistriesOutput struct {
	NextToken  string         `json:"nextToken,omitempty"`
	Registries []RegistryWire `json:"registries"`
}

func (h *Handler) handleDescribeRegistries(
	_ context.Context, _ *describeRegistriesInput,
) (*describeRegistriesOutput, error) {
	registries, err := h.Backend.DescribeRegistries()
	if err != nil {
		return nil, err
	}

	out := make([]RegistryWire, 0, len(registries))
	for _, r := range registries {
		out = append(out, toRegistryWire(r))
	}

	return &describeRegistriesOutput{Registries: out}, nil
}

type getRegistryCatalogDataInput struct{}

type getRegistryCatalogDataOutput struct {
	RegistryCatalogData RegistryCatalogDataWire `json:"registryCatalogData"`
}

func (h *Handler) handleGetRegistryCatalogData(
	_ context.Context, _ *getRegistryCatalogDataInput,
) (*getRegistryCatalogDataOutput, error) {
	cd, err := h.Backend.GetRegistryCatalogData()
	if err != nil {
		return nil, err
	}

	return &getRegistryCatalogDataOutput{RegistryCatalogData: RegistryCatalogDataWire(cd)}, nil
}

type putRegistryCatalogDataInput struct {
	DisplayName string `json:"displayName,omitempty"`
}

type putRegistryCatalogDataOutput struct {
	RegistryCatalogData RegistryCatalogDataWire `json:"registryCatalogData"`
}

func (h *Handler) handlePutRegistryCatalogData(
	_ context.Context, in *putRegistryCatalogDataInput,
) (*putRegistryCatalogDataOutput, error) {
	cd, err := h.Backend.PutRegistryCatalogData(in.DisplayName)
	if err != nil {
		return nil, err
	}

	return &putRegistryCatalogDataOutput{RegistryCatalogData: RegistryCatalogDataWire(cd)}, nil
}
