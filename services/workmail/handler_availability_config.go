package workmail

import (
	"context"
	"encoding/json"
)

// ---- Availability Configurations ----

type ewsProviderJSON struct {
	EwsEndpoint string `json:"EwsEndpoint"`
	EwsUsername string `json:"EwsUsername"`
	EwsPassword string `json:"EwsPassword"`
}

type lambdaProviderJSON struct {
	LambdaArn string `json:"LambdaArn"`
}

type createAvailabilityConfigReq struct {
	EwsProvider    *ewsProviderJSON    `json:"EwsProvider"`
	LambdaProvider *lambdaProviderJSON `json:"LambdaProvider"`
	OrganizationID string              `json:"OrganizationId"`
	DomainName     string              `json:"DomainName"`
}

func (h *Handler) handleCreateAvailabilityConfiguration(
	_ context.Context, req *createAvailabilityConfigReq,
) (*struct{}, error) {
	var ewsProv *AvailabilityEwsProvider
	var lambdaARN string
	if req.EwsProvider != nil {
		ewsProv = &AvailabilityEwsProvider{
			EwsEndpoint: req.EwsProvider.EwsEndpoint,
			EwsUsername: req.EwsProvider.EwsUsername,
			EwsPassword: req.EwsProvider.EwsPassword,
		}
	} else if req.LambdaProvider != nil {
		lambdaARN = req.LambdaProvider.LambdaArn
	}
	_, err := h.Backend.CreateAvailabilityConfiguration(req.OrganizationID, req.DomainName, ewsProv, lambdaARN)
	if err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type deleteAvailabilityConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

func (h *Handler) handleDeleteAvailabilityConfiguration(
	_ context.Context, req *deleteAvailabilityConfigReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteAvailabilityConfiguration(req.OrganizationID, req.DomainName)
}

type updateAvailabilityConfigReq struct {
	EwsProvider    *ewsProviderJSON    `json:"EwsProvider"`
	LambdaProvider *lambdaProviderJSON `json:"LambdaProvider"`
	OrganizationID string              `json:"OrganizationId"`
	DomainName     string              `json:"DomainName"`
}

func (h *Handler) handleUpdateAvailabilityConfiguration(
	_ context.Context, req *updateAvailabilityConfigReq,
) (*struct{}, error) {
	var ewsProv *AvailabilityEwsProvider
	var lambdaARN string
	if req.EwsProvider != nil {
		ewsProv = &AvailabilityEwsProvider{
			EwsEndpoint: req.EwsProvider.EwsEndpoint,
			EwsUsername: req.EwsProvider.EwsUsername,
			EwsPassword: req.EwsProvider.EwsPassword,
		}
	} else if req.LambdaProvider != nil {
		lambdaARN = req.LambdaProvider.LambdaArn
	}

	return &struct{}{}, h.Backend.UpdateAvailabilityConfiguration(
		req.OrganizationID,
		req.DomainName,
		ewsProv,
		lambdaARN,
	)
}

type listAvailabilityConfigsReq struct {
	OrganizationID string `json:"OrganizationId"`
	MaxResults     *int32 `json:"MaxResults"`
	NextToken      string `json:"NextToken"`
}

type availabilityConfigJSON struct {
	EwsProvider    *json.RawMessage `json:"EwsProvider,omitempty"`
	LambdaProvider *json.RawMessage `json:"LambdaProvider,omitempty"`
	DomainName     string           `json:"DomainName"`
	ProviderType   string           `json:"ProviderType"`
	DateCreated    int64            `json:"DateCreated"`
	DateModified   int64            `json:"DateModified"`
}

type listAvailabilityConfigsResp struct {
	NextToken                  string                   `json:"NextToken,omitempty"`
	AvailabilityConfigurations []availabilityConfigJSON `json:"AvailabilityConfigurations"`
}

func (h *Handler) handleListAvailabilityConfigurations(
	_ context.Context, req *listAvailabilityConfigsReq,
) (*listAvailabilityConfigsResp, error) {
	maxResults := int32(0)
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}
	cfgs, next, err := h.Backend.ListAvailabilityConfigurations(req.OrganizationID, maxResults, req.NextToken)
	if err != nil {
		return nil, err
	}
	result := make([]availabilityConfigJSON, 0, len(cfgs))
	for _, c := range cfgs {
		item := availabilityConfigJSON{
			DomainName:   c.DomainName,
			ProviderType: c.ProviderType,
			DateCreated:  c.DateCreated.Unix(),
			DateModified: c.DateModified.Unix(),
		}
		if c.ProviderType == providerEWS {
			raw, _ := json.Marshal(map[string]string{
				"EwsEndpoint": c.EwsEndpoint,
				"EwsUsername": c.EwsUsername,
			})
			rm := json.RawMessage(raw)
			item.EwsProvider = &rm
		} else {
			raw, _ := json.Marshal(map[string]string{"LambdaArn": c.LambdaARN})
			rm := json.RawMessage(raw)
			item.LambdaProvider = &rm
		}
		result = append(result, item)
	}

	return &listAvailabilityConfigsResp{AvailabilityConfigurations: result, NextToken: next}, nil
}

type testAvailabilityConfigReq struct {
	EwsProvider    *ewsProviderJSON    `json:"EwsProvider"`
	LambdaProvider *lambdaProviderJSON `json:"LambdaProvider"`
	OrganizationID string              `json:"OrganizationId"`
	DomainName     string              `json:"DomainName"`
}

type testAvailabilityConfigResp struct {
	FailureReason string `json:"FailureReason,omitempty"`
	TestPassed    bool   `json:"TestPassed"`
}

func (h *Handler) handleTestAvailabilityConfiguration(
	_ context.Context, req *testAvailabilityConfigReq,
) (*testAvailabilityConfigResp, error) {
	passed, reason, err := h.Backend.TestAvailabilityConfiguration(req.OrganizationID, req.DomainName)
	if err != nil {
		return nil, err
	}

	return &testAvailabilityConfigResp{TestPassed: passed, FailureReason: reason}, nil
}
