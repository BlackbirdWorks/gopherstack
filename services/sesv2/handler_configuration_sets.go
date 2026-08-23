package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

type createConfigurationSetInput struct {
	ConfigurationSetName string     `json:"ConfigurationSetName"`
	Tags                 []tagEntry `json:"Tags"`
}

// trackingOptionsOutput mirrors types.TrackingOptions. CustomRedirectDomain
// has no omitempty: it is required on the real wire shape
// (aws-sdk-go-v2/service/sesv2/types/types.go's TrackingOptions), but is
// only optional on PutConfigurationSetTrackingOptionsInput -- a real client
// can set HttpsPolicy alone, leaving CustomRedirectDomain genuinely empty,
// and the required key must still be present (empty string), not dropped.
type trackingOptionsOutput struct {
	CustomRedirectDomain string `json:"CustomRedirectDomain"`
	HTTPSPolicy          string `json:"HttpsPolicy,omitempty"`
}

type deliveryOptionsOutput struct {
	TLSPolicy       string `json:"TlsPolicy,omitempty"`
	SendingPoolName string `json:"SendingPoolName,omitempty"`
}

type reputationOptionsOutput struct {
	ReputationMetricsEnabled bool `json:"ReputationMetricsEnabled"`
}

type sendingOptionsOutput struct {
	SendingEnabled bool `json:"SendingEnabled"`
}

type suppressionOptionsOutput struct {
	SuppressedReasons []string `json:"SuppressedReasons,omitempty"`
}

type archivingOptionsOutput struct {
	ArchiveARN string `json:"ArchiveArn,omitempty"`
}

type vdmOptionsOutput struct {
	DashboardOptions map[string]any `json:"DashboardOptions,omitempty"`
	GuardianOptions  map[string]any `json:"GuardianOptions,omitempty"`
}

type createConfigurationSetOutput struct{}

type getConfigurationSetOutput struct {
	TrackingOptions      *trackingOptionsOutput    `json:"TrackingOptions,omitempty"`
	DeliveryOptions      *deliveryOptionsOutput    `json:"DeliveryOptions,omitempty"`
	ReputationOptions    *reputationOptionsOutput  `json:"ReputationOptions,omitempty"`
	SendingOptions       *sendingOptionsOutput     `json:"SendingOptions,omitempty"`
	SuppressionOptions   *suppressionOptionsOutput `json:"SuppressionOptions,omitempty"`
	ArchivingOptions     *archivingOptionsOutput   `json:"ArchivingOptions,omitempty"`
	VdmOptions           *vdmOptionsOutput         `json:"VdmOptions,omitempty"`
	ConfigurationSetName string                    `json:"ConfigurationSetName"`
	Tags                 []tagEntry                `json:"Tags,omitempty"`
}

// listConfigurationSetsOutput.ConfigurationSets is a plain array of names --
// see ListConfigurationSetsOutput in aws-sdk-go-v2/service/sesv2, whose
// ConfigurationSets field is []string, not a list of name-wrapping objects.
type listConfigurationSetsOutput struct {
	NextToken         string   `json:"NextToken,omitempty"`
	ConfigurationSets []string `json:"ConfigurationSets"`
}

func (h *Handler) handleCreateConfigurationSet(c *echo.Context) (any, error) {
	var in createConfigurationSetInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	if _, err := h.Backend.CreateConfigurationSet(in.ConfigurationSetName, tagsFromEntries(in.Tags)); err != nil {
		return nil, err
	}

	return &createConfigurationSetOutput{}, nil
}

func (h *Handler) handleGetConfigurationSet(name string) (any, error) {
	cs, err := h.Backend.GetConfigurationSet(name)
	if err != nil {
		return nil, err
	}

	out := &getConfigurationSetOutput{
		ConfigurationSetName: cs.Name,
		SendingOptions:       &sendingOptionsOutput{SendingEnabled: cs.SendingEnabled},
		ReputationOptions:    &reputationOptionsOutput{ReputationMetricsEnabled: cs.ReputationMetricsEnabled},
		Tags:                 tagsToEntries(cs.Tags),
	}

	if cs.TrackingCustomRedirectDomain != "" || cs.TrackingHTTPSPolicy != "" {
		out.TrackingOptions = &trackingOptionsOutput{
			CustomRedirectDomain: cs.TrackingCustomRedirectDomain,
			HTTPSPolicy:          cs.TrackingHTTPSPolicy,
		}
	}

	if cs.DeliveryTLSPolicy != "" || cs.DeliverySendingPoolName != "" {
		out.DeliveryOptions = &deliveryOptionsOutput{
			TLSPolicy:       cs.DeliveryTLSPolicy,
			SendingPoolName: cs.DeliverySendingPoolName,
		}
	}

	if len(cs.SuppressionReasons) > 0 {
		out.SuppressionOptions = &suppressionOptionsOutput{
			SuppressedReasons: cs.SuppressionReasons,
		}
	}

	if cs.ArchivingOptions != nil {
		out.ArchivingOptions = &archivingOptionsOutput{
			ArchiveARN: cs.ArchivingOptions.ArchiveARN,
		}
	}

	if cs.VdmOptions != nil {
		out.VdmOptions = &vdmOptionsOutput{
			DashboardOptions: cs.VdmOptions.DashboardOptions,
			GuardianOptions:  cs.VdmOptions.GuardianOptions,
		}
	}

	return out, nil
}

func (h *Handler) handleListConfigurationSets(c *echo.Context) any {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListConfigurationSets(nextToken, 0)

	names := make([]string, 0, len(pg.Data))

	for _, cs := range pg.Data {
		names = append(names, cs.Name)
	}

	return &listConfigurationSetsOutput{
		ConfigurationSets: names,
		NextToken:         pg.Next,
	}
}

func (h *Handler) handleDeleteConfigurationSet(name string) (any, error) {
	if err := h.Backend.DeleteConfigurationSet(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// configuration set attribute handlers

func (h *Handler) handlePutConfigurationSetArchivingOptions(
	c *echo.Context,
	name string,
) (any, error) {
	var in struct {
		ArchiveARN string `json:"ArchiveArn"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetArchivingOptions(
		name,
		in.ArchiveARN,
	)
}

func (h *Handler) handlePutConfigurationSetDeliveryOptions(
	c *echo.Context,
	name string,
) (any, error) {
	var in struct {
		SendingPoolName string `json:"SendingPoolName"`
		TLSPolicy       string `json:"TlsPolicy"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetDeliveryOptions(
		name,
		in.TLSPolicy,
		in.SendingPoolName,
	)
}

func (h *Handler) handlePutConfigurationSetReputationOptions(
	c *echo.Context,
	name string,
) (any, error) {
	var in struct {
		ReputationMetricsEnabled bool `json:"ReputationMetricsEnabled"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetReputationOptions(
		name,
		in.ReputationMetricsEnabled,
	)
}

func (h *Handler) handlePutConfigurationSetSendingOptions(
	c *echo.Context,
	name string,
) (any, error) {
	var in struct {
		SendingEnabled bool `json:"SendingEnabled"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetSendingOptions(name, in.SendingEnabled)
}

func (h *Handler) handlePutConfigurationSetSuppressionOptions(
	c *echo.Context,
	name string,
) (any, error) {
	var in struct {
		SuppressedReasons []string `json:"SuppressedReasons"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetSuppressionOptions(
		name,
		in.SuppressedReasons,
	)
}

func (h *Handler) handlePutConfigurationSetTrackingOptions(
	c *echo.Context,
	name string,
) (any, error) {
	var in struct {
		CustomRedirectDomain string `json:"CustomRedirectDomain"`
		HTTPSPolicy          string `json:"HttpsPolicy"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetTrackingOptions(
		name,
		in.CustomRedirectDomain,
		in.HTTPSPolicy,
	)
}
