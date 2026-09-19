package sesv2

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

// eventDestinationDefinitionInput mirrors types.EventDestinationDefinition.
type eventDestinationDefinitionInput struct {
	CloudWatchDestination *struct {
		DimensionConfigurations []struct {
			DimensionName         string `json:"DimensionName"`
			DimensionValueSource  string `json:"DimensionValueSource"`
			DefaultDimensionValue string `json:"DefaultDimensionValue"`
		} `json:"DimensionConfigurations"`
	} `json:"CloudWatchDestination"`
	EventBridgeDestination *struct {
		EventBusArn string `json:"EventBusArn"`
	} `json:"EventBridgeDestination"`
	KinesisFirehoseDestination *struct {
		IamRoleArn        string `json:"IamRoleArn"`
		DeliveryStreamArn string `json:"DeliveryStreamArn"`
	} `json:"KinesisFirehoseDestination"`
	PinpointDestination *struct {
		ApplicationArn string `json:"ApplicationArn"`
	} `json:"PinpointDestination"`
	SnsDestination *struct {
		TopicArn string `json:"TopicArn"`
	} `json:"SnsDestination"`
	MatchingEventTypes []string `json:"MatchingEventTypes"`
	Enabled            bool     `json:"Enabled"`
}

func (in eventDestinationDefinitionInput) toConfig() EventDestinationConfig {
	cfg := EventDestinationConfig{
		Enabled:            in.Enabled,
		MatchingEventTypes: in.MatchingEventTypes,
	}

	if d := in.CloudWatchDestination; d != nil {
		dims := make([]CloudWatchDimensionConfiguration, 0, len(d.DimensionConfigurations))
		for _, dc := range d.DimensionConfigurations {
			dims = append(dims, CloudWatchDimensionConfiguration{
				DimensionName:         dc.DimensionName,
				DimensionValueSource:  dc.DimensionValueSource,
				DefaultDimensionValue: dc.DefaultDimensionValue,
			})
		}

		cfg.CloudWatchDestination = &CloudWatchDestination{DimensionConfigurations: dims}
	}

	if d := in.EventBridgeDestination; d != nil {
		cfg.EventBridgeDestination = &EventBridgeDestination{EventBusArn: d.EventBusArn}
	}

	if d := in.KinesisFirehoseDestination; d != nil {
		cfg.KinesisFirehoseDestination = &KinesisFirehoseDestination{
			IamRoleArn:        d.IamRoleArn,
			DeliveryStreamArn: d.DeliveryStreamArn,
		}
	}

	if d := in.PinpointDestination; d != nil {
		cfg.PinpointDestination = &PinpointDestination{ApplicationArn: d.ApplicationArn}
	}

	if d := in.SnsDestination; d != nil {
		cfg.SnsDestination = &SnsDestination{TopicArn: d.TopicArn}
	}

	return cfg
}

type createConfigurationSetEventDestinationInput struct {
	EventDestinationName string                          `json:"EventDestinationName"`
	EventDestination     eventDestinationDefinitionInput `json:"EventDestination"`
}

func (h *Handler) handleCreateConfigurationSetEventDestination(
	c *echo.Context,
	configSetName string,
) (any, error) {
	var in createConfigurationSetEventDestinationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateConfigurationSetEventDestination(
		configSetName,
		in.EventDestinationName,
		in.EventDestination.toConfig(),
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// configuration set event destination handlers

func (h *Handler) handleGetConfigurationSetEventDestinations(configSetName string) (any, error) {
	dests, err := h.Backend.GetConfigurationSetEventDestinations(configSetName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"EventDestinations": toEventDestinationOutputs(dests)}, nil
}

func (h *Handler) handleDeleteConfigurationSetEventDestination(
	c *echo.Context,
	configSetName string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid event destination path", ErrInvalidInput)
	}

	destName := segments[3]

	if decoded, err := url.PathUnescape(destName); err == nil {
		destName = decoded
	}

	if err := h.Backend.DeleteConfigurationSetEventDestination(configSetName, destName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateConfigurationSetEventDestinationInput struct {
	EventDestination eventDestinationDefinitionInput `json:"EventDestination"`
}

func (h *Handler) handleUpdateConfigurationSetEventDestination(
	c *echo.Context,
	configSetName string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid event destination path", ErrInvalidInput)
	}

	destName := segments[3]

	if decoded, err := url.PathUnescape(destName); err == nil {
		destName = decoded
	}

	var in updateConfigurationSetEventDestinationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateConfigurationSetEventDestination(
		configSetName, destName, in.EventDestination.toConfig(),
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
