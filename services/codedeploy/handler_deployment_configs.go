package codedeploy

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// minimumHealthyHostsEntry is the wire format for minimum healthy hosts config.
type minimumHealthyHostsEntry struct {
	Type  string `json:"type,omitempty"`
	Value int    `json:"value,omitempty"`
}

// timeBasedCanaryEntry is the wire format for canary traffic routing.
type timeBasedCanaryEntry struct {
	CanaryPercentage int `json:"canaryPercentage,omitempty"`
	CanaryInterval   int `json:"canaryInterval,omitempty"`
}

// timeBasedLinearEntry is the wire format for linear traffic routing.
type timeBasedLinearEntry struct {
	LinearPercentage int `json:"linearPercentage,omitempty"`
	LinearInterval   int `json:"linearInterval,omitempty"`
}

// trafficRoutingConfigEntry is the wire format for traffic routing configuration.
type trafficRoutingConfigEntry struct {
	TimeBasedCanary *timeBasedCanaryEntry `json:"timeBasedCanary,omitempty"`
	TimeBasedLinear *timeBasedLinearEntry `json:"timeBasedLinear,omitempty"`
	Type            string                `json:"type,omitempty"`
}

// zonalConfigEntry is the wire format for zonal deployment configuration.
type zonalConfigEntry struct {
	MinimumHealthyHostsPerZone        *minimumHealthyHostsEntry `json:"minimumHealthyHostsPerZone,omitempty"`
	FirstZoneMonitorDurationInSeconds int                       `json:"firstZoneMonitorDurationInSeconds,omitempty"`
	MonitorDurationInSeconds          int                       `json:"monitorDurationInSeconds,omitempty"`
}

type createDeploymentConfigInput struct {
	MinimumHealthyHosts  *minimumHealthyHostsEntry  `json:"minimumHealthyHosts"`
	TrafficRoutingConfig *trafficRoutingConfigEntry `json:"trafficRoutingConfig"`
	ZonalConfig          *zonalConfigEntry          `json:"zonalConfig"`
	DeploymentConfigName string                     `json:"deploymentConfigName"`
	ComputePlatform      string                     `json:"computePlatform"`
}

type createDeploymentConfigOutput struct {
	DeploymentConfigID string `json:"deploymentConfigId"`
}

func (h *Handler) handleCreateDeploymentConfig(
	_ context.Context,
	in *createDeploymentConfigInput,
) (*createDeploymentConfigOutput, error) {
	if in.DeploymentConfigName == "" {
		return nil, fmt.Errorf("%w: deploymentConfigName is required", errInvalidRequest)
	}

	var mhh *MinimumHealthyHosts
	if in.MinimumHealthyHosts != nil {
		mhh = &MinimumHealthyHosts{Type: in.MinimumHealthyHosts.Type, Value: in.MinimumHealthyHosts.Value}
	}

	var trc *TrafficRoutingConfig
	if in.TrafficRoutingConfig != nil {
		trc = &TrafficRoutingConfig{Type: in.TrafficRoutingConfig.Type}
		if in.TrafficRoutingConfig.TimeBasedCanary != nil {
			trc.TimeBasedCanary = &TimeBasedCanary{
				CanaryPercentage: in.TrafficRoutingConfig.TimeBasedCanary.CanaryPercentage,
				CanaryInterval:   in.TrafficRoutingConfig.TimeBasedCanary.CanaryInterval,
			}
		}
		if in.TrafficRoutingConfig.TimeBasedLinear != nil {
			trc.TimeBasedLinear = &TimeBasedLinear{
				LinearPercentage: in.TrafficRoutingConfig.TimeBasedLinear.LinearPercentage,
				LinearInterval:   in.TrafficRoutingConfig.TimeBasedLinear.LinearInterval,
			}
		}
	}

	var zc *ZonalConfig
	if in.ZonalConfig != nil {
		zc = &ZonalConfig{
			FirstZoneMonitorDurationInSeconds: in.ZonalConfig.FirstZoneMonitorDurationInSeconds,
			MonitorDurationInSeconds:          in.ZonalConfig.MonitorDurationInSeconds,
		}
		if in.ZonalConfig.MinimumHealthyHostsPerZone != nil {
			zc.MinimumHealthyHostsPerZone = &MinimumHealthyHosts{
				Type:  in.ZonalConfig.MinimumHealthyHostsPerZone.Type,
				Value: in.ZonalConfig.MinimumHealthyHostsPerZone.Value,
			}
		}
	}

	cfg, err := h.Backend.CreateDeploymentConfig(in.DeploymentConfigName, in.ComputePlatform, mhh, trc, zc)
	if err != nil {
		return nil, err
	}

	return &createDeploymentConfigOutput{DeploymentConfigID: cfg.DeploymentConfigID}, nil
}

type getDeploymentConfigInput struct {
	DeploymentConfigName string `json:"deploymentConfigName"`
}

type deploymentConfigInfo struct {
	MinimumHealthyHosts  *minimumHealthyHostsEntry  `json:"minimumHealthyHosts,omitempty"`
	TrafficRoutingConfig *trafficRoutingConfigEntry `json:"trafficRoutingConfig,omitempty"`
	ZonalConfig          *zonalConfigEntry          `json:"zonalConfig,omitempty"`
	DeploymentConfigID   string                     `json:"deploymentConfigId"`
	DeploymentConfigName string                     `json:"deploymentConfigName"`
	ComputePlatform      string                     `json:"computePlatform"`
	CreateTime           float64                    `json:"createTime"`
}

type getDeploymentConfigOutput struct {
	DeploymentConfigInfo deploymentConfigInfo `json:"deploymentConfigInfo"`
}

func deploymentConfigToInfo(cfg *DeploymentConfig) deploymentConfigInfo {
	info := deploymentConfigInfo{
		DeploymentConfigID:   cfg.DeploymentConfigID,
		DeploymentConfigName: cfg.DeploymentConfigName,
		ComputePlatform:      cfg.ComputePlatform,
		CreateTime:           awstime.Epoch(cfg.CreateTime),
	}

	if cfg.MinimumHealthyHosts != nil {
		info.MinimumHealthyHosts = &minimumHealthyHostsEntry{
			Type:  cfg.MinimumHealthyHosts.Type,
			Value: cfg.MinimumHealthyHosts.Value,
		}
	}

	if cfg.TrafficRoutingConfig != nil {
		trc := &trafficRoutingConfigEntry{Type: cfg.TrafficRoutingConfig.Type}
		if cfg.TrafficRoutingConfig.TimeBasedCanary != nil {
			trc.TimeBasedCanary = &timeBasedCanaryEntry{
				CanaryPercentage: cfg.TrafficRoutingConfig.TimeBasedCanary.CanaryPercentage,
				CanaryInterval:   cfg.TrafficRoutingConfig.TimeBasedCanary.CanaryInterval,
			}
		}
		if cfg.TrafficRoutingConfig.TimeBasedLinear != nil {
			trc.TimeBasedLinear = &timeBasedLinearEntry{
				LinearPercentage: cfg.TrafficRoutingConfig.TimeBasedLinear.LinearPercentage,
				LinearInterval:   cfg.TrafficRoutingConfig.TimeBasedLinear.LinearInterval,
			}
		}
		info.TrafficRoutingConfig = trc
	}

	if cfg.ZonalConfig != nil {
		zc := &zonalConfigEntry{
			FirstZoneMonitorDurationInSeconds: cfg.ZonalConfig.FirstZoneMonitorDurationInSeconds,
			MonitorDurationInSeconds:          cfg.ZonalConfig.MonitorDurationInSeconds,
		}
		if cfg.ZonalConfig.MinimumHealthyHostsPerZone != nil {
			zc.MinimumHealthyHostsPerZone = &minimumHealthyHostsEntry{
				Type:  cfg.ZonalConfig.MinimumHealthyHostsPerZone.Type,
				Value: cfg.ZonalConfig.MinimumHealthyHostsPerZone.Value,
			}
		}
		info.ZonalConfig = zc
	}

	return info
}

func (h *Handler) handleGetDeploymentConfig(
	_ context.Context,
	in *getDeploymentConfigInput,
) (*getDeploymentConfigOutput, error) {
	if in.DeploymentConfigName == "" {
		return nil, fmt.Errorf("%w: deploymentConfigName is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetDeploymentConfig(in.DeploymentConfigName)
	if err != nil {
		return nil, err
	}

	return &getDeploymentConfigOutput{DeploymentConfigInfo: deploymentConfigToInfo(cfg)}, nil
}

type listDeploymentConfigsInput struct{}

type listDeploymentConfigsOutput struct {
	DeploymentConfigsList []string `json:"deploymentConfigsList"`
}

func (h *Handler) handleListDeploymentConfigs(
	_ context.Context,
	_ *listDeploymentConfigsInput,
) (*listDeploymentConfigsOutput, error) {
	return &listDeploymentConfigsOutput{DeploymentConfigsList: h.Backend.ListDeploymentConfigs()}, nil
}

type deleteDeploymentConfigInput struct {
	DeploymentConfigName string `json:"deploymentConfigName"`
}

type deleteDeploymentConfigOutput struct{}

func (h *Handler) handleDeleteDeploymentConfig(
	_ context.Context,
	in *deleteDeploymentConfigInput,
) (*deleteDeploymentConfigOutput, error) {
	if in.DeploymentConfigName == "" {
		return nil, fmt.Errorf("%w: deploymentConfigName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteDeploymentConfig(in.DeploymentConfigName); err != nil {
		return nil, err
	}

	return &deleteDeploymentConfigOutput{}, nil
}
