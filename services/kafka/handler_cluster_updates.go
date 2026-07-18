package kafka

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type rebootBrokerInput struct {
	BrokerIDs []string `json:"brokerIds"`
}

type clusterOperationOutput struct {
	ClusterOperationArn string `json:"clusterOperationArn"`
}

func (h *Handler) handleRebootBroker(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in rebootBrokerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	op, err := h.Backend.RebootBroker(ctx, clusterArn, in.BrokerIDs)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

// requireCurrentVersion validates that the supplied currentVersion matches the
// cluster's recorded CurrentVersion, enforcing AWS MSK's optimistic-lock guard.
// It writes an error response and returns (false, err) when validation fails so
// callers can do: if ok, err := h.requireCurrentVersion(...); !ok { return err }.
func (h *Handler) requireCurrentVersion(
	ctx context.Context,
	c *echo.Context,
	clusterArn, version string,
) (bool, error) {
	if version == "" {
		return false, h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"currentVersion is required",
		)
	}

	cl, err := h.Backend.DescribeCluster(ctx, clusterArn)
	if err != nil {
		return false, h.writeBackendError(c, err)
	}

	if cl.CurrentVersion != version {
		return false, h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"The specified cluster version is not current. Current version: "+cl.CurrentVersion+".",
		)
	}

	return true, nil
}

type updateBrokerCountInput struct {
	CurrentVersion            string `json:"currentVersion"`
	TargetNumberOfBrokerNodes int32  `json:"targetNumberOfBrokerNodes"`
}

type updateBrokerStorageInput struct {
	CurrentVersion            string                `json:"currentVersion"`
	TargetBrokerEBSVolumeInfo []brokerEBSVolumeInfo `json:"targetBrokerEBSVolumeInfo"`
}

type brokerEBSVolumeInfo struct {
	KafkaBrokerNodeID string `json:"kafkaBrokerNodeId"`
	VolumeSizeGB      int32  `json:"volumeSizeGB"`
}

type updateBrokerTypeInput struct {
	CurrentVersion     string `json:"currentVersion"`
	TargetInstanceType string `json:"targetInstanceType"`
}

type updateClusterConfigurationInput struct {
	CurrentVersion    string            `json:"currentVersion"`
	ConfigurationInfo ConfigurationInfo `json:"configurationInfo"`
}

type updateClusterKafkaVersionInput struct {
	CurrentVersion     string `json:"currentVersion"`
	TargetKafkaVersion string `json:"targetKafkaVersion"`
}

func (h *Handler) handleUpdateBrokerCount(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateBrokerCountInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateBrokerCount(ctx, clusterArn, in.TargetNumberOfBrokerNodes)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateBrokerStorage(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateBrokerStorageInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	var volumeSize int32
	if len(in.TargetBrokerEBSVolumeInfo) > 0 {
		volumeSize = in.TargetBrokerEBSVolumeInfo[0].VolumeSizeGB
	}

	op, err := h.Backend.UpdateBrokerStorage(ctx, clusterArn, volumeSize)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateBrokerType(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateBrokerTypeInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateBrokerType(ctx, clusterArn, in.TargetInstanceType)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateClusterConfiguration(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateClusterConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateClusterConfiguration(ctx,
		clusterArn,
		in.ConfigurationInfo.Arn,
		in.ConfigurationInfo.Revision,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateClusterKafkaVersion(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateClusterKafkaVersionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateClusterKafkaVersion(ctx, clusterArn, in.TargetKafkaVersion)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

type updateConnectivityInput struct {
	ConnectivityInfo *ConnectivityInfo `json:"connectivityInfo"`
	CurrentVersion   string            `json:"currentVersion"`
}

func (h *Handler) handleUpdateConnectivity(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateConnectivityInput
	if err := decodeJSONBody(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateConnectivity(ctx, clusterArn, UpdateConnectivitySettings{
		ConnectivityInfo: in.ConnectivityInfo,
	})
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

type updateMonitoringInput struct {
	OpenMonitoring     *OpenMonitoring `json:"openMonitoring"`
	LoggingInfo        *LoggingInfo    `json:"loggingInfo"`
	EnhancedMonitoring string          `json:"enhancedMonitoring"`
	CurrentVersion     string          `json:"currentVersion"`
}

func (h *Handler) handleUpdateMonitoring(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateMonitoringInput
	if err := decodeJSONBody(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateMonitoring(ctx, clusterArn, UpdateMonitoringSettings{
		EnhancedMonitoring: in.EnhancedMonitoring,
		OpenMonitoring:     in.OpenMonitoring,
		LoggingInfo:        in.LoggingInfo,
	})
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

func (h *Handler) handleUpdateRebalancing(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	_ []byte,
) error {
	op, err := h.Backend.UpdateRebalancing(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

type updateSecurityInput struct {
	ClientAuthentication *ClientAuthentication `json:"clientAuthentication"`
	EncryptionInfo       *EncryptionInfo       `json:"encryptionInfo"`
	CurrentVersion       string                `json:"currentVersion"`
}

func (h *Handler) handleUpdateSecurity(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateSecurityInput
	if err := decodeJSONBody(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateSecurity(ctx, clusterArn, UpdateSecuritySettings{
		ClientAuthentication: in.ClientAuthentication,
		EncryptionInfo:       in.EncryptionInfo,
	})
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}

type updateStorageInput struct {
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput"`
	StorageMode           string                 `json:"storageMode"`
	CurrentVersion        string                 `json:"currentVersion"`
	VolumeSizeGB          int32                  `json:"volumeSizeGB"`
}

func (h *Handler) handleUpdateStorage(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in updateStorageInput
	if err := decodeJSONBody(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", err.Error())
	}

	if ok, err := h.requireCurrentVersion(ctx, c, clusterArn, in.CurrentVersion); !ok {
		return err
	}

	op, err := h.Backend.UpdateStorage(ctx, clusterArn, UpdateStorageSettings{
		StorageMode:           in.StorageMode,
		VolumeSizeGB:          in.VolumeSizeGB,
		ProvisionedThroughput: in.ProvisionedThroughput,
	})
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		clusterOperationOutput{ClusterOperationArn: op.ClusterOperationArn},
	)
}
