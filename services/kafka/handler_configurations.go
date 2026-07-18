package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type createConfigurationInput struct {
	Name             string   `json:"name"`
	Description      string   `json:"description,omitempty"`
	ServerProperties string   `json:"serverProperties"`
	KafkaVersions    []string `json:"kafkaVersions"`
}

type createConfigurationOutput struct {
	Arn            string                `json:"arn"`
	Name           string                `json:"name"`
	State          string                `json:"state"`
	LatestRevision configurationRevision `json:"latestRevision"`
}

type configurationRevision struct {
	Description string `json:"description,omitempty"`
	Revision    int64  `json:"revision"`
}

type describeConfigurationOutput struct {
	Arn            string                `json:"arn"`
	Name           string                `json:"name"`
	Description    string                `json:"description,omitempty"`
	State          string                `json:"state"`
	LatestRevision configurationRevision `json:"latestRevision"`
	KafkaVersions  []string              `json:"kafkaVersions,omitempty"`
}

type listConfigurationsOutput struct {
	NextToken      string           `json:"nextToken,omitempty"`
	Configurations []*Configuration `json:"configurations"`
}

func (h *Handler) handleCreateConfiguration(
	ctx context.Context,
	c *echo.Context,
	body []byte,
) error {
	var in createConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	config, err := h.Backend.CreateConfiguration(ctx,
		in.Name,
		in.Description,
		in.KafkaVersions,
		in.ServerProperties,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createConfigurationOutput{
		Arn:   config.Arn,
		Name:  config.Name,
		State: ClusterStateActive,
		LatestRevision: configurationRevision{
			Revision:    1,
			Description: config.Description,
		},
	})
}

func (h *Handler) handleListConfigurations(ctx context.Context, c *echo.Context) error {
	all := h.Backend.ListConfigurations(ctx)

	token := c.Request().URL.Query().Get("nextToken")
	offset := decodeKafkaPageToken(token)

	offset = min(offset, len(all))

	page := all[offset:]
	pageSize := kafkaPageSize(c)

	var nextToken string

	if len(page) > pageSize {
		page = page[:pageSize]
		nextToken = encodeKafkaPageToken(offset + pageSize)
	}

	return c.JSON(
		http.StatusOK,
		listConfigurationsOutput{Configurations: page, NextToken: nextToken},
	)
}

func (h *Handler) handleDescribeConfiguration(
	ctx context.Context,
	c *echo.Context,
	configArn string,
) error {
	config, err := h.Backend.DescribeConfiguration(ctx, configArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeConfigurationOutput{
		Arn:           config.Arn,
		Name:          config.Name,
		Description:   config.Description,
		KafkaVersions: config.KafkaVersions,
		State:         ClusterStateActive,
		LatestRevision: configurationRevision{
			Revision:    1,
			Description: config.Description,
		},
	})
}

func (h *Handler) handleDeleteConfiguration(
	ctx context.Context,
	c *echo.Context,
	configArn string,
) error {
	if err := h.Backend.DeleteConfiguration(ctx, configArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type listConfigurationRevisionsOutput struct {
	NextToken string                   `json:"nextToken,omitempty"`
	Revisions []*ConfigurationRevision `json:"revisions"`
}

type updateConfigurationInput struct {
	Description      string `json:"description,omitempty"`
	ServerProperties string `json:"serverProperties,omitempty"`
}

func (h *Handler) handleDescribeConfigurationRevision(
	ctx context.Context,
	c *echo.Context,
	resource string,
) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid resource")
	}

	configArn := parts[0]
	revisionStr := parts[1]

	var revision int64
	if _, err := fmt.Sscanf(revisionStr, "%d", &revision); err != nil {
		revision = 1
	}

	rev, err := h.Backend.DescribeConfigurationRevision(ctx, configArn, revision)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, rev)
}

func (h *Handler) handleListConfigurationRevisions(
	ctx context.Context,
	c *echo.Context,
	configArn string,
) error {
	all, err := h.Backend.ListConfigurationRevisions(ctx, configArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	token := c.Request().URL.Query().Get("nextToken")
	offset := decodeKafkaPageToken(token)

	offset = min(offset, len(all))

	page := all[offset:]
	pageSize := kafkaPageSize(c)

	var nextToken string

	if len(page) > pageSize {
		page = page[:pageSize]
		nextToken = encodeKafkaPageToken(offset + pageSize)
	}

	return c.JSON(
		http.StatusOK,
		listConfigurationRevisionsOutput{Revisions: page, NextToken: nextToken},
	)
}

func (h *Handler) handleUpdateConfiguration(
	ctx context.Context,
	c *echo.Context,
	configArn string,
	body []byte,
) error {
	var in updateConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	config, err := h.Backend.UpdateConfiguration(
		ctx,
		configArn,
		in.Description,
		in.ServerProperties,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createConfigurationOutput{
		Arn:   config.Arn,
		Name:  config.Name,
		State: ClusterStateActive,
		LatestRevision: configurationRevision{
			Revision:    1,
			Description: config.Description,
		},
	})
}
