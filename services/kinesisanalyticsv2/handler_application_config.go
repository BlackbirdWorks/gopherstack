package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type cwlOptionInput struct {
	LogStreamARN string `json:"LogStreamARN"`
	RoleARN      string `json:"RoleARN,omitempty"`
}

type addApplicationCWLOptionInput struct {
	CloudWatchLoggingOption     *cwlOptionInput `json:"CloudWatchLoggingOption"`
	ApplicationName             string          `json:"ApplicationName"`
	CurrentApplicationVersionID int64           `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationCWLOptionOutput struct {
	ApplicationARN                      string                        `json:"ApplicationARN"`
	OperationID                         string                        `json:"OperationId,omitempty"`
	CloudWatchLoggingOptionDescriptions []CloudWatchLoggingOptionDesc `json:"CloudWatchLoggingOptionDescriptions"`
	ApplicationVersionID                int64                         `json:"ApplicationVersionId"`
}

type kinesisStreamsInputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type kinesisFirehoseInputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type lambdaProcessorInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type inputProcessingConfigInput struct {
	InputLambdaProcessor *lambdaProcessorInput `json:"InputLambdaProcessor,omitempty"`
}

type inputConfig struct {
	KinesisStreamsInput          *kinesisStreamsInputConfig  `json:"KinesisStreamsInput,omitempty"`
	KinesisFirehoseInput         *kinesisFirehoseInputConfig `json:"KinesisFirehoseInput,omitempty"`
	InputProcessingConfiguration *inputProcessingConfigInput `json:"InputProcessingConfiguration,omitempty"`
	NamePrefix                   string                      `json:"NamePrefix,omitempty"`
}

type addApplicationInputInput struct {
	Input                       *inputConfig `json:"Input"`
	ApplicationName             string       `json:"ApplicationName"`
	CurrentApplicationVersionID int64        `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationInputOutput struct {
	ApplicationARN       string             `json:"ApplicationARN"`
	InputDescriptions    []InputDescription `json:"InputDescriptions"`
	ApplicationVersionID int64              `json:"ApplicationVersionId"`
}

type addInputProcessingConfigInput struct {
	InputProcessingConfiguration *inputProcessingConfigInput `json:"InputProcessingConfiguration"`
	ApplicationName              string                      `json:"ApplicationName"`
	InputID                      string                      `json:"InputId"`
	CurrentApplicationVersionID  int64                       `json:"CurrentApplicationVersionId,omitempty"`
}

type addInputProcessingConfigOutput struct {
	InputProcessingConfigurationDescription *InputProcessingConfigurationDesc `json:"InputProcessingConfigurationDescription,omitempty"` //nolint:lll // AWS API name
	ApplicationARN                          string                            `json:"ApplicationARN"`
	InputID                                 string                            `json:"InputId"`
	ApplicationVersionID                    int64                             `json:"ApplicationVersionId"`
}

type kinesisStreamsOutputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type kinesisFirehoseOutputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type lambdaOutputConfig struct {
	ResourceARN string `json:"ResourceARN"`
}

type destinationSchemaInput struct {
	RecordFormatType string `json:"RecordFormatType"`
}

type outputConfig struct {
	KinesisStreamsOutput  *kinesisStreamsOutputConfig  `json:"KinesisStreamsOutput,omitempty"`
	KinesisFirehoseOutput *kinesisFirehoseOutputConfig `json:"KinesisFirehoseOutput,omitempty"`
	LambdaOutput          *lambdaOutputConfig          `json:"LambdaOutput,omitempty"`
	DestinationSchema     *destinationSchemaInput      `json:"DestinationSchema,omitempty"`
	Name                  string                       `json:"Name,omitempty"`
}

type addApplicationOutputInput struct {
	Output                      *outputConfig `json:"Output"`
	ApplicationName             string        `json:"ApplicationName"`
	CurrentApplicationVersionID int64         `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationOutputOutput struct {
	ApplicationARN       string              `json:"ApplicationARN"`
	OutputDescriptions   []OutputDescription `json:"OutputDescriptions"`
	ApplicationVersionID int64               `json:"ApplicationVersionId"`
}

type s3ReferenceDataSourceConfig struct {
	BucketARN string `json:"BucketARN"`
	FileKey   string `json:"FileKey"`
}

type refDataSourceConfig struct {
	S3ReferenceDataSource *s3ReferenceDataSourceConfig `json:"S3ReferenceDataSource,omitempty"`
	TableName             string                       `json:"TableName,omitempty"`
}

type addApplicationRefDataSourceInput struct {
	ReferenceDataSource         *refDataSourceConfig `json:"ReferenceDataSource"`
	ApplicationName             string               `json:"ApplicationName"`
	CurrentApplicationVersionID int64                `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationRefDataSourceOutput struct {
	ApplicationARN                  string                           `json:"ApplicationARN"`
	ReferenceDataSourceDescriptions []ReferenceDataSourceDescription `json:"ReferenceDataSourceDescriptions"`
	ApplicationVersionID            int64                            `json:"ApplicationVersionId"`
}

type vpcConfigInput struct {
	SubnetIDs        []string `json:"SubnetIds"`
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
}

type addApplicationVpcConfigInput struct {
	VpcConfiguration            *vpcConfigInput `json:"VpcConfiguration"`
	ApplicationName             string          `json:"ApplicationName"`
	CurrentApplicationVersionID int64           `json:"CurrentApplicationVersionId,omitempty"`
}

type addApplicationVpcConfigOutput struct {
	VpcConfigurationDescription *VpcConfigurationDescription `json:"VpcConfigurationDescription,omitempty"`
	ApplicationARN              string                       `json:"ApplicationARN"`
	OperationID                 string                       `json:"OperationId,omitempty"`
	ApplicationVersionID        int64                        `json:"ApplicationVersionId"`
}

type deleteApplicationCWLOptionInput struct {
	CloudWatchLoggingOptionID   string `json:"CloudWatchLoggingOptionId"`
	ApplicationName             string `json:"ApplicationName"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteApplicationCWLOptionOutput struct {
	ApplicationARN                      string                        `json:"ApplicationARN"`
	OperationID                         string                        `json:"OperationId,omitempty"`
	CloudWatchLoggingOptionDescriptions []CloudWatchLoggingOptionDesc `json:"CloudWatchLoggingOptionDescriptions"`
	ApplicationVersionID                int64                         `json:"ApplicationVersionId"`
}

type deleteInputProcessingConfigInput struct {
	ApplicationName             string `json:"ApplicationName"`
	InputID                     string `json:"InputId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteInputProcessingConfigOutput struct {
	ApplicationARN       string `json:"ApplicationARN"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type deleteApplicationOutputInput struct {
	ApplicationName             string `json:"ApplicationName"`
	OutputID                    string `json:"OutputId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteApplicationOutputOutput struct {
	ApplicationARN       string `json:"ApplicationARN"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type deleteApplicationRefDataSourceInput struct {
	ApplicationName             string `json:"ApplicationName"`
	ReferenceID                 string `json:"ReferenceId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteApplicationRefDataSourceOutput struct {
	ApplicationARN       string `json:"ApplicationARN"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type deleteApplicationVpcConfigInput struct {
	ApplicationName             string `json:"ApplicationName"`
	VpcConfigurationID          string `json:"VpcConfigurationId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId,omitempty"`
}

type deleteApplicationVpcConfigOutput struct {
	ApplicationARN       string `json:"ApplicationARN"`
	OperationID          string `json:"OperationId,omitempty"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

func (h *Handler) handleAddApplicationCloudWatchLoggingOption(ctx context.Context, c *echo.Context, body []byte) error {
	var in addApplicationCWLOptionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.CloudWatchLoggingOption == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "CloudWatchLoggingOption is required")
	}

	opID, err := h.Backend.AddApplicationCloudWatchLoggingOption(
		ctx,
		in.ApplicationName,
		in.CurrentApplicationVersionID,
		in.CloudWatchLoggingOption.LogStreamARN,
		in.CloudWatchLoggingOption.RoleARN,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationCWLOptionOutput{
		ApplicationARN:                      app.ApplicationARN,
		OperationID:                         opID,
		ApplicationVersionID:                app.ApplicationVersionID,
		CloudWatchLoggingOptionDescriptions: app.CloudWatchLoggingOptionDescs,
	})
}

//nolint:dupl // add input/output handlers share structure but are semantically distinct operations
func (h *Handler) handleAddApplicationInput(
	ctx context.Context,
	c *echo.Context,
	body []byte,
) error {
	var in addApplicationInputInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.Input == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "Input is required")
	}

	desc := buildInputDescription(in.Input)

	if err := h.Backend.AddApplicationInput(ctx, in.ApplicationName, in.CurrentApplicationVersionID, desc); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationInputOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
		InputDescriptions:    app.InputDescriptions,
	})
}

func (h *Handler) handleAddApplicationInputProcessingConfiguration(
	ctx context.Context, c *echo.Context, body []byte,
) error {
	var in addInputProcessingConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	var config *InputProcessingConfigurationDesc
	if in.InputProcessingConfiguration != nil && in.InputProcessingConfiguration.InputLambdaProcessor != nil {
		config = &InputProcessingConfigurationDesc{
			InputLambdaProcessor: &LambdaProcessorDesc{
				ResourceARN: in.InputProcessingConfiguration.InputLambdaProcessor.ResourceARN,
			},
		}
	}

	if err := h.Backend.AddApplicationInputProcessingConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.InputID, config,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	var desc *InputProcessingConfigurationDesc

	for i := range app.InputDescriptions {
		if app.InputDescriptions[i].InputID == in.InputID {
			desc = app.InputDescriptions[i].InputProcessingConfigurationDescription

			break
		}
	}

	return c.JSON(http.StatusOK, addInputProcessingConfigOutput{
		ApplicationARN:                          app.ApplicationARN,
		ApplicationVersionID:                    app.ApplicationVersionID,
		InputID:                                 in.InputID,
		InputProcessingConfigurationDescription: desc,
	})
}

//nolint:dupl // add input/output handlers share structure but are semantically distinct operations
func (h *Handler) handleAddApplicationOutput(
	ctx context.Context,
	c *echo.Context,
	body []byte,
) error {
	var in addApplicationOutputInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.Output == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "Output is required")
	}

	desc := buildOutputDescription(in.Output)

	if err := h.Backend.AddApplicationOutput(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, desc,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationOutputOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
		OutputDescriptions:   app.OutputDescriptions,
	})
}

//nolint:dupl // add input/reference-data-source handlers share structure but are semantically distinct operations
func (h *Handler) handleAddApplicationReferenceDataSource(ctx context.Context, c *echo.Context, body []byte) error {
	var in addApplicationRefDataSourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.ReferenceDataSource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "ReferenceDataSource is required")
	}

	ref := buildRefDataSourceDescription(in.ReferenceDataSource)

	if err := h.Backend.AddApplicationReferenceDataSource(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, ref,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, addApplicationRefDataSourceOutput{
		ApplicationARN:                  app.ApplicationARN,
		ApplicationVersionID:            app.ApplicationVersionID,
		ReferenceDataSourceDescriptions: app.ReferenceDataSourceDescriptions,
	})
}

func (h *Handler) handleAddApplicationVpcConfiguration(ctx context.Context, c *echo.Context, body []byte) error {
	var in addApplicationVpcConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if in.VpcConfiguration == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "VpcConfiguration is required")
	}

	vpc := buildVpcConfigDescription(in.VpcConfiguration)

	opID, err := h.Backend.AddApplicationVpcConfiguration(ctx, in.ApplicationName, in.CurrentApplicationVersionID, vpc)
	if err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	var vpcDesc *VpcConfigurationDescription

	if len(app.VpcConfigurationDescriptions) > 0 {
		last := app.VpcConfigurationDescriptions[len(app.VpcConfigurationDescriptions)-1]
		vpcDesc = &last
	}

	return c.JSON(http.StatusOK, addApplicationVpcConfigOutput{
		ApplicationARN:              app.ApplicationARN,
		OperationID:                 opID,
		ApplicationVersionID:        app.ApplicationVersionID,
		VpcConfigurationDescription: vpcDesc,
	})
}

func (h *Handler) handleDeleteApplicationCloudWatchLoggingOption(
	ctx context.Context, c *echo.Context, body []byte,
) error {
	var in deleteApplicationCWLOptionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	opID, err := h.Backend.DeleteApplicationCloudWatchLoggingOption(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.CloudWatchLoggingOptionID,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationCWLOptionOutput{
		ApplicationARN:                      app.ApplicationARN,
		OperationID:                         opID,
		ApplicationVersionID:                app.ApplicationVersionID,
		CloudWatchLoggingOptionDescriptions: app.CloudWatchLoggingOptionDescs,
	})
}

func (h *Handler) handleDeleteApplicationInputProcessingConfiguration(
	ctx context.Context, c *echo.Context, body []byte,
) error {
	var in deleteInputProcessingConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationInputProcessingConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.InputID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteInputProcessingConfigOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
	})
}

func (h *Handler) handleDeleteApplicationOutput(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteApplicationOutputInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationOutput(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.OutputID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationOutputOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
	})
}

func (h *Handler) handleDeleteApplicationReferenceDataSource(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteApplicationRefDataSourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationReferenceDataSource(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.ReferenceID,
	); err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationRefDataSourceOutput{
		ApplicationARN:       app.ApplicationARN,
		ApplicationVersionID: app.ApplicationVersionID,
	})
}

func (h *Handler) handleDeleteApplicationVpcConfiguration(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteApplicationVpcConfigInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	opID, err := h.Backend.DeleteApplicationVpcConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.VpcConfigurationID,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	app, err := h.Backend.DescribeApplication(ctx, in.ApplicationName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteApplicationVpcConfigOutput{
		ApplicationARN:       app.ApplicationARN,
		OperationID:          opID,
		ApplicationVersionID: app.ApplicationVersionID,
	})
}

// buildInputDescription converts an inputConfig to an InputDescription.
func buildInputDescription(in *inputConfig) InputDescription {
	desc := InputDescription{
		NamePrefix: in.NamePrefix,
	}

	if in.KinesisStreamsInput != nil {
		desc.KinesisStreamsInputDescription = &KinesisStreamsInputDesc{
			ResourceARN: in.KinesisStreamsInput.ResourceARN,
		}
	}

	if in.KinesisFirehoseInput != nil {
		desc.KinesisFirehoseInputDescription = &KinesisFirehoseInputDesc{
			ResourceARN: in.KinesisFirehoseInput.ResourceARN,
		}
	}

	if in.InputProcessingConfiguration != nil && in.InputProcessingConfiguration.InputLambdaProcessor != nil {
		desc.InputProcessingConfigurationDescription = &InputProcessingConfigurationDesc{
			InputLambdaProcessor: &LambdaProcessorDesc{
				ResourceARN: in.InputProcessingConfiguration.InputLambdaProcessor.ResourceARN,
			},
		}
	}

	return desc
}

// buildOutputDescription converts an outputConfig to an OutputDescription.
func buildOutputDescription(out *outputConfig) OutputDescription {
	desc := OutputDescription{
		Name: out.Name,
	}

	if out.KinesisStreamsOutput != nil {
		desc.KinesisStreamsOutputDescription = &KinesisStreamsOutputDesc{
			ResourceARN: out.KinesisStreamsOutput.ResourceARN,
		}
	}

	if out.KinesisFirehoseOutput != nil {
		desc.KinesisFirehoseOutputDescription = &KinesisFirehoseOutputDesc{
			ResourceARN: out.KinesisFirehoseOutput.ResourceARN,
		}
	}

	if out.LambdaOutput != nil {
		desc.LambdaOutputDescription = &LambdaOutputDesc{
			ResourceARN: out.LambdaOutput.ResourceARN,
		}
	}

	if out.DestinationSchema != nil {
		desc.DestinationSchema = &DestinationSchemaDesc{
			RecordFormatType: out.DestinationSchema.RecordFormatType,
		}
	}

	return desc
}

// buildRefDataSourceDescription converts a refDataSourceConfig to a
// ReferenceDataSourceDescription. Shared by handleAddApplicationReferenceDataSource
// and buildInitialConfig (CreateApplication's inline
// SqlApplicationConfiguration.ReferenceDataSources) so both paths produce
// identical shapes.
func buildRefDataSourceDescription(in *refDataSourceConfig) ReferenceDataSourceDescription {
	ref := ReferenceDataSourceDescription{
		TableName: in.TableName,
	}

	if in.S3ReferenceDataSource != nil {
		ref.S3ReferenceDataSourceDescription = &S3ReferenceDataSourceDesc{
			BucketARN: in.S3ReferenceDataSource.BucketARN,
			FileKey:   in.S3ReferenceDataSource.FileKey,
		}
	}

	return ref
}

// buildVpcConfigDescription converts a vpcConfigInput to a
// VpcConfigurationDescription. Shared by handleAddApplicationVpcConfiguration
// and buildInitialConfig (CreateApplication's inline VpcConfigurations) so
// both paths produce identical shapes.
func buildVpcConfigDescription(in *vpcConfigInput) VpcConfigurationDescription {
	return VpcConfigurationDescription{
		SubnetIDs:        in.SubnetIDs,
		SecurityGroupIDs: in.SecurityGroupIDs,
	}
}
