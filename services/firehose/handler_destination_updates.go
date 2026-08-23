package firehose

import (
	"context"
	"encoding/json"
	"fmt"
)

// aosUpdateField holds the AmazonOpenSearch update field separately so its long name
// does not drive gofmt alignment in updateDestinationInput. Embedding keeps
// JSON marshaling transparent.
type aosUpdateField struct {
	AmazonOpenSearchServiceDestinationUpdate *openSearchDestinationInput `json:"AmazonOpenSearchServiceDestinationUpdate"` //nolint:lll // AWS field name
}

type updateDestinationInput struct {
	aosUpdateField
	S3DestinationUpdate            *s3DestinationInput            `json:"S3DestinationUpdate"`
	ExtendedS3DestinationUpdate    *s3DestinationInput            `json:"ExtendedS3DestinationUpdate"`
	HTTPEndpointDestinationUpdate  *httpEndpointDestinationInput  `json:"HttpEndpointDestinationUpdate"`
	RedshiftDestinationUpdate      *redshiftDestinationInput      `json:"RedshiftDestinationUpdate"`
	ElasticsearchDestinationUpdate *elasticsearchDestinationInput `json:"ElasticsearchDestinationUpdate"` //nolint:lll // AWS field name
	SplunkDestinationUpdate        *splunkDestinationInput        `json:"SplunkDestinationUpdate"`
	IcebergDestinationUpdate       *icebergDestinationInput       `json:"IcebergDestinationUpdate"`
	SnowflakeDestinationUpdate     *snowflakeDestinationInput     `json:"SnowflakeDestinationUpdate"`
	DeliveryStreamName             string                         `json:"DeliveryStreamName"`
	CurrentDeliveryStreamVersionID string                         `json:"CurrentDeliveryStreamVersionId"`
	DestinationID                  string                         `json:"DestinationId"`
	// AmazonOpenSearchServerlessDestinationUpdate: see createDeliveryStreamInput's
	// AmazonOpenSearchServerlessDestinationConfiguration doc comment -- same
	// unimplemented-11th-destination-type reasoning, captured only to reject explicitly.
	AmazonOpenSearchServerlessDestinationUpdate json.RawMessage `json:"AmazonOpenSearchServerlessDestinationUpdate,omitempty"` //nolint:lll // AWS field name
}

type updateDestinationOutput struct{}

func (h *Handler) handleUpdateDestination(
	ctx context.Context,
	in *updateDestinationInput,
) (*updateDestinationOutput, error) {
	// AmazonOpenSearchServerlessDestinationUpdate is a real destination type this
	// backend has no field/build path for -- reject explicitly rather than falling
	// through to applyDestinationUpdate's generic "got 0" message, which would
	// misreport that the caller supplied nothing.
	if in.AmazonOpenSearchServerlessDestinationUpdate != nil {
		return nil, fmt.Errorf(
			"%w: AmazonOpenSearchServerlessDestinationUpdate is not supported by this emulator",
			ErrValidation)
	}

	rawS3 := in.ExtendedS3DestinationUpdate
	if rawS3 == nil {
		rawS3 = in.S3DestinationUpdate
	}

	if rawS3 != nil {
		if err := validateDataFormatConversion(rawS3.DataFormatConversionConfiguration); err != nil {
			return nil, err
		}
	}

	update := UpdateDestinationInput{
		S3Destination:            buildS3DestinationDescription(rawS3),
		HTTPEndpointDestination:  buildHTTPEndpointDestination(in.HTTPEndpointDestinationUpdate),
		RedshiftDestination:      buildRedshiftDestination(in.RedshiftDestinationUpdate),
		OpenSearchDestination:    buildOpenSearchDestination(in.AmazonOpenSearchServiceDestinationUpdate),
		ElasticsearchDestination: buildElasticsearchDestination(in.ElasticsearchDestinationUpdate),
		SplunkDestination:        buildSplunkDestination(in.SplunkDestinationUpdate),
		IcebergDestination:       buildIcebergDestination(in.IcebergDestinationUpdate),
		SnowflakeDestination:     buildSnowflakeDestination(in.SnowflakeDestinationUpdate),
	}

	if err := h.Backend.UpdateDestination(
		ctx,
		in.DeliveryStreamName,
		in.CurrentDeliveryStreamVersionID,
		update,
	); err != nil {
		return nil, err
	}

	return &updateDestinationOutput{}, nil
}
