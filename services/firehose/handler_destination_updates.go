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

	if in.AmazonOpenSearchServiceDestinationUpdate != nil {
		if err := validateDocumentIDOptions(in.AmazonOpenSearchServiceDestinationUpdate.DocumentIDOptions); err != nil {
			return nil, err
		}
	}
	if in.ElasticsearchDestinationUpdate != nil {
		if err := validateDocumentIDOptions(in.ElasticsearchDestinationUpdate.DocumentIDOptions); err != nil {
			return nil, err
		}
	}

	update := buildUpdateDestinationInput(in, rawS3)

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

// buildUpdateDestinationInput builds the update descriptions without the Create-time S3
// defaults, so omitted members survive the merge in applyDestinationUpdate.
func buildUpdateDestinationInput(in *updateDestinationInput, rawS3 *s3DestinationInput) UpdateDestinationInput {
	update := UpdateDestinationInput{
		DestinationID:            in.DestinationID,
		S3Destination:            buildS3DestinationDescription(rawS3),
		HTTPEndpointDestination:  buildHTTPEndpointDestination(in.HTTPEndpointDestinationUpdate),
		RedshiftDestination:      buildRedshiftDestination(in.RedshiftDestinationUpdate),
		OpenSearchDestination:    buildOpenSearchDestination(in.AmazonOpenSearchServiceDestinationUpdate),
		ElasticsearchDestination: buildElasticsearchDestination(in.ElasticsearchDestinationUpdate),
		SplunkDestination:        buildSplunkDestination(in.SplunkDestinationUpdate),
		IcebergDestination:       buildIcebergDestination(in.IcebergDestinationUpdate),
		SnowflakeDestination:     buildSnowflakeDestination(in.SnowflakeDestinationUpdate),
	}

	stripS3Defaults(rawS3, update.S3Destination)

	if rs := in.RedshiftDestinationUpdate; rs != nil && update.RedshiftDestination != nil {
		stripS3Defaults(firstS3(rs.S3Configuration, rs.S3Update), update.RedshiftDestination.S3Destination)
		stripBackupDefaults(
			firstBackup(rs.S3BackupConfiguration, rs.S3BackupUpdate),
			update.RedshiftDestination.S3BackupDescription,
		)
	}

	if sf := in.SnowflakeDestinationUpdate; sf != nil && update.SnowflakeDestination != nil {
		stripS3Defaults(firstS3(sf.S3Configuration, sf.S3Update), update.SnowflakeDestination.S3Destination)
	}

	if ic := in.IcebergDestinationUpdate; ic != nil && update.IcebergDestination != nil {
		stripS3Defaults(ic.S3Configuration, update.IcebergDestination.S3Destination)
	}

	if d := update.S3Destination; d != nil && rawS3 != nil {
		stripBackupDefaults(firstBackup(rawS3.S3BackupConfiguration, rawS3.S3BackupUpdate), d.S3BackupDescription)
	}

	if h := in.HTTPEndpointDestinationUpdate; h != nil && update.HTTPEndpointDestination != nil {
		stripBackupDefaults(
			firstBackup(h.S3Configuration, h.S3Update),
			update.HTTPEndpointDestination.S3BackupDescription,
		)
	}

	if o := in.AmazonOpenSearchServiceDestinationUpdate; o != nil && update.OpenSearchDestination != nil {
		stripBackupDefaults(
			firstBackup(o.S3Configuration, o.S3Update),
			update.OpenSearchDestination.S3BackupDescription,
		)
	}

	if sp := in.SplunkDestinationUpdate; sp != nil && update.SplunkDestination != nil {
		stripBackupDefaults(firstBackup(sp.S3Configuration, sp.S3Update), update.SplunkDestination.S3BackupDescription)
	}

	return update
}

func firstS3(a, b *s3DestinationInput) *s3DestinationInput {
	if a != nil {
		return a
	}

	return b
}

func firstBackup(a, b *s3BackupInput) *s3BackupInput {
	if a != nil {
		return a
	}

	return b
}

func stripS3Defaults(raw *s3DestinationInput, d *S3DestinationDescription) {
	if raw == nil || d == nil {
		return
	}

	if raw.BufferingHints == nil {
		d.BufferingHints = nil
	}

	if raw.EncryptionConfiguration == nil {
		d.EncryptionConfiguration = nil
	}

	if raw.CompressionFormat == "" {
		d.CompressionFormat = ""
	}
}

func stripBackupDefaults(raw *s3BackupInput, d *S3BackupDescription) {
	if raw == nil || d == nil {
		return
	}

	if raw.BufferingHints == nil {
		d.BufferingHints = nil
	}

	if raw.EncryptionConfiguration == nil {
		d.EncryptionConfiguration = nil
	}

	if raw.CompressionFormat == "" {
		d.CompressionFormat = ""
	}
}
