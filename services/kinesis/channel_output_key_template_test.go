package kinesis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesissdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestCreateChannel_OutputKeyTemplate(t *testing.T) {
	t.Parallel()

	client := newTestKinesisClient(t, kinesis.NewHandler(kinesis.NewInMemoryBackend()))
	streamARN := createOnDemandStream(t, client, "key-template-stream")

	tests := []struct {
		name        string
		template    string
		compression kinesissdktypes.S3CompressionType
		wantErr     bool
	}{
		{
			name: "valid_extension", template: "!{channel-name}/!{yyyy}/!{MM}/!{dd}/!{HH}/records!{extension}",
			compression: kinesissdktypes.S3CompressionTypeGzip,
		},
		{
			name:        "valid_hive_literal_ext",
			template:    "data/stream=!{stream-name}/!{yyyy}-!{MM}-!{dd}/!{HH}!{mm}!{extension:.json}",
			compression: kinesissdktypes.S3CompressionTypeZstd,
		},
		{
			name:        "valid_minimal_no_compression",
			template:    "!{channel-name}/!{channel-id}",
			compression: kinesissdktypes.S3CompressionTypeNone,
		},
		{
			name:        "leading_slash",
			template:    "/data/!{channel-name}!{extension}",
			compression: kinesissdktypes.S3CompressionTypeGzip,
			wantErr:     true,
		},
		{
			name:        "traversal",
			template:    "data/../!{channel-name}!{extension}",
			compression: kinesissdktypes.S3CompressionTypeGzip,
			wantErr:     true,
		},
		{
			name:        "dot_segment",
			template:    "data/./!{channel-name}!{extension}",
			compression: kinesissdktypes.S3CompressionTypeGzip,
			wantErr:     true,
		},
		{
			name:        "double_slash",
			template:    "data//!{channel-name}!{extension}",
			compression: kinesissdktypes.S3CompressionTypeGzip,
			wantErr:     true,
		},
		{
			name:        "extension_not_last",
			template:    "!{channel-name}!{extension}/!{yyyy}",
			compression: kinesissdktypes.S3CompressionTypeGzip,
			wantErr:     true,
		},
		{
			name:        "unknown_variable",
			template:    "data/!{partition-id}!{extension}",
			compression: kinesissdktypes.S3CompressionTypeGzip,
			wantErr:     true,
		},
		{
			name:        "compression_without_extension",
			template:    "data/!{channel-name}",
			compression: kinesissdktypes.S3CompressionTypeGzip,
			wantErr:     true,
		},
		{
			name:        "unclosed",
			template:    "data/!{channel-name}/!{yyyy",
			compression: kinesissdktypes.S3CompressionTypeNone,
			wantErr:     true,
		},
		{
			name:        "bad_literal_char",
			template:    "data/a b/!{channel-name}",
			compression: kinesissdktypes.S3CompressionTypeNone,
			wantErr:     true,
		},
		{
			name:        "two_extensions",
			template:    "!{extension}!{extension}",
			compression: kinesissdktypes.S3CompressionTypeGzip,
			wantErr:     true,
		},
		{
			name:        "uppercase_literal_ext",
			template:    "!{channel-name}!{extension:.JSON}",
			compression: kinesissdktypes.S3CompressionTypeNone,
			wantErr:     true,
		},
		{
			name: "too_long", template: longKeyTemplate(), compression: kinesissdktypes.S3CompressionTypeNone,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := minimalS3DestinationConfig()
			cfg.StorageConfiguration.OutputKeyTemplate = aws.String(tt.template)
			cfg.StorageConfiguration.CompressionType = tt.compression

			_, err := client.CreateChannel(t.Context(), &kinesissdk.CreateChannelInput{
				ChannelName:                aws.String("chan-" + tt.name),
				ServiceExecutionRoleARN:    aws.String(channelTestServiceRoleARN),
				StreamConfigurationList:    minimalStreamConfigList(streamARN),
				S3DestinationConfiguration: cfg,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func longKeyTemplate() string {
	b := make([]byte, 0, 1000)
	for range 990 {
		b = append(b, 'a')
	}

	return string(b)
}
