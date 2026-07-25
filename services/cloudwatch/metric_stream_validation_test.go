package cloudwatch_test

import (
	"net/http"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestBackend_PutMetricStream_RequiredFields locks that Name, FirehoseArn,
// RoleArn, and OutputFormat are all required on every PutMetricStream call
// (aws-sdk-go-v2's PutMetricStreamInput marks all four "This member is
// required" -- PutMetricStream is a full-replace PUT, not a patch, so this
// holds for updates too, not just creates). Previously gopherstack only
// checked Name; FirehoseArn/RoleArn/OutputFormat could be silently omitted.
func TestBackend_PutMetricStream_RequiredFields(t *testing.T) {
	t.Parallel()

	validStream := func() *cloudwatch.MetricStream {
		return &cloudwatch.MetricStream{
			Name:         "s",
			FirehoseArn:  "arn:aws:firehose:us-east-1:123:deliverystream/ds",
			RoleArn:      "arn:aws:iam::123:role/r",
			OutputFormat: "json",
		}
	}

	tests := []struct {
		mutate func(*cloudwatch.MetricStream)
		name   string
	}{
		{name: "missing Name", mutate: func(s *cloudwatch.MetricStream) { s.Name = "" }},
		{name: "missing FirehoseArn", mutate: func(s *cloudwatch.MetricStream) { s.FirehoseArn = "" }},
		{name: "missing RoleArn", mutate: func(s *cloudwatch.MetricStream) { s.RoleArn = "" }},
		{name: "missing OutputFormat", mutate: func(s *cloudwatch.MetricStream) { s.OutputFormat = "" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			stream := validStream()
			tt.mutate(stream)

			err := b.PutMetricStream(stream)
			require.Error(t, err)
		})
	}

	t.Run("all fields present succeeds", func(t *testing.T) {
		t.Parallel()

		b := cloudwatch.NewInMemoryBackend()
		require.NoError(t, b.PutMetricStream(validStream()))
	})
}

// TestBackend_PutMetricStream_OutputFormatEnum locks that OutputFormat must be
// one of the three documented values (json, opentelemetry0.7,
// opentelemetry1.0 -- types.MetricStreamOutputFormat in
// aws-sdk-go-v2/service/cloudwatch); anything else must be rejected instead of
// silently stored.
func TestBackend_PutMetricStream_OutputFormatEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		outputFormat string
		wantErr      bool
	}{
		{name: "json", outputFormat: "json"},
		{name: "opentelemetry0.7", outputFormat: "opentelemetry0.7"},
		{name: "opentelemetry1.0", outputFormat: "opentelemetry1.0"},
		{name: "unknown value", outputFormat: "protobuf", wantErr: true},
		{name: "wrong case", outputFormat: "JSON", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			err := b.PutMetricStream(&cloudwatch.MetricStream{
				Name:         "s",
				FirehoseArn:  "arn:aws:firehose:us-east-1:123:deliverystream/ds",
				RoleArn:      "arn:aws:iam::123:role/r",
				OutputFormat: tt.outputFormat,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestBackend_PutMetricStream_FiltersMutuallyExclusive locks that
// IncludeFilters and ExcludeFilters cannot both be set on the same call ("You
// cannot include ExcludeFilters and IncludeFilters in the same operation" --
// PutMetricStreamInput doc comment). Previously gopherstack accepted both
// simultaneously with no error.
func TestBackend_PutMetricStream_FiltersMutuallyExclusive(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.PutMetricStream(&cloudwatch.MetricStream{
		Name:           "s",
		FirehoseArn:    "arn:aws:firehose:us-east-1:123:deliverystream/ds",
		RoleArn:        "arn:aws:iam::123:role/r",
		OutputFormat:   "json",
		IncludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
		ExcludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/Lambda"}},
	})
	require.Error(t, err)
}

// TestHandler_PutMetricStream_ValidationErrors locks the XML wire shape for
// the validation failures above: HTTP 400 InvalidParameterValue, not a 500
// InternalFailure (the backend error is a validation error, not a server
// fault).
func TestHandler_PutMetricStream_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "missing FirehoseArn",
			body: "Action=PutMetricStream&Name=s&RoleArn=arn%3Aaws%3Aiam%3A%3A123%3Arole%2Fr" +
				"&OutputFormat=json",
		},
		{
			name: "missing RoleArn",
			body: "Action=PutMetricStream&Name=s" +
				"&FirehoseArn=arn%3Aaws%3Afirehose%3Aus-east-1%3A123%3Adeliverystream%2Fds" +
				"&OutputFormat=json",
		},
		{
			name: "missing OutputFormat",
			body: "Action=PutMetricStream&Name=s" +
				"&FirehoseArn=arn%3Aaws%3Afirehose%3Aus-east-1%3A123%3Adeliverystream%2Fds" +
				"&RoleArn=arn%3Aaws%3Aiam%3A%3A123%3Arole%2Fr",
		},
		{
			name: "invalid OutputFormat",
			body: "Action=PutMetricStream&Name=s" +
				"&FirehoseArn=arn%3Aaws%3Afirehose%3Aus-east-1%3A123%3Adeliverystream%2Fds" +
				"&RoleArn=arn%3Aaws%3Aiam%3A%3A123%3Arole%2Fr&OutputFormat=protobuf",
		},
		{
			name: "both Include and Exclude filters",
			body: "Action=PutMetricStream&Name=s" +
				"&FirehoseArn=arn%3Aaws%3Afirehose%3Aus-east-1%3A123%3Adeliverystream%2Fds" +
				"&RoleArn=arn%3Aaws%3Aiam%3A%3A123%3Arole%2Fr&OutputFormat=json" +
				"&IncludeFilters.member.1.Namespace=AWS%2FEC2" +
				"&ExcludeFilters.member.1.Namespace=AWS%2FLambda",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
		})
	}
}

// TestCBOR_PutMetricStream_ValidationErrors locks the same validation on the
// independent rpc-v2-cbor wire path.
func TestCBOR_PutMetricStream_ValidationErrors(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postCBOR(t, h, "PutMetricStream", cbor.Map{
		"Name":        cbor.String("s"),
		"FirehoseArn": cbor.String("arn:aws:firehose:us-east-1:123:deliverystream/ds"),
		"RoleArn":     cbor.String("arn:aws:iam::123:role/r"),
		// OutputFormat omitted.
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = postCBOR(t, h, "PutMetricStream", cbor.Map{
		"Name":         cbor.String("s"),
		"FirehoseArn":  cbor.String("arn:aws:firehose:us-east-1:123:deliverystream/ds"),
		"RoleArn":      cbor.String("arn:aws:iam::123:role/r"),
		"OutputFormat": cbor.String("not-a-real-format"),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
