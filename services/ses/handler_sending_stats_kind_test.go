package ses_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getSendStatisticsResponseXML renders the exact GetSendStatisticsResponse
// envelope handler_sending_stats.go produces, with a caller-supplied literal
// for DeliveryAttempts -- used to reproduce, byte for byte, what the old
// float64-typed xmlSendDataPoint field would have marshaled for a value at
// or above the 1e6 threshold where Go's encoding/xml float formatting
// switches to scientific notation (verified empirically: 999999 -> "999999",
// 1000000 -> "1e+06"), versus what the current int64-typed field marshals.
func getSendStatisticsResponseXML(deliveryAttemptsLiteral string) []byte {
	return []byte(fmt.Sprintf(`<?xml version="1.0"?>
<GetSendStatisticsResponse xmlns="http://ses.amazonaws.com/doc/2010-12-01/">
  <GetSendStatisticsResult>
    <SendDataPoints>
      <member>
        <Timestamp>2024-01-01T00:00:00Z</Timestamp>
        <DeliveryAttempts>%s</DeliveryAttempts>
        <Bounces>1</Bounces>
        <Complaints>2</Complaints>
        <Rejects>3</Rejects>
      </member>
    </SendDataPoints>
  </GetSendStatisticsResult>
  <ResponseMetadata><RequestId>kind-fix-test</RequestId></ResponseMetadata>
</GetSendStatisticsResponse>`, deliveryAttemptsLiteral))
}

// newRawXMLSESClient points a real aws-sdk-go-v2 SES client at a server that
// serves a fixed, caller-supplied XML body for every request -- used to
// prove a wire-shape claim about the SDK's own deserializer directly,
// independent of gopherstack's business logic (GetSendStatistics's
// DeliveryAttempts count is structurally capped by maxRetainedEmails=10000,
// far below the 1e6 threshold, so no production code path can currently
// manufacture this value; the wire-shape defect is still real -- it would
// resurface the moment that cap changes -- which is exactly why the fix
// belongs at the type level, not behind a business-logic-reachability test).
func newRawXMLSESClient(t *testing.T, body []byte) *sessdk.Client {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sessdk.NewFromConfig(cfg, func(o *sessdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetSendStatistics_DeliveryAttemptsWireKind proves the SendDataPoint
// counter fields (DeliveryAttempts/Bounces/Complaints/Rejects) must decode
// as the SDK's int64 (ses@v1.37.4 types/types.go:1208-1221, decoded via
// strconv.ParseInt at deserializers.go), not gopherstack's former float64.
// Go's encoding/xml marshals a float64 holding a whole number via
// strconv.FormatFloat(..., 'g', -1, 64), which switches to scientific
// notation ("1e+06") once the value reaches 1,000,000 -- a form
// strconv.ParseInt rejects outright, so the real client would fail to
// decode DeliveryAttempts entirely once traffic reached seven digits.
func TestGetSendStatistics_DeliveryAttemptsWireKind(t *testing.T) {
	t.Parallel()

	t.Run("digits_below_1e6_decode_either_way", func(t *testing.T) {
		t.Parallel()

		client := newRawXMLSESClient(t, getSendStatisticsResponseXML("999999"))

		out, err := client.GetSendStatistics(t.Context(), &sessdk.GetSendStatisticsInput{})
		require.NoError(t, err)
		require.Len(t, out.SendDataPoints, 1)
		assert.Equal(t, int64(999999), out.SendDataPoints[0].DeliveryAttempts)
	})

	t.Run("scientific_notation_from_old_float64_marshal_fails_to_decode", func(t *testing.T) {
		t.Parallel()

		// "1.234567e+06" is exactly what a float64-typed DeliveryAttempts field
		// holding 1234567 would have marshaled (encoding/xml's 'g' float format).
		client := newRawXMLSESClient(t, getSendStatisticsResponseXML("1.234567e+06"))

		_, err := client.GetSendStatistics(t.Context(), &sessdk.GetSendStatisticsInput{})
		require.Error(t, err, "the real SDK deserializer must reject a scientific-notation int64 field")
	})

	t.Run("plain_digits_at_and_above_1e6_decode_correctly", func(t *testing.T) {
		t.Parallel()

		client := newRawXMLSESClient(t, getSendStatisticsResponseXML("1234567"))

		out, err := client.GetSendStatistics(t.Context(), &sessdk.GetSendStatisticsInput{})
		require.NoError(t, err)
		require.Len(t, out.SendDataPoints, 1)
		assert.Equal(t, int64(1234567), out.SendDataPoints[0].DeliveryAttempts,
			"the fixed int64-typed field always marshals plain digits, which decode at any magnitude")
	})
}
