package route53_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestUpdateHostedZoneFeatures_UnknownZone_RealClient covers a missing-error
// bug: UpdateHostedZoneFeatures never validated that HostedZoneId names a
// real zone -- it always returned success unconditionally, ignoring the
// path entirely. UpdateHostedZoneFeatures's own deserializer
// (awsRestxml_deserializeOpErrorUpdateHostedZoneFeatures, route53@v1.65.6
// deserializers.go) models NoSuchHostedZone for exactly this case.
func TestUpdateHostedZoneFeatures_UnknownZone_RealClient(t *testing.T) {
	t.Parallel()

	backend := route53.NewInMemoryBackend()
	client := newTestRoute53Client(t, route53.NewHandler(backend))
	ctx := t.Context()

	_, err := client.UpdateHostedZoneFeatures(ctx, &route53sdk.UpdateHostedZoneFeaturesInput{
		HostedZoneId:              aws.String("Z_NO_SUCH_ZONE"),
		EnableAcceleratedRecovery: aws.Bool(true),
	})
	require.Error(t, err)

	var nshz *route53types.NoSuchHostedZone
	require.ErrorAs(t, err, &nshz, "expected a real NoSuchHostedZone from the SDK deserializer")
}
