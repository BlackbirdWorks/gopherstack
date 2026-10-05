package vpclattice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticetypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientToken_ReplayAndMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(c *vpclatticesdk.Client, name string, tok *string) (*string, error)
		name   string
	}{
		{name: "service", create: func(c *vpclatticesdk.Client, name string, tok *string) (*string, error) {
			o, err := c.CreateService(t.Context(), &vpclatticesdk.CreateServiceInput{Name: &name, ClientToken: tok})
			if err != nil {
				return nil, err
			}

			return o.Id, nil
		}},
		{name: "service_network", create: func(c *vpclatticesdk.Client, name string, tok *string) (*string, error) {
			o, err := c.CreateServiceNetwork(
				t.Context(), &vpclatticesdk.CreateServiceNetworkInput{Name: &name, ClientToken: tok},
			)
			if err != nil {
				return nil, err
			}

			return o.Id, nil
		}},
		{name: "target_group", create: func(c *vpclatticesdk.Client, name string, tok *string) (*string, error) {
			o, err := c.CreateTargetGroup(t.Context(), &vpclatticesdk.CreateTargetGroupInput{
				Name: &name, Type: vpclatticetypes.TargetGroupTypeLambda, ClientToken: tok,
			})
			if err != nil {
				return nil, err
			}

			return o.Id, nil
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			tok := aws.String("0123456789-token")

			id1, err := tt.create(c, "res-a", tok)
			require.NoError(t, err)
			id2, err := tt.create(c, "res-a", tok)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(id1), aws.ToString(id2))

			_, err = tt.create(c, "res-b", tok)
			var ae smithy.APIError
			require.ErrorAs(t, err, &ae)
			assert.Equal(t, "ConflictException", ae.ErrorCode())
		})
	}
}

func TestServiceNetwork_SharingConfigRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sharing *vpclatticetypes.SharingConfig
		want    *bool
		name    string
	}{
		{name: "enabled", sharing: &vpclatticetypes.SharingConfig{Enabled: aws.Bool(true)}, want: aws.Bool(true)},
		{name: "disabled", sharing: &vpclatticetypes.SharingConfig{Enabled: aws.Bool(false)}, want: aws.Bool(false)},
		{name: "unset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			created, err := c.CreateServiceNetwork(t.Context(), &vpclatticesdk.CreateServiceNetworkInput{
				Name: aws.String("sn"), SharingConfig: tt.sharing,
			})
			require.NoError(t, err)

			got, err := c.GetServiceNetwork(t.Context(), &vpclatticesdk.GetServiceNetworkInput{
				ServiceNetworkIdentifier: created.Id,
			})
			require.NoError(t, err)

			for _, cfg := range []*vpclatticetypes.SharingConfig{created.SharingConfig, got.SharingConfig} {
				if tt.want == nil {
					assert.Nil(t, cfg)

					continue
				}

				require.NotNil(t, cfg)
				assert.Equal(t, *tt.want, aws.ToBool(cfg.Enabled))
			}
		})
	}
}

func TestListResourceConfigurations_DomainVerificationFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  []string
		match bool
	}{
		{name: "matching_id", match: true, want: []string{"rc-verified"}},
		{name: "other_id", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			dv, err := c.StartDomainVerification(t.Context(), &vpclatticesdk.StartDomainVerificationInput{
				DomainName: aws.String("verify.example.com"),
			})
			require.NoError(t, err)

			for _, in := range []*vpclatticesdk.CreateResourceConfigurationInput{
				{
					Name: aws.String("rc-verified"), Type: vpclatticetypes.ResourceConfigurationTypeSingle,
					DomainVerificationIdentifier: dv.Id,
				},
				{Name: aws.String("rc-plain"), Type: vpclatticetypes.ResourceConfigurationTypeSingle},
			} {
				_, err = c.CreateResourceConfiguration(t.Context(), in)
				require.NoError(t, err)
			}

			id := dv.Id
			if !tt.match {
				id = aws.String("dv-other")
			}

			out, err := c.ListResourceConfigurations(t.Context(), &vpclatticesdk.ListResourceConfigurationsInput{
				DomainVerificationIdentifier: id,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Items))
			for _, it := range out.Items {
				got = append(got, aws.ToString(it.Name))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
