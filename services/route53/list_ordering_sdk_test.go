package route53_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func TestListHostedZonesByName_ReversedLabelOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dnsName string
		want    []string
	}{
		{name: "all", want: []string{"a.com.", "z.a.com.", "b.org."}},
		{name: "start_at_subdomain", dnsName: "z.a.com.", want: []string{"z.a.com.", "b.org."}},
		{name: "start_between", dnsName: "b.com.", want: []string{"b.org."}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRoute53Client(t, route53.NewHandler(route53.NewInMemoryBackend()))

			for _, n := range []string{"b.org.", "z.a.com.", "a.com."} {
				_, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
					Name: aws.String(n), CallerReference: aws.String("ref-" + n),
				})
				require.NoError(t, err)
			}

			in := &route53sdk.ListHostedZonesByNameInput{}
			if tt.dnsName != "" {
				in.DNSName = aws.String(tt.dnsName)
			}

			out, err := client.ListHostedZonesByName(t.Context(), in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.HostedZones))
			for _, z := range out.HostedZones {
				got = append(got, aws.ToString(z.Name))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestListResourceRecordSets_ReversedLabelOrderAndStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		startName string
		startType string
		want      []string
		wantErr   bool
	}{
		{
			name: "all",
			want: []string{"example.com.", "example.com.", "a.y.example.com.", "z.example.com."},
		},
		{
			name:      "start_name_in_reversed_order",
			startName: "a.y.example.com.",
			want:      []string{"a.y.example.com.", "z.example.com."},
		},
		{
			name:      "start_name_nonexistent_uses_next_greater",
			startName: "b.y.example.com.",
			want:      []string{"z.example.com."},
		},
		{name: "type_without_name_is_invalid", startType: "A", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRoute53Client(t, route53.NewHandler(route53.NewInMemoryBackend()))

			zone, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
				Name: aws.String("example.com."), CallerReference: aws.String("ref"),
			})
			require.NoError(t, err)

			changes := make([]types.Change, 0, 2)
			for _, n := range []string{"z.example.com.", "a.y.example.com."} {
				changes = append(changes, types.Change{
					Action: types.ChangeActionCreate,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name: aws.String(n), Type: types.RRTypeA, TTL: aws.Int64(60),
						ResourceRecords: []types.ResourceRecord{{Value: aws.String("10.0.0.1")}},
					},
				})
			}

			_, err = client.ChangeResourceRecordSets(t.Context(), &route53sdk.ChangeResourceRecordSetsInput{
				HostedZoneId: zone.HostedZone.Id, ChangeBatch: &types.ChangeBatch{Changes: changes},
			})
			require.NoError(t, err)

			in := &route53sdk.ListResourceRecordSetsInput{HostedZoneId: zone.HostedZone.Id}
			if tt.startName != "" {
				in.StartRecordName = aws.String(tt.startName)
			}

			if tt.startType != "" {
				in.StartRecordType = types.RRType(tt.startType)
			}

			out, err := client.ListResourceRecordSets(t.Context(), in)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidInput")

				return
			}

			require.NoError(t, err)

			var got []string
			for _, r := range out.ResourceRecordSets {
				got = append(got, aws.ToString(r.Name))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
