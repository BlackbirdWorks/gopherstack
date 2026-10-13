package route53_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func aRecord(action types.ChangeAction, name, ip string) types.Change {
	return types.Change{
		Action: action,
		ResourceRecordSet: &types.ResourceRecordSet{
			Name: aws.String(name), Type: types.RRTypeA, TTL: aws.Int64(60),
			ResourceRecords: []types.ResourceRecord{{Value: aws.String(ip)}},
		},
	}
}

func TestDeepFlows_ChangeBatchSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		changes     []types.Change
		wantPresent []string
		wantAbsent  []string
		wantErr     bool
	}{
		{
			name: "two_creates_distinct",
			changes: []types.Change{
				aRecord("CREATE", "a.example.com", "1.1.1.1"),
				aRecord("CREATE", "b.example.com", "1.1.1.2"),
			},
			wantPresent: []string{"a.example.com.", "b.example.com."},
		},
		{
			name: "duplicate_create_in_batch_is_atomic",
			changes: []types.Change{
				aRecord("CREATE", "a.example.com", "1.1.1.1"),
				aRecord("CREATE", "a.example.com", "1.1.1.1"),
			},
			wantErr:    true,
			wantAbsent: []string{"a.example.com."},
		},
		{
			name: "create_then_upsert",
			changes: []types.Change{
				aRecord("CREATE", "a.example.com", "1.1.1.1"),
				aRecord("UPSERT", "a.example.com", "1.1.1.9"),
			},
			wantPresent: []string{"a.example.com."},
		},
		{
			name:       "outside_zone",
			changes:    []types.Change{aRecord("CREATE", "www.other.org", "1.1.1.1")},
			wantErr:    true,
			wantAbsent: []string{"www.other.org."},
		},
		{
			name:       "sibling_suffix_is_not_in_zone",
			changes:    []types.Change{aRecord("CREATE", "notexample.com", "1.1.1.1")},
			wantErr:    true,
			wantAbsent: []string{"notexample.com."},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRoute53Client(t, route53.NewHandler(route53.NewInMemoryBackend()))

			zone, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
				Name: aws.String("example.com"), CallerReference: aws.String("ref"),
			})
			require.NoError(t, err)

			_, err = client.ChangeResourceRecordSets(t.Context(), &route53sdk.ChangeResourceRecordSetsInput{
				HostedZoneId: zone.HostedZone.Id,
				ChangeBatch:  &types.ChangeBatch{Changes: tt.changes},
			})
			if tt.wantErr {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "InvalidChangeBatch", apiErr.ErrorCode())
			} else {
				require.NoError(t, err)
			}

			list, err := client.ListResourceRecordSets(t.Context(), &route53sdk.ListResourceRecordSetsInput{
				HostedZoneId: zone.HostedZone.Id,
			})
			require.NoError(t, err)

			names := make(map[string]bool)
			for _, rr := range list.ResourceRecordSets {
				names[aws.ToString(rr.Name)] = true
			}

			for _, n := range tt.wantPresent {
				assert.True(t, names[n], n)
			}

			for _, n := range tt.wantAbsent {
				assert.False(t, names[n], n)
			}
		})
	}
}
