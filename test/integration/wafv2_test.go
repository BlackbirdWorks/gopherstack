package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	wafv2types "github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_Wafv2_CreateAndListWebACL(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createWafv2Client(t)
	ctx := t.Context()
	name := "integ-wafv2-" + uuid.NewString()[:8]

	tests := []struct {
		name    string
		aclName string
		scope   wafv2types.Scope
		wantErr bool
	}{
		{
			name:    "create_regional",
			aclName: name,
			scope:   wafv2types.ScopeRegional,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.CreateWebACL(ctx, &wafv2sdk.CreateWebACLInput{
				Name:  aws.String(tt.aclName),
				Scope: tt.scope,
				DefaultAction: &wafv2types.DefaultAction{
					Allow: &wafv2types.AllowAction{},
				},
				VisibilityConfig: &wafv2types.VisibilityConfig{
					CloudWatchMetricsEnabled: false,
					MetricName:               aws.String(tt.aclName),
					SampledRequestsEnabled:   false,
				},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Summary)
			assert.NotEmpty(t, out.Summary.Id)
			assert.Equal(t, tt.aclName, aws.ToString(out.Summary.Name))

			listOut, err := client.ListWebACLs(ctx, &wafv2sdk.ListWebACLsInput{
				Scope: tt.scope,
			})
			require.NoError(t, err)

			assert.True(t, webACLInList(listOut.WebACLs, tt.aclName), "created WebACL should appear in ListWebACLs")
		})
	}
}

func TestIntegration_Wafv2_CreateIPSet(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createWafv2Client(t)
	ctx := t.Context()
	name := "integ-ipset-" + uuid.NewString()[:8]

	tests := []struct {
		name      string
		setName   string
		scope     wafv2types.Scope
		ipVersion wafv2types.IPAddressVersion
		addresses []string
		wantErr   bool
	}{
		{
			name:      "create_ipv4_set",
			setName:   name,
			scope:     wafv2types.ScopeRegional,
			ipVersion: wafv2types.IPAddressVersionIpv4,
			addresses: []string{"192.0.2.0/24"},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.CreateIPSet(ctx, &wafv2sdk.CreateIPSetInput{
				Name:             aws.String(tt.setName),
				Scope:            tt.scope,
				IPAddressVersion: tt.ipVersion,
				Addresses:        tt.addresses,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Summary)
			assert.NotEmpty(t, out.Summary.Id)
			assert.Equal(t, tt.setName, aws.ToString(out.Summary.Name))
		})
	}
}

// webACLInList returns true if a WebACL with the given name exists in the list.
func webACLInList(list []wafv2types.WebACLSummary, name string) bool {
	for _, w := range list {
		if aws.ToString(w.Name) == name {
			return true
		}
	}

	return false
}
