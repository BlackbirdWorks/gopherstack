package resourcegroupstaggingapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/organizations"
	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

type orgConfig struct{ h service.Registerable }

func (c orgConfig) GetOrganizationsHandler() service.Registerable { return c.h }

func TestProviderWiresOrganizationsTagPolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"tags":{"CostCenter":{"report_required_tag_for":{"@@assign":["ec2:instance"]}}}}`

	tests := []struct {
		name      string
		wantTypes []string
		attach    bool
	}{
		{name: "attached_policy", attach: true, wantTypes: []string{"ec2:instance"}},
		{name: "no_policy", attach: false, wantTypes: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			orgs := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			_, root, err := orgs.CreateOrganization("ALL")
			require.NoError(t, err)

			if tt.attach {
				_, err = orgs.EnablePolicyType(root.ID, "TAG_POLICY")
				require.NoError(t, err)

				p, perr := orgs.CreatePolicy("tp", "", policy, "TAG_POLICY", nil)
				require.NoError(t, perr)
				require.NoError(t, orgs.AttachPolicy(p.PolicySummary.ID, root.ID))
			}

			cfg := orgConfig{h: organizations.NewHandler(orgs)}
			reg, err := (&resourcegroupstaggingapi.Provider{}).Init(&service.AppContext{Config: cfg})
			require.NoError(t, err)

			h, ok := reg.(*resourcegroupstaggingapi.Handler)
			require.True(t, ok)

			out := h.Backend.ListRequiredTags(context.Background(), &resourcegroupstaggingapi.ListRequiredTagsInput{})
			got := make([]string, 0, len(out.RequiredTags))

			for _, rt := range out.RequiredTags {
				got = append(got, *rt.ResourceType)
			}

			assert.Equal(t, tt.wantTypes, got)
		})
	}
}
