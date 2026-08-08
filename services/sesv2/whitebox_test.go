package sesv2

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wantCreationTimeTagValue is the tag value newTaggableTenant/
// newTaggableEmailIdentity set at creation and
// TestCreateOpsWithTags_CreationTimeTagsRoundTrip asserts round-trips back
// -- a named const rather than a repeated literal so it doesn't tip
// goconst's threshold for the package.
const wantCreationTimeTagValue = "gopherstack-82lk"

func newTaggableTenant(t *testing.T, b *InMemoryBackend) string {
	t.Helper()

	out, err := b.CreateTenant("tag-tenant", map[string]string{"Env": wantCreationTimeTagValue})
	require.NoError(t, err)

	return out.TenantARN
}

func newTaggableEmailIdentity(t *testing.T, b *InMemoryBackend) string {
	t.Helper()

	const identity = "tag-identity@example.com"

	_, err := b.CreateEmailIdentity(identity, "", map[string]string{"Env": wantCreationTimeTagValue})
	require.NoError(t, err)

	return b.identityARN(identity)
}

// TestCreateOpsWithTags_CreationTimeTagsRoundTrip tags a resource of every
// kind gopherstack currently routes creation-time tags into b.resourceTags
// for, and reads the tags back through ListTagsForResource -- the read path
// TagResource, UntagResource and cross-service GetResources all share. This
// is the regression guard for gopherstack-82lk: CreateTenant accepted tags
// and wrote them only to the tenant's own local map, so ListTagsForResource
// (and, since sesv2 was wired into cross-service tag queries, GetResources)
// never saw them. CreateEmailIdentity had the identical bug.
func TestCreateOpsWithTags_CreationTimeTagsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *InMemoryBackend) string
		name  string
	}{
		{name: "tenant", setup: newTaggableTenant},
		{name: "email_identity", setup: newTaggableEmailIdentity},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			resourceARN := tt.setup(t, b)

			got, err := b.ListTagsForResource(resourceARN)
			require.NoError(t, err)
			assert.Equal(t, wantCreationTimeTagValue, got["Env"])
		})
	}
}

// TestCreateOps_AllCategorized walks every exported Create* method on
// InMemoryBackend and requires it to appear in exactly one of the three
// buckets below. A newly added Create* method that isn't sorted into one of
// them fails here instead of silently shipping with the same
// accepted-then-dropped tags bug as gopherstack-82lk.
func TestCreateOps_AllCategorized(t *testing.T) {
	t.Parallel()

	// fixedWithTags accept tags at creation and are proven, by
	// TestCreateOpsWithTags_CreationTimeTagsRoundTrip above, to route them
	// into b.resourceTags.
	fixedWithTags := map[string]bool{
		"CreateTenant":        true,
		"CreateEmailIdentity": true,
	}

	// knownGapWithTags accept Tags in the pinned aws-sdk-go-v2/service/sesv2
	// v1.66.4 Input struct (cited per entry) but gopherstack doesn't yet
	// route creation-time tags for them into b.resourceTags -- either the
	// backend method takes a tags param and stores it only locally
	// (multi-region endpoint), or the wire handler never decodes a Tags
	// field from the request body at all (the rest). Tracked for follow-up;
	// move an entry to fixedWithTags and give it a row above once fixed.
	knownGapWithTags := map[string]bool{
		// api_op_CreateMultiRegionEndpoint.go: Tags []types.Tag. CreateMultiRegionEndpoint
		// (multi_region_endpoints.go) stores tags on the endpoint's own map only;
		// GetMultiRegionEndpointOutput has no Tags field in the real SDK either
		// (api_op_GetMultiRegionEndpoint.go), so today they're not readable anywhere.
		// Not yet fixed here: gopherstack has no verified real-AWS ARN resource-type
		// segment for multi-region endpoints to key b.resourceTags with.
		"CreateMultiRegionEndpoint": true,
		// api_op_CreateConfigurationSet.go: Tags []types.Tag; handler_configuration_sets.go's
		// createConfigurationSetInput has no Tags field, so the wire layer drops it outright.
		"CreateConfigurationSet": true,
		// api_op_CreateContactList.go: Tags []types.Tag; handler_contact_lists.go's
		// createContactListInput has no Tags field.
		"CreateContactList": true,
		// api_op_CreateCustomVerificationEmailTemplate.go: Tags []types.Tag; no Tags field
		// in handler_custom_verification_email_templates.go's input struct.
		"CreateCustomVerificationEmailTemplate": true,
		// api_op_CreateDedicatedIpPool.go: Tags []types.Tag; no Tags field in
		// handler_dedicated_ip_pools.go's input struct.
		"CreateDedicatedIPPool": true,
		// api_op_CreateDeliverabilityTestReport.go: Tags []types.Tag; CreateDeliverabilityTestReport
		// (deliverability.go) takes no tags param at all.
		"CreateDeliverabilityTestReport": true,
		// api_op_CreateEmailTemplate.go: Tags []types.Tag; no Tags field in
		// handler_email_templates.go's input struct.
		"CreateEmailTemplate": true,
	}

	// untaggable have no Tags field on their Input struct in the pinned
	// aws-sdk-go-v2/service/sesv2 v1.66.4, confirmed per entry below --
	// listed explicitly (not omitted) so a real gap can't hide as
	// "presumably untaggable".
	untaggable := map[string]bool{
		"CreateContact":                          true, // api_op_CreateContact.go: no Tags field
		"CreateConfigurationSetEventDestination": true, // api_op_CreateConfigurationSetEventDestination.go: no Tags field
		"CreateExportJob":                        true, // api_op_CreateExportJob.go: no Tags field
		"CreateImportJob":                        true, // api_op_CreateImportJob.go: no Tags field
		"CreateTenantResourceAssociation":        true, // api_op_CreateTenantResourceAssociation.go: no Tags field
		"CreateEmailIdentityPolicy":              true, // gopherstack-internal helper; no corresponding SDK Create op
	}

	typ := reflect.TypeFor[*InMemoryBackend]()

	for m := range typ.Methods() {
		if !strings.HasPrefix(m.Name, "Create") {
			continue
		}

		buckets := 0
		for _, set := range []map[string]bool{fixedWithTags, knownGapWithTags, untaggable} {
			if set[m.Name] {
				buckets++
			}
		}

		assert.Equalf(t, 1, buckets, "%s must appear in exactly one of fixedWithTags/"+
			"knownGapWithTags/untaggable in whitebox_test.go's TestCreateOps_AllCategorized", m.Name)
	}
}
