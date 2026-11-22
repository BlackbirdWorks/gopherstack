package app

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

const (
	inventoryTestAccount = "000000000000"
	inventoryTestRegion  = "us-east-1"
	inventoryTagKey      = "Team"
	inventoryTagValue    = "emu"
)

// inventoryCase creates one resource through the service's SDK client and returns its ARN. When tagged is set
// the resource carries the Team=emu tag.
type inventoryCase struct {
	create func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string
	name   string
}

func inventoryTagMap(tagged bool) map[string]string {
	if !tagged {
		return nil
	}

	return map[string]string{inventoryTagKey: inventoryTagValue}
}

func inventoryARN(svc, resource string) string {
	return arn.Build(svc, inventoryTestRegion, inventoryTestAccount, resource)
}

func inventoryARNNoAccount(svc, resource string) string {
	return arn.Build(svc, inventoryTestRegion, "", resource)
}

type inventoryEnv struct {
	rgt *rgtapibackend.InMemoryBackend
	re  *resourceexplorer2.Client
	cfg aws.Config
}

func newInventoryEnv(t *testing.T) *inventoryEnv {
	t.Helper()

	fx := newSFNFixture(t)
	byName := serviceByName(fx.services)

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok)

	rgt, ok := rgtH.Backend.(*rgtapibackend.InMemoryBackend)
	require.True(t, ok)

	re := resourceexplorer2.NewFromConfig(fx.cfg)

	_, err := re.CreateIndex(t.Context(), &resourceexplorer2.CreateIndexInput{})
	require.NoError(t, err)

	view, err := re.CreateView(t.Context(), &resourceexplorer2.CreateViewInput{ViewName: aws.String("all")})
	require.NoError(t, err)

	_, err = re.AssociateDefaultView(
		t.Context(),
		&resourceexplorer2.AssociateDefaultViewInput{ViewArn: view.View.ViewArn},
	)
	require.NoError(t, err)

	return &inventoryEnv{rgt: rgt, re: re, cfg: fx.cfg}
}

func (e *inventoryEnv) taggedARNs(ctx context.Context, t *testing.T) map[string]bool {
	t.Helper()

	out, err := e.rgt.GetResources(ctx, &rgtapibackend.GetResourcesInput{})
	require.NoError(t, err)

	arns := make(map[string]bool, len(out.ResourceTagMappingList))
	for _, r := range out.ResourceTagMappingList {
		arns[r.ResourceARN] = true
	}

	return arns
}

func (e *inventoryEnv) inventoryARNs(ctx context.Context) map[string]rgtapibackend.TaggedResource {
	all := e.rgt.AllResources(ctx)
	out := make(map[string]rgtapibackend.TaggedResource, len(all))

	for _, r := range all {
		out[r.ResourceARN] = r
	}

	return out
}

func (e *inventoryEnv) searchARNs(ctx context.Context, t *testing.T, keyword string) []string {
	t.Helper()

	out, err := e.re.Search(ctx, &resourceexplorer2.SearchInput{
		QueryString: aws.String(keyword), MaxResults: aws.Int32(100),
	})
	require.NoError(t, err)

	arns := make([]string, 0, len(out.Resources))
	for _, r := range out.Resources {
		arns = append(arns, aws.ToString(r.Arn))
	}

	return arns
}

// TestInventoryUntaggedResources creates one untagged and one tagged resource per service through the real SDK
// and checks that the tagging aggregation keeps only the tagged one while the inventory and Resource Explorer
// return both.
func TestInventoryUntaggedResources(t *testing.T) {
	t.Parallel()

	env := newInventoryEnv(t)
	ctx := t.Context()

	type created struct{ untagged, tagged string }

	arns := make(map[string]created)

	for _, tc := range inventoryCases() {
		arns[tc.name] = created{
			untagged: tc.create(ctx, t, env.cfg, "inv-"+tc.name+"-plain", false),
			tagged:   tc.create(ctx, t, env.cfg, "inv-"+tc.name+"-tagged", true),
		}
	}

	taggedSet := env.taggedARNs(ctx, t)
	inv := env.inventoryARNs(ctx)

	for _, tc := range inventoryCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := arns[tc.name]

			require.NotEmpty(t, c.untagged)
			require.NotEmpty(t, c.tagged)

			assert.True(t, taggedSet[c.tagged], "tagging API must list the tagged %s", c.tagged)
			assert.False(t, taggedSet[c.untagged], "tagging API must not list the untagged %s", c.untagged)

			assert.Contains(t, inv, c.untagged)
			assert.Contains(t, inv, c.tagged)
			assert.Equal(t, inventoryTagValue, inv[c.tagged].Tags[inventoryTagKey])
			assert.Empty(t, inv[c.untagged].Tags)

			found := env.searchARNs(t.Context(), t, lastARNSegment(c.untagged))
			assert.Contains(t, found, c.untagged, "Resource Explorer must find the untagged resource")
		})
	}
}

func lastARNSegment(arnStr string) string {
	if i := strings.LastIndexAny(arnStr, "/:"); i >= 0 {
		return arnStr[i+1:]
	}

	return arnStr
}

var inventoryResourceTypeRe = regexp.MustCompile(`^[a-z0-9-]+(:[A-Za-z0-9_./-]+)?$`)

// TestInventoryContract holds every inventory record to the tagging aggregation's shape: a superset of the
// tagged listing, with a parseable ARN and a service-prefixed resource type.
func TestInventoryContract(t *testing.T) {
	t.Parallel()

	env := newInventoryEnv(t)
	ctx := t.Context()

	for _, tc := range inventoryCases() {
		tc.create(ctx, t, env.cfg, "ic-"+tc.name+"-plain", false)
		tc.create(ctx, t, env.cfg, "ic-"+tc.name+"-tagged", true)
	}

	tagged := env.taggedARNs(ctx, t)
	inv := env.inventoryARNs(ctx)

	require.NotEmpty(t, inv)

	for taggedARN := range tagged {
		assert.Contains(t, inv, taggedARN, "inventory must be a superset of the tagged listing")
	}

	for arnStr, r := range inv {
		parsed, err := awsarn.Parse(arnStr)
		require.NoError(t, err, arnStr)

		assert.NotEmpty(t, parsed.Service, arnStr)
		assert.NotEmpty(t, parsed.Resource, arnStr)
		assert.Regexp(t, inventoryResourceTypeRe, r.ResourceType, arnStr)
		assert.True(t, strings.HasPrefix(r.ResourceType, parsed.Service), "%s: type %q", arnStr, r.ResourceType)
	}
}

// inventoryRecorder counts the providers and inventory providers a wiring pass registers.
type inventoryRecorder struct {
	rgtapibackend.StorageBackend
	providers int
	inventory int
}

func (r *inventoryRecorder) RegisterProvider(p rgtapibackend.ResourceProvider) {
	r.providers++
	r.StorageBackend.RegisterProvider(p)
}

func (r *inventoryRecorder) RegisterFilteredProvider(p rgtapibackend.FilteredResourceProvider) {
	r.providers++
	r.StorageBackend.RegisterFilteredProvider(p)
}

func (r *inventoryRecorder) RegisterInventoryProvider(p rgtapibackend.ResourceProvider) {
	r.inventory++

	if reg, ok := r.StorageBackend.(inventoryRegistrar); ok {
		reg.RegisterInventoryProvider(p)
	}
}

// inventoryExemptProviders names the tagging providers whose listing already returns every resource of the
// service, so they register no separate inventory provider.
func inventoryExemptProviders() map[string]string {
	return map[string]string{
		"DynamoDB":       "TaggedTables lists every table",
		"SQS":            "TaggedQueues lists every queue",
		"SNS":            "TaggedTopics lists every topic",
		"Lambda":         "TaggedFunctions lists every function",
		"KMS":            "TaggedKeys lists every key",
		"SecretsManager": "TaggedSecrets lists every live secret",
		"S3":             "TaggedResources lists every bucket",
	}
}

// TestTaggingWiringRegistersInventory fails when a service is wired into the tagging aggregation without an
// inventory source, unless it is one of the providers that already list every resource.
func TestTaggingWiringRegistersInventory(t *testing.T) {
	t.Parallel()

	byName, rgtH := newRGTWiringEnv(t)
	rec := &inventoryRecorder{
		StorageBackend: rgtapibackend.NewInMemoryBackend(inventoryTestAccount, inventoryTestRegion),
	}

	wireResourceGroupsTagging(&rgtapibackend.Handler{Backend: rec}, byName)

	require.NotNil(t, rgtH)
	assert.Positive(t, rec.providers)
	assert.Equal(t, len(inventoryExemptProviders()), rec.providers-rec.inventory,
		"every wired provider except the exempt ones needs an inventory provider (providers=%d inventory=%d)",
		rec.providers, rec.inventory)
}
