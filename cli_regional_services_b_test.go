package main

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	kinesisvideobackend "github.com/blackbirdworks/gopherstack/services/kinesisvideo"
	mgnbackend "github.com/blackbirdworks/gopherstack/services/mgn"
	personalizebackend "github.com/blackbirdworks/gopherstack/services/personalize"
	rekognitionbackend "github.com/blackbirdworks/gopherstack/services/rekognition"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"
	timestreamwritebackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

func TestInitializeServices_TaggingBridgeFollowsRegionForRegionalServices(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	euCtx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: euRegion, Account: crossAcct})
	tags := map[string]string{"Team": "eu"}

	tests := []struct {
		create func(t *testing.T) string
		name   string
	}{
		{
			name: "rekognition",
			create: func(t *testing.T) string {
				t.Helper()

				h, ok := byName["Rekognition"].(*rekognitionbackend.Handler)
				require.True(t, ok)

				bk, ok := h.BackendFor(euRegion).(*rekognitionbackend.InMemoryBackend)
				require.True(t, ok)

				c, err := bk.CreateCollection("eu-collection", tags)
				require.NoError(t, err)

				return c.CollectionARN
			},
		},
		{
			name: "personalize",
			create: func(t *testing.T) string {
				t.Helper()

				h, ok := byName["Personalize"].(*personalizebackend.Handler)
				require.True(t, ok)

				dg, err := h.BackendFor(euRegion).CreateDatasetGroup("eu-dg", "", "", "", tags)
				require.NoError(t, err)

				return dg.DatasetGroupArn
			},
		},
		{
			name: "swf",
			create: func(t *testing.T) string {
				t.Helper()

				h, ok := byName["SWF"].(*swfbackend.Handler)
				require.True(t, ok)

				bk, ok := h.BackendFor(euRegion).(*swfbackend.InMemoryBackend)
				require.True(t, ok)
				require.NoError(t, bk.RegisterDomain("eu-domain", "", "1"))

				d, err := bk.DescribeDomain("eu-domain")
				require.NoError(t, err)
				require.NoError(t, bk.TagResource(d.Arn, tags))

				return d.Arn
			},
		},
		{
			name: "mgn",
			create: func(t *testing.T) string {
				t.Helper()

				h, ok := byName["MGN"].(*mgnbackend.Handler)
				require.True(t, ok)

				bk := h.BackendFor(euRegion)
				bk.InitializeService()

				a, err := bk.CreateApplication("eu-app", "", tags)
				require.NoError(t, err)

				return a.Arn
			},
		},
	}

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			arn := tc.create(t)
			in := &rgtapibackend.GetResourcesInput{
				TagFilters: []rgtapibackend.TagFilter{{Key: "Team", Values: []string{"eu"}}},
			}

			euOut, err := rgtH.Backend.GetResources(euCtx, in)
			require.NoError(t, err)

			homeOut, err := rgtH.Backend.GetResources(t.Context(), in)
			require.NoError(t, err)

			assert.Contains(t, taggedARNs(euOut), arn)
			assert.NotContains(t, taggedARNs(homeOut), arn)
		})
	}
}

func TestTimestreamTagWriter_FollowsARNRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	h, ok := byName["TimestreamWrite"].(*timestreamwritebackend.Handler)
	require.True(t, ok)

	const euARN = "arn:aws:timestream:" + euRegion + ":" + crossAcct + ":scheduled-query/q"

	w := &timestreamTagWriter{handler: h}
	require.NoError(t, w.TagResource(euARN, map[string]string{"k": "v"}))

	assert.Equal(t, map[string]string{"k": "v"}, h.BackendFor(euRegion).ListTagsForResource(euARN))
	assert.Empty(t, h.Backend.ListTagsForResource(euARN))

	require.NoError(t, w.UntagResource(euARN, []string{"k"}))
	assert.Empty(t, h.BackendFor(euRegion).ListTagsForResource(euARN))
}

const cfnServicesBTemplate = `{"Resources":{
"S":{"Type":"AWS::KinesisVideo::Stream","Properties":{"Name":"cfn-stream","DataRetentionInHours":1}},
"D":{"Type":"AWS::SWF::Domain","Properties":{"Name":"cfn-domain","WorkflowExecutionRetentionPeriodInDays":"1"}}}}`

func TestInitializeServices_CloudFormationProvisionsServicesBInStackRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	cfnH, ok := byName["CloudFormation"].(*cfnbackend.Handler)
	require.True(t, ok)

	kvH, ok := byName["KinesisVideo"].(*kinesisvideobackend.Handler)
	require.True(t, ok)

	swfH, ok := byName["SWF"].(*swfbackend.Handler)
	require.True(t, ok)

	regionFormCall(t, cfnH.Handler(), usWest2, url.Values{
		"Action": {"CreateStack"}, "Version": {"2010-05-15"}, "StackName": {"west-services-b"},
		"TemplateBody": {cfnServicesBTemplate},
	})

	streams := func(region string) int {
		out := regionRESTCall(t, kvH.Handler(), region, http.MethodPost, "/listStreams", `{}`)
		list, _ := out["StreamInfoList"].([]any)

		return len(list)
	}

	domains := func(region string) int {
		bk, isSWF := swfH.BackendFor(region).(*swfbackend.InMemoryBackend)
		require.True(t, isSWF)

		list, err := bk.ListDomains("REGISTERED")
		require.NoError(t, err)

		return len(list)
	}

	tests := []struct {
		name   string
		region string
		want   int
	}{
		{name: "us-west-2", region: usWest2, want: 1},
		{name: "us-east-1", region: crossHome},
		{name: "eu-west-1", region: euRegion},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, streams(tc.region), "kinesisvideo")
			assert.Equal(t, tc.want, domains(tc.region), "swf")
		})
	}
}
