package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// This file covers gopherstack-uox6 (value-semantics bugs invisible to
// shape-based sweeps) for QuickSight's Search* filter surface. Every case
// seeds at least two records that differ on the filtered attribute and
// asserts both that the matching record comes back AND that the
// non-matching one is excluded -- a single-record fixture can't distinguish
// "filtered correctly" from "returned everything".

// TestSearchActionConnectors_TypeFilter covers ACTION_CONNECTOR_TYPE
// (ActionConnectorSearchFilterNameEnum, quicksight@v1.123.1 types/types.go):
// actionConnectorMatchesFilters (actionconnector.go) previously checked only
// ACTION_CONNECTOR_NAME via matchesAllNameFilters and passed every other
// filter Name through unconditionally -- including ACTION_CONNECTOR_TYPE,
// even though storedActionConnector.Type is a plain tracked field, not an
// untracked ownership ARN. A client filtering by connector type got every
// connector back regardless of type.
func TestSearchActionConnectors_TypeFilter(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	authConfig := map[string]any{"AuthenticationType": "NO_AUTH"}
	_, err := backend.CreateActionConnector(
		"000000000000", "ac-http", "HTTP Connector", "GENERIC_HTTP", "", "", authConfig, nil, nil,
	)
	require.NoError(t, err)
	_, err = backend.CreateActionConnector(
		"000000000000", "ac-jira", "Jira Connector", "JIRA_CLOUD", "", "", authConfig, nil, nil,
	)
	require.NoError(t, err)

	out, err := client.SearchActionConnectors(ctx, &quicksightsdk.SearchActionConnectorsInput{
		AwsAccountId: aws.String("000000000000"),
		Filters: []types.ActionConnectorSearchFilter{
			{
				Name:     types.ActionConnectorSearchFilterNameEnumActionConnectorType,
				Operator: types.FilterOperatorStringEquals,
				Value:    aws.String("JIRA_CLOUD"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.ActionConnectorSummaries, 1)
	assert.Equal(t, "ac-jira", aws.ToString(out.ActionConnectorSummaries[0].ActionConnectorId))
}

// TestSearchFlows_DescriptionFilter covers FieldName's assetDescription
// (quicksight@v1.123.1 types/enums.go): flowMatchesFilters (flow.go)
// previously checked only assetName via matchesAllNameFilters and passed
// assetDescription through unconditionally, even though
// storedFlow.Description is tracked. A client filtering by description got
// every flow back regardless of description.
func TestSearchFlows_DescriptionFilter(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	def := map[string]any{"steps": []any{}}
	_, err := backend.CreateFlow("000000000000", "Nightly ETL", "runs the nightly ingestion pipeline", def, nil)
	require.NoError(t, err)
	_, err = backend.CreateFlow("000000000000", "Weekly Report", "emails a weekly summary", def, nil)
	require.NoError(t, err)

	out, err := client.SearchFlows(ctx, &quicksightsdk.SearchFlowsInput{
		AwsAccountId: aws.String("000000000000"),
		Filters: []types.SearchFlowsFilter{
			{
				Name:     types.FieldNameFlowDescription,
				Operator: types.SearchFilterOperatorStringLike,
				Value:    aws.String("ingestion"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.FlowSummaryList, 1)
	assert.Equal(t, "Nightly ETL", aws.ToString(out.FlowSummaryList[0].Name))
}

// TestSearchKnowledgeBases_FilterSemantics covers three related
// KnowledgeBaseSearchFilter bugs (knowledgebases.go), all found reading this
// operation's own types rather than a sibling's:
//
//   - KnowledgeBaseSearchFilterName documents KNOWLEDGE_BASE_ID,
//     DATASOURCE_ARN, PRIMARY_OWNER and KNOWLEDGE_BASE_SIZE_BYTES alongside
//     KNOWLEDGE_BASE_NAME; only the name filter was checked, the rest passed
//     through unconditionally despite being plain tracked fields.
//   - KnowledgeBaseSearchOperator's wire values ("STRING_EQUALS",
//     "STRING_LIKE", "GREATER_THAN_OR_EQUALS", "LESS_THAN_OR_EQUALS") are
//     uppercase-underscore, unlike FilterOperator's ("StringEquals",
//     "StringLike") used by every other Search op here. The shared
//     matchesNameFilter compared against "StringLike", which the wire never
//     sends for this operation, so STRING_LIKE requests silently fell back
//     to exact-equality comparison even for the one filter (name) that was
//     implemented.
func TestSearchKnowledgeBases_FilterSemantics(t *testing.T) {
	t.Parallel()

	newBackend := func(t *testing.T) (*quicksight.InMemoryBackend, *quicksightsdk.Client) {
		t.Helper()

		backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
		h := quicksight.NewHandler(backend)

		return backend, newTestQuickSightClient(t, h)
	}

	t.Run("id filter", func(t *testing.T) {
		t.Parallel()

		backend, client := newBackend(t)
		ctx := t.Context()

		_, err := backend.CreateKnowledgeBase(
			"000000000000", "kb-support", "Support KB", "", "arn:aws:s3:::b1", "", nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)
		_, err = backend.CreateKnowledgeBase(
			"000000000000", "kb-billing", "Billing KB", "", "arn:aws:s3:::b2", "", nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)

		out, err := client.SearchKnowledgeBases(ctx, &quicksightsdk.SearchKnowledgeBasesInput{
			AwsAccountId: aws.String("000000000000"),
			Filters: []types.KnowledgeBaseSearchFilter{
				{
					Name:     types.KnowledgeBaseSearchFilterNameKnowledgeBaseId,
					Operator: types.KnowledgeBaseSearchOperatorStringEquals,
					Value:    aws.String("kb-support"),
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.KnowledgeBaseSummaries, 1)
		assert.Equal(t, "kb-support", aws.ToString(out.KnowledgeBaseSummaries[0].KnowledgeBaseId))
	})

	t.Run("datasource arn filter", func(t *testing.T) {
		t.Parallel()

		backend, client := newBackend(t)
		ctx := t.Context()

		_, err := backend.CreateKnowledgeBase(
			"000000000000", "kb1", "KB One", "", "arn:aws:s3:::alpha", "", nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)
		_, err = backend.CreateKnowledgeBase(
			"000000000000", "kb2", "KB Two", "", "arn:aws:s3:::beta", "", nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)

		out, err := client.SearchKnowledgeBases(ctx, &quicksightsdk.SearchKnowledgeBasesInput{
			AwsAccountId: aws.String("000000000000"),
			Filters: []types.KnowledgeBaseSearchFilter{
				{
					Name:     types.KnowledgeBaseSearchFilterNameDatasourceArn,
					Operator: types.KnowledgeBaseSearchOperatorStringEquals,
					Value:    aws.String("arn:aws:s3:::beta"),
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.KnowledgeBaseSummaries, 1)
		assert.Equal(t, "kb2", aws.ToString(out.KnowledgeBaseSummaries[0].KnowledgeBaseId))
	})

	t.Run("primary owner filter", func(t *testing.T) {
		t.Parallel()

		backend, client := newBackend(t)
		ctx := t.Context()

		ownerArn := "arn:aws:quicksight:us-east-1:000000000000:user/default/alice"
		_, err := backend.CreateKnowledgeBase(
			"000000000000", "kb-mine", "Mine", "", "arn:aws:s3:::b", ownerArn, nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)
		otherOwnerArn := "arn:aws:quicksight:us-east-1:000000000000:user/default/bob"
		_, err = backend.CreateKnowledgeBase(
			"000000000000", "kb-other", "Other", "", "arn:aws:s3:::b", otherOwnerArn, nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)

		out, err := client.SearchKnowledgeBases(ctx, &quicksightsdk.SearchKnowledgeBasesInput{
			AwsAccountId: aws.String("000000000000"),
			Filters: []types.KnowledgeBaseSearchFilter{
				{
					Name:     types.KnowledgeBaseSearchFilterNamePrimaryOwner,
					Operator: types.KnowledgeBaseSearchOperatorStringEquals,
					Value:    aws.String(ownerArn),
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.KnowledgeBaseSummaries, 1)
		assert.Equal(t, "kb-mine", aws.ToString(out.KnowledgeBaseSummaries[0].KnowledgeBaseId))
	})

	t.Run("size bytes GTE and LTE", func(t *testing.T) {
		t.Parallel()

		backend, client := newBackend(t)
		ctx := t.Context()

		// CreateKnowledgeBase has no request field for
		// KnowledgeBaseSizeBytes (it's computed from ingested documents),
		// so every KB this backend creates starts at size 0. That still
		// distinguishes the two operators: >=0 must include it, >=1 must
		// exclude it, and the mirror image for <=.
		_, err := backend.CreateKnowledgeBase(
			"000000000000", "kb1", "KB", "", "arn:aws:s3:::b", "", nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)

		gteZero, err := client.SearchKnowledgeBases(ctx, &quicksightsdk.SearchKnowledgeBasesInput{
			AwsAccountId: aws.String("000000000000"),
			Filters: []types.KnowledgeBaseSearchFilter{
				{
					Name:     types.KnowledgeBaseSearchFilterNameKnowledgeBaseSizeBytes,
					Operator: types.KnowledgeBaseSearchOperatorGreaterThanOrEquals,
					Value:    aws.String("0"),
				},
			},
		})
		require.NoError(t, err)
		assert.Len(t, gteZero.KnowledgeBaseSummaries, 1, "size 0 >= 0 must match")

		gteOne, err := client.SearchKnowledgeBases(ctx, &quicksightsdk.SearchKnowledgeBasesInput{
			AwsAccountId: aws.String("000000000000"),
			Filters: []types.KnowledgeBaseSearchFilter{
				{
					Name:     types.KnowledgeBaseSearchFilterNameKnowledgeBaseSizeBytes,
					Operator: types.KnowledgeBaseSearchOperatorGreaterThanOrEquals,
					Value:    aws.String("1"),
				},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, gteOne.KnowledgeBaseSummaries, "size 0 >= 1 must not match")

		lteMinusOne, err := client.SearchKnowledgeBases(ctx, &quicksightsdk.SearchKnowledgeBasesInput{
			AwsAccountId: aws.String("000000000000"),
			Filters: []types.KnowledgeBaseSearchFilter{
				{
					Name:     types.KnowledgeBaseSearchFilterNameKnowledgeBaseSizeBytes,
					Operator: types.KnowledgeBaseSearchOperatorLessThanOrEquals,
					Value:    aws.String("-1"),
				},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, lteMinusOne.KnowledgeBaseSummaries, "size 0 <= -1 must not match")
	})

	t.Run("name filter honors STRING_LIKE substring match", func(t *testing.T) {
		t.Parallel()

		backend, client := newBackend(t)
		ctx := t.Context()

		_, err := backend.CreateKnowledgeBase(
			"000000000000", "kb-support", "Support KB", "", "arn:aws:s3:::b", "", nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)
		_, err = backend.CreateKnowledgeBase(
			"000000000000", "kb-billing", "Billing KB", "", "arn:aws:s3:::b", "", nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)

		out, err := client.SearchKnowledgeBases(ctx, &quicksightsdk.SearchKnowledgeBasesInput{
			AwsAccountId: aws.String("000000000000"),
			Filters: []types.KnowledgeBaseSearchFilter{
				{
					Name:     types.KnowledgeBaseSearchFilterNameKnowledgeBaseName,
					Operator: types.KnowledgeBaseSearchOperatorStringLike,
					Value:    aws.String("Support"),
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.KnowledgeBaseSummaries, 1)
		assert.Equal(t, "kb-support", aws.ToString(out.KnowledgeBaseSummaries[0].KnowledgeBaseId))
	})
}

// TestSearchSpaces_IDFilter covers SPACE_ID
// (SpaceQuickSightSearchFilterName, quicksight@v1.123.1 types/enums.go):
// spaceMatchesFilters (spaces.go) previously checked only SPACE_NAME via
// matchesAllNameFilters and passed SPACE_ID through unconditionally, even
// though storedSpace.SpaceID is a plain tracked field.
func TestSearchSpaces_IDFilter(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	_, err := backend.CreateSpace("000000000000", "space-support", "Support", "")
	require.NoError(t, err)
	_, err = backend.CreateSpace("000000000000", "space-billing", "Billing", "")
	require.NoError(t, err)

	out, err := client.SearchSpaces(ctx, &quicksightsdk.SearchSpacesInput{
		AwsAccountId: aws.String("000000000000"),
		Filters: []types.SpaceQuicksightSearchFilter{
			{
				Name:     types.SpaceQuickSightSearchFilterNameSpaceId,
				Operator: types.SpaceSearchOperatorStringEquals,
				Value:    aws.String("space-billing"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.SpaceSummaries, 1)
	assert.Equal(t, "space-billing", aws.ToString(out.SpaceSummaries[0].SpaceId))
}

// TestSearchActionConnectors_MultipleFiltersAND proves multiple filters
// combine with AND (matching every other Search op's matchesAllNameFilters/
// actionConnectorMatchesFilters loop): a name filter that matches one
// connector combined with a type filter that matches a different connector
// must match neither.
func TestSearchActionConnectors_MultipleFiltersAND(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	authConfig := map[string]any{"AuthenticationType": "NO_AUTH"}
	_, err := backend.CreateActionConnector(
		"000000000000", "ac-http", "HTTP Connector", "GENERIC_HTTP", "", "", authConfig, nil, nil,
	)
	require.NoError(t, err)
	_, err = backend.CreateActionConnector(
		"000000000000", "ac-jira", "Jira Connector", "JIRA_CLOUD", "", "", authConfig, nil, nil,
	)
	require.NoError(t, err)

	out, err := client.SearchActionConnectors(ctx, &quicksightsdk.SearchActionConnectorsInput{
		AwsAccountId: aws.String("000000000000"),
		Filters: []types.ActionConnectorSearchFilter{
			{
				Name:     types.ActionConnectorSearchFilterNameEnumActionConnectorName,
				Operator: types.FilterOperatorStringEquals,
				Value:    aws.String("HTTP Connector"),
			},
			{
				Name:     types.ActionConnectorSearchFilterNameEnumActionConnectorType,
				Operator: types.FilterOperatorStringEquals,
				Value:    aws.String("JIRA_CLOUD"),
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(
		t, out.ActionConnectorSummaries,
		"a name match for one connector ANDed with a type match for another must match neither",
	)
}
