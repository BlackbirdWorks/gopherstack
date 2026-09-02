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

// This file covers gopherstack-uox6's List*/Describe* slice: the 121
// operations outside the Search family (13 operations, already swept). Each
// case seeds enough records to distinguish "filtered correctly" from
// "returned everything", per the standing brief.

// TestListThemes_TypeFilter covers ListThemesInput.Type
// (quicksight@v1.123.1 api_op_ListThemes.go: "ALL (default) - Display all
// existing themes... CUSTOM... QUICKSIGHT"). handleListThemes
// (handler_themes.go) never read the "type" query parameter the real
// serializer emits (serializers.go's
// awsRestjson1_serializeOpHttpBindingsListThemesInput sets
// encoder.SetQuery("type")), so a client asking for QUICKSIGHT-only themes
// got every CUSTOM theme back instead. This backend never seeds a
// QUICKSIGHT-type starting theme (CreateTheme always sets
// themeTypeCustom), so QUICKSIGHT is exactly the type no stored theme can
// legally carry -- filtering on it must return empty.
func TestListThemes_TypeFilter(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"
	backend := quicksight.NewInMemoryBackend(accountID, "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	_, err := backend.CreateTheme(accountID, "theme-a", "Theme A", "SEASIDE", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = backend.CreateTheme(accountID, "theme-b", "Theme B", "MIDNIGHT", "", nil, nil, nil)
	require.NoError(t, err)

	all, err := client.ListThemes(ctx, &quicksightsdk.ListThemesInput{AwsAccountId: aws.String(accountID)})
	require.NoError(t, err)
	assert.Len(t, all.ThemeSummaryList, 2, "no Type filter must return every theme")

	custom, err := client.ListThemes(ctx, &quicksightsdk.ListThemesInput{
		AwsAccountId: aws.String(accountID),
		Type:         types.ThemeTypeCustom,
	})
	require.NoError(t, err)
	assert.Len(t, custom.ThemeSummaryList, 2, "Type=CUSTOM must return both (every stored theme is CUSTOM)")

	qs, err := client.ListThemes(ctx, &quicksightsdk.ListThemesInput{
		AwsAccountId: aws.String(accountID),
		Type:         types.ThemeTypeQuicksight,
	})
	require.NoError(t, err)
	assert.Empty(t, qs.ThemeSummaryList, "Type=QUICKSIGHT must exclude every CUSTOM theme")
}

// TestDescribeKeyRegistration_DefaultKeyOnly covers
// DescribeKeyRegistrationInput.DefaultKeyOnly (quicksight@v1.123.1
// api_op_DescribeKeyRegistration.go: "Determines whether the request
// returns the default key only", bound to the "default-key-only" query
// parameter per serializers.go). handleDescribeKeyRegistration
// (handler_account.go) never read it at all, so every registered key came
// back regardless of the flag even though RegisteredCustomerManagedKey.
// DefaultKey is real, request-settable data (via UpdateKeyRegistration).
func TestDescribeKeyRegistration_DefaultKeyOnly(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"
	backend := quicksight.NewInMemoryBackend(accountID, "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	_, err := client.UpdateKeyRegistration(ctx, &quicksightsdk.UpdateKeyRegistrationInput{
		AwsAccountId: aws.String(accountID),
		KeyRegistration: []types.RegisteredCustomerManagedKey{
			{KeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/non-default"), DefaultKey: false},
			{KeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/default"), DefaultKey: true},
		},
	})
	require.NoError(t, err)

	all, err := client.DescribeKeyRegistration(ctx, &quicksightsdk.DescribeKeyRegistrationInput{
		AwsAccountId: aws.String(accountID),
	})
	require.NoError(t, err)
	assert.Len(t, all.KeyRegistration, 2, "omitted DefaultKeyOnly must return every key")

	defOnly, err := client.DescribeKeyRegistration(ctx, &quicksightsdk.DescribeKeyRegistrationInput{
		AwsAccountId:   aws.String(accountID),
		DefaultKeyOnly: true,
	})
	require.NoError(t, err)
	require.Len(t, defOnly.KeyRegistration, 1, "DefaultKeyOnly=true must exclude the non-default key")
	assert.True(t, defOnly.KeyRegistration[0].DefaultKey)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/default", aws.ToString(defOnly.KeyRegistration[0].KeyArn))
}

// TestListUsersIndexCapacity_PrefixFilter covers
// ListUsersIndexCapacityInput.Filters' UserNameOrEmail member
// (quicksight@v1.123.1 api_op_ListUsersIndexCapacity.go, types.go's
// UserNameOrEmailFilter: "starts-with match" against username or email).
// handleListUsersIndexCapacity (handler_userindexcapacity.go) never read
// "filters" from the request body at all -- Filters/SortBy/SortOrder were
// deliberately treated as no-ops "for wire compatibility", but that
// precedent (matchesNameFilter's handling of genuinely *unrecognized*
// search-filter attributes) doesn't apply to a documented, backed field.
func TestListUsersIndexCapacity_PrefixFilter(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"
	backend := quicksight.NewInMemoryBackend(accountID, "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	_, err := backend.RegisterUser(accountID, "default", "alice", "alice@example.com", "READER", "QUICKSIGHT", "", nil)
	require.NoError(t, err)
	_, err = backend.RegisterUser(accountID, "default", "bob", "bob@example.com", "READER", "QUICKSIGHT", "", nil)
	require.NoError(t, err)

	all, err := client.ListUsersIndexCapacity(ctx, &quicksightsdk.ListUsersIndexCapacityInput{
		AwsAccountId: aws.String(accountID),
	})
	require.NoError(t, err)
	assert.Len(t, all.Users, 2, "no filter must return every user")

	filtered, err := client.ListUsersIndexCapacity(ctx, &quicksightsdk.ListUsersIndexCapacityInput{
		AwsAccountId: aws.String(accountID),
		Filters: []types.UserIndexCapacityFilter{
			&types.UserIndexCapacityFilterMemberUserNameOrEmail{
				Value: types.UserNameOrEmailFilter{Prefix: aws.String("ali")},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, filtered.Users, 1, "prefix filter must exclude the non-matching user")
	assert.Equal(t, "alice", aws.ToString(filtered.Users[0].UserName))
}

// TestListUsersIndexCapacity_CapacityBytesFilter covers the
// TotalCapacityBytes union member (CapacityBytesRangeFilter: "MinBytes...
// inclusive"). This backend has no ingestion pipeline (userindexcapacity.go
// documents TotalCapacityBytes as always the honest sum of real
// KnowledgeBase/Space ownership, currently always 0 since neither carries a
// request-settable size), so every user's TotalCapacityBytes is provably 0
// -- the boundary itself (MinBytes=0 vs MinBytes=1) is what distinguishes
// "filter applied" from "filter ignored" here, same technique the prior
// Search-family pass used for KNOWLEDGE_BASE_SIZE_BYTES.
func TestListUsersIndexCapacity_CapacityBytesFilter(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"
	backend := quicksight.NewInMemoryBackend(accountID, "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	_, err := backend.RegisterUser(accountID, "default", "alice", "alice@example.com", "READER", "QUICKSIGHT", "", nil)
	require.NoError(t, err)

	includesZero, err := client.ListUsersIndexCapacity(ctx, &quicksightsdk.ListUsersIndexCapacityInput{
		AwsAccountId: aws.String(accountID),
		Filters: []types.UserIndexCapacityFilter{
			&types.UserIndexCapacityFilterMemberTotalCapacityBytes{
				Value: types.CapacityBytesRangeFilter{MinBytes: aws.Int64(0)},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, includesZero.Users, 1, "MinBytes=0 is inclusive and every user's capacity is 0")

	excludesZero, err := client.ListUsersIndexCapacity(ctx, &quicksightsdk.ListUsersIndexCapacityInput{
		AwsAccountId: aws.String(accountID),
		Filters: []types.UserIndexCapacityFilter{
			&types.UserIndexCapacityFilterMemberTotalCapacityBytes{
				Value: types.CapacityBytesRangeFilter{MinBytes: aws.Int64(1)},
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, excludesZero.Users, "MinBytes=1 must exclude every user (all capacities are 0)")
}

// TestDescribeAccountCustomization_Resolved covers
// DescribeAccountCustomizationInput.Resolved (quicksight@v1.123.1
// api_op_DescribeAccountCustomization.go: "works with the other parameters
// to determine which view... Omit this flag... to reveal customizations
// that are configured at different levels"). handleDescribeAccountCustomization
// (handler_account.go) never read it, so a namespace-scoped lookup with
// Resolved=true still did an exact-key lookup: a client asking for the
// resolved (effective) view of a namespace that has no customization of its
// own -- only an account-level default -- got ErrAccountCustomizationNotFound
// (404) instead of the account-level customization real AWS falls back to.
func TestDescribeAccountCustomization_Resolved(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"
	backend := quicksight.NewInMemoryBackend(accountID, "us-east-1")
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	_, err := backend.CreateAccountCustomization(accountID, "", "acct-theme", "acct-template")
	require.NoError(t, err)

	_, err = client.DescribeAccountCustomization(ctx, &quicksightsdk.DescribeAccountCustomizationInput{
		AwsAccountId: aws.String(accountID),
		Namespace:    aws.String("default"),
	})
	require.Error(t, err, "unresolved namespace lookup with no namespace-level entry must still 404")

	resolved, err := client.DescribeAccountCustomization(ctx, &quicksightsdk.DescribeAccountCustomizationInput{
		AwsAccountId: aws.String(accountID),
		Namespace:    aws.String("default"),
		Resolved:     true,
	})
	require.NoError(t, err, "resolved lookup must fall back to the account-level customization")
	assert.Equal(t, "acct-theme", aws.ToString(resolved.AccountCustomization.DefaultTheme))
	assert.Equal(t, "acct-template", aws.ToString(resolved.AccountCustomization.DefaultEmailCustomizationTemplate))

	_, err = backend.CreateAccountCustomization(accountID, "default", "ns-theme", "")
	require.NoError(t, err)

	merged, err := client.DescribeAccountCustomization(ctx, &quicksightsdk.DescribeAccountCustomizationInput{
		AwsAccountId: aws.String(accountID),
		Namespace:    aws.String("default"),
		Resolved:     true,
	})
	require.NoError(t, err)
	assert.Equal(t, "ns-theme", aws.ToString(merged.AccountCustomization.DefaultTheme),
		"namespace-level value must win where set")
	assert.Equal(t, "acct-template", aws.ToString(merged.AccountCustomization.DefaultEmailCustomizationTemplate),
		"account-level value must fill in where the namespace level has nothing set")
}
