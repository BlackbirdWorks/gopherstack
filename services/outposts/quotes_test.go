package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func minimalCreateQuoteInput() *outpostssdk.CreateQuoteInput {
	return &outpostssdk.CreateQuoteInput{
		CountryCode: aws.String("US"),
		RequestedCapacities: []types.QuoteCapacity{
			{
				QuoteCapacityType: types.QuoteCapacityTypeEc2,
				Unit:              aws.String("m5.24xlarge"),
				Quantity:          aws.Float32(4),
			},
		},
	}
}

func TestCreateQuote_WithoutOutpost(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	out, err := client.CreateQuote(t.Context(), minimalCreateQuoteInput())
	require.NoError(t, err)
	require.Equal(t, types.QuoteStatusCreated, out.Quote.QuoteStatus)
	require.Empty(t, aws.ToString(out.Quote.OutpostArn))
	require.NotEmpty(t, out.Quote.QuoteOptions)
	require.NotEmpty(t, out.Quote.QuoteOptions[0].PricingOptions)

	foundMissingOutpostCheck := false

	for _, req := range out.Quote.OrderingRequirements {
		if req.OrderingRequirementType == types.OrderingRequirementTypeOutpostIdMissingOnQuoteError {
			require.Equal(t, types.OrderingRequirementStatusFail, req.Status)

			foundMissingOutpostCheck = true
		}
	}

	require.True(t, foundMissingOutpostCheck)
}

func TestCreateQuote_WithOutpost(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	in := minimalCreateQuoteInput()
	in.OutpostIdentifier = created.OutpostId

	out, err := client.CreateQuote(t.Context(), in)
	require.NoError(t, err)
	require.Equal(t, aws.ToString(created.OutpostArn), aws.ToString(out.Quote.OutpostArn))

	got := map[types.OrderingRequirementType]types.OrderingRequirementStatus{}
	for _, req := range out.Quote.OrderingRequirements {
		got[req.OrderingRequirementType] = req.Status
	}

	// createTestSite/createTestOutpost seed a bare, RACK-type Outpost with no
	// addresses and no rack physical properties -- see ordering_requirements_test.go
	// (package outposts) for the full 12-check matrix, including the
	// contact-info checks the real SDK client's own validators.go can't
	// reach here (every Address field is client-side required once an
	// address is non-nil at all).
	want := []struct {
		reqType types.OrderingRequirementType
		status  types.OrderingRequirementStatus
	}{
		{
			types.OrderingRequirementTypeOutpostIdMissingOnQuoteError,
			types.OrderingRequirementStatusPass,
		},
		{types.OrderingRequirementTypeOutpostNotFoundError, types.OrderingRequirementStatusPass},
		{types.OrderingRequirementTypeOutpostActiveCheckError, types.OrderingRequirementStatusPass},
		{
			types.OrderingRequirementTypeOutpostRenewalRequiredError,
			types.OrderingRequirementStatusExempt,
		},
		{
			types.OrderingRequirementTypeOperatingAddressExistenceCheckError,
			types.OrderingRequirementStatusFail,
		},
		{
			types.OrderingRequirementTypeShippingAddressExistenceCheckError,
			types.OrderingRequirementStatusFail,
		},
		{
			types.OrderingRequirementTypeCountryCodeMismatchCheckError,
			types.OrderingRequirementStatusExempt,
		},
		{
			types.OrderingRequirementTypeValidZipCodeCheckError,
			types.OrderingRequirementStatusExempt,
		},
		{
			types.OrderingRequirementTypeRackPhysicalPropertiesCheckError,
			types.OrderingRequirementStatusFail,
		},
	}

	for _, w := range want {
		assert.Equal(t, w.status, got[w.reqType], w.reqType)
	}
}

// TestUpdateQuote_OutpostDeletedAfterAssociation proves OUTPOST_NOT_FOUND_ERROR
// fires when the Outpost associated with a quote is deleted afterward --
// distinct from OUTPOST_ID_MISSING_ON_QUOTE_ERROR, which only fires when no
// OutpostID was ever set. DeleteOutpost has no FK check against Quotes, so
// this is real, reachable state (not a synthetic scenario).
func TestUpdateQuote_OutpostDeletedAfterAssociation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	in := minimalCreateQuoteInput()
	in.OutpostIdentifier = created.OutpostId
	quote, err := client.CreateQuote(t.Context(), in)
	require.NoError(t, err)

	_, err = client.DeleteOutpost(
		t.Context(),
		&outpostssdk.DeleteOutpostInput{OutpostId: created.OutpostId},
	)
	require.NoError(t, err)

	// UpdateQuote (with OutpostIdentifier omitted, so the existing
	// association is kept) recomputes OrderingRequirements against the
	// now-deleted Outpost.
	updated, err := client.UpdateQuote(t.Context(), &outpostssdk.UpdateQuoteInput{
		QuoteIdentifier: quote.Quote.QuoteId,
		Description:     aws.String("re-evaluated after Outpost deletion"),
	})
	require.NoError(t, err)

	got := map[types.OrderingRequirementType]types.OrderingRequirementStatus{}
	for _, req := range updated.Quote.OrderingRequirements {
		got[req.OrderingRequirementType] = req.Status
	}

	assert.Equal(
		t,
		types.OrderingRequirementStatusPass,
		got[types.OrderingRequirementTypeOutpostIdMissingOnQuoteError],
	)
	assert.Equal(
		t,
		types.OrderingRequirementStatusFail,
		got[types.OrderingRequirementTypeOutpostNotFoundError],
	)
	assert.Equal(
		t,
		types.OrderingRequirementStatusExempt,
		got[types.OrderingRequirementTypeOutpostActiveCheckError],
	)
}

func TestUpdateQuote_OutpostIdentifierTriState(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	in := minimalCreateQuoteInput()
	in.OutpostIdentifier = created.OutpostId
	quote, err := client.CreateQuote(t.Context(), in)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(quote.Quote.OutpostArn))

	// Explicit empty string clears the association.
	cleared, err := client.UpdateQuote(t.Context(), &outpostssdk.UpdateQuoteInput{
		QuoteIdentifier:   quote.Quote.QuoteId,
		OutpostIdentifier: aws.String(""),
	})
	require.NoError(t, err)
	require.Empty(t, aws.ToString(cleared.Quote.OutpostArn))

	// A subsequent update that omits OutpostIdentifier entirely must not
	// re-associate it.
	unchanged, err := client.UpdateQuote(t.Context(), &outpostssdk.UpdateQuoteInput{
		QuoteIdentifier: quote.Quote.QuoteId,
		Description:     aws.String("updated description"),
	})
	require.NoError(t, err)
	require.Empty(t, aws.ToString(unchanged.Quote.OutpostArn))
	require.Equal(t, "updated description", aws.ToString(unchanged.Quote.Description))
}

func TestDeleteQuote(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	out, err := client.CreateQuote(t.Context(), minimalCreateQuoteInput())
	require.NoError(t, err)

	_, err = client.DeleteQuote(
		t.Context(),
		&outpostssdk.DeleteQuoteInput{QuoteIdentifier: out.Quote.QuoteId},
	)
	require.NoError(t, err)

	_, err = client.GetQuote(
		t.Context(),
		&outpostssdk.GetQuoteInput{QuoteIdentifier: out.Quote.QuoteId},
	)
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}

func TestListQuotes(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.CreateQuote(t.Context(), minimalCreateQuoteInput())
	require.NoError(t, err)

	out, err := client.ListQuotes(t.Context(), &outpostssdk.ListQuotesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.Quotes)
}

func TestCreateOrder_ConsumesQuote(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	in := minimalCreateQuoteInput()
	in.OutpostIdentifier = created.OutpostId
	quote, err := client.CreateQuote(t.Context(), in)
	require.NoError(t, err)

	order, err := client.CreateOrder(t.Context(), &outpostssdk.CreateOrderInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionAllUpfront,
		QuoteIdentifier:   quote.Quote.QuoteId,
		LineItems: []types.LineItemRequest{
			{CatalogItemId: aws.String("OR-RACKM05"), Quantity: aws.Int32(1)},
		},
	})
	require.NoError(t, err)

	got, err := client.GetQuote(
		t.Context(),
		&outpostssdk.GetQuoteInput{QuoteIdentifier: quote.Quote.QuoteId},
	)
	require.NoError(t, err)
	require.Equal(t, types.QuoteStatusOrderSubmitted, got.Quote.QuoteStatus)
	require.Equal(t, aws.ToString(order.Order.OrderId), aws.ToString(got.Quote.SubmittedOrderId))

	// The now-consumed quote cannot be reused for a second order.
	_, err = client.CreateOrder(t.Context(), &outpostssdk.CreateOrderInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionAllUpfront,
		QuoteIdentifier:   quote.Quote.QuoteId,
		LineItems: []types.LineItemRequest{
			{CatalogItemId: aws.String("OR-RACKM05"), Quantity: aws.Int32(1)},
		},
	})
	require.Error(t, err)

	var ce *types.ConflictException
	require.ErrorAs(t, err, &ce)
}
