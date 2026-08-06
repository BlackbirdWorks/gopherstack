package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"
)

func minimalCreateQuoteInput() *outpostssdk.CreateQuoteInput {
	return &outpostssdk.CreateQuoteInput{
		CountryCode: aws.String("US"),
		RequestedCapacities: []types.QuoteCapacity{
			{QuoteCapacityType: types.QuoteCapacityTypeEc2, Unit: aws.String("m5.24xlarge"), Quantity: aws.Float32(4)},
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

	for _, req := range out.Quote.OrderingRequirements {
		switch req.OrderingRequirementType {
		case types.OrderingRequirementTypeOutpostIdMissingOnQuoteError:
			require.Equal(t, types.OrderingRequirementStatusPass, req.Status)
		case types.OrderingRequirementTypeOutpostActiveCheckError:
			require.Equal(t, types.OrderingRequirementStatusPass, req.Status)
		default:
			// The other 15 OrderingRequirementType values are real,
			// wire-accurate enum members this backend never emits -- see
			// PARITY.md.
		}
	}
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

	_, err = client.DeleteQuote(t.Context(), &outpostssdk.DeleteQuoteInput{QuoteIdentifier: out.Quote.QuoteId})
	require.NoError(t, err)

	_, err = client.GetQuote(t.Context(), &outpostssdk.GetQuoteInput{QuoteIdentifier: out.Quote.QuoteId})
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

	got, err := client.GetQuote(t.Context(), &outpostssdk.GetQuoteInput{QuoteIdentifier: quote.Quote.QuoteId})
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
