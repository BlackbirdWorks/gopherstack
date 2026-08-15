package sns_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestListOriginationNumbersEmptyByDefault verifies that a fresh backend returns an empty
// list with no next-page token. AWS SNS exposes no public "create origination number" API
// (numbers are provisioned via Pinpoint / AWS End User Messaging), so an empty list is the
// AWS-accurate result for a fresh account.
func TestListOriginationNumbersEmptyByDefault(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	nums, token, err := b.ListOriginationNumbers("", 0)
	require.NoError(t, err)
	assert.Empty(t, nums)
	assert.Empty(t, token)
}

// TestListOriginationNumbersReturnsSeeded verifies that numbers seeded via the internal
// SeedOriginationNumber helper are returned by ListOriginationNumbers, sorted by phone
// number. SeedOriginationNumber exists because AWS provides no public create API.
func TestListOriginationNumbersReturnsSeeded(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	b.SeedOriginationNumber(sns.XMLOriginationPhone{
		PhoneNumber:        "+12025550181",
		Iso2CountryCode:    "US",
		RouteType:          "Transactional",
		NumberCapabilities: []string{"SMS"},
	})
	b.SeedOriginationNumber(sns.XMLOriginationPhone{
		PhoneNumber:        "+12025550100",
		Iso2CountryCode:    "US",
		RouteType:          "Promotional",
		NumberCapabilities: []string{"SMS", "VOICE"},
	})

	nums, token, err := b.ListOriginationNumbers("", 0)
	require.NoError(t, err)
	assert.Empty(t, token, "all results fit on one page")
	require.Len(t, nums, 2)
	// Results are sorted by phone number.
	assert.Equal(t, "+12025550100", nums[0].PhoneNumber)
	assert.Equal(t, "+12025550181", nums[1].PhoneNumber)
	assert.Equal(t, "Promotional", nums[0].RouteType)
	assert.Equal(t, []string{"SMS", "VOICE"}, nums[0].NumberCapabilities)
}

// TestListOriginationNumbersPagination verifies MaxResults paging and that the returned
// NextToken can be used to fetch subsequent pages until the list is exhausted.
func TestListOriginationNumbersPagination(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	// Seed numbers in an order that is not already sorted to confirm deterministic paging.
	for _, p := range []string{"+12025550003", "+12025550001", "+12025550004", "+12025550002"} {
		b.SeedOriginationNumber(sns.XMLOriginationPhone{
			PhoneNumber:        p,
			Iso2CountryCode:    "US",
			RouteType:          "Transactional",
			NumberCapabilities: []string{"SMS"},
		})
	}

	// First page: MaxResults=2 should return 2 results and a non-empty token.
	page1, token1, err := b.ListOriginationNumbers("", 2)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, token1)
	assert.Equal(t, "+12025550001", page1[0].PhoneNumber)
	assert.Equal(t, "+12025550002", page1[1].PhoneNumber)

	// Second page: using the returned token should return the remaining 2 results and an
	// empty token (last page reached).
	page2, token2, err := b.ListOriginationNumbers(token1, 2)
	require.NoError(t, err)
	require.Len(t, page2, 2)
	assert.Empty(t, token2)
	assert.Equal(t, "+12025550003", page2[0].PhoneNumber)
	assert.Equal(t, "+12025550004", page2[1].PhoneNumber)
}

// TestListOriginationNumbersMaxResultsClamped verifies that a MaxResults value exceeding the
// AWS cap (30) is clamped rather than returning more than the maximum per page.
func TestListOriginationNumbersMaxResultsClamped(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	// Seed 31 numbers so that, with the cap of 30, a second page is required.
	for i := range 31 {
		b.SeedOriginationNumber(sns.XMLOriginationPhone{
			// Zero-padded so lexical sort matches numeric order.
			PhoneNumber:        "+1202555" + pad4(i),
			Iso2CountryCode:    "US",
			RouteType:          "Transactional",
			NumberCapabilities: []string{"SMS"},
		})
	}

	// Request far more than the cap; should be clamped to 30 and yield a next token.
	page, token, err := b.ListOriginationNumbers("", 1000)
	require.NoError(t, err)
	assert.Len(t, page, 30)
	assert.NotEmpty(t, token)

	page2, token2, err := b.ListOriginationNumbers(token, 1000)
	require.NoError(t, err)
	assert.Len(t, page2, 1)
	assert.Empty(t, token2)
}

// TestListOriginationNumbersInvalidToken verifies that a malformed NextToken is rejected.
func TestListOriginationNumbersInvalidToken(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	_, _, err := b.ListOriginationNumbers("not-a-valid-token", 0)
	require.Error(t, err)
}

// TestListOriginationNumbers_CreatedAtAndStatusWireRoundTrip drives the real
// aws-sdk-go-v2 SNS client against a seeded backend and asserts CreatedAt and
// Status decode correctly. Both are real members of
// aws-sdk-go-v2/service/sns/types.PhoneNumberInformation
// (types/types.go:82-103) that were previously absent from gopherstack's own
// XMLOriginationPhone wire struct entirely, so a real client always decoded
// a nil CreatedAt and an empty Status regardless of what was seeded
// (gopherstack-3tpf structural diff).
func TestListOriginationNumbers_CreatedAtAndStatusWireRoundTrip(t *testing.T) {
	t.Parallel()

	backend := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := sns.NewHandler(backend)
	client := newTestSNSClient(t, h)

	created := time.Date(2026, 1, 15, 12, 30, 0, 0, time.UTC)
	backend.SeedOriginationNumber(sns.XMLOriginationPhone{
		PhoneNumber:        "+12025550181",
		Iso2CountryCode:    "US",
		RouteType:          "Transactional",
		Status:             "Verified",
		CreatedAt:          &created,
		NumberCapabilities: []string{"SMS"},
	})

	out, err := client.ListOriginationNumbers(t.Context(), &snssdk.ListOriginationNumbersInput{})
	require.NoError(t, err)
	require.Len(t, out.PhoneNumbers, 1)

	got := out.PhoneNumbers[0]
	assert.Equal(t, "Verified", aws.ToString(got.Status))
	require.NotNil(t, got.CreatedAt)
	assert.True(t, created.Equal(*got.CreatedAt), "want %s, got %s", created, *got.CreatedAt)
}

func pad4(i int) string {
	s := []byte{'0', '0', '0', '0'}
	n := len(s) - 1
	for i > 0 && n >= 0 {
		s[n] = byte('0' + i%10)
		i /= 10
		n--
	}

	return string(s)
}
