package connect_test

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeShares struct {
	live map[string]string
	mu   sync.Mutex
	n    int
}

func (f *fakeShares) CreateShare(_ context.Context, _, target, resource string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.n++
	id := "share-" + string(rune('a'+f.n))
	arn := "arn:aws:ram:us-east-1:" + testAccount + ":resource-share/" + id
	f.live[arn] = target + "|" + resource

	return arn, nil
}

func (f *fakeShares) DeleteShare(_ context.Context, shareARN string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.live, shareARN)

	return nil
}

func TestAnalyticsDataSetCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dataSet string
		code    string
	}{
		{"known table", "contact_record", ""},
		{"unknown table", "not_a_table", "InvalidParameterException"},
		{"empty", "", "InvalidParameterException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			inst := aws.String(f.newInstance(t, "catalog"))

			_, err := f.client.AssociateAnalyticsDataSet(t.Context(),
				&sdk.AssociateAnalyticsDataSetInput{InstanceId: inst, DataSetId: aws.String(tt.dataSet)})
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestAnalyticsDataSetResourceShares(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	shares := &fakeShares{live: map[string]string{}}
	f.backend.SetShares(shares)
	inst := aws.String(f.newInstance(t, "shares"))

	out, err := f.client.AssociateAnalyticsDataSet(t.Context(), &sdk.AssociateAnalyticsDataSetInput{
		InstanceId: inst, DataSetId: aws.String("contact_record"), TargetAccountId: aws.String("111122223333"),
	})
	require.NoError(t, err)
	require.Len(t, shares.live, 1)
	assert.Contains(t, shares.live, aws.ToString(out.ResourceShareArn))
	assert.Contains(t, shares.live[aws.ToString(out.ResourceShareArn)], "111122223333|arn:aws:connect:")

	_, err = f.client.DisassociateAnalyticsDataSet(t.Context(), &sdk.DisassociateAnalyticsDataSetInput{
		InstanceId: inst, DataSetId: aws.String("contact_record"), TargetAccountId: aws.String("111122223333"),
	})
	require.NoError(t, err)
	assert.Empty(t, shares.live)
}
