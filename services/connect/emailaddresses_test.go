package connect_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *fixture) newEmail(t *testing.T, inst *string, address string) string {
	t.Helper()

	out, err := f.client.CreateEmailAddress(t.Context(), &sdk.CreateEmailAddressInput{
		InstanceId: inst, EmailAddress: aws.String(address),
	})
	require.NoError(t, err)

	return aws.ToString(out.EmailAddressId)
}

func TestEmailAddressLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "email"))

	made, err := f.client.CreateEmailAddress(t.Context(), &sdk.CreateEmailAddressInput{
		InstanceId: inst, EmailAddress: aws.String("support@example.com"), DisplayName: aws.String("Support"),
		Description: aws.String("front door"), Tags: map[string]string{"team": "care"},
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(made.EmailAddressArn), "/email-address/"+aws.ToString(made.EmailAddressId))

	tests := []struct {
		name    string
		address string
		code    string
	}{
		{name: "duplicate case-insensitive", address: "SUPPORT@example.com", code: "DuplicateResourceException"},
		{name: "no domain", address: "support", code: "InvalidParameterException"},
		{name: "display form", address: "Support <support2@example.com>", code: "InvalidParameterException"},
	}
	for _, tt := range tests {
		_, cerr := f.client.CreateEmailAddress(t.Context(), &sdk.CreateEmailAddressInput{
			InstanceId: inst, EmailAddress: aws.String(tt.address),
		})
		assert.Equal(t, tt.code, apiCode(cerr), tt.name)
	}

	got, err := f.client.DescribeEmailAddress(t.Context(),
		&sdk.DescribeEmailAddressInput{InstanceId: inst, EmailAddressId: made.EmailAddressId})
	require.NoError(t, err)
	assert.Equal(t, "support@example.com", aws.ToString(got.EmailAddress))
	assert.Equal(t, "Support", aws.ToString(got.DisplayName))
	assert.Equal(t, "front door", aws.ToString(got.Description))
	assert.Equal(t, map[string]string{"team": "care"}, got.Tags)
	assert.Equal(t, "2026-03-01T12:00:00.000Z", aws.ToString(got.CreateTimestamp))
	assert.Empty(t, got.AliasConfigurations)

	f.clock.advance(5 * time.Minute)

	upd, err := f.client.UpdateEmailAddressMetadata(t.Context(), &sdk.UpdateEmailAddressMetadataInput{
		InstanceId: inst, EmailAddressId: made.EmailAddressId, DisplayName: aws.String("Care Desk"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(made.EmailAddressArn), aws.ToString(upd.EmailAddressArn))

	got, err = f.client.DescribeEmailAddress(t.Context(),
		&sdk.DescribeEmailAddressInput{InstanceId: inst, EmailAddressId: made.EmailAddressId})
	require.NoError(t, err)
	assert.Equal(t, "Care Desk", aws.ToString(got.DisplayName))
	assert.Equal(t, "front door", aws.ToString(got.Description), "untouched members stay")
	assert.Equal(t, "2026-03-01T12:05:00.000Z", aws.ToString(got.ModifiedTimestamp))
	assert.Equal(t, "2026-03-01T12:00:00.000Z", aws.ToString(got.CreateTimestamp))

	tags, err := f.client.ListTagsForResource(
		t.Context(),
		&sdk.ListTagsForResourceInput{ResourceArn: made.EmailAddressArn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "care"}, tags.Tags)

	dst := f.restored(t)
	got, err = dst.client.DescribeEmailAddress(t.Context(),
		&sdk.DescribeEmailAddressInput{InstanceId: inst, EmailAddressId: made.EmailAddressId})
	require.NoError(t, err)
	assert.Equal(t, "Care Desk", aws.ToString(got.DisplayName))

	_, err = f.client.DeleteEmailAddress(t.Context(),
		&sdk.DeleteEmailAddressInput{InstanceId: inst, EmailAddressId: made.EmailAddressId})
	require.NoError(t, err)

	for name, call := range map[string]func() error{
		"describe": func() error {
			_, e := f.client.DescribeEmailAddress(t.Context(),
				&sdk.DescribeEmailAddressInput{InstanceId: inst, EmailAddressId: made.EmailAddressId})

			return e
		},
		"delete": func() error {
			_, e := f.client.DeleteEmailAddress(t.Context(),
				&sdk.DeleteEmailAddressInput{InstanceId: inst, EmailAddressId: made.EmailAddressId})

			return e
		},
		"update": func() error {
			_, e := f.client.UpdateEmailAddressMetadata(t.Context(), &sdk.UpdateEmailAddressMetadataInput{
				InstanceId: inst, EmailAddressId: made.EmailAddressId, DisplayName: aws.String("x"),
			})

			return e
		},
	} {
		assert.Equal(t, "ResourceNotFoundException", apiCode(call()), name)
	}

	f.newEmail(t, inst, "support@example.com")
}

func TestEmailAddressAliases(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "aliases"))
	primary := aws.String(f.newEmail(t, inst, "main@example.com"))
	help := aws.String(f.newEmail(t, inst, "help@example.com"))
	sales := aws.String(f.newEmail(t, inst, "sales@example.com"))
	cfg := func(id *string) *types.AliasConfiguration { return &types.AliasConfiguration{EmailAddressId: id} }

	_, err := f.client.AssociateEmailAddressAlias(t.Context(), &sdk.AssociateEmailAddressAliasInput{
		InstanceId: inst, EmailAddressId: primary, AliasConfiguration: cfg(help),
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		primary *string
		alias   *string
		code    string
	}{
		{name: "self", primary: primary, alias: primary, code: "InvalidRequestException"},
		{name: "unknown alias", primary: primary, alias: aws.String("nope"), code: "ResourceNotFoundException"},
		{name: "unknown primary", primary: aws.String("nope"), alias: sales, code: "ResourceNotFoundException"},
		{name: "already an alias", primary: sales, alias: help, code: "ResourceConflictException"},
		{name: "primary is an alias", primary: help, alias: sales, code: "ResourceConflictException"},
		{name: "alias has aliases", primary: sales, alias: primary, code: "ResourceConflictException"},
	}
	for _, tt := range tests {
		_, aerr := f.client.AssociateEmailAddressAlias(t.Context(), &sdk.AssociateEmailAddressAliasInput{
			InstanceId: inst, EmailAddressId: tt.primary, AliasConfiguration: cfg(tt.alias),
		})
		assert.Equal(t, tt.code, apiCode(aerr), tt.name)
	}

	got, err := f.client.DescribeEmailAddress(t.Context(),
		&sdk.DescribeEmailAddressInput{InstanceId: inst, EmailAddressId: primary})
	require.NoError(t, err)
	require.Len(t, got.AliasConfigurations, 1)
	assert.Equal(t, aws.ToString(help), aws.ToString(got.AliasConfigurations[0].EmailAddressId))

	_, err = f.client.DeleteEmailAddress(
		t.Context(),
		&sdk.DeleteEmailAddressInput{InstanceId: inst, EmailAddressId: primary},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err), "primary with an alias")
	_, err = f.client.DeleteEmailAddress(
		t.Context(),
		&sdk.DeleteEmailAddressInput{InstanceId: inst, EmailAddressId: help},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err), "address that is an alias")

	_, err = f.client.DisassociateEmailAddressAlias(t.Context(), &sdk.DisassociateEmailAddressAliasInput{
		InstanceId: inst, EmailAddressId: primary, AliasConfiguration: cfg(sales),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "sales was never an alias")

	_, err = f.client.DisassociateEmailAddressAlias(t.Context(), &sdk.DisassociateEmailAddressAliasInput{
		InstanceId: inst, EmailAddressId: primary, AliasConfiguration: cfg(help),
	})
	require.NoError(t, err)

	got, err = f.client.DescribeEmailAddress(t.Context(),
		&sdk.DescribeEmailAddressInput{InstanceId: inst, EmailAddressId: primary})
	require.NoError(t, err)
	assert.Empty(t, got.AliasConfigurations)

	_, err = f.client.DeleteEmailAddress(
		t.Context(),
		&sdk.DeleteEmailAddressInput{InstanceId: inst, EmailAddressId: help},
	)
	require.NoError(t, err, "free again after the alias was removed")
}

func TestQueueEmailAddresses(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "qemail"))
	queue, _, _ := f.defaultQueue(t, aws.ToString(inst))
	q := aws.String(queue)
	a := aws.String(f.newEmail(t, inst, "a@example.com"))
	b := aws.String(f.newEmail(t, inst, "b@example.com"))
	c := aws.String(f.newEmail(t, inst, "c@example.com"))
	cfgs := func(ids ...*string) []types.EmailAddressConfig {
		out := make([]types.EmailAddressConfig, 0, len(ids))
		for _, id := range ids {
			out = append(out, types.EmailAddressConfig{EmailAddressId: id})
		}

		return out
	}

	_, err := f.client.AssociateQueueEmailAddresses(t.Context(), &sdk.AssociateQueueEmailAddressesInput{
		InstanceId: inst, QueueId: q, EmailAddressesConfig: cfgs(a, b),
	})
	require.NoError(t, err)

	_, err = f.client.AssociateQueueEmailAddresses(t.Context(), &sdk.AssociateQueueEmailAddressesInput{
		InstanceId: inst, QueueId: q, EmailAddressesConfig: cfgs(b, aws.String("ghost")),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.AssociateQueueEmailAddresses(t.Context(), &sdk.AssociateQueueEmailAddressesInput{
		InstanceId: inst, QueueId: aws.String("nope"), EmailAddressesConfig: cfgs(a),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.UpdateQueueOutboundEmailConfig(t.Context(), &sdk.UpdateQueueOutboundEmailConfigInput{
		InstanceId: inst, QueueId: q,
		OutboundEmailConfig: &types.OutboundEmailConfig{OutboundEmailAddressId: aws.String("ghost")},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "default outbound address must exist")

	_, err = f.client.UpdateQueueOutboundEmailConfig(t.Context(), &sdk.UpdateQueueOutboundEmailConfigInput{
		InstanceId: inst, QueueId: q, OutboundEmailConfig: &types.OutboundEmailConfig{OutboundEmailAddressId: c},
	})
	require.NoError(t, err)

	list, err := f.client.ListQueueEmailAddresses(
		t.Context(),
		&sdk.ListQueueEmailAddressesInput{InstanceId: inst, QueueId: q},
	)
	require.NoError(t, err)
	require.Len(t, list.EmailAddressMetadataList, 3)
	assert.Equal(t, aws.ToString(c), aws.ToString(list.EmailAddressMetadataList[0].Id), "default outbound first")
	assert.True(t, list.EmailAddressMetadataList[0].IsDefaultOutboundEmail)
	assert.False(t, list.EmailAddressMetadataList[1].IsDefaultOutboundEmail)
	assert.Contains(t, aws.ToString(list.EmailAddressMetadataList[1].Arn), "/email-address/")
	assert.NotNil(t, list.LastModifiedTime)

	page, err := f.client.ListQueueEmailAddresses(t.Context(),
		&sdk.ListQueueEmailAddressesInput{InstanceId: inst, QueueId: q, MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	assert.Len(t, page.EmailAddressMetadataList, 2)
	require.NotEmpty(t, aws.ToString(page.NextToken))

	_, err = f.client.DeleteEmailAddress(t.Context(), &sdk.DeleteEmailAddressInput{InstanceId: inst, EmailAddressId: a})
	assert.Equal(t, "ResourceInUseException", apiCode(err), "associated with a queue")
	_, err = f.client.DeleteEmailAddress(t.Context(), &sdk.DeleteEmailAddressInput{InstanceId: inst, EmailAddressId: c})
	assert.Equal(t, "ResourceInUseException", apiCode(err), "default outbound address of a queue")

	_, err = f.client.DisassociateQueueEmailAddresses(t.Context(), &sdk.DisassociateQueueEmailAddressesInput{
		InstanceId: inst, QueueId: q, EmailAddressesId: []string{aws.ToString(a), aws.ToString(c)},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "c was never associated, only the default")

	_, err = f.client.DisassociateQueueEmailAddresses(t.Context(), &sdk.DisassociateQueueEmailAddressesInput{
		InstanceId: inst, QueueId: q, EmailAddressesId: []string{aws.ToString(a)},
	})
	require.NoError(t, err)

	dst := f.restored(t)
	list, err = dst.client.ListQueueEmailAddresses(
		t.Context(),
		&sdk.ListQueueEmailAddressesInput{InstanceId: inst, QueueId: q},
	)
	require.NoError(t, err)
	require.Len(t, list.EmailAddressMetadataList, 2)

	_, err = f.client.DeleteEmailAddress(t.Context(), &sdk.DeleteEmailAddressInput{InstanceId: inst, EmailAddressId: a})
	require.NoError(t, err)
}

func TestQueueEmailAddressLimit(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "qlimit"))
	queue, _, _ := f.defaultQueue(t, aws.ToString(inst))

	cfgs := make([]types.EmailAddressConfig, 0, 50)

	for i := range 50 {
		id := f.newEmail(t, inst, "u"+strconv.Itoa(i)+"@example.com")
		cfgs = append(cfgs, types.EmailAddressConfig{EmailAddressId: aws.String(id)})
	}

	_, err := f.client.AssociateQueueEmailAddresses(t.Context(), &sdk.AssociateQueueEmailAddressesInput{
		InstanceId: inst, QueueId: aws.String(queue), EmailAddressesConfig: cfgs,
	})
	assert.Equal(t, "LimitExceededException", apiCode(err))

	_, err = f.client.AssociateQueueEmailAddresses(t.Context(), &sdk.AssociateQueueEmailAddressesInput{
		InstanceId: inst, QueueId: aws.String(queue), EmailAddressesConfig: cfgs[:49],
	})
	require.NoError(t, err, "49 additional addresses are allowed")

	list, err := f.client.ListQueueEmailAddresses(t.Context(),
		&sdk.ListQueueEmailAddressesInput{InstanceId: inst, QueueId: aws.String(queue), MaxResults: aws.Int32(100)})
	require.NoError(t, err)
	assert.Len(t, list.EmailAddressMetadataList, 49)
}

func TestSearchEmailAddresses(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "emsearch"))

	for _, addr := range []string{"sales-eu@example.com", "sales-us@example.com", "legal@example.org"} {
		_, err := f.client.CreateEmailAddress(t.Context(), &sdk.CreateEmailAddressInput{
			InstanceId: inst, EmailAddress: aws.String(addr), DisplayName: aws.String("D " + addr),
			Tags: map[string]string{"tld": addr[len(addr)-3:]},
		})
		require.NoError(t, err)
	}

	str := func(field, value string, cmp types.StringComparisonType) *types.EmailAddressSearchCriteria {
		return &types.EmailAddressSearchCriteria{StringCondition: &types.StringCondition{
			FieldName: aws.String(field), Value: aws.String(value), ComparisonType: cmp,
		}}
	}
	tests := []struct {
		criteria *types.EmailAddressSearchCriteria
		filter   *types.EmailAddressSearchFilter
		name     string
		want     []string
	}{
		{
			name: "prefix", criteria: str("EmailAddress", "sales", types.StringComparisonTypeStartsWith),
			want: []string{"sales-eu@example.com", "sales-us@example.com"},
		},
		{
			name: "display name", criteria: str("DisplayName", "legal", types.StringComparisonTypeContains),
			want: []string{"legal@example.org"},
		},
		{
			name: "and", criteria: &types.EmailAddressSearchCriteria{AndConditions: []types.EmailAddressSearchCriteria{
				*str("EmailAddress", "sales", types.StringComparisonTypeStartsWith),
				*str("EmailAddress", "-eu", types.StringComparisonTypeContains),
			}},
			want: []string{"sales-eu@example.com"},
		},
		{
			name: "tag filter", filter: &types.EmailAddressSearchFilter{TagFilter: &types.ControlPlaneTagFilter{
				TagCondition: &types.TagCondition{TagKey: aws.String("tld"), TagValue: aws.String("org")},
			}},
			want: []string{"legal@example.org"},
		},
	}
	for _, tt := range tests {
		got, err := f.client.SearchEmailAddresses(t.Context(), &sdk.SearchEmailAddressesInput{
			InstanceId: inst, SearchCriteria: tt.criteria, SearchFilter: tt.filter,
		})
		require.NoError(t, err, tt.name)

		var addrs []string
		for _, e := range got.EmailAddresses {
			addrs = append(addrs, aws.ToString(e.EmailAddress))
			assert.NotEmpty(t, aws.ToString(e.EmailAddressId), tt.name)
		}

		assert.ElementsMatch(t, tt.want, addrs, tt.name)
		assert.EqualValues(t, len(tt.want), aws.ToInt64(got.ApproximateTotalCount), tt.name)
	}

	_, err := f.client.SearchEmailAddresses(t.Context(), &sdk.SearchEmailAddressesInput{
		InstanceId: inst, SearchCriteria: str("Bogus", "x", types.StringComparisonTypeExact),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}
