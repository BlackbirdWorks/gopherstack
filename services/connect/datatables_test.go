package connect_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *fixture) newDataTable(t *testing.T, inst *string, name string, level types.DataTableLockLevel) string {
	t.Helper()

	out, err := f.client.CreateDataTable(t.Context(), &sdk.CreateDataTableInput{
		InstanceId: inst, Name: aws.String(name), TimeZone: aws.String("Europe/Berlin"),
		Status: types.DataTableStatusPublished, ValueLockLevel: level,
	})
	require.NoError(t, err)

	return aws.ToString(out.Id)
}

func (f *fixture) newDTAttr(
	t *testing.T, inst, table *string, name string, vt types.DataTableAttributeValueType, primary bool,
	val *types.Validation,
) {
	t.Helper()

	_, err := f.client.CreateDataTableAttribute(t.Context(), &sdk.CreateDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, Name: aws.String(name), ValueType: vt, Primary: primary, Validation: val,
	})
	require.NoError(t, err)
}

func pv(name, value string) types.PrimaryValue {
	return types.PrimaryValue{AttributeName: aws.String(name), Value: aws.String(value)}
}

func delID(attr, site string) types.DataTableDeleteValueIdentifier {
	return types.DataTableDeleteValueIdentifier{
		AttributeName: aws.String(attr), PrimaryValues: []types.PrimaryValue{pv("site", site)},
		LockVersion: &types.DataTableLockVersion{},
	}
}

func TestDataTableLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "dtlife"))

	created, err := f.client.CreateDataTable(t.Context(), &sdk.CreateDataTableInput{
		InstanceId: inst, Name: aws.String("Holiday Hours"), Description: aws.String("per site"),
		TimeZone: aws.String("America/New_York"), Status: types.DataTableStatusPublished,
		ValueLockLevel: types.DataTableLockLevelAttribute, Tags: map[string]string{"team": "ops"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.LockVersion.DataTable))
	assert.Contains(t, aws.ToString(created.Arn), "/data-table/"+aws.ToString(created.Id))

	tests := []struct {
		in   sdk.CreateDataTableInput
		name string
		code string
	}{
		{
			name: "duplicate case-insensitive",
			in: sdk.CreateDataTableInput{
				Name: aws.String("holiday hours"), TimeZone: aws.String("UTC"), Status: types.DataTableStatusPublished,
				ValueLockLevel: types.DataTableLockLevelNone,
			},
			code: "DuplicateResourceException",
		},
		{
			name: "reserved prefix",
			in: sdk.CreateDataTableInput{
				Name: aws.String("AWS:x"), TimeZone: aws.String("UTC"), Status: types.DataTableStatusPublished,
				ValueLockLevel: types.DataTableLockLevelNone,
			},
			code: "InvalidParameterException",
		},
		{
			name: "bad zone",
			in: sdk.CreateDataTableInput{
				Name: aws.String("zoned"), TimeZone: aws.String("Mars/Base"), Status: types.DataTableStatusPublished,
				ValueLockLevel: types.DataTableLockLevelNone,
			},
			code: "InvalidParameterException",
		},
		{
			name: "bad status",
			in: sdk.CreateDataTableInput{
				Name: aws.String("nostatus"), TimeZone: aws.String("UTC"), Status: "ARCHIVED",
				ValueLockLevel: types.DataTableLockLevelNone,
			},
			code: "InvalidParameterException",
		},
		{
			name: "bad lock level",
			in: sdk.CreateDataTableInput{
				Name: aws.String("lock"), TimeZone: aws.String("UTC"), Status: types.DataTableStatusPublished,
				ValueLockLevel: "SOMETIMES",
			},
			code: "InvalidParameterException",
		},
	}
	for _, tt := range tests {
		in := tt.in
		in.InstanceId = inst
		_, cerr := f.client.CreateDataTable(t.Context(), &in)
		assert.Equal(t, tt.code, apiCode(cerr), tt.name)
	}

	for _, id := range []string{aws.ToString(created.Id), aws.ToString(created.Arn) + ":$LATEST"} {
		got, derr := f.client.DescribeDataTable(t.Context(),
			&sdk.DescribeDataTableInput{InstanceId: inst, DataTableId: aws.String(id)})
		require.NoError(t, derr, id)
		assert.Equal(t, "Holiday Hours", aws.ToString(got.DataTable.Name))
		assert.Equal(t, "per site", aws.ToString(got.DataTable.Description))
		assert.Equal(t, "America/New_York", aws.ToString(got.DataTable.TimeZone))
		assert.Equal(t, types.DataTableLockLevelAttribute, got.DataTable.ValueLockLevel)
		assert.Equal(t, map[string]string{"team": "ops"}, got.DataTable.Tags)
		assert.NotNil(t, got.DataTable.LastModifiedTime)
	}

	_, err = f.client.DescribeDataTable(t.Context(),
		&sdk.DescribeDataTableInput{InstanceId: inst, DataTableId: aws.String(aws.ToString(created.Id) + ":v1")})
	assert.Equal(t, "InvalidParameterException", apiCode(err), "unknown alias")

	_, err = f.client.DescribeDataTable(t.Context(),
		&sdk.DescribeDataTableInput{InstanceId: inst, DataTableId: aws.String("nope")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	tags, err := f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: created.Arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "ops"}, tags.Tags)

	for i := range 3 {
		f.newDataTable(t, inst, fmt.Sprintf("extra-%d", i), types.DataTableLockLevelNone)
	}

	page, err := f.client.ListDataTables(
		t.Context(),
		&sdk.ListDataTablesInput{InstanceId: inst, MaxResults: aws.Int32(3)},
	)
	require.NoError(t, err)
	assert.Len(t, page.DataTableSummaryList, 3)
	require.NotEmpty(t, aws.ToString(page.NextToken))

	rest, err := f.client.ListDataTables(t.Context(),
		&sdk.ListDataTablesInput{InstanceId: inst, NextToken: page.NextToken})
	require.NoError(t, err)
	assert.Len(t, rest.DataTableSummaryList, 1)

	before := aws.ToString(created.LockVersion.DataTable)
	upd, err := f.client.UpdateDataTableMetadata(t.Context(), &sdk.UpdateDataTableMetadataInput{
		InstanceId: inst, DataTableId: created.Id, Name: aws.String("Holidays"), TimeZone: aws.String("UTC"),
		ValueLockLevel: types.DataTableLockLevelValue, Description: aws.String("all sites"),
	})
	require.NoError(t, err)
	assert.NotEqual(t, before, aws.ToString(upd.LockVersion.DataTable), "lock version changes")

	got, err := f.client.DescribeDataTable(
		t.Context(),
		&sdk.DescribeDataTableInput{InstanceId: inst, DataTableId: created.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "Holidays", aws.ToString(got.DataTable.Name))
	assert.Equal(t, "UTC", aws.ToString(got.DataTable.TimeZone))
	assert.Equal(t, types.DataTableLockLevelValue, got.DataTable.ValueLockLevel)
	assert.Equal(t, aws.ToString(upd.LockVersion.DataTable), aws.ToString(got.DataTable.LockVersion.DataTable))

	_, err = f.client.UpdateDataTableMetadata(t.Context(), &sdk.UpdateDataTableMetadataInput{
		InstanceId: inst, DataTableId: created.Id, Name: aws.String("EXTRA-0"), TimeZone: aws.String("UTC"),
		ValueLockLevel: types.DataTableLockLevelNone,
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err), "rename onto existing name")

	dst := f.restored(t)
	got, err = dst.client.DescribeDataTable(
		t.Context(),
		&sdk.DescribeDataTableInput{InstanceId: inst, DataTableId: created.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "Holidays", aws.ToString(got.DataTable.Name))
	assert.Equal(t, map[string]string{"team": "ops"}, got.DataTable.Tags)

	_, err = f.client.DeleteDataTable(t.Context(), &sdk.DeleteDataTableInput{InstanceId: inst, DataTableId: created.Id})
	require.NoError(t, err)
	_, err = f.client.DeleteDataTable(t.Context(), &sdk.DeleteDataTableInput{InstanceId: inst, DataTableId: created.Id})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
	_, err = f.client.UpdateDataTableMetadata(t.Context(),
		&sdk.UpdateDataTableMetadataInput{
			InstanceId: inst, DataTableId: created.Id, Name: aws.String("again"), TimeZone: aws.String("UTC"),
			ValueLockLevel: types.DataTableLockLevelNone,
		})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: created.Arn})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "tags gone with the table")
}

func TestSearchDataTables(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "dtsearch"))

	for _, name := range []string{"alpha", "alphabet", "beta"} {
		out, err := f.client.CreateDataTable(t.Context(), &sdk.CreateDataTableInput{
			InstanceId: inst, Name: aws.String(name), TimeZone: aws.String("UTC"),
			Status: types.DataTableStatusPublished, ValueLockLevel: types.DataTableLockLevelNone,
			Tags: map[string]string{"len": strconv.Itoa(len(name))},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(out.Id))
	}

	tests := []struct {
		criteria *types.DataTableSearchCriteria
		filter   *types.DataTableSearchFilter
		name     string
		want     []string
	}{
		{
			name: "starts with",
			criteria: &types.DataTableSearchCriteria{StringCondition: &types.StringCondition{
				FieldName: aws.String(
					"name",
				),
				Value:          aws.String("alpha"),
				ComparisonType: types.StringComparisonTypeStartsWith,
			}},
			want: []string{"alpha", "alphabet"},
		},
		{
			name: "exact",
			criteria: &types.DataTableSearchCriteria{StringCondition: &types.StringCondition{
				FieldName: aws.String(
					"name",
				),
				Value:          aws.String("BETA"),
				ComparisonType: types.StringComparisonTypeExact,
			}},
			want: []string{"beta"},
		},
		{
			name: "or of exacts",
			criteria: &types.DataTableSearchCriteria{OrConditions: []types.DataTableSearchCriteria{
				{StringCondition: &types.StringCondition{
					FieldName: aws.String(
						"name",
					),
					Value:          aws.String("alpha"),
					ComparisonType: types.StringComparisonTypeExact,
				}},
				{StringCondition: &types.StringCondition{
					FieldName: aws.String(
						"name",
					),
					Value:          aws.String("beta"),
					ComparisonType: types.StringComparisonTypeExact,
				}},
			}},
			want: []string{"alpha", "beta"},
		},
		{
			name: "tag filter",
			filter: &types.DataTableSearchFilter{AttributeFilter: &types.ControlPlaneAttributeFilter{
				TagCondition: &types.TagCondition{TagKey: aws.String("len"), TagValue: aws.String("8")},
			}},
			want: []string{"alphabet"},
		},
	}
	for _, tt := range tests {
		got, err := f.client.SearchDataTables(t.Context(), &sdk.SearchDataTablesInput{
			InstanceId: inst, SearchCriteria: tt.criteria, SearchFilter: tt.filter,
		})
		require.NoError(t, err, tt.name)

		var names []string
		for _, d := range got.DataTables {
			names = append(names, aws.ToString(d.Name))
		}

		assert.ElementsMatch(t, tt.want, names, tt.name)
		assert.EqualValues(t, len(tt.want), aws.ToInt64(got.ApproximateTotalCount), tt.name)
	}

	_, err := f.client.SearchDataTables(t.Context(), &sdk.SearchDataTablesInput{
		InstanceId: inst, SearchCriteria: &types.DataTableSearchCriteria{StringCondition: &types.StringCondition{
			FieldName: aws.String("bogus"), Value: aws.String("x"), ComparisonType: types.StringComparisonTypeExact,
		}},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func TestDataTableAttributes(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "dtattr"))
	table := aws.String(f.newDataTable(t, inst, "attrs", types.DataTableLockLevelNone))

	made, err := f.client.CreateDataTableAttribute(t.Context(), &sdk.CreateDataTableAttributeInput{
		InstanceId:  inst,
		DataTableId: table,
		Name:        aws.String(" site "),
		ValueType:   types.DataTableAttributeValueTypeText,
		Primary:     true,
		Description: aws.String("the site"),
	})
	require.NoError(t, err)
	assert.Equal(t, "site", aws.ToString(made.Name), "name is trimmed")
	assert.NotEmpty(t, aws.ToString(made.AttributeId))
	assert.NotEmpty(t, aws.ToString(made.LockVersion.Attribute))
	assert.NotEmpty(t, aws.ToString(made.LockVersion.DataTable))

	tests := []struct {
		in   sdk.CreateDataTableAttributeInput
		name string
		code string
	}{
		{
			name: "duplicate",
			in: sdk.CreateDataTableAttributeInput{
				Name:      aws.String("SITE"),
				ValueType: types.DataTableAttributeValueTypeText,
			},
			code: "DuplicateResourceException",
		},
		{
			name: "reserved",
			in: sdk.CreateDataTableAttributeInput{
				Name:      aws.String("connect:x"),
				ValueType: types.DataTableAttributeValueTypeText,
			},
			code: "InvalidParameterException",
		},
		{
			name: "bad type",
			in:   sdk.CreateDataTableAttributeInput{Name: aws.String("t"), ValueType: "DATE"},
			code: "InvalidParameterException",
		},
		{
			name: "inverted range",
			in: sdk.CreateDataTableAttributeInput{
				Name: aws.String("n"), ValueType: types.DataTableAttributeValueTypeNumber,
				Validation: &types.Validation{Minimum: 10, Maximum: 5},
			},
			code: "InvalidParameterException",
		},
	}
	for _, tt := range tests {
		in := tt.in
		in.InstanceId, in.DataTableId = inst, table
		_, cerr := f.client.CreateDataTableAttribute(t.Context(), &in)
		assert.Equal(t, tt.code, apiCode(cerr), tt.name)
	}

	_, err = f.client.CreateDataTableAttribute(t.Context(), &sdk.CreateDataTableAttributeInput{
		InstanceId: inst, DataTableId: aws.String("missing"), Name: aws.String("x"),
		ValueType: types.DataTableAttributeValueTypeText,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	f.newDTAttr(t, inst, table, "open", types.DataTableAttributeValueTypeBoolean, false, nil)

	got, err := f.client.DescribeDataTableAttribute(t.Context(), &sdk.DescribeDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("SITE"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(made.AttributeId), aws.ToString(got.Attribute.AttributeId))
	assert.True(t, got.Attribute.Primary)
	assert.Equal(t, "the site", aws.ToString(got.Attribute.Description))
	assert.Equal(t, aws.ToString(table), aws.ToString(got.Attribute.DataTableId))
	assert.Contains(t, aws.ToString(got.Attribute.DataTableArn), aws.ToString(table))

	_, err = f.client.DescribeDataTableAttribute(t.Context(), &sdk.DescribeDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("ghost"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	list, err := f.client.ListDataTableAttributes(t.Context(),
		&sdk.ListDataTableAttributesInput{InstanceId: inst, DataTableId: table})
	require.NoError(t, err)
	require.Len(t, list.Attributes, 2)
	assert.Equal(t, "site", aws.ToString(list.Attributes[0].Name), "creation order")

	byID, err := f.client.ListDataTableAttributes(t.Context(), &sdk.ListDataTableAttributesInput{
		InstanceId: inst, DataTableId: table, AttributeIds: []string{aws.ToString(made.AttributeId)},
	})
	require.NoError(t, err)
	require.Len(t, byID.Attributes, 1)
	assert.Equal(t, "site", aws.ToString(byID.Attributes[0].Name))

	paged, err := f.client.ListDataTableAttributes(t.Context(),
		&sdk.ListDataTableAttributesInput{InstanceId: inst, DataTableId: table, MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	assert.Len(t, paged.Attributes, 1)
	assert.NotEmpty(t, aws.ToString(paged.NextToken))

	upd, err := f.client.UpdateDataTableAttribute(t.Context(), &sdk.UpdateDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("open"), Name: aws.String("is_open"),
		ValueType: types.DataTableAttributeValueTypeText, Description: aws.String("renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "is_open", aws.ToString(upd.Name))
	assert.NotEmpty(t, aws.ToString(upd.LockVersion.Attribute))

	_, err = f.client.UpdateDataTableAttribute(t.Context(), &sdk.UpdateDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("is_open"), Name: aws.String("Site"),
		ValueType: types.DataTableAttributeValueTypeText,
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	renamed, err := f.client.DescribeDataTableAttribute(t.Context(), &sdk.DescribeDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("is_open"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.DataTableAttributeValueTypeText, renamed.Attribute.ValueType)
	assert.Equal(t, "renamed", aws.ToString(renamed.Attribute.Description))

	_, err = f.client.DescribeDataTableAttribute(t.Context(), &sdk.DescribeDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("open"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "old name is gone")

	_, err = f.client.DeleteDataTableAttribute(t.Context(), &sdk.DeleteDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("is_open"),
	})
	require.NoError(t, err)

	_, err = f.client.DeleteDataTableAttribute(t.Context(), &sdk.DeleteDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("is_open"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	dst := f.restored(t)
	list, err = dst.client.ListDataTableAttributes(t.Context(),
		&sdk.ListDataTableAttributesInput{InstanceId: inst, DataTableId: table})
	require.NoError(t, err)
	require.Len(t, list.Attributes, 1)
	assert.Equal(t, "site", aws.ToString(list.Attributes[0].Name))
}

func TestDataTableValues(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "dtvals"))
	table := aws.String(f.newDataTable(t, inst, "vals", types.DataTableLockLevelNone))
	f.newDTAttr(t, inst, table, "site", types.DataTableAttributeValueTypeText, true, nil)
	f.newDTAttr(t, inst, table, "open", types.DataTableAttributeValueTypeBoolean, false, nil)
	f.newDTAttr(t, inst, table, "capacity", types.DataTableAttributeValueTypeNumber, false,
		&types.Validation{Minimum: 1, Maximum: 100, MultipleOf: 1})
	f.newDTAttr(t, inst, table, "tier", types.DataTableAttributeValueTypeText, false,
		&types.Validation{Enum: &types.ValidationEnum{Strict: true, Values: []string{"gold", "silver"}}})
	f.newDTAttr(t, inst, table, "tags", types.DataTableAttributeValueTypeTextList, false,
		&types.Validation{MaxValues: 2})

	val := func(site, attr, v string) types.DataTableValue {
		return types.DataTableValue{
			AttributeName: aws.String(
				attr,
			),
			Value:         aws.String(v),
			PrimaryValues: []types.PrimaryValue{pv("site", site)},
		}
	}

	created, err := f.client.BatchCreateDataTableValue(t.Context(), &sdk.BatchCreateDataTableValueInput{
		InstanceId: inst, DataTableId: table,
		Values: []types.DataTableValue{
			val("nyc", "open", "true"),
			val("nyc", "capacity", "40"),
			val("sfo", "open", "false"),
			val("nyc", "open", "false"),
			val("nyc", "tier", "bronze"),
			val("nyc", "capacity", "abc"),
			val("nyc", "capacity", "500"),
			val("nyc", "capacity", "2.5"),
			val("nyc", "open", "maybe"),
			val("nyc", "tags", `["a","b","c"]`),
			val("nyc", "ghost", "x"),
			{AttributeName: aws.String("open"), Value: aws.String("true")},
			val("nyc", "site", "nyc"),
		},
	})
	require.NoError(t, err)
	require.Len(t, created.Successful, 3)
	require.Len(t, created.Failed, 10)

	assert.Equal(t, "open", aws.ToString(created.Successful[0].AttributeName))
	assert.NotEmpty(t, aws.ToString(created.Successful[0].RecordId))
	assert.NotEmpty(t, aws.ToString(created.Successful[0].LockVersion.Value))
	assert.Equal(t, created.Successful[0].RecordId, created.Successful[1].RecordId, "same primary values, same record")
	assert.NotEqual(t, created.Successful[0].RecordId, created.Successful[2].RecordId)

	failed := map[string]string{}
	for _, fl := range created.Failed {
		failed[aws.ToString(fl.AttributeName)+"="+strconv.Itoa(len(fl.PrimaryValues))] += aws.ToString(fl.Message) + "|"
	}

	assert.Contains(t, failed["open=1"], "already exists")
	assert.Contains(t, failed["tier=1"], "enum")
	assert.Contains(t, failed["capacity=1"], "number")
	assert.Contains(t, failed["tags=1"], "at most 2")
	assert.Contains(t, failed["ghost=1"], "not found")
	assert.Contains(t, failed["open=0"], "primary")
	assert.Contains(t, failed["site=1"], "primary")

	desc, err := f.client.BatchDescribeDataTableValue(t.Context(), &sdk.BatchDescribeDataTableValueInput{
		InstanceId: inst, DataTableId: table,
		Values: []types.DataTableValueIdentifier{
			{AttributeName: aws.String("capacity"), PrimaryValues: []types.PrimaryValue{pv("site", "nyc")}},
			{AttributeName: aws.String("capacity"), PrimaryValues: []types.PrimaryValue{pv("site", "sfo")}},
			{AttributeName: aws.String("site"), PrimaryValues: []types.PrimaryValue{pv("site", "nyc")}},
			{AttributeName: aws.String("capacity"), PrimaryValues: []types.PrimaryValue{pv("nope", "x")}},
		},
	})
	require.NoError(t, err)
	require.Len(t, desc.Successful, 2)
	require.Len(t, desc.Failed, 2)
	assert.Equal(t, "40", aws.ToString(desc.Successful[0].Value))
	assert.Equal(t, "capacity", aws.ToString(desc.Successful[0].AttributeName))
	assert.NotEmpty(t, aws.ToString(desc.Successful[0].AttributeId))
	require.Len(t, desc.Successful[0].PrimaryValues, 1)
	assert.Equal(t, "nyc", aws.ToString(desc.Successful[0].PrimaryValues[0].Value))
	assert.Equal(t, "nyc", aws.ToString(desc.Successful[1].Value), "a primary attribute describes the record key")

	upd, err := f.client.BatchUpdateDataTableValue(t.Context(), &sdk.BatchUpdateDataTableValueInput{
		InstanceId: inst, DataTableId: table,
		Values: []types.DataTableValue{
			val("nyc", "capacity", "60"),
			val("sfo", "capacity", "10"),
			val("nyc", "tier", "gold"),
			val("nyc", "capacity", "0.5"),
		},
	})
	require.NoError(t, err)
	assert.Len(t, upd.Successful, 1)
	assert.Len(t, upd.Failed, 3, "missing value, invalid value and a value that was never created")
	assert.NotEqual(
		t,
		aws.ToString(created.Successful[1].LockVersion.Value),
		aws.ToString(upd.Successful[0].LockVersion.Value),
	)

	cap60, err := f.client.BatchDescribeDataTableValue(t.Context(), &sdk.BatchDescribeDataTableValueInput{
		InstanceId: inst, DataTableId: table,
		Values: []types.DataTableValueIdentifier{
			{AttributeName: aws.String("capacity"), PrimaryValues: []types.PrimaryValue{pv("site", "nyc")}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "60", aws.ToString(cap60.Successful[0].Value))

	vals, err := f.client.ListDataTableValues(
		t.Context(),
		&sdk.ListDataTableValuesInput{InstanceId: inst, DataTableId: table},
	)
	require.NoError(t, err)
	assert.Len(t, vals.Values, 3)

	filtered, err := f.client.ListDataTableValues(t.Context(), &sdk.ListDataTableValuesInput{
		InstanceId: inst, DataTableId: table,
		PrimaryAttributeValues: []types.PrimaryAttributeValueFilter{
			{AttributeName: aws.String("site"), Values: []string{"sfo"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, filtered.Values, 1)
	assert.Equal(t, "open", aws.ToString(filtered.Values[0].AttributeName))
	assert.Equal(t, "false", aws.ToString(filtered.Values[0].Value))
	assert.Equal(t, types.DataTableAttributeValueTypeBoolean, filtered.Values[0].ValueType)
	assert.NotEmpty(t, aws.ToString(filtered.Values[0].RecordId))

	_, err = f.client.ListDataTableValues(t.Context(), &sdk.ListDataTableValuesInput{
		InstanceId: inst, DataTableId: table,
		PrimaryAttributeValues: []types.PrimaryAttributeValueFilter{
			{AttributeName: aws.String("ghost"), Values: []string{"x"}},
		},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	prim, err := f.client.ListDataTablePrimaryValues(t.Context(),
		&sdk.ListDataTablePrimaryValuesInput{InstanceId: inst, DataTableId: table})
	require.NoError(t, err)
	require.Len(t, prim.PrimaryValuesList, 2)
	assert.Equal(t, "site", aws.ToString(prim.PrimaryValuesList[0].PrimaryValues[0].AttributeName))

	byRecord, err := f.client.ListDataTablePrimaryValues(t.Context(), &sdk.ListDataTablePrimaryValuesInput{
		InstanceId: inst, DataTableId: table, RecordIds: []string{aws.ToString(created.Successful[2].RecordId)},
	})
	require.NoError(t, err)
	require.Len(t, byRecord.PrimaryValuesList, 1)
	assert.Equal(t, "sfo", aws.ToString(byRecord.PrimaryValuesList[0].PrimaryValues[0].Value))

	eval, err := f.client.EvaluateDataTableValues(t.Context(), &sdk.EvaluateDataTableValuesInput{
		InstanceId: inst, DataTableId: table, TimeZone: aws.String("Asia/Tokyo"),
		Values: []types.DataTableValueEvaluationSet{
			{
				PrimaryValues:  []types.PrimaryValue{pv("site", "nyc")},
				AttributeNames: []string{"capacity", "tier", "ghost"},
			},
			{PrimaryValues: []types.PrimaryValue{pv("site", "lax")}, AttributeNames: []string{"capacity"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, eval.Values, 4)
	assert.True(t, eval.Values[0].Found)
	assert.Equal(t, "60", aws.ToString(eval.Values[0].EvaluatedValue))
	assert.Equal(t, types.DataTableAttributeValueTypeNumber, eval.Values[0].ValueType)
	assert.False(t, eval.Values[1].Found, "tier was never set")
	assert.False(t, eval.Values[1].Error)
	assert.True(t, eval.Values[2].Error, "unknown attribute")
	assert.False(t, eval.Values[3].Found, "no such record")

	evalPage, err := f.client.EvaluateDataTableValues(t.Context(), &sdk.EvaluateDataTableValuesInput{
		InstanceId: inst, DataTableId: table, MaxResults: aws.Int32(1),
		Values: []types.DataTableValueEvaluationSet{
			{PrimaryValues: []types.PrimaryValue{pv("site", "nyc")}, AttributeNames: []string{"capacity", "open"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, evalPage.Values, 1)
	assert.NotEmpty(t, aws.ToString(evalPage.NextToken))

	_, err = f.client.EvaluateDataTableValues(t.Context(), &sdk.EvaluateDataTableValuesInput{
		InstanceId: inst, DataTableId: table, TimeZone: aws.String("Mars/Base"),
		Values: []types.DataTableValueEvaluationSet{{
			PrimaryValues: []types.PrimaryValue{pv("site", "nyc")}, AttributeNames: []string{"capacity"},
		}},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	uprim, err := f.client.UpdateDataTablePrimaryValues(t.Context(), &sdk.UpdateDataTablePrimaryValuesInput{
		InstanceId:  inst,
		DataTableId: table,
		LockVersion: &types.DataTableLockVersion{},
		PrimaryValues: []types.PrimaryValue{
			pv("site", "nyc"),
		},
		NewPrimaryValues: []types.PrimaryValue{pv("site", "bos")},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(uprim.LockVersion.PrimaryValues))

	tests := []struct {
		old, next string
		name      string
		code      string
	}{
		{old: "nyc", next: "bos2", name: "old key is gone", code: "ResourceNotFoundException"},
		{old: "bos", next: "sfo", name: "new key taken", code: "DuplicateResourceException"},
	}
	for _, tt := range tests {
		_, cerr := f.client.UpdateDataTablePrimaryValues(t.Context(), &sdk.UpdateDataTablePrimaryValuesInput{
			InstanceId:  inst,
			DataTableId: table,
			LockVersion: &types.DataTableLockVersion{},
			PrimaryValues: []types.PrimaryValue{
				pv("site", tt.old),
			},
			NewPrimaryValues: []types.PrimaryValue{pv("site", tt.next)},
		})
		assert.Equal(t, tt.code, apiCode(cerr), tt.name)
	}

	_, err = f.client.UpdateDataTablePrimaryValues(t.Context(), &sdk.UpdateDataTablePrimaryValuesInput{
		InstanceId: inst, DataTableId: table, LockVersion: &types.DataTableLockVersion{},
		PrimaryValues: []types.PrimaryValue{pv("site", "bos")}, NewPrimaryValues: []types.PrimaryValue{pv("zone", "x")},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err), "new key must name the primary attributes")

	moved, err := f.client.BatchDescribeDataTableValue(t.Context(), &sdk.BatchDescribeDataTableValueInput{
		InstanceId: inst, DataTableId: table,
		Values: []types.DataTableValueIdentifier{
			{AttributeName: aws.String("capacity"), PrimaryValues: []types.PrimaryValue{pv("site", "bos")}},
		},
	})
	require.NoError(t, err)
	require.Len(t, moved.Successful, 1, "values follow the record to its new primary values")
	assert.Equal(t, "60", aws.ToString(moved.Successful[0].Value))

	del, err := f.client.BatchDeleteDataTableValue(t.Context(), &sdk.BatchDeleteDataTableValueInput{
		InstanceId: inst, DataTableId: table,
		Values: []types.DataTableDeleteValueIdentifier{
			delID("capacity", "bos"),
			delID("capacity", "bos"),
			delID("open", "sfo"),
		},
	})
	require.NoError(t, err)
	assert.Len(t, del.Successful, 2)
	require.Len(t, del.Failed, 1)
	assert.Contains(t, aws.ToString(del.Failed[0].Message), "not found")

	left, err := f.client.ListDataTableValues(
		t.Context(),
		&sdk.ListDataTableValuesInput{InstanceId: inst, DataTableId: table},
	)
	require.NoError(t, err)
	require.Len(t, left.Values, 1, "only bos/open remains")
	assert.Equal(t, "open", aws.ToString(left.Values[0].AttributeName))

	dst := f.restored(t)
	left, err = dst.client.ListDataTableValues(
		t.Context(),
		&sdk.ListDataTableValuesInput{InstanceId: inst, DataTableId: table},
	)
	require.NoError(t, err)
	require.Len(t, left.Values, 1)

	records, err := f.client.ListDataTablePrimaryValues(t.Context(),
		&sdk.ListDataTablePrimaryValuesInput{InstanceId: inst, DataTableId: table})
	require.NoError(t, err)
	assert.Len(t, records.PrimaryValuesList, 1, "records without values disappear")

	_, err = f.client.DeleteDataTable(t.Context(), &sdk.DeleteDataTableInput{InstanceId: inst, DataTableId: table})
	require.NoError(t, err)
	_, err = f.client.ListDataTableValues(
		t.Context(),
		&sdk.ListDataTableValuesInput{InstanceId: inst, DataTableId: table},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestDataTableValuesWithoutPrimary(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "dtflat"))
	table := aws.String(f.newDataTable(t, inst, "flat", types.DataTableLockLevelNone))
	f.newDTAttr(t, inst, table, "message", types.DataTableAttributeValueTypeText, false, nil)

	out, err := f.client.BatchCreateDataTableValue(t.Context(), &sdk.BatchCreateDataTableValueInput{
		InstanceId: inst, DataTableId: table,
		Values: []types.DataTableValue{
			{AttributeName: aws.String("message"), Value: aws.String("hello")},
			{AttributeName: aws.String("message"), Value: aws.String("again")},
			{
				AttributeName: aws.String("message"), Value: aws.String("x"),
				PrimaryValues: []types.PrimaryValue{pv("site", "nyc")},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.Successful, 1)
	assert.Len(t, out.Failed, 2)

	_, err = f.client.UpdateDataTablePrimaryValues(t.Context(), &sdk.UpdateDataTablePrimaryValuesInput{
		InstanceId: inst, DataTableId: table, LockVersion: &types.DataTableLockVersion{},
		PrimaryValues: []types.PrimaryValue{}, NewPrimaryValues: []types.PrimaryValue{},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err), "no primary attributes to update")

	ev, err := f.client.EvaluateDataTableValues(t.Context(), &sdk.EvaluateDataTableValuesInput{
		InstanceId: inst, DataTableId: table,
		Values: []types.DataTableValueEvaluationSet{{AttributeNames: []string{"message"}}},
	})
	require.NoError(t, err)
	require.Len(t, ev.Values, 1)
	assert.Equal(t, "hello", aws.ToString(ev.Values[0].EvaluatedValue))
}

func TestDataTableLockLevels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stale    func(old *types.DataTableLockVersion) *types.DataTableLockVersion
		name     string
		level    types.DataTableLockLevel
		wantFail bool
	}{
		{
			name: "none ignores locks", level: types.DataTableLockLevelNone, wantFail: false,
			stale: func(*types.DataTableLockVersion) *types.DataTableLockVersion {
				return &types.DataTableLockVersion{Value: aws.String("stale")}
			},
		},
		{
			name: "value level stale", level: types.DataTableLockLevelValue, wantFail: true,
			stale: func(*types.DataTableLockVersion) *types.DataTableLockVersion {
				return &types.DataTableLockVersion{Value: aws.String("stale")}
			},
		},
		{
			name: "value level current", level: types.DataTableLockLevelValue, wantFail: false,
			stale: func(old *types.DataTableLockVersion) *types.DataTableLockVersion { return old },
		},
		{
			name: "primary level stale", level: types.DataTableLockLevelPrimaryValue, wantFail: true,
			stale: func(*types.DataTableLockVersion) *types.DataTableLockVersion {
				return &types.DataTableLockVersion{PrimaryValues: aws.String("stale")}
			},
		},
		{
			name: "attribute level stale", level: types.DataTableLockLevelAttribute, wantFail: true,
			stale: func(*types.DataTableLockVersion) *types.DataTableLockVersion {
				return &types.DataTableLockVersion{Attribute: aws.String("stale")}
			},
		},
		{
			name: "table level stale", level: types.DataTableLockLevelDataTable, wantFail: true,
			stale: func(*types.DataTableLockVersion) *types.DataTableLockVersion {
				return &types.DataTableLockVersion{DataTable: aws.String("stale")}
			},
		},
		{
			name: "mismatch at another level is ignored", level: types.DataTableLockLevelValue, wantFail: false,
			stale: func(*types.DataTableLockVersion) *types.DataTableLockVersion {
				return &types.DataTableLockVersion{DataTable: aws.String("stale")}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			inst := aws.String(f.newInstance(t, "dtlock"))
			table := aws.String(f.newDataTable(t, inst, "locks", tt.level))
			f.newDTAttr(t, inst, table, "k", types.DataTableAttributeValueTypeText, true, nil)
			f.newDTAttr(t, inst, table, "v", types.DataTableAttributeValueTypeText, false, nil)

			key := []types.PrimaryValue{pv("k", "a")}
			made, err := f.client.BatchCreateDataTableValue(t.Context(), &sdk.BatchCreateDataTableValueInput{
				InstanceId:  inst,
				DataTableId: table,
				Values: []types.DataTableValue{
					{AttributeName: aws.String("v"), Value: aws.String("1"), PrimaryValues: key},
				},
			})
			require.NoError(t, err)
			require.Len(t, made.Successful, 1)

			lv := tt.stale(made.Successful[0].LockVersion)
			out, err := f.client.BatchUpdateDataTableValue(t.Context(), &sdk.BatchUpdateDataTableValueInput{
				InstanceId: inst, DataTableId: table,
				Values: []types.DataTableValue{{
					AttributeName: aws.String("v"), Value: aws.String("2"), PrimaryValues: key, LockVersion: lv,
				}},
			})
			require.NoError(t, err)

			if tt.wantFail {
				require.Len(t, out.Failed, 1)
				assert.Contains(t, aws.ToString(out.Failed[0].Message), "lock version")
				assert.Empty(t, out.Successful)

				return
			}

			assert.Empty(t, out.Failed)
			assert.Len(t, out.Successful, 1)
		})
	}
}

func TestDataTablePrimaryUpdateLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		level types.DataTableLockLevel
		code  string
	}{
		{
			name:  "primary level conflicts",
			level: types.DataTableLockLevelPrimaryValue,
			code:  "ConditionalOperationFailedException",
		},
		{name: "none level ignores", level: types.DataTableLockLevelNone, code: ""},
		{name: "value level ignores primary lock", level: types.DataTableLockLevelValue, code: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			inst := aws.String(f.newInstance(t, "dtplock"))
			table := aws.String(f.newDataTable(t, inst, "plock", tt.level))
			f.newDTAttr(t, inst, table, "k", types.DataTableAttributeValueTypeText, true, nil)
			f.newDTAttr(t, inst, table, "v", types.DataTableAttributeValueTypeText, false, nil)

			made, err := f.client.BatchCreateDataTableValue(t.Context(), &sdk.BatchCreateDataTableValueInput{
				InstanceId: inst, DataTableId: table,
				Values: []types.DataTableValue{
					{
						AttributeName: aws.String(
							"v",
						),
						Value:         aws.String("1"),
						PrimaryValues: []types.PrimaryValue{pv("k", "a")},
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, made.Successful, 1)

			in := &sdk.UpdateDataTablePrimaryValuesInput{
				InstanceId:       inst,
				DataTableId:      table,
				LockVersion:      &types.DataTableLockVersion{PrimaryValues: aws.String("old")},
				PrimaryValues:    []types.PrimaryValue{pv("k", "a")},
				NewPrimaryValues: []types.PrimaryValue{pv("k", "b")},
			}
			_, err = f.client.UpdateDataTablePrimaryValues(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))

			if tt.code != "" {
				in.LockVersion = &types.DataTableLockVersion{
					PrimaryValues: made.Successful[0].LockVersion.PrimaryValues,
				}
				_, err = f.client.UpdateDataTablePrimaryValues(t.Context(), in)
				require.NoError(t, err, "the current lock version goes through")
			}
		})
	}
}

func TestDataTablePrimaryAttributeChanges(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "dtprim"))
	table := aws.String(f.newDataTable(t, inst, "prim", types.DataTableLockLevelNone))
	f.newDTAttr(t, inst, table, "a", types.DataTableAttributeValueTypeText, true, nil)
	f.newDTAttr(t, inst, table, "b", types.DataTableAttributeValueTypeText, false, nil)

	_, err := f.client.BatchCreateDataTableValue(t.Context(), &sdk.BatchCreateDataTableValueInput{
		InstanceId: inst, DataTableId: table,
		Values: []types.DataTableValue{
			{AttributeName: aws.String("b"), Value: aws.String("x"), PrimaryValues: []types.PrimaryValue{pv("a", "1")}},
			{AttributeName: aws.String("b"), Value: aws.String("x"), PrimaryValues: []types.PrimaryValue{pv("a", "2")}},
		},
	})
	require.NoError(t, err)

	_, err = f.client.UpdateDataTableAttribute(t.Context(), &sdk.UpdateDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("a"), Name: aws.String("a"),
		ValueType: types.DataTableAttributeValueTypeText, Primary: false,
	})
	assert.Equal(t, "ResourceConflictException", apiCode(err), "dropping the key would merge two records")

	_, err = f.client.DeleteDataTableAttribute(t.Context(), &sdk.DeleteDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("a"),
	})
	assert.Equal(t, "ResourceConflictException", apiCode(err))

	f.newDTAttr(t, inst, table, "c", types.DataTableAttributeValueTypeNumber, true, nil)

	out, err := f.client.ListDataTablePrimaryValues(t.Context(),
		&sdk.ListDataTablePrimaryValuesInput{InstanceId: inst, DataTableId: table})
	require.NoError(t, err)
	require.Len(t, out.PrimaryValuesList, 2)

	for _, r := range out.PrimaryValuesList {
		require.Len(t, r.PrimaryValues, 2, "existing records take the empty value of the new primary attribute")
		assert.Equal(t, "c", aws.ToString(r.PrimaryValues[1].AttributeName))
		assert.Equal(t, "0", aws.ToString(r.PrimaryValues[1].Value))
	}

	_, err = f.client.UpdateDataTableAttribute(t.Context(), &sdk.UpdateDataTableAttributeInput{
		InstanceId: inst, DataTableId: table, AttributeName: aws.String("b"), Name: aws.String("b"),
		ValueType: types.DataTableAttributeValueTypeText, Primary: true,
	})
	require.NoError(t, err)

	out, err = f.client.ListDataTablePrimaryValues(t.Context(),
		&sdk.ListDataTablePrimaryValuesInput{InstanceId: inst, DataTableId: table})
	require.NoError(t, err)
	require.Len(t, out.PrimaryValuesList, 2)
	assert.Len(t, out.PrimaryValuesList[0].PrimaryValues, 3, "b joined the record key with its stored value")
	assert.Equal(t, "x", aws.ToString(out.PrimaryValuesList[0].PrimaryValues[1].Value))

	vals, err := f.client.ListDataTableValues(
		t.Context(),
		&sdk.ListDataTableValuesInput{InstanceId: inst, DataTableId: table},
	)
	require.NoError(t, err)
	assert.Empty(t, vals.Values, "the stored values became key parts")
}
