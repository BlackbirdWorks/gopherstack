package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testCaseBody = `{"steps":[{"type":"observe"}]}`

func (e *contactEnv) chatEntry() *types.TestCaseEntryPoint {
	return &types.TestCaseEntryPoint{
		Type:                     types.TestCaseEntryPointTypeChat,
		ChatEntryPointParameters: &types.ChatEntryPointParameters{FlowId: aws.String(e.flow)},
	}
}

func (e *contactEnv) newTestCase(
	t *testing.T,
	name string,
	mut func(*sdk.CreateTestCaseInput),
) *sdk.CreateTestCaseOutput {
	t.Helper()

	in := &sdk.CreateTestCaseInput{
		InstanceId: aws.String(
			e.inst,
		),
		Name:        aws.String(name),
		Content:     aws.String(testCaseBody),
		Description: aws.String("d"),
		EntryPoint:  e.chatEntry(),
		Tags:        map[string]string{"suite": "smoke"},
	}
	if mut != nil {
		mut(in)
	}

	out, err := e.f.client.CreateTestCase(t.Context(), in)
	require.NoError(t, err)

	return out
}

func TestTestCaseVersions(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)
	created := e.newTestCase(t, "login", nil)
	assert.Contains(t, aws.ToString(created.TestCaseArn), "/test-case/"+aws.ToString(created.TestCaseId))

	describe := func(status types.TestCaseStatus) (*sdk.DescribeTestCaseOutput, error) {
		return e.f.client.DescribeTestCase(
			t.Context(),
			&sdk.DescribeTestCaseInput{InstanceId: inst, TestCaseId: created.TestCaseId, Status: status},
		)
	}

	saved, err := describe("")
	require.NoError(t, err)
	tc := saved.TestCase
	assert.Equal(t, types.TestCaseStatusSaved, tc.Status)
	assert.JSONEq(t, testCaseBody, aws.ToString(tc.Content))
	assert.Equal(t, "d", aws.ToString(tc.Description))
	assert.Equal(t, map[string]string{"suite": "smoke"}, tc.Tags)
	assert.Len(t, aws.ToString(tc.TestCaseSha256), 64)
	assert.Equal(t, e.flow, aws.ToString(tc.EntryPoint.ChatEntryPointParameters.FlowId))

	_, err = describe(types.TestCaseStatusPublished)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "nothing published yet")

	e.f.clock.advance(60e9)

	_, err = e.f.client.UpdateTestCase(t.Context(), &sdk.UpdateTestCaseInput{
		InstanceId: inst, TestCaseId: created.TestCaseId, Status: types.TestCaseStatusPublished,
		Content: aws.String(`{"steps":[]}`),
	})
	require.NoError(t, err)

	def, err := describe("")
	require.NoError(t, err)
	assert.Equal(t, types.TestCaseStatusPublished, def.TestCase.Status, "published wins by default")
	assert.JSONEq(t, `{"steps":[]}`, aws.ToString(def.TestCase.Content))
	assert.NotEqual(t, aws.ToString(tc.TestCaseSha256), aws.ToString(def.TestCase.TestCaseSha256))

	old, err := describe(types.TestCaseStatusSaved)
	require.NoError(t, err)
	assert.JSONEq(t, testCaseBody, aws.ToString(old.TestCase.Content), "the saved version is untouched")
	assert.True(t, def.TestCase.LastModifiedTime.After(*old.TestCase.LastModifiedTime))

	_, err = e.f.client.UpdateTestCase(t.Context(), &sdk.UpdateTestCaseInput{
		InstanceId:  inst,
		TestCaseId:  created.TestCaseId,
		Name:        aws.String("login v2"),
		Description: aws.String("changed"),
	})
	require.NoError(t, err)

	renamed, err := describe(types.TestCaseStatusSaved)
	require.NoError(t, err)
	assert.Equal(t, "login v2", aws.ToString(renamed.TestCase.Name))
	assert.Equal(t, "changed", aws.ToString(renamed.TestCase.Description))

	_, err = e.f.client.DeleteTestCase(
		t.Context(),
		&sdk.DeleteTestCaseInput{InstanceId: inst, TestCaseId: created.TestCaseId},
	)
	require.NoError(t, err)

	_, err = describe("")
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = e.f.client.DeleteTestCase(
		t.Context(),
		&sdk.DeleteTestCaseInput{InstanceId: inst, TestCaseId: created.TestCaseId},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestCreateTestCaseValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(e *contactEnv, in *sdk.CreateTestCaseInput)
		name string
		code string
	}{
		{
			name: "saved needs no entry point",
			mut:  func(_ *contactEnv, in *sdk.CreateTestCaseInput) { in.EntryPoint = nil },
		},
		{
			name: "saved content is not validated",
			mut:  func(_ *contactEnv, in *sdk.CreateTestCaseInput) { in.Content = aws.String("not json") },
		},
		{
			name: "published needs entry point",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.CreateTestCaseInput) {
				in.Status, in.EntryPoint = types.TestCaseStatusPublished, nil
			},
		},
		{
			name: "published needs json content",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.CreateTestCaseInput) {
				in.Status, in.Content = types.TestCaseStatusPublished, aws.String("not json")
			},
		},
		{
			name: "published ok",
			mut:  func(_ *contactEnv, in *sdk.CreateTestCaseInput) { in.Status = types.TestCaseStatusPublished },
		},
		{
			name: "empty content",
			code: "InvalidParameterException",
			mut:  func(_ *contactEnv, in *sdk.CreateTestCaseInput) { in.Content = aws.String("") },
		},
		{
			name: "bad init data",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.CreateTestCaseInput) {
				in.InitializationData = aws.String("{")
			},
		},
		{
			name: "voice entry without params",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.CreateTestCaseInput) {
				in.EntryPoint = &types.TestCaseEntryPoint{Type: types.TestCaseEntryPointTypeVoiceCall}
			},
		},
		{
			name: "chat entry without params",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.CreateTestCaseInput) {
				in.EntryPoint = &types.TestCaseEntryPoint{Type: types.TestCaseEntryPointTypeChat}
			},
		},
		{
			name: "unknown flow",
			code: "ResourceNotFoundException",
			mut: func(_ *contactEnv, in *sdk.CreateTestCaseInput) {
				in.EntryPoint.ChatEntryPointParameters.FlowId = aws.String("nope")
			},
		},
		{
			name: "bad status",
			code: "InvalidParameterException",
			mut:  func(_ *contactEnv, in *sdk.CreateTestCaseInput) { in.Status = "DRAFT" },
		},
		{
			name: "unknown instance",
			code: "ResourceNotFoundException",
			mut: func(_ *contactEnv, in *sdk.CreateTestCaseInput) {
				in.InstanceId = aws.String("missing")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			in := &sdk.CreateTestCaseInput{
				InstanceId: aws.String(
					e.inst,
				),
				Name:       aws.String("t"),
				Content:    aws.String(testCaseBody),
				EntryPoint: e.chatEntry(),
			}
			tt.mut(e, in)

			_, err := e.f.client.CreateTestCase(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestTestCaseNamesAreUnique(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	first := e.newTestCase(t, "Smoke", nil)

	_, err := e.f.client.CreateTestCase(t.Context(), &sdk.CreateTestCaseInput{
		InstanceId: aws.String(e.inst), Name: aws.String("smoke"), Content: aws.String(testCaseBody),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	second := e.newTestCase(t, "other", nil)

	_, err = e.f.client.UpdateTestCase(t.Context(), &sdk.UpdateTestCaseInput{
		InstanceId: aws.String(e.inst), TestCaseId: second.TestCaseId, Name: aws.String("SMOKE"),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = e.f.client.UpdateTestCase(t.Context(), &sdk.UpdateTestCaseInput{
		InstanceId: aws.String(e.inst), TestCaseId: first.TestCaseId, Name: aws.String("SMOKE"),
	})
	assert.NoError(t, err, "renaming to its own name is fine")
}

func TestListAndSearchTestCases(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	e.newTestCase(t, "beta", nil)
	e.newTestCase(t, "alpha", func(in *sdk.CreateTestCaseInput) { in.Status = types.TestCaseStatusPublished })
	e.newTestCase(t, "gamma", func(in *sdk.CreateTestCaseInput) {
		in.Tags = map[string]string{"suite": "full"}
		in.Description = aws.String("covers the checkout")
	})

	list, err := e.f.client.ListTestCases(
		t.Context(),
		&sdk.ListTestCasesInput{InstanceId: aws.String(e.inst), MaxResults: aws.Int32(2)},
	)
	require.NoError(t, err)
	require.Len(t, list.TestCaseSummaryList, 2)
	require.NotNil(t, list.NextToken)
	assert.Equal(t, "alpha", aws.ToString(list.TestCaseSummaryList[0].Name))
	assert.Equal(t, types.TestCaseStatusPublished, list.TestCaseSummaryList[0].Status)
	assert.Equal(t, "beta", aws.ToString(list.TestCaseSummaryList[1].Name))

	search := func(in *sdk.SearchTestCasesInput) []types.TestCase {
		in.InstanceId = aws.String(e.inst)
		out, serr := e.f.client.SearchTestCases(t.Context(), in)
		require.NoError(t, serr)

		return out.TestCases
	}

	byName := search(&sdk.SearchTestCasesInput{SearchCriteria: &types.TestCaseSearchCriteria{
		StringCondition: &types.StringCondition{
			FieldName:      aws.String("name"),
			Value:          aws.String("amm"),
			ComparisonType: types.StringComparisonTypeContains,
		},
	}})
	require.Len(t, byName, 1)
	assert.Equal(t, "gamma", aws.ToString(byName[0].Name))

	published := search(
		&sdk.SearchTestCasesInput{
			SearchCriteria: &types.TestCaseSearchCriteria{StatusCondition: types.TestCaseStatusPublished},
		},
	)
	require.Len(t, published, 1)
	assert.Equal(t, "alpha", aws.ToString(published[0].Name))

	byTag := search(
		&sdk.SearchTestCasesInput{SearchFilter: &types.TestCaseSearchFilter{TagFilter: &types.ControlPlaneTagFilter{
			TagCondition: &types.TagCondition{TagKey: aws.String("suite"), TagValue: aws.String("full")},
		}}},
	)
	require.Len(t, byTag, 1)

	assert.Len(t, search(&sdk.SearchTestCasesInput{}), 3)
}

func TestTestCasesSurviveRestore(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	created := e.newTestCase(t, "persisted", nil)

	r := e.f.restored(t)
	got, err := r.client.DescribeTestCase(
		t.Context(),
		&sdk.DescribeTestCaseInput{InstanceId: aws.String(e.inst), TestCaseId: created.TestCaseId},
	)
	require.NoError(t, err)
	assert.Equal(t, "persisted", aws.ToString(got.TestCase.Name))

	tags, err := r.client.ListTagsForResource(
		t.Context(),
		&sdk.ListTagsForResourceInput{ResourceArn: created.TestCaseArn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"suite": "smoke"}, tags.Tags)
}
