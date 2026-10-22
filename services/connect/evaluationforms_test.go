package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func textQuestion(ref string) types.EvaluationFormItem {
	return &types.EvaluationFormItemMemberQuestion{Value: types.EvaluationFormQuestion{
		RefId: aws.String(ref), Title: aws.String("Q " + ref), QuestionType: types.EvaluationFormQuestionTypeText,
	}}
}

func formItems(refs ...string) []types.EvaluationFormItem {
	qs := make([]types.EvaluationFormItem, 0, len(refs))
	for _, r := range refs {
		qs = append(qs, textQuestion(r))
	}

	return []types.EvaluationFormItem{&types.EvaluationFormItemMemberSection{Value: types.EvaluationFormSection{
		RefId: aws.String("sec"), Title: aws.String("Section"), Items: qs,
	}}}
}

func TestEvaluationFormCreateValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "formvalid"))

	tests := []struct {
		mutate func(in *sdk.CreateEvaluationFormInput)
		name   string
	}{
		{name: "duplicate ref", mutate: func(in *sdk.CreateEvaluationFormInput) { in.Items = formItems("a", "a") }},
		{
			name:   "ref clashes with section",
			mutate: func(in *sdk.CreateEvaluationFormInput) { in.Items = formItems("sec") },
		},
		{name: "bad question type", mutate: func(in *sdk.CreateEvaluationFormInput) {
			in.Items = []types.EvaluationFormItem{
				&types.EvaluationFormItemMemberQuestion{Value: types.EvaluationFormQuestion{
					RefId: aws.String("a"), Title: aws.String("t"), QuestionType: "RIDDLE",
				}},
			}
		}},
		{name: "numeric without properties", mutate: func(in *sdk.CreateEvaluationFormInput) {
			in.Items = []types.EvaluationFormItem{
				&types.EvaluationFormItemMemberQuestion{Value: types.EvaluationFormQuestion{
					RefId: aws.String(
						"a",
					),
					Title:        aws.String("t"),
					QuestionType: types.EvaluationFormQuestionTypeNumeric,
				}},
			}
		}},
		{name: "blank section title", mutate: func(in *sdk.CreateEvaluationFormInput) {
			in.Items = []types.EvaluationFormItem{
				&types.EvaluationFormItemMemberSection{Value: types.EvaluationFormSection{
					RefId: aws.String("s"), Title: aws.String(" "), Items: []types.EvaluationFormItem{},
				}},
			}
		}},
		{name: "bad scoring mode", mutate: func(in *sdk.CreateEvaluationFormInput) {
			in.ScoringStrategy = &types.EvaluationFormScoringStrategy{
				Mode:   "VIBES",
				Status: types.EvaluationFormScoringStatusEnabled,
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreateEvaluationFormInput{
				InstanceId: inst,
				Title:      aws.String("f-" + tt.name),
				Items:      formItems("a"),
			}
			tt.mutate(in)
			_, err := f.client.CreateEvaluationForm(t.Context(), in)
			assert.Equal(t, "InvalidParameterException", apiCode(err))
		})
	}
}

func TestEvaluationFormLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "forms"))

	created, err := f.client.CreateEvaluationForm(t.Context(), &sdk.CreateEvaluationFormInput{
		InstanceId: inst, Title: aws.String("QA"), Items: formItems("a", "b"), AsDraft: true,
		Tags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	_, err = f.client.CreateEvaluationForm(t.Context(), &sdk.CreateEvaluationFormInput{
		InstanceId: inst, Title: aws.String("QA"), Items: formItems("a"),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	got, err := f.client.DescribeEvaluationForm(t.Context(), &sdk.DescribeEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.EvaluationFormVersionStatusDraft, got.EvaluationForm.Status)
	assert.Equal(t, int32(1), got.EvaluationForm.EvaluationFormVersion)
	assert.False(t, got.EvaluationForm.Locked)
	assert.Len(t, got.EvaluationForm.Items, 1)
	assert.Equal(t, "v", got.EvaluationForm.Tags["k"])

	upd, err := f.client.UpdateEvaluationForm(t.Context(), &sdk.UpdateEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: 1,
		Title: aws.String("QA v1"), Items: formItems("a"), AsDraft: true,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), upd.EvaluationFormVersion)

	act, err := f.client.ActivateEvaluationForm(t.Context(), &sdk.ActivateEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, created.EvaluationFormArn, act.EvaluationFormArn)

	_, err = f.client.ActivateEvaluationForm(t.Context(), &sdk.ActivateEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: 1,
	})
	assert.Equal(t, "ResourceConflictException", apiCode(err), "already active")

	_, err = f.client.UpdateEvaluationForm(t.Context(), &sdk.UpdateEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: 1,
		Title: aws.String("QA v1"), Items: formItems("a"), AsDraft: true,
	})
	assert.Equal(t, "ResourceConflictException", apiCode(err), "locked version")

	v2, err := f.client.UpdateEvaluationForm(t.Context(), &sdk.UpdateEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: 1,
		Title: aws.String("QA v2"), Items: formItems("a", "b"), AsDraft: true, CreateNewVersion: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), v2.EvaluationFormVersion)

	_, err = f.client.ActivateEvaluationForm(t.Context(), &sdk.ActivateEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: 2,
	})
	require.NoError(t, err)

	versions, err := f.client.ListEvaluationFormVersions(t.Context(), &sdk.ListEvaluationFormVersionsInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId,
	})
	require.NoError(t, err)
	require.Len(t, versions.EvaluationFormVersionSummaryList, 2)
	assert.Equal(t, int32(2), versions.EvaluationFormVersionSummaryList[0].EvaluationFormVersion)
	assert.Equal(t, types.EvaluationFormVersionStatusActive, versions.EvaluationFormVersionSummaryList[0].Status)
	assert.Equal(
		t,
		types.EvaluationFormVersionStatusDraft,
		versions.EvaluationFormVersionSummaryList[1].Status,
		"previous active returns to draft",
	)

	list, err := f.client.ListEvaluationForms(t.Context(), &sdk.ListEvaluationFormsInput{InstanceId: inst})
	require.NoError(t, err)
	require.Len(t, list.EvaluationFormSummaryList, 1)
	assert.Equal(t, int32(2), list.EvaluationFormSummaryList[0].LatestVersion)
	assert.Equal(t, int32(2), aws.ToInt32(list.EvaluationFormSummaryList[0].ActiveVersion))
	assert.Equal(t, "QA v2", aws.ToString(list.EvaluationFormSummaryList[0].Title))

	_, err = f.client.DeleteEvaluationForm(t.Context(), &sdk.DeleteEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: aws.Int32(2),
	})
	assert.Equal(t, "ResourceInUseException", apiCode(err), "active version")

	_, err = f.client.DeactivateEvaluationForm(t.Context(), &sdk.DeactivateEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: 2,
	})
	require.NoError(t, err)

	dst := f.restored(t)
	got, err = dst.client.DescribeEvaluationForm(t.Context(), &sdk.DescribeEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.True(t, got.EvaluationForm.Locked)

	_, err = f.client.DeleteEvaluationForm(t.Context(), &sdk.DeleteEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: aws.Int32(1),
	})
	require.NoError(t, err)
	_, err = f.client.DescribeEvaluationForm(t.Context(), &sdk.DescribeEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: created.EvaluationFormId, EvaluationFormVersion: aws.Int32(1),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteEvaluationForm(
		t.Context(),
		&sdk.DeleteEvaluationFormInput{InstanceId: inst, EvaluationFormId: created.EvaluationFormId},
	)
	require.NoError(t, err)
	_, err = f.client.DeleteEvaluationForm(
		t.Context(),
		&sdk.DeleteEvaluationFormInput{InstanceId: inst, EvaluationFormId: created.EvaluationFormId},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestEvaluationFormValidationAndSearch(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "formsearch"))

	empty, err := f.client.CreateEvaluationForm(t.Context(), &sdk.CreateEvaluationFormInput{
		InstanceId: inst, Title: aws.String("Empty section"), AsDraft: true,
		Items: []types.EvaluationFormItem{&types.EvaluationFormItemMemberSection{Value: types.EvaluationFormSection{
			RefId: aws.String("s"), Title: aws.String("S"), Items: []types.EvaluationFormItem{},
		}}},
	})
	require.NoError(t, err)

	_, err = f.client.GetEvaluationFormValidation(t.Context(), &sdk.GetEvaluationFormValidationInput{
		InstanceId: inst, EvaluationFormId: empty.EvaluationFormId,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "never validated")

	_, err = f.client.StartEvaluationFormValidation(t.Context(), &sdk.StartEvaluationFormValidationInput{
		InstanceId: inst, EvaluationFormId: empty.EvaluationFormId, EvaluationFormVersion: 1,
	})
	require.NoError(t, err)

	busy, err := f.client.GetEvaluationFormValidation(t.Context(), &sdk.GetEvaluationFormValidationInput{
		InstanceId: inst, EvaluationFormId: empty.EvaluationFormId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.EvaluationFormValidationStatusInProgress, busy.Status)

	f.clock.advance(2 * time.Second)

	done, err := f.client.GetEvaluationFormValidation(t.Context(), &sdk.GetEvaluationFormValidationInput{
		InstanceId: inst, EvaluationFormId: empty.EvaluationFormId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.EvaluationFormValidationStatusCompleted, done.Status)
	require.Len(t, done.Findings, 1)
	assert.Equal(t, types.EvaluationFormValidationFindingSeverityWarning, done.Findings[0].Severity)

	desc, err := f.client.DescribeEvaluationForm(t.Context(), &sdk.DescribeEvaluationFormInput{
		InstanceId: inst, EvaluationFormId: empty.EvaluationFormId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.EvaluationFormValidationStatusCompleted, desc.EvaluationForm.LatestValidationStatus)

	_, err = f.client.CreateEvaluationForm(t.Context(), &sdk.CreateEvaluationFormInput{
		InstanceId: inst, Title: aws.String("Support QA"), Items: formItems("a"), Description: aws.String("calls"),
	})
	require.NoError(t, err)

	out, err := f.client.SearchEvaluationForms(t.Context(), &sdk.SearchEvaluationFormsInput{
		InstanceId: inst,
		SearchCriteria: &types.EvaluationFormSearchCriteria{
			StringCondition: strCond("title", "support", types.StringComparisonTypeStartsWith),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.EvaluationFormSearchSummaryList, 1)
	assert.Equal(t, types.EvaluationFormVersionStatusActive, out.EvaluationFormSearchSummaryList[0].Status)
	assert.Equal(t, "calls", aws.ToString(out.EvaluationFormSearchSummaryList[0].Description))

	_, err = f.client.SearchEvaluationForms(t.Context(), &sdk.SearchEvaluationFormsInput{
		InstanceId: inst,
		SearchCriteria: &types.EvaluationFormSearchCriteria{
			AndConditions: []types.EvaluationFormSearchCriteria{{BooleanCondition: &types.BooleanCondition{}}},
		},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))
}
