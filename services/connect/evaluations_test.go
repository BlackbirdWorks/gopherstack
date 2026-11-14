package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scoredFormItems() []types.EvaluationFormItem {
	pick := func(ref string, score int32, fail bool) types.EvaluationFormSingleSelectQuestionOption {
		return types.EvaluationFormSingleSelectQuestionOption{
			RefId: aws.String(ref), Text: aws.String(ref), Score: score, AutomaticFail: fail,
		}
	}

	return []types.EvaluationFormItem{&types.EvaluationFormItemMemberSection{Value: types.EvaluationFormSection{
		RefId: aws.String("sec"), Title: aws.String("Service"), Items: []types.EvaluationFormItem{
			&types.EvaluationFormItemMemberQuestion{Value: types.EvaluationFormQuestion{
				RefId: aws.String(
					"greet",
				),
				Title:        aws.String("Greeting"),
				QuestionType: types.EvaluationFormQuestionTypeSingleselect,
				QuestionTypeProperties: &types.EvaluationFormQuestionTypePropertiesMemberSingleSelect{
					Value: types.EvaluationFormSingleSelectQuestionProperties{
						Options: []types.EvaluationFormSingleSelectQuestionOption{
							pick("yes", 5, false), pick("no", 0, false), pick("rude", 0, true),
						},
					},
				},
			}},
			&types.EvaluationFormItemMemberQuestion{Value: types.EvaluationFormQuestion{
				RefId: aws.String(
					"wait",
				),
				Title:        aws.String("Wait seconds"),
				QuestionType: types.EvaluationFormQuestionTypeNumeric,
				QuestionTypeProperties: &types.EvaluationFormQuestionTypePropertiesMemberNumeric{
					Value: types.EvaluationFormNumericQuestionProperties{
						MinValue: 0,
						MaxValue: 600,
						Options: []types.EvaluationFormNumericQuestionOption{
							{MinValue: 0, MaxValue: 60, Score: 5}, {MinValue: 61, MaxValue: 600, Score: 0},
						},
					},
				},
			}},
			textQuestion("comment"),
		},
	}}}
}

type evalEnv struct {
	*contactEnv
	form    string
	contact string
}

func newEvalEnv(t *testing.T, active bool) *evalEnv {
	t.Helper()

	e := newContactEnv(t)
	form, err := e.f.client.CreateEvaluationForm(t.Context(), &sdk.CreateEvaluationFormInput{
		InstanceId: aws.String(e.inst),
		Title:      aws.String("QA"),
		Items:      scoredFormItems(),
		AsDraft:    !active,
		ScoringStrategy: &types.EvaluationFormScoringStrategy{
			Mode:   types.EvaluationFormScoringModeQuestionOnly,
			Status: types.EvaluationFormScoringStatusEnabled,
		},
	})
	require.NoError(t, err)

	return &evalEnv{
		contactEnv: e,
		form:       aws.ToString(form.EvaluationFormId),
		contact:    e.outbound(t, "+442071230001", true),
	}
}

func (e *evalEnv) start(t *testing.T) *sdk.StartContactEvaluationOutput {
	t.Helper()

	out, err := e.f.client.StartContactEvaluation(t.Context(), &sdk.StartContactEvaluationInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(e.contact), EvaluationFormId: aws.String(e.form),
		Tags: map[string]string{"batch": "1"},
	})
	require.NoError(t, err)

	return out
}

func str(v string) types.EvaluationAnswerInput {
	return types.EvaluationAnswerInput{Value: &types.EvaluationAnswerDataMemberStringValue{Value: v}}
}

func num(v float64) types.EvaluationAnswerInput {
	return types.EvaluationAnswerInput{Value: &types.EvaluationAnswerDataMemberNumericValue{Value: v}}
}

func TestContactEvaluationLifecycle(t *testing.T) {
	t.Parallel()

	e := newEvalEnv(t, true)
	inst := aws.String(e.inst)
	started := e.start(t)
	assert.Contains(t, aws.ToString(started.EvaluationArn), "/evaluation/"+aws.ToString(started.EvaluationId))

	describe := func() *sdk.DescribeContactEvaluationOutput {
		out, err := e.f.client.DescribeContactEvaluation(t.Context(),
			&sdk.DescribeContactEvaluationInput{InstanceId: inst, EvaluationId: started.EvaluationId})
		require.NoError(t, err)

		return out
	}

	draft := describe()
	assert.Equal(t, types.EvaluationStatusDraft, draft.Evaluation.Status)
	assert.Equal(t, e.contact, aws.ToString(draft.Evaluation.Metadata.ContactId))
	assert.Equal(t, "QA", aws.ToString(draft.EvaluationForm.Title))
	assert.Equal(t, e.form, aws.ToString(draft.EvaluationForm.EvaluationFormId))
	assert.EqualValues(t, 1, draft.EvaluationForm.EvaluationFormVersion)
	assert.Equal(t, map[string]string{"batch": "1"}, draft.Evaluation.Tags)
	assert.Nil(t, draft.Evaluation.Metadata.Score)

	_, err := e.f.client.UpdateContactEvaluation(t.Context(), &sdk.UpdateContactEvaluationInput{
		InstanceId:   inst,
		EvaluationId: started.EvaluationId,
		Answers:      map[string]types.EvaluationAnswerInput{"greet": str("yes")},
		Notes:        map[string]types.EvaluationNote{"greet": {Value: aws.String("friendly")}},
		UpdatedBy: &types.EvaluatorUserUnionMemberConnectUserArn{
			Value: "arn:aws:connect:us-east-1:123456789012:instance/x/agent/y",
		},
	})
	require.NoError(t, err)

	saved := describe()
	require.Contains(t, saved.Evaluation.Answers, "greet")
	assert.Equal(t, "friendly", aws.ToString(saved.Evaluation.Notes["greet"].Value))
	assert.Contains(t, aws.ToString(saved.Evaluation.Metadata.EvaluatorArn), "/agent/y")

	_, err = e.f.client.SubmitContactEvaluation(t.Context(), &sdk.SubmitContactEvaluationInput{
		InstanceId: inst, EvaluationId: started.EvaluationId,
		Answers: map[string]types.EvaluationAnswerInput{"wait": num(30), "comment": str("ok")},
	})
	require.NoError(t, err)

	done := describe()
	assert.Equal(t, types.EvaluationStatusSubmitted, done.Evaluation.Status)
	require.NotNil(t, done.Evaluation.Metadata.Score)
	assert.InDelta(t, 100, done.Evaluation.Metadata.Score.Percentage, 0.001)
	assert.EqualValues(t, 10, done.Evaluation.Metadata.Score.EarnedPoints)
	assert.EqualValues(t, 10, done.Evaluation.Metadata.Score.MaxBasePoint)
	require.Contains(t, done.Evaluation.Scores, "sec")
	assert.InDelta(t, 100, done.Evaluation.Scores["sec"].Percentage, 0.001)
	assert.Len(t, done.Evaluation.Answers, 3)

	_, err = e.f.client.UpdateContactEvaluation(t.Context(), &sdk.UpdateContactEvaluationInput{
		InstanceId:   inst,
		EvaluationId: started.EvaluationId,
		Answers:      map[string]types.EvaluationAnswerInput{"comment": str("late")},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err), "submitted evaluations are locked")

	list, err := e.f.client.ListContactEvaluations(
		t.Context(),
		&sdk.ListContactEvaluationsInput{InstanceId: inst, ContactId: aws.String(e.contact)},
	)
	require.NoError(t, err)
	require.Len(t, list.EvaluationSummaryList, 1)
	assert.Equal(t, "QA", aws.ToString(list.EvaluationSummaryList[0].EvaluationFormTitle))
	assert.Equal(t, types.EvaluationStatusSubmitted, list.EvaluationSummaryList[0].Status)

	other, err := e.f.client.ListContactEvaluations(
		t.Context(),
		&sdk.ListContactEvaluationsInput{InstanceId: inst, ContactId: aws.String("other")},
	)
	require.NoError(t, err)
	assert.Empty(t, other.EvaluationSummaryList)

	_, err = e.f.client.DeleteContactEvaluation(
		t.Context(),
		&sdk.DeleteContactEvaluationInput{InstanceId: inst, EvaluationId: started.EvaluationId},
	)
	require.NoError(t, err)

	_, err = e.f.client.DescribeContactEvaluation(
		t.Context(),
		&sdk.DescribeContactEvaluationInput{InstanceId: inst, EvaluationId: started.EvaluationId},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestContactEvaluationScoring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		answers map[string]types.EvaluationAnswerInput
		name    string
		percent float64
		earned  int32
		max     int32
		fail    bool
	}{
		{
			name:    "half",
			answers: map[string]types.EvaluationAnswerInput{"greet": str("yes"), "wait": num(300)},
			percent: 50,
			earned:  5,
			max:     10,
		},
		{
			name:    "zero",
			answers: map[string]types.EvaluationAnswerInput{"greet": str("no"), "wait": num(300)},
			percent: 0,
			earned:  0,
			max:     10,
		},
		{
			name:    "automatic fail",
			answers: map[string]types.EvaluationAnswerInput{"greet": str("rude"), "wait": num(1)},
			percent: 0,
			earned:  5,
			max:     10,
			fail:    true,
		},
		{
			name: "not applicable", percent: 100, earned: 5, max: 5,
			answers: map[string]types.EvaluationAnswerInput{
				"greet": str("yes"), "wait": {Value: &types.EvaluationAnswerDataMemberNotApplicable{Value: true}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEvalEnv(t, true)
			started := e.start(t)

			_, err := e.f.client.SubmitContactEvaluation(t.Context(), &sdk.SubmitContactEvaluationInput{
				InstanceId: aws.String(e.inst), EvaluationId: started.EvaluationId, Answers: tt.answers,
			})
			require.NoError(t, err)

			got, err := e.f.client.DescribeContactEvaluation(t.Context(),
				&sdk.DescribeContactEvaluationInput{InstanceId: aws.String(e.inst), EvaluationId: started.EvaluationId})
			require.NoError(t, err)

			s := got.Evaluation.Metadata.Score
			require.NotNil(t, s)
			assert.InDelta(t, tt.percent, s.Percentage, 0.001)
			assert.Equal(t, tt.earned, s.EarnedPoints)
			assert.Equal(t, tt.max, s.MaxBasePoint)
			assert.Equal(t, tt.fail, s.AutomaticFail)
		})
	}
}

func TestContactEvaluationValidation(t *testing.T) {
	t.Parallel()

	e := newEvalEnv(t, true)
	started := e.start(t)

	update := func(answers map[string]types.EvaluationAnswerInput, notes map[string]types.EvaluationNote) error {
		_, err := e.f.client.UpdateContactEvaluation(t.Context(), &sdk.UpdateContactEvaluationInput{
			InstanceId: aws.String(e.inst), EvaluationId: started.EvaluationId, Answers: answers, Notes: notes,
		})

		return err
	}

	for name, answers := range map[string]map[string]types.EvaluationAnswerInput{
		"unknown question":     {"nope": str("x")},
		"unknown option":       {"greet": str("maybe")},
		"numeric out of range": {"wait": num(601)},
		"numeric as string":    {"wait": str("5")},
		"text without value":   {"comment": str("")},
	} {
		assert.Equal(t, "InvalidParameterException", apiCode(update(answers, nil)), name)
	}

	assert.Equal(
		t,
		"InvalidParameterException",
		apiCode(update(nil, map[string]types.EvaluationNote{"nope": {Value: aws.String("x")}})),
	)
	require.NoError(t, update(map[string]types.EvaluationAnswerInput{"greet": str("yes")}, nil))

	bad := func(mut func(*sdk.StartContactEvaluationInput)) error {
		in := &sdk.StartContactEvaluationInput{
			InstanceId:       aws.String(e.inst),
			ContactId:        aws.String(e.contact),
			EvaluationFormId: aws.String(e.form),
		}
		mut(in)

		_, err := e.f.client.StartContactEvaluation(t.Context(), in)

		return err
	}

	assert.Equal(
		t,
		"ContactNotFoundException",
		apiCode(bad(func(in *sdk.StartContactEvaluationInput) { in.ContactId = aws.String("nope") })),
	)
	assert.Equal(
		t,
		"ResourceNotFoundException",
		apiCode(bad(func(in *sdk.StartContactEvaluationInput) { in.EvaluationFormId = aws.String("nope") })),
	)

	draftForm := newEvalEnv(t, false)
	_, err := draftForm.f.client.StartContactEvaluation(t.Context(), &sdk.StartContactEvaluationInput{
		InstanceId: aws.String(
			draftForm.inst,
		),
		ContactId:        aws.String(draftForm.contact),
		EvaluationFormId: aws.String(draftForm.form),
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err), "draft forms cannot be evaluated against")
}

func TestContactEvaluationsSurviveRestore(t *testing.T) {
	t.Parallel()

	e := newEvalEnv(t, true)
	started := e.start(t)

	r := e.f.restored(t)
	got, err := r.client.DescribeContactEvaluation(t.Context(),
		&sdk.DescribeContactEvaluationInput{InstanceId: aws.String(e.inst), EvaluationId: started.EvaluationId})
	require.NoError(t, err)
	assert.Equal(t, types.EvaluationStatusDraft, got.Evaluation.Status)

	tags, err := r.client.ListTagsForResource(
		t.Context(),
		&sdk.ListTagsForResourceInput{ResourceArn: started.EvaluationArn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"batch": "1"}, tags.Tags)
}

func TestContactEvaluationsOnContact(t *testing.T) {
	t.Parallel()

	e := newEvalEnv(t, true)
	inst := aws.String(e.inst)

	contact := func() *types.Contact {
		return e.describe(t, e.contact)
	}

	assert.Empty(t, contact().ContactEvaluations)
	assert.Equal(t, aws.ToString(contact().InitialContactId), aws.ToString(contact().ContactAssociationId))
	assert.NotEmpty(t, aws.ToString(contact().ContactAssociationId))

	started := e.start(t)

	got := contact().ContactEvaluations[e.form]
	assert.Equal(t, types.StatusInProgress, got.Status)
	assert.Equal(t, aws.ToString(started.EvaluationArn), aws.ToString(got.EvaluationArn))
	assert.Equal(t, e.form, aws.ToString(got.FormId))
	assert.NotNil(t, got.StartTimestamp)
	assert.Nil(t, got.EndTimestamp)

	_, err := e.f.client.SubmitContactEvaluation(t.Context(), &sdk.SubmitContactEvaluationInput{
		InstanceId: inst, EvaluationId: started.EvaluationId,
		Answers: map[string]types.EvaluationAnswerInput{"greet": str("yes"), "wait": num(30), "comment": str("ok")},
	})
	require.NoError(t, err)

	got = contact().ContactEvaluations[e.form]
	assert.Equal(t, types.StatusComplete, got.Status)
	assert.NotNil(t, got.EndTimestamp)

	_, err = e.f.client.DeleteContactEvaluation(t.Context(), &sdk.DeleteContactEvaluationInput{
		InstanceId: inst, EvaluationId: started.EvaluationId,
	})
	require.NoError(t, err)
	assert.Empty(t, contact().ContactEvaluations)
}

func TestTaskTemplateInfoOnContact(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)

	tmpl, err := e.f.client.CreateTaskTemplate(t.Context(), &sdk.CreateTaskTemplateInput{
		InstanceId: aws.String(e.inst), Name: aws.String("followup"), Status: types.TaskTemplateStatusActive,
		Fields: []types.TaskTemplateField{tmplField("Description", types.TaskTemplateFieldTypeDescription)},
	})
	require.NoError(t, err)

	out, err := e.f.client.StartTaskContact(t.Context(), &sdk.StartTaskContactInput{
		InstanceId: aws.String(
			e.inst,
		),
		TaskTemplateId: tmpl.Id,
		Name:           aws.String("t"),
		ContactFlowId:  aws.String(e.flow),
	})
	require.NoError(t, err)

	got := e.describe(t, aws.ToString(out.ContactId))
	require.NotNil(t, got.TaskTemplateInfo)
	assert.Equal(t, aws.ToString(tmpl.Arn), aws.ToString(got.TaskTemplateInfo.Arn))
	assert.Equal(t, "followup", aws.ToString(got.TaskTemplateInfo.Name))

	assert.Nil(t, e.describe(t, e.task(t)).TaskTemplateInfo)
}
