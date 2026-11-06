package connect

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	labelEvaluation  = "Evaluation"
	evalDraft        = "DRAFT"
	evalSubmitted    = "SUBMITTED"
	evalTypeStandard = "STANDARD"
	maxEvalNote      = 3072
	qtSingle         = "SINGLESELECT"
	qtMulti          = "MULTISELECT"
	qtNumeric        = "NUMERIC"
	pctScale         = 100
)

// EvaluationAnswerData is the value of one answer.
type EvaluationAnswerData struct {
	NumericValue  *float64 `json:"NumericValue,omitempty"`
	NotApplicable *bool    `json:"NotApplicable,omitempty"`
	StringValue   string   `json:"StringValue,omitempty"`
	DateTimeValue string   `json:"DateTimeValue,omitempty"`
	StringValues  []string `json:"StringValues,omitempty"`
}

// EvaluationAnswer wraps an answer value as the SDK sends and returns it.
type EvaluationAnswer struct {
	Value EvaluationAnswerData `json:"Value"`
}

// EvaluationNote wraps a note.
type EvaluationNote struct {
	Value string `json:"Value"`
}

// EvaluationScore is a computed score.
type EvaluationScore struct {
	Percentage    float64 `json:"Percentage"`
	NotApplicable bool    `json:"NotApplicable"`
	AutomaticFail bool    `json:"AutomaticFail"`
	EarnedPoints  int32   `json:"EarnedPoints"`
	MaxBasePoint  int32   `json:"MaxBasePoint"`
}

type evaluationRec struct {
	CreatedTime    awstime.EpochTime           `json:"createdTime"`
	LastModified   awstime.EpochTime           `json:"lastModified"`
	Answers        map[string]EvaluationAnswer `json:"answers,omitempty"`
	Notes          map[string]EvaluationNote   `json:"notes,omitempty"`
	Scores         map[string]EvaluationScore  `json:"scores,omitempty"`
	Tags           map[string]string           `json:"tags,omitempty"`
	Score          *EvaluationScore            `json:"score,omitempty"`
	ID             string                      `json:"id"`
	InstanceID     string                      `json:"instanceId"`
	Arn            string                      `json:"arn"`
	ContactID      string                      `json:"contactId"`
	FormID         string                      `json:"formId"`
	Status         string                      `json:"status"`
	EvaluatorArn   string                      `json:"evaluatorArn,omitempty"`
	FormVersion    int32                       `json:"formVersion"`
	AutoEvaluation bool                        `json:"autoEvaluation,omitempty"`
}

// EvaluationMetadata describes what an evaluation is about.
type EvaluationMetadata struct {
	Score          *EvaluationScore `json:"Score,omitempty"`
	AutoEvaluation *struct {
		AutoEvaluationEnabled bool `json:"AutoEvaluationEnabled"`
	} `json:"AutoEvaluation,omitempty"`
	ContactID      string `json:"ContactId"`
	EvaluatorArn   string `json:"EvaluatorArn,omitempty"`
	ContactAgentID string `json:"ContactAgentId,omitempty"`
}

// Evaluation is a contact evaluation.
type Evaluation struct {
	Metadata         EvaluationMetadata          `json:"Metadata"`
	Answers          map[string]EvaluationAnswer `json:"Answers"`
	Notes            map[string]EvaluationNote   `json:"Notes"`
	Scores           map[string]EvaluationScore  `json:"Scores"`
	Tags             map[string]string           `json:"Tags,omitempty"`
	CreatedTime      awstime.EpochTime           `json:"CreatedTime"`
	LastModifiedTime awstime.EpochTime           `json:"LastModifiedTime"`
	EvaluationID     string                      `json:"EvaluationId"`
	EvaluationArn    string                      `json:"EvaluationArn"`
	Status           string                      `json:"Status"`
	EvaluationType   string                      `json:"EvaluationType"`
}

// EvaluationFormContent is the form version an evaluation was started from.
type EvaluationFormContent struct {
	EvaluationFormArn string `json:"EvaluationFormArn"`
	EvaluationFormID  string `json:"EvaluationFormId"`
	formContent
	EvaluationFormVersion int32 `json:"EvaluationFormVersion"`
}

type evaluationDescribeOut struct {
	Evaluation     Evaluation            `json:"Evaluation"`
	EvaluationForm EvaluationFormContent `json:"EvaluationForm"`
}

// EvaluationSummary is the list shape of a contact evaluation.
type EvaluationSummary struct {
	Score                 *EvaluationScore  `json:"Score,omitempty"`
	CreatedTime           awstime.EpochTime `json:"CreatedTime"`
	LastModifiedTime      awstime.EpochTime `json:"LastModifiedTime"`
	EvaluationID          string            `json:"EvaluationId"`
	EvaluationArn         string            `json:"EvaluationArn"`
	EvaluationFormTitle   string            `json:"EvaluationFormTitle"`
	EvaluationFormID      string            `json:"EvaluationFormId"`
	Status                string            `json:"Status"`
	EvaluatorArn          string            `json:"EvaluatorArn,omitempty"`
	EvaluationType        string            `json:"EvaluationType"`
	AutoEvaluationEnabled bool              `json:"AutoEvaluationEnabled"`
}

// StartContactEvaluationInput is the StartContactEvaluation request.
type StartContactEvaluationInput struct {
	AutoEvaluationConfiguration *struct {
		Enabled bool `json:"Enabled"`
	}
	Tags             map[string]string
	ContactID        string `json:"ContactId"`
	EvaluationFormID string `json:"EvaluationFormId"`
	ClientToken      string
	instPath
}

type evaluationRef struct {
	EvaluationID string `json:"-" path:"EvaluationId"`
	instPath
}

// EvaluatorUser identifies who saved or submitted an evaluation.
type EvaluatorUser struct {
	ConnectUserArn string `json:"ConnectUserArn,omitempty"`
}

// UpdateContactEvaluationInput is the UpdateContactEvaluation request.
type UpdateContactEvaluationInput struct {
	Answers   map[string]EvaluationAnswer
	Notes     map[string]EvaluationNote
	UpdatedBy *EvaluatorUser
	evaluationRef
}

// SubmitContactEvaluationInput is the SubmitContactEvaluation request.
type SubmitContactEvaluationInput struct {
	Answers     map[string]EvaluationAnswer
	Notes       map[string]EvaluationNote
	SubmittedBy *EvaluatorUser
	evaluationRef
}

type evaluationIDsOut struct {
	EvaluationID  string `json:"EvaluationId"`
	EvaluationArn string `json:"EvaluationArn"`
}

type evalQuestion struct {
	Props struct {
		Numeric *struct {
			Options  []evalNumRange `json:"Options"`
			MinValue float64        `json:"MinValue"`
			MaxValue float64        `json:"MaxValue"`
		} `json:"Numeric"`
		SingleSelect *evalOptions `json:"SingleSelect"`
		MultiSelect  *evalOptions `json:"MultiSelect"`
	} `json:"QuestionTypeProperties"`
	RefID        string `json:"RefId"`
	QuestionType string `json:"QuestionType"`
}

type evalOptions struct {
	Options []struct {
		RefID         string `json:"RefId"`
		Score         int32  `json:"Score"`
		AutomaticFail bool   `json:"AutomaticFail"`
	} `json:"Options"`
}

type evalNumRange struct {
	MinValue      float64 `json:"MinValue"`
	MaxValue      float64 `json:"MaxValue"`
	Score         int32   `json:"Score"`
	AutomaticFail bool    `json:"AutomaticFail"`
}

type evalSection struct {
	RefID string            `json:"RefId"`
	Items []json.RawMessage `json:"Items"`
}

// evalTally accumulates points over answered questions.
type evalTally struct {
	earned, maxPoints int32
	fail, scored      bool
}

func (t evalTally) plus(o evalTally) evalTally {
	return evalTally{
		earned:    t.earned + o.earned,
		maxPoints: t.maxPoints + o.maxPoints,
		fail:      t.fail || o.fail,
		scored:    t.scored || o.scored,
	}
}

func (t evalTally) score() EvaluationScore {
	s := EvaluationScore{
		EarnedPoints:  t.earned,
		MaxBasePoint:  t.maxPoints,
		AutomaticFail: t.fail,
		NotApplicable: !t.scored,
	}
	if t.maxPoints > 0 && !t.fail {
		s.Percentage = float64(t.earned) / float64(t.maxPoints) * pctScale
	}

	return s
}

// formQuestions flattens the questions of the form items, calling visit for each with the enclosing section refs.
func formQuestions(items []json.RawMessage, sections []string, visit func(q evalQuestion, sections []string)) {
	for _, raw := range items {
		var u struct {
			Question *evalQuestion `json:"Question"`
			Section  *evalSection  `json:"Section"`
		}

		if json.Unmarshal(raw, &u) != nil {
			continue
		}

		if u.Question != nil {
			visit(*u.Question, sections)
		}

		if u.Section != nil {
			formQuestions(u.Section.Items, append(slices.Clone(sections), u.Section.RefID), visit)
		}
	}
}

// questionTally scores one answered question from its option scores.
func questionTally(q evalQuestion, a EvaluationAnswer) evalTally {
	if a.Value.NotApplicable != nil && *a.Value.NotApplicable {
		return evalTally{}
	}

	t := evalTally{scored: true}

	switch {
	case q.Props.SingleSelect != nil:
		for _, o := range q.Props.SingleSelect.Options {
			t.maxPoints = max(t.maxPoints, o.Score)

			if o.RefID == a.Value.StringValue {
				t.earned, t.fail = o.Score, o.AutomaticFail
			}
		}
	case q.Props.Numeric != nil && a.Value.NumericValue != nil:
		for _, o := range q.Props.Numeric.Options {
			t.maxPoints = max(t.maxPoints, o.Score)

			if *a.Value.NumericValue >= o.MinValue && *a.Value.NumericValue <= o.MaxValue {
				t.earned, t.fail = o.Score, o.AutomaticFail
			}
		}
	default:
		return evalTally{}
	}

	return t
}

// scoreEvaluation scores answered questions per section and overall; unscored forms yield no scores.
func scoreEvaluation(
	c *formContent,
	answers map[string]EvaluationAnswer,
) (map[string]EvaluationScore, *EvaluationScore) {
	var strategy struct {
		Status string `json:"Status"`
	}

	if len(c.ScoringStrategy) == 0 || json.Unmarshal(c.ScoringStrategy, &strategy) != nil ||
		strategy.Status != "ENABLED" {
		return nil, nil
	}

	total, sections := evalTally{}, map[string]evalTally{}

	formQuestions(c.Items, nil, func(q evalQuestion, within []string) {
		a, ok := answers[q.RefID]
		if !ok {
			return
		}

		t := questionTally(q, a)
		total = total.plus(t)

		for _, s := range within {
			sections[s] = sections[s].plus(t)
		}
	})

	scores := map[string]EvaluationScore{}
	for ref, t := range sections {
		scores[ref] = t.score()
	}

	overall := total.score()

	return scores, &overall
}

func checkAnswer(q evalQuestion, a EvaluationAnswer) error {
	v := a.Value
	if v.NotApplicable != nil && *v.NotApplicable {
		return nil
	}

	switch q.QuestionType {
	case qtSingle:
		if q.Props.SingleSelect == nil || !hasOption(q.Props.SingleSelect, v.StringValue) {
			return invalidParamErr("answer to %q must be the RefId of one of its options in StringValue", q.RefID)
		}
	case qtMulti:
		return checkMultiAnswer(q, v)
	case qtNumeric:
		return checkNumericAnswer(q, v)
	case "DATETIME":
		if v.DateTimeValue == "" {
			return invalidParamErr("answer to %q needs a DateTimeValue", q.RefID)
		}
	default:
		if v.StringValue == "" {
			return invalidParamErr("answer to %q needs a StringValue", q.RefID)
		}
	}

	return nil
}

func checkMultiAnswer(q evalQuestion, v EvaluationAnswerData) error {
	if q.Props.MultiSelect == nil || len(v.StringValues) == 0 ||
		slices.ContainsFunc(v.StringValues, func(s string) bool { return !hasOption(q.Props.MultiSelect, s) }) {
		return invalidParamErr("answer to %q must list RefIds of its options in StringValues", q.RefID)
	}

	return nil
}

func checkNumericAnswer(q evalQuestion, v EvaluationAnswerData) error {
	n := q.Props.Numeric
	if n == nil || v.NumericValue == nil || *v.NumericValue < n.MinValue || *v.NumericValue > n.MaxValue {
		return invalidParamErr("answer to %q must be a NumericValue within its range", q.RefID)
	}

	return nil
}

func hasOption(o *evalOptions, ref string) bool {
	return slices.ContainsFunc(o.Options, func(x struct {
		RefID         string `json:"RefId"`
		Score         int32  `json:"Score"`
		AutomaticFail bool   `json:"AutomaticFail"`
	},
	) bool {
		return x.RefID == ref
	})
}

func (b *Backend) evalFormVersion(inst string, r *evaluationRec) (*evalFormVersion, error) {
	f, err := b.evalForms.get(inst, r.FormID)
	if err != nil {
		return nil, err
	}

	v := f.version(r.FormVersion)
	if v == nil {
		return nil, notFoundErr("EvaluationFormVersion", r.FormID)
	}

	return v, nil
}

// StartContactEvaluation opens a draft evaluation of a contact against the active version of a form.
func (b *Backend) StartContactEvaluation(
	ctx context.Context,
	in *StartContactEvaluationInput,
) (*evaluationIDsOut, error) {
	if in.ContactID == "" || in.EvaluationFormID == "" {
		return nil, invalidParamErr("ContactId and EvaluationFormId are required")
	}

	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}

	return writeInstance(b, "StartContactEvaluation", in.InstanceID, func(inst string) (*evaluationIDsOut, error) {
		if !b.contacts.has(inst, in.ContactID) {
			return nil, contactNotFoundErr(in.ContactID)
		}

		form, err := b.evalForms.get(inst, in.EvaluationFormID)
		if err != nil {
			return nil, err
		}

		active := form.active()
		if active == nil {
			return nil, invalidRequestErr("evaluation form %q has no active version", in.EvaluationFormID)
		}

		id, now := newID(), awstime.NewEpochTime(b.now())
		rec := &evaluationRec{
			InstanceID: inst, ID: id, Arn: b.buildARN(ctx, "instance/"+inst+"/evaluation/"+id), ContactID: in.ContactID,
			FormID: form.ID, FormVersion: active.Version, Status: evalDraft, CreatedTime: now, LastModified: now,
			Answers: map[string]EvaluationAnswer{}, Notes: map[string]EvaluationNote{}, Tags: cloneTags(in.Tags),
			AutoEvaluation: in.AutoEvaluationConfiguration != nil && in.AutoEvaluationConfiguration.Enabled,
		}
		b.evaluations.put(rec)

		return &evaluationIDsOut{EvaluationID: id, EvaluationArn: rec.Arn}, nil
	})
}

// applyEvaluation validates answers and notes against the form and merges them into the draft.
func (b *Backend) applyEvaluation(
	inst string,
	r *evaluationRec,
	answers map[string]EvaluationAnswer,
	notes map[string]EvaluationNote,
	by *EvaluatorUser,
) error {
	if r.Status != evalDraft {
		return invalidRequestErr("evaluation %q is %s and cannot change", r.ID, r.Status)
	}

	v, err := b.evalFormVersion(inst, r)
	if err != nil {
		return err
	}

	questions := map[string]evalQuestion{}

	formQuestions(v.Content.Items, nil, func(q evalQuestion, _ []string) { questions[q.RefID] = q })

	for ref, a := range answers {
		q, ok := questions[ref]
		if !ok {
			return invalidParamErr("%q is not a question of the evaluation form", ref)
		}

		if err = checkAnswer(q, a); err != nil {
			return err
		}
	}

	for ref, n := range notes {
		if _, ok := questions[ref]; !ok || len(n.Value) > maxEvalNote {
			return invalidParamErr("note %q must belong to a question and be at most %d characters", ref, maxEvalNote)
		}
	}

	maps.Copy(r.Answers, answers)
	maps.Copy(r.Notes, notes)

	if by != nil && by.ConnectUserArn != "" {
		r.EvaluatorArn = by.ConnectUserArn
	}

	r.LastModified = awstime.NewEpochTime(b.now())

	return nil
}

// UpdateContactEvaluation saves answers and notes of a draft evaluation.
func (b *Backend) UpdateContactEvaluation(in *UpdateContactEvaluationInput) (*evaluationIDsOut, error) {
	return writeInstance(b, "UpdateContactEvaluation", in.InstanceID, func(inst string) (*evaluationIDsOut, error) {
		r, err := b.evaluations.get(inst, in.EvaluationID)
		if err != nil {
			return nil, err
		}

		if err = b.applyEvaluation(inst, r, in.Answers, in.Notes, in.UpdatedBy); err != nil {
			return nil, err
		}

		return &evaluationIDsOut{EvaluationID: r.ID, EvaluationArn: r.Arn}, nil
	})
}

// SubmitContactEvaluation saves the final answers, scores the evaluation and locks it.
func (b *Backend) SubmitContactEvaluation(in *SubmitContactEvaluationInput) (*evaluationIDsOut, error) {
	return writeInstance(b, "SubmitContactEvaluation", in.InstanceID, func(inst string) (*evaluationIDsOut, error) {
		r, err := b.evaluations.get(inst, in.EvaluationID)
		if err != nil {
			return nil, err
		}

		if err = b.applyEvaluation(inst, r, in.Answers, in.Notes, in.SubmittedBy); err != nil {
			return nil, err
		}

		v, err := b.evalFormVersion(inst, r)
		if err != nil {
			return nil, err
		}

		r.Scores, r.Score = scoreEvaluation(&v.Content, r.Answers)
		r.Status = evalSubmitted

		return &evaluationIDsOut{EvaluationID: r.ID, EvaluationArn: r.Arn}, nil
	})
}

func (b *Backend) evaluationView(inst string, r *evaluationRec) Evaluation {
	meta := EvaluationMetadata{ContactID: r.ContactID, EvaluatorArn: r.EvaluatorArn, Score: r.Score}
	if c, ok := b.contacts.table.Get(inst + "/" + r.ContactID); ok {
		meta.ContactAgentID = c.agentID()
	}

	if r.AutoEvaluation {
		meta.AutoEvaluation = &struct {
			AutoEvaluationEnabled bool `json:"AutoEvaluationEnabled"`
		}{AutoEvaluationEnabled: true}
	}

	scores := r.Scores
	if scores == nil {
		scores = map[string]EvaluationScore{}
	}

	return Evaluation{
		Metadata:         meta,
		Answers:          maps2(r.Answers),
		Notes:            maps2(r.Notes),
		Scores:           maps2(scores),
		Tags:             cloneTags(r.Tags),
		CreatedTime:      r.CreatedTime,
		LastModifiedTime: r.LastModified,
		EvaluationID:     r.ID,
		EvaluationArn:    r.Arn,
		Status:           r.Status,
		EvaluationType:   evalTypeStandard,
	}
}

func maps2[V any](m map[string]V) map[string]V {
	out := make(map[string]V, len(m))
	maps.Copy(out, m)

	return out
}

// DescribeContactEvaluation returns an evaluation and the form version it follows.
func (b *Backend) DescribeContactEvaluation(in *evaluationRef) (*evaluationDescribeOut, error) {
	return readInstance(
		b,
		"DescribeContactEvaluation",
		in.InstanceID,
		func(inst string) (*evaluationDescribeOut, error) {
			r, err := b.evaluations.get(inst, in.EvaluationID)
			if err != nil {
				return nil, err
			}

			v, err := b.evalFormVersion(inst, r)
			if err != nil {
				return nil, err
			}

			form, _ := b.evalForms.get(inst, r.FormID)

			return &evaluationDescribeOut{
				Evaluation: b.evaluationView(inst, r),
				EvaluationForm: EvaluationFormContent{
					formContent:           v.Content,
					EvaluationFormArn:     form.Arn,
					EvaluationFormID:      form.ID,
					EvaluationFormVersion: v.Version,
				},
			}, nil
		},
	)
}

// ListContactEvaluations lists the evaluations of one contact, newest first.
func (b *Backend) ListContactEvaluations(inst, contactID string) ([]EvaluationSummary, error) {
	if strings.TrimSpace(contactID) == "" {
		return nil, invalidParamErr("contactId is required")
	}

	return readInstance(b, "ListContactEvaluations", inst, func(inst string) ([]EvaluationSummary, error) {
		recs := slices.DeleteFunc(
			b.evaluations.in(inst),
			func(r *evaluationRec) bool { return r.ContactID != contactID },
		)
		slices.SortStableFunc(
			recs,
			func(a, c *evaluationRec) int { return c.CreatedTime.Time.Compare(a.CreatedTime.Time) },
		)

		out := make([]EvaluationSummary, 0, len(recs))

		for _, r := range recs {
			title := ""
			if v, err := b.evalFormVersion(inst, r); err == nil {
				title = v.Content.Title
			}

			out = append(out, EvaluationSummary{
				Score: r.Score, CreatedTime: r.CreatedTime, LastModifiedTime: r.LastModified, EvaluationID: r.ID,
				EvaluationArn: r.Arn, EvaluationFormTitle: title, EvaluationFormID: r.FormID, Status: r.Status,
				EvaluatorArn: r.EvaluatorArn, EvaluationType: evalTypeStandard, AutoEvaluationEnabled: r.AutoEvaluation,
			})
		}

		return out, nil
	})
}

// DeleteContactEvaluation removes an evaluation.
func (b *Backend) DeleteContactEvaluation(inst, id string) (*empty, error) {
	return writeInstance(b, "DeleteContactEvaluation", inst, func(inst string) (*empty, error) {
		return nil, b.evaluations.del(inst, id)
	})
}
