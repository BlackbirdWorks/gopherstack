package connect

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	maxFormTitleLen = 128
	maxFormDepth    = 6
	maxFormItems    = 500
	formDraft       = "DRAFT"
	formActive      = "ACTIVE"
	findingError    = "ERROR"
	findingWarning  = "WARNING"
	validationDone  = "COMPLETED"
	validationBusy  = "IN_PROGRESS"
)

func formQuestionTypes() []string {
	return []string{"TEXT", "SINGLESELECT", "NUMERIC", "MULTISELECT", "DATETIME"}
}

// questionProperty maps a question type to the QuestionTypeProperties member it requires.
var questionProperty = map[string]string{ //nolint:gochecknoglobals // immutable lookup table
	"NUMERIC": "Numeric", "SINGLESELECT": "SingleSelect", "MULTISELECT": "MultiSelect",
}

// FormFinding is one validation finding.
type FormFinding struct {
	Description string `json:"Description"`
	IssueCode   string `json:"IssueCode"`
	Severity    string `json:"Severity"`
	Suggestion  string `json:"Suggestion,omitempty"`
}

type evalFormVersion struct {
	LastModifiedTime  awstime.EpochTime  `json:"lastModifiedTime"`
	CreatedTime       awstime.EpochTime  `json:"createdTime"`
	LastActivatedTime *awstime.EpochTime `json:"lastActivatedTime,omitempty"`
	ValidationStarted *awstime.EpochTime `json:"validationStarted,omitempty"`
	Content           formContent        `json:"content"`
	CreatedBy         string             `json:"createdBy"`
	LastModifiedBy    string             `json:"lastModifiedBy"`
	LastActivatedBy   string             `json:"lastActivatedBy,omitempty"`
	Status            string             `json:"status"`
	Findings          []FormFinding      `json:"findings,omitempty"`
	Version           int32              `json:"version"`
	Locked            bool               `json:"locked"`
}

type formContent struct {
	ScoringStrategy             json.RawMessage   `json:"ScoringStrategy,omitempty"`
	AutoEvaluationConfiguration json.RawMessage   `json:"AutoEvaluationConfiguration,omitempty"`
	ReviewConfiguration         json.RawMessage   `json:"ReviewConfiguration,omitempty"`
	TargetConfiguration         json.RawMessage   `json:"TargetConfiguration,omitempty"`
	LanguageConfiguration       json.RawMessage   `json:"LanguageConfiguration,omitempty"`
	Title                       string            `json:"Title"`
	Description                 string            `json:"Description,omitempty"`
	AIVersion                   string            `json:"AIVersion,omitempty"`
	Items                       []json.RawMessage `json:"Items"`
}

type evalFormRec struct {
	Tags       map[string]string `json:"tags,omitempty"`
	InstanceID string            `json:"instanceId"`
	ID         string            `json:"id"`
	Arn        string            `json:"arn"`
	Versions   []evalFormVersion `json:"versions"`
}

// EvaluationForm is the DescribeEvaluationForm shape.
type EvaluationForm struct {
	CreatedTime            awstime.EpochTime  `json:"CreatedTime"`
	LastModifiedTime       awstime.EpochTime  `json:"LastModifiedTime"`
	Tags                   map[string]string  `json:"Tags,omitempty"`
	LastValidationTime     *awstime.EpochTime `json:"LastValidationTime,omitempty"`
	EvaluationFormArn      string             `json:"EvaluationFormArn"`
	CreatedBy              string             `json:"CreatedBy"`
	EvaluationFormID       string             `json:"EvaluationFormId"`
	LastModifiedBy         string             `json:"LastModifiedBy"`
	Status                 string             `json:"Status"`
	LatestValidationStatus string             `json:"LatestValidationStatus,omitempty"`
	formContent
	EvaluationFormVersion int32 `json:"EvaluationFormVersion"`
	Locked                bool  `json:"Locked"`
}

// FormSummary is the list shape of an evaluation form.
type FormSummary struct {
	CreatedTime       awstime.EpochTime  `json:"CreatedTime"`
	LastModifiedTime  awstime.EpochTime  `json:"LastModifiedTime"`
	LastActivatedTime *awstime.EpochTime `json:"LastActivatedTime,omitempty"`
	ActiveVersion     *int32             `json:"ActiveVersion,omitempty"`
	CreatedBy         string             `json:"CreatedBy"`
	EvaluationFormArn string             `json:"EvaluationFormArn"`
	EvaluationFormID  string             `json:"EvaluationFormId"`
	LastModifiedBy    string             `json:"LastModifiedBy"`
	LastActivatedBy   string             `json:"LastActivatedBy,omitempty"`
	Title             string             `json:"Title"`
	LatestVersion     int32              `json:"LatestVersion"`
}

// FormVersionSummary is the list shape of an evaluation form version.
type FormVersionSummary struct {
	CreatedTime           awstime.EpochTime `json:"CreatedTime"`
	LastModifiedTime      awstime.EpochTime `json:"LastModifiedTime"`
	CreatedBy             string            `json:"CreatedBy"`
	EvaluationFormArn     string            `json:"EvaluationFormArn"`
	EvaluationFormID      string            `json:"EvaluationFormId"`
	LastModifiedBy        string            `json:"LastModifiedBy"`
	Status                string            `json:"Status"`
	EvaluationFormVersion int32             `json:"EvaluationFormVersion"`
	Locked                bool              `json:"Locked"`
}

// FormSearchSummary is the SearchEvaluationForms result shape.
type FormSearchSummary struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Description string            `json:"Description,omitempty"`
	AIVersion   string            `json:"AIVersion,omitempty"`
	Status      string            `json:"Status"`
	FormSummary
}

// FormRequest carries every evaluation form request; each op reads only its own members.
type FormRequest struct {
	CreateNewVersion *bool `json:"CreateNewVersion"`
	Tags             map[string]string
	ClientToken      string `json:"ClientToken"`
	FormID           string `json:"-"                     path:"EvaluationFormId"`
	QueryVersion     string `json:"-"                                             query:"version"`
	NextToken        string `json:"-"                                             query:"nextToken"`
	instPath
	formContent
	MaxResults int32 `json:"-"                                             query:"maxResults"`
	Version    int32 `json:"EvaluationFormVersion"`
	AsDraft    bool
}

// FormOut is the wire shape of an evaluation form operation result.
type FormOut struct {
	Arn     string `json:"EvaluationFormArn"`
	ID      string `json:"EvaluationFormId"`
	Version int32  `json:"EvaluationFormVersion,omitempty"`
}

// FormValidationOut is the GetEvaluationFormValidation response.
type FormValidationOut struct {
	StartedTime           *awstime.EpochTime `json:"StartedTime,omitempty"`
	EvaluationFormID      string             `json:"EvaluationFormId"`
	Status                string             `json:"Status"`
	Findings              []FormFinding      `json:"Findings"`
	EvaluationFormVersion int32              `json:"EvaluationFormVersion"`
}

func (r *evalFormRec) latest() *evalFormVersion { return &r.Versions[len(r.Versions)-1] }

func (r *evalFormRec) version(v int32) *evalFormVersion {
	i := slices.IndexFunc(r.Versions, func(x evalFormVersion) bool { return x.Version == v })
	if i < 0 {
		return nil
	}

	return &r.Versions[i]
}

func (r *evalFormRec) active() *evalFormVersion {
	i := slices.IndexFunc(r.Versions, func(x evalFormVersion) bool { return x.Status == formActive })
	if i < 0 {
		return nil
	}

	return &r.Versions[i]
}

func (b *Backend) formByID(inst, id string) (*evalFormRec, error) {
	return b.evalForms.get(inst, id)
}

// pickVersion resolves the optional version query: absent means the latest.
func pickVersion(r *evalFormRec, raw string) (*evalFormVersion, error) {
	if raw == "" {
		return r.latest(), nil
	}

	n, err := parseInt32(raw)
	if err != nil {
		return nil, invalidParamErr("version must be an integer")
	}

	v := r.version(n)
	if v == nil {
		return nil, notFoundErr("EvaluationFormVersion", raw)
	}

	return v, nil
}

type formWalk struct {
	refs     map[string]bool
	findings []FormFinding
	questions,
	count int
}

func (w *formWalk) problem(sev, code, desc string) {
	w.findings = append(w.findings, FormFinding{Description: desc, IssueCode: code, Severity: sev})
}

func (w *formWalk) item(raw json.RawMessage, depth int) {
	var u map[string]json.RawMessage
	if err := json.Unmarshal(raw, &u); err != nil || len(u) != 1 {
		w.problem(findingError, "InvalidItem", "an item must be an object with exactly one of Section or Question")

		return
	}

	w.count++

	if body, ok := u["Section"]; ok {
		w.section(body, depth)

		return
	}

	if body, ok := u["Question"]; ok {
		w.question(body)

		return
	}

	w.problem(findingError, "InvalidItem", "an item must be a Section or a Question")
}

func (w *formWalk) ref(ref string) {
	switch {
	case strings.TrimSpace(ref) == "":
		w.problem(findingError, "MissingRefId", "every item needs a RefId")
	case w.refs[ref]:
		w.problem(findingError, "DuplicateRefId", "RefId "+ref+" is used twice")
	default:
		w.refs[ref] = true
	}
}

func (w *formWalk) section(raw json.RawMessage, depth int) {
	var s struct {
		RefID string            `json:"RefId"`
		Title string            `json:"Title"`
		Items []json.RawMessage `json:"Items"`
	}

	if json.Unmarshal(raw, &s) != nil {
		w.problem(findingError, "InvalidSection", "a Section is not valid")

		return
	}

	w.ref(s.RefID)

	if strings.TrimSpace(s.Title) == "" {
		w.problem(findingError, "MissingTitle", "section "+s.RefID+" needs a Title")
	}

	if depth >= maxFormDepth {
		w.problem(findingError, "TooDeep", "sections are nested too deeply")

		return
	}

	for _, it := range s.Items {
		w.item(it, depth+1)
	}
}

func (w *formWalk) question(raw json.RawMessage) {
	var q struct {
		Props        map[string]json.RawMessage `json:"QuestionTypeProperties"`
		RefID        string                     `json:"RefId"`
		Title        string                     `json:"Title"`
		QuestionType string                     `json:"QuestionType"`
	}

	if json.Unmarshal(raw, &q) != nil {
		w.problem(findingError, "InvalidQuestion", "a Question is not valid")

		return
	}

	w.questions++
	w.ref(q.RefID)

	if strings.TrimSpace(q.Title) == "" {
		w.problem(findingError, "MissingTitle", "question "+q.RefID+" needs a Title")
	}

	if !slices.Contains(formQuestionTypes(), q.QuestionType) {
		w.problem(findingError, "InvalidQuestionType", "question "+q.RefID+" has an unknown QuestionType")

		return
	}

	if want, ok := questionProperty[q.QuestionType]; ok {
		if _, has := q.Props[want]; !has {
			w.problem(findingError, "MissingProperties", "question "+q.RefID+" needs QuestionTypeProperties."+want)
		}
	}
}

// inspectForm walks the items and reports structural problems and warnings.
func inspectForm(items []json.RawMessage) *formWalk {
	w := &formWalk{refs: map[string]bool{}}
	for _, it := range items {
		w.item(it, 0)
	}

	if w.questions == 0 {
		w.problem(findingWarning, "NoQuestions", "the form has no questions")
	}

	return w
}

func checkFormContent(c *formContent) error {
	if err := firstError(checkName("Title", c.Title, maxFormTitleLen), checkDescription(c.Description)); err != nil {
		return err
	}

	if len(c.Items) == 0 || len(c.Items) > maxFormItems {
		return invalidParamErr("Items must contain 1-%d items", maxFormItems)
	}

	for _, f := range inspectForm(c.Items).findings {
		if f.Severity == findingError {
			return invalidParamErr("%s", f.Description)
		}
	}

	if len(c.ScoringStrategy) > 0 {
		var s struct {
			Mode   string `json:"Mode"`
			Status string `json:"Status"`
		}
		if json.Unmarshal(c.ScoringStrategy, &s) != nil {
			return invalidParamErr("ScoringStrategy is not valid")
		}

		return firstError(
			oneOf("ScoringStrategy.Mode", s.Mode, "QUESTION_ONLY", "SECTION_ONLY", "POINTS_BASED"),
			oneOf("ScoringStrategy.Status", s.Status, "ENABLED", "DISABLED"),
		)
	}

	return nil
}

func formTitleTaken(b *Backend, inst, title, except string) bool {
	return nameExists(b.evalForms, inst, func(r *evalFormRec) string { return r.latest().Content.Title }, title, except)
}

func (b *Backend) newFormVersion(ctx context.Context, n int32, asDraft bool, c *formContent) evalFormVersion {
	now := awstime.NewEpochTime(b.now())
	who := b.callerARN(ctx)
	v := evalFormVersion{
		Content: *c, Version: n, Status: formDraft, CreatedBy: who, CreatedTime: now,
		LastModifiedBy: who, LastModifiedTime: now,
	}

	if !asDraft {
		v.Status, v.Locked, v.LastActivatedBy, v.LastActivatedTime = formActive, true, who, &now
	}

	return v
}

// CreateEvaluationForm creates an evaluation form with its first version.
func (b *Backend) CreateEvaluationForm(ctx context.Context, in *FormRequest) (*FormOut, error) {
	if err := firstError(checkFormContent(&in.formContent), checkTags(in.Tags)); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateEvaluationForm", in.InstanceID, func(inst string) (*FormOut, error) {
		if formTitleTaken(b, inst, in.Title, "") {
			return nil, duplicateErr("EvaluationForm", in.Title)
		}

		id := newID()
		rec := &evalFormRec{
			Tags: cloneTags(
				in.Tags,
			),
			InstanceID: inst,
			ID:         id,
			Arn:        b.buildARN(ctx, "instance/"+inst+"/evaluation-form/"+id),
			Versions:   []evalFormVersion{b.newFormVersion(ctx, 1, in.AsDraft, &in.formContent)},
		}
		b.evalForms.put(rec)

		return &FormOut{Arn: rec.Arn, ID: id}, nil
	})
}

// UpdateEvaluationForm edits a draft version in place, or creates a new version of a locked one.
func (b *Backend) UpdateEvaluationForm(ctx context.Context, in *FormRequest) (*FormOut, error) {
	if err := checkFormContent(&in.formContent); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateEvaluationForm", in.InstanceID, func(inst string) (*FormOut, error) {
		r, err := b.formByID(inst, in.FormID)
		if err != nil {
			return nil, err
		}

		v := r.version(in.Version)
		if v == nil {
			return nil, notFoundErr("EvaluationFormVersion", itoa32(in.Version))
		}

		if formTitleTaken(b, inst, in.Title, in.FormID) {
			return nil, duplicateErr("EvaluationForm", in.Title)
		}

		if v.Locked {
			return b.reviseLocked(ctx, r, in)
		}

		v.Content = in.formContent
		v.LastModifiedTime, v.LastModifiedBy = awstime.NewEpochTime(b.now()), b.callerARN(ctx)
		v.Findings, v.ValidationStarted = nil, nil

		if !in.AsDraft {
			b.activate(ctx, r, v)
		}

		return &FormOut{Arn: r.Arn, ID: r.ID, Version: v.Version}, nil
	})
}

func (b *Backend) reviseLocked(ctx context.Context, r *evalFormRec, in *FormRequest) (*FormOut, error) {
	if in.CreateNewVersion == nil || !*in.CreateNewVersion {
		return nil, resourceConflictErr("version %d is locked; set CreateNewVersion to change it", in.Version)
	}

	next := r.latest().Version + 1
	nv := b.newFormVersion(ctx, next, true, &in.formContent)
	r.Versions = append(r.Versions, nv)

	if !in.AsDraft {
		b.activate(ctx, r, r.latest())
	}

	return &FormOut{Arn: r.Arn, ID: r.ID, Version: next}, nil
}

// activate makes v the single active version; the previous one returns to draft but stays locked.
func (b *Backend) activate(ctx context.Context, r *evalFormRec, v *evalFormVersion) {
	if cur := r.active(); cur != nil && cur.Version != v.Version {
		cur.Status = formDraft
	}

	now := awstime.NewEpochTime(b.now())
	v.Status, v.Locked, v.LastActivatedBy, v.LastActivatedTime = formActive, true, b.callerARN(ctx), &now
}

func (b *Backend) formView(r *evalFormRec, v *evalFormVersion) EvaluationForm {
	out := EvaluationForm{
		formContent: v.Content, CreatedTime: r.Versions[0].CreatedTime, LastModifiedTime: v.LastModifiedTime,
		Tags: cloneTags(r.Tags), CreatedBy: v.CreatedBy, EvaluationFormArn: r.Arn, EvaluationFormID: r.ID,
		LastModifiedBy: v.LastModifiedBy, Status: v.Status, EvaluationFormVersion: v.Version, Locked: v.Locked,
	}

	if v.ValidationStarted != nil {
		out.LastValidationTime = v.ValidationStarted
		out.LatestValidationStatus = b.validationStatus(v)
	}

	return out
}

func (b *Backend) validationStatus(v *evalFormVersion) string {
	if b.now().Sub(v.ValidationStarted.Time) < b.delay {
		return validationBusy
	}

	return validationDone
}

// DescribeEvaluationForm returns one version of a form, the latest by default.
func (b *Backend) DescribeEvaluationForm(in *FormRequest) (*EvaluationForm, error) {
	return readInstance(b, "DescribeEvaluationForm", in.InstanceID, func(inst string) (*EvaluationForm, error) {
		r, err := b.formByID(inst, in.FormID)
		if err != nil {
			return nil, err
		}

		v, err := pickVersion(r, in.QueryVersion)
		if err != nil {
			return nil, err
		}

		out := b.formView(r, v)

		return &out, nil
	})
}

func formSummary(r *evalFormRec) FormSummary {
	latest := r.latest()
	s := FormSummary{
		CreatedTime:       r.Versions[0].CreatedTime,
		LastModifiedTime:  latest.LastModifiedTime,
		CreatedBy:         r.Versions[0].CreatedBy,
		EvaluationFormArn: r.Arn,
		EvaluationFormID:  r.ID,
		LastModifiedBy:    latest.LastModifiedBy,
		Title:             latest.Content.Title,
		LatestVersion:     latest.Version,
	}

	if a := r.active(); a != nil {
		ver := a.Version
		s.ActiveVersion, s.LastActivatedBy, s.LastActivatedTime = &ver, a.LastActivatedBy, a.LastActivatedTime
	}

	return s
}

// ListEvaluationForms pages through forms.
func (b *Backend) ListEvaluationForms(in *FormRequest) ([]FormSummary, string, error) {
	return listIn(b, "ListEvaluationForms", in.InstanceID, b.evalForms, func(r *evalFormRec) (FormSummary, bool) {
		return formSummary(r), true
	}, in.NextToken, in.MaxResults)
}

// ListEvaluationFormVersions pages through the versions of a form, newest first.
func (b *Backend) ListEvaluationFormVersions(in *FormRequest) ([]FormVersionSummary, string, error) {
	all, err := readInstance(
		b,
		"ListEvaluationFormVersions",
		in.InstanceID,
		func(inst string) ([]FormVersionSummary, error) {
			r, ferr := b.formByID(inst, in.FormID)
			if ferr != nil {
				return nil, ferr
			}

			out := make([]FormVersionSummary, 0, len(r.Versions))

			for _, v := range slices.Backward(r.Versions) {
				out = append(out, FormVersionSummary{
					CreatedTime:           v.CreatedTime,
					LastModifiedTime:      v.LastModifiedTime,
					CreatedBy:             v.CreatedBy,
					EvaluationFormArn:     r.Arn,
					EvaluationFormID:      r.ID,
					LastModifiedBy:        v.LastModifiedBy,
					Status:                v.Status,
					EvaluationFormVersion: v.Version,
					Locked:                v.Locked,
				})
			}

			return out, nil
		},
	)
	if err != nil {
		return nil, "", err
	}

	return paged(all, in.NextToken, in.MaxResults)
}

func (b *Backend) setFormStatus(ctx context.Context, op string, in *FormRequest, on bool) (*FormOut, error) {
	return writeInstance(b, op, in.InstanceID, func(inst string) (*FormOut, error) {
		r, err := b.formByID(inst, in.FormID)
		if err != nil {
			return nil, err
		}

		v := r.version(in.Version)
		if v == nil {
			return nil, notFoundErr("EvaluationFormVersion", itoa32(in.Version))
		}

		if on == (v.Status == formActive) {
			return nil, resourceConflictErr("version %d is already %s", in.Version, v.Status)
		}

		if on {
			b.activate(ctx, r, v)
		} else {
			v.Status = formDraft
		}

		v.LastModifiedTime, v.LastModifiedBy = awstime.NewEpochTime(b.now()), b.callerARN(ctx)

		return &FormOut{Arn: r.Arn, ID: r.ID, Version: v.Version}, nil
	})
}

// ActivateEvaluationForm makes a version the active one.
func (b *Backend) ActivateEvaluationForm(ctx context.Context, in *FormRequest) (*FormOut, error) {
	return b.setFormStatus(ctx, "ActivateEvaluationForm", in, true)
}

// DeactivateEvaluationForm returns an active version to draft.
func (b *Backend) DeactivateEvaluationForm(ctx context.Context, in *FormRequest) (*FormOut, error) {
	return b.setFormStatus(ctx, "DeactivateEvaluationForm", in, false)
}

// DeleteEvaluationForm deletes one inactive version, or the whole form when no version is given.
func (b *Backend) DeleteEvaluationForm(in *FormRequest) (*empty, error) {
	return writeInstance(b, "DeleteEvaluationForm", in.InstanceID, func(inst string) (*empty, error) {
		r, err := b.formByID(inst, in.FormID)
		if err != nil {
			return nil, err
		}

		if in.QueryVersion == "" {
			if r.active() != nil {
				return nil, inUseErr("EvaluationForm %q has an active version", in.FormID)
			}

			return nil, b.evalForms.del(inst, in.FormID)
		}

		v, err := pickVersion(r, in.QueryVersion)
		if err != nil {
			return nil, err
		}

		if v.Status == formActive {
			return nil, inUseErr("version %d is active", v.Version)
		}

		r.Versions = slices.DeleteFunc(r.Versions, func(x evalFormVersion) bool { return x.Version == v.Version })
		if len(r.Versions) == 0 {
			return nil, b.evalForms.del(inst, in.FormID)
		}

		return &empty{}, nil
	})
}

// StartEvaluationFormValidation validates a version and records the findings.
func (b *Backend) StartEvaluationFormValidation(in *FormRequest) (*FormOut, error) {
	return writeInstance(b, "StartEvaluationFormValidation", in.InstanceID, func(inst string) (*FormOut, error) {
		r, err := b.formByID(inst, in.FormID)
		if err != nil {
			return nil, err
		}

		v := r.version(in.Version)
		if v == nil {
			return nil, notFoundErr("EvaluationFormVersion", itoa32(in.Version))
		}

		now := awstime.NewEpochTime(b.now())
		v.ValidationStarted, v.Findings = &now, inspectForm(v.Content.Items).findings

		return &FormOut{Arn: r.Arn, ID: r.ID, Version: v.Version}, nil
	})
}

// GetEvaluationFormValidation returns the findings of the latest validation.
func (b *Backend) GetEvaluationFormValidation(in *FormRequest) (*FormValidationOut, error) {
	return readInstance(b, "GetEvaluationFormValidation", in.InstanceID, func(inst string) (*FormValidationOut, error) {
		r, err := b.formByID(inst, in.FormID)
		if err != nil {
			return nil, err
		}

		v, err := pickVersion(r, in.QueryVersion)
		if err != nil {
			return nil, err
		}

		if v.ValidationStarted == nil {
			return nil, notFoundErr("EvaluationFormValidation", in.FormID)
		}

		out := &FormValidationOut{
			StartedTime: v.ValidationStarted, EvaluationFormID: r.ID, Status: b.validationStatus(v),
			Findings: []FormFinding{}, EvaluationFormVersion: v.Version,
		}
		if out.Status == validationDone {
			out.Findings = orEmpty(slices.Clone(v.Findings))
		}

		return out, nil
	})
}

// SearchEvaluationForms searches forms by title and description.
func (b *Backend) SearchEvaluationForms(in *SearchRequest) (*SearchResult, error) {
	if err := rejectCriteria(
		in.SearchCriteria,
		"BooleanCondition",
		"DateTimeCondition",
		"NumberCondition",
	); err != nil {
		return nil, err
	}

	spec := searchSpec{fields: []string{"title", fieldDescription}}

	return runSearch(b, "SearchEvaluationForms", "EvaluationFormSearchSummaryList", in, b.evalForms, spec,
		func(r *evalFormRec) searchSubject {
			c := r.latest().Content

			return searchSubject{
				fields: map[string]string{"title": c.Title, fieldDescription: c.Description},
				tags:   r.Tags,
			}
		},
		func(r *evalFormRec) FormSearchSummary {
			latest := r.latest()

			return FormSearchSummary{
				FormSummary: formSummary(r), Tags: cloneTags(r.Tags), Description: latest.Content.Description,
				AIVersion: latest.Content.AIVersion, Status: latest.Status,
			}
		})
}

// rejectCriteria refuses criteria members the emulator does not evaluate, at any nesting level.
func rejectCriteria(raw json.RawMessage, members ...string) error {
	if len(raw) == 0 {
		return nil
	}

	var node map[string]json.RawMessage
	if json.Unmarshal(raw, &node) != nil {
		return invalidParamErr("SearchCriteria is not valid")
	}

	for _, m := range members {
		if v, ok := node[m]; ok && string(v) != jsonNull {
			return invalidRequestErr("search criterion %s is not supported", m)
		}
	}

	for _, key := range []string{"AndConditions", "OrConditions"} {
		var subs []json.RawMessage
		if json.Unmarshal(node[key], &subs) != nil {
			continue
		}

		for _, s := range subs {
			if err := rejectCriteria(s, members...); err != nil {
				return err
			}
		}
	}

	return nil
}
