package connect

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	scoringQuestionOnly = "QUESTION_ONLY"
	scoringSectionOnly  = "SECTION_ONLY"
	scoringEnabled      = "ENABLED"
)

type scoreThreshold struct {
	PerformanceCategory string  `json:"PerformanceCategory"`
	MinScorePercentage  float64 `json:"MinScorePercentage"`
	MaxScorePercentage  float64 `json:"MaxScorePercentage"`
}

// evalTally accumulates points over answered questions.
type evalTally struct {
	failTarget        string
	earned, maxPoints int32
	fail, scored      bool
}

type scoredQuestion struct {
	ref        string
	sections   []string
	thresholds []scoreThreshold
	tally      evalTally
	weight     float64
}

type sectionInfo struct {
	thresholds []scoreThreshold
	weight     float64
}

type formScoring struct {
	sections  map[string]sectionInfo
	questions []scoredQuestion
	order     []string
}

func (t evalTally) plus(o evalTally) evalTally {
	return evalTally{
		earned:    t.earned + o.earned,
		maxPoints: t.maxPoints + o.maxPoints,
		fail:      t.fail || o.fail,
		scored:    t.scored || o.scored,
	}
}

func thresholdCategory(pct float64, ts []scoreThreshold) string {
	for _, t := range ts {
		if pct >= t.MinScorePercentage && pct <= t.MaxScorePercentage {
			return t.PerformanceCategory
		}
	}

	return ""
}

func (t evalTally) score(ts []scoreThreshold) EvaluationScore {
	s := EvaluationScore{
		EarnedPoints:  t.earned,
		MaxBasePoint:  t.maxPoints,
		AutomaticFail: t.fail,
		NotApplicable: !t.scored,
	}
	if t.maxPoints > 0 && !t.fail {
		s.Percentage = float64(t.earned) / float64(t.maxPoints) * pctScale
	}

	if t.scored {
		s.PerformanceCategory = thresholdCategory(s.Percentage, ts)
	}

	return s
}

// questionTally scores one answered question from its option scores.
func questionTally(q evalQuestion, a EvaluationAnswer) evalTally {
	if a.Value.NotApplicable != nil && *a.Value.NotApplicable {
		return evalTally{}
	}

	t := evalTally{scored: true}

	pick := func(score int32, fail bool, target string) {
		t.earned, t.fail, t.failTarget = score, fail, target
	}

	target := func(a *struct {
		TargetSection string `json:"TargetSection"`
	},
	) string {
		if a == nil {
			return ""
		}

		return a.TargetSection
	}

	switch {
	case q.Props.SingleSelect != nil:
		for _, o := range q.Props.SingleSelect.Options {
			t.maxPoints = max(t.maxPoints, o.Score)

			if o.RefID == a.Value.StringValue {
				pick(o.Score, o.AutomaticFail, target(o.AutoFail))
			}
		}
	case q.Props.Numeric != nil && a.Value.NumericValue != nil:
		for _, o := range q.Props.Numeric.Options {
			t.maxPoints = max(t.maxPoints, o.Score)

			if *a.Value.NumericValue >= o.MinValue && *a.Value.NumericValue <= o.MaxValue {
				pick(o.Score, o.AutomaticFail, target(o.AutoFail))
			}
		}
	default:
		return evalTally{}
	}

	return t
}

type questionScoring struct {
	Thresholds []scoreThreshold `json:"ScoreThresholds"`
	Excluded   bool             `json:"IsExcludedFromScoring"`
}

type scoreItemExtras struct {
	Scoring    *questionScoring `json:"ScoringConfiguration"`
	Thresholds []scoreThreshold `json:"ScoreThresholds"`
	RefID      string           `json:"RefId"`
	Items      []jsontext.Value `json:"Items"`
	Weight     float64          `json:"Weight"`
	Excluded   bool             `json:"IsExcludedFromScoring"`
}

// formQuestions flattens the questions of the form items, calling visit for each with the enclosing section refs.
func formQuestions(items []jsontext.Value, sections []string, visit func(q evalQuestion, sections []string)) {
	for _, raw := range items {
		var u struct {
			Question *evalQuestion `json:"Question"`
			Section  *evalSection  `json:"Section"`
		}

		if json.Unmarshal(raw, &u, jsonx.V1()) != nil {
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

// collect walks the form items and tallies the answered, non-excluded questions.
func (f *formScoring) collect(items []jsontext.Value, within []string, answers map[string]EvaluationAnswer) {
	for _, raw := range items {
		var u struct {
			Question *jsontext.Value `json:"Question"`
			Section  *jsontext.Value `json:"Section"`
		}

		if json.Unmarshal(raw, &u, jsonx.V1()) != nil {
			continue
		}

		switch {
		case u.Question != nil:
			f.collectQuestion(*u.Question, within, answers)
		case u.Section != nil:
			var sec scoreItemExtras
			if json.Unmarshal(*u.Section, &sec, jsonx.V1()) != nil || sec.Excluded {
				continue
			}

			f.sections[sec.RefID] = sectionInfo{weight: sec.Weight, thresholds: sec.Thresholds}
			f.order = append(f.order, sec.RefID)
			f.collect(sec.Items, append(slices.Clone(within), sec.RefID), answers)
		}
	}
}

func (f *formScoring) collectQuestion(raw jsontext.Value, within []string, answers map[string]EvaluationAnswer) {
	var q evalQuestion

	var x scoreItemExtras

	if json.Unmarshal(raw, &q, jsonx.V1()) != nil || json.Unmarshal(raw, &x, jsonx.V1()) != nil {
		return
	}

	a, ok := answers[q.RefID]
	if !ok || (x.Scoring != nil && x.Scoring.Excluded) {
		return
	}

	sq := scoredQuestion{ref: q.RefID, sections: within, weight: x.Weight, tally: questionTally(q, a)}
	if x.Scoring != nil {
		sq.thresholds = x.Scoring.Thresholds
	}

	f.questions = append(f.questions, sq)
}

// zeroFailures applies automatic fails: a target section scores 0, an untargeted fail fails the form.
func (f *formScoring) zeroFailures() bool {
	formFail, targets := false, map[string]bool{}

	for _, q := range f.questions {
		switch {
		case !q.tally.fail:
		case q.tally.failTarget == "":
			formFail = true
		default:
			targets[q.tally.failTarget] = true
		}
	}

	for i := range f.questions {
		q := &f.questions[i]
		if slices.ContainsFunc(q.sections, func(s string) bool { return targets[s] }) {
			q.tally.earned, q.tally.fail = 0, true
		}
	}

	return formFail
}

func appliedWeight(w, total float64) *float64 {
	v := w / total * pctScale

	return &v
}

// weighted scores items by weight: sums of w*percentage over the items that were not N/A, renormalised.
func weighted(items []weightedItem) (float64, float64, bool) {
	var sum, total float64

	for _, it := range items {
		if it.maxPoints > 0 && it.weight > 0 {
			sum += it.weight * float64(it.earned) / float64(it.maxPoints)
			total += it.weight
		}
	}

	if total == 0 {
		return 0, 0, false
	}

	return sum / total * pctScale, total, true
}

type weightedItem struct {
	weight            float64
	earned, maxPoints int32
}

// scoreEvaluation scores answered questions per section, per question and overall; unscored forms yield no scores.
func scoreEvaluation(
	c *formContent,
	answers map[string]EvaluationAnswer,
) (map[string]EvaluationScore, *EvaluationScore) {
	var strategy struct {
		Mode       string           `json:"Mode"`
		Status     string           `json:"Status"`
		Thresholds []scoreThreshold `json:"ScoreThresholds"`
	}

	if len(c.ScoringStrategy) == 0 || json.Unmarshal(c.ScoringStrategy, &strategy, jsonx.V1()) != nil ||
		strategy.Status != scoringEnabled {
		return nil, nil
	}

	f := &formScoring{sections: map[string]sectionInfo{}}
	f.collect(c.Items, nil, answers)

	formFail := f.zeroFailures()
	scores := map[string]EvaluationScore{}
	sectionTally := map[string]evalTally{}
	total := evalTally{}

	for _, q := range f.questions {
		total = total.plus(q.tally)

		for _, s := range q.sections {
			sectionTally[s] = sectionTally[s].plus(q.tally)
		}
	}

	total.fail = formFail

	for ref, t := range sectionTally {
		scores[ref] = t.score(f.sections[ref].thresholds)
	}

	overall := total.score(strategy.Thresholds)

	switch strategy.Mode {
	case scoringQuestionOnly:
		f.applyQuestionWeights(scores, &overall, strategy.Thresholds)
	case scoringSectionOnly:
		f.applySectionWeights(scores, sectionTally, &overall, strategy.Thresholds)
	default:
		for _, q := range f.questions {
			scores[q.ref] = q.tally.score(q.thresholds)
		}
	}

	return scores, &overall
}

// scoreQuestions records the score and applied weight of every question and returns the weighted items.
func (f *formScoring) scoreQuestions(scores map[string]EvaluationScore) ([]weightedItem, map[string][]weightedItem) {
	all := make([]weightedItem, 0, len(f.questions))
	perSection := map[string][]weightedItem{}
	total := 0.0

	for _, q := range f.questions {
		it := weightedItem{weight: q.weight, earned: q.tally.earned, maxPoints: q.tally.maxPoints}
		all = append(all, it)

		for _, s := range q.sections {
			perSection[s] = append(perSection[s], it)
		}

		if q.counts() {
			total += q.weight
		}

		scores[q.ref] = q.tally.score(q.thresholds)
	}

	for _, q := range f.questions {
		if sc := scores[q.ref]; q.counts() && total > 0 {
			sc.AppliedWeight = appliedWeight(q.weight, total)
			scores[q.ref] = sc
		}
	}

	return all, perSection
}

func (q *scoredQuestion) counts() bool { return q.tally.maxPoints > 0 && q.weight > 0 }

func (f *formScoring) applyQuestionWeights(
	scores map[string]EvaluationScore,
	overall *EvaluationScore,
	ts []scoreThreshold,
) {
	all, perSection := f.scoreQuestions(scores)

	for ref, items := range perSection {
		pct, _, ok := weighted(items)
		if !ok {
			continue
		}

		if sc := scores[ref]; !sc.AutomaticFail {
			sc.Percentage = pct
			sc.PerformanceCategory = thresholdCategory(pct, f.sections[ref].thresholds)
			scores[ref] = sc
		}
	}

	if pct, _, ok := weighted(all); ok && !overall.AutomaticFail {
		overall.Percentage = pct
		overall.PerformanceCategory = thresholdCategory(pct, ts)
	}
}

func (f *formScoring) applySectionWeights(
	scores map[string]EvaluationScore,
	tallies map[string]evalTally,
	overall *EvaluationScore,
	ts []scoreThreshold,
) {
	var items []weightedItem

	for _, ref := range f.order {
		t, ok := tallies[ref]
		if !ok || !isTopLevel(f, ref) {
			continue
		}

		items = append(items, weightedItem{weight: f.sections[ref].weight, earned: t.earned, maxPoints: t.maxPoints})
	}

	_, total, ok := weighted(items)
	if !ok {
		return
	}

	for _, ref := range f.order {
		if t, has := tallies[ref]; has && isTopLevel(f, ref) && t.maxPoints > 0 && f.sections[ref].weight > 0 {
			sc := scores[ref]
			sc.AppliedWeight = appliedWeight(f.sections[ref].weight, total)
			scores[ref] = sc
		}
	}

	if pct, _, _ := weighted(items); !overall.AutomaticFail {
		overall.Percentage = pct
		overall.PerformanceCategory = thresholdCategory(pct, ts)
	}
}

// isTopLevel reports whether a section is not nested in another section.
func isTopLevel(f *formScoring, ref string) bool {
	for _, q := range f.questions {
		if i := slices.Index(q.sections, ref); i >= 0 {
			return i == 0
		}
	}

	return true
}
