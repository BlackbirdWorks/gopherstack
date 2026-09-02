package quicksight

import (
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	defaultTopicRefreshType = "FULL_REFRESH"

	// topicUserExperienceVersionNewReaderExperience: CreateTopicV2 sets this
	// automatically since CreateTopicV2Input has no such parameter of its own.
	topicUserExperienceVersionNewReaderExperience = "NEW_READER_EXPERIENCE"

	// filterTopicName is the SearchTopics filter Name for matching on a topic's
	// display name (the "TOPIC_NAME" filter attribute per the QuickSight API).
	filterTopicName = "TOPIC_NAME"

	// QAResultType values (see types.QAResultType in aws-sdk-go-v2).
	qaResultTypeGeneratedAnswer = "GENERATED_ANSWER"
	qaResultTypeNoAnswer        = "NO_ANSWER"

	// GeneratedAnswerStatus value used for topic-grounded answers (see
	// types.GeneratedAnswerStatus in aws-sdk-go-v2).
	generatedAnswerStatusRetrieved = "ANSWER_RETRIEVED"
)

// storedTopicRefreshSchedule is the persisted representation of one QuickSight
// topic refresh schedule, keyed by DatasetId.
type storedTopicRefreshSchedule struct {
	ScheduleConfig map[string]any `json:"scheduleConfig,omitempty"`
	DatasetID      string         `json:"datasetId"`
	DatasetArn     string         `json:"datasetArn"`
	RefreshType    string         `json:"refreshType"`
	IsEnabled      bool           `json:"isEnabled"`
}

func (s *storedTopicRefreshSchedule) toTopicRefreshSchedule() *TopicRefreshSchedule {
	return &TopicRefreshSchedule{
		DatasetID:      s.DatasetID,
		DatasetArn:     s.DatasetArn,
		RefreshType:    s.RefreshType,
		IsEnabled:      s.IsEnabled,
		ScheduleConfig: s.ScheduleConfig,
	}
}

// storedTopicRefresh is the persisted representation of a single topic refresh
// execution's status, keyed by RefreshId.
type storedTopicRefresh struct {
	CreatedTime   time.Time `json:"createdTime"`
	RefreshID     string    `json:"refreshId"`
	RefreshStatus string    `json:"refreshStatus"`
}

func (r *storedTopicRefresh) toTopicRefreshDetails() *TopicRefreshDetails {
	return &TopicRefreshDetails{
		RefreshID:     r.RefreshID,
		RefreshStatus: r.RefreshStatus,
	}
}

// storedTopicReviewedAnswer is the persisted representation of one reviewed answer
// attached to a QuickSight topic.
type storedTopicReviewedAnswer struct {
	PrimaryVisual map[string]any `json:"primaryVisual,omitempty"`
	Template      map[string]any `json:"template,omitempty"`
	AnswerID      string         `json:"answerId"`
	DatasetArn    string         `json:"datasetArn"`
	Question      string         `json:"question"`
	Mode          string         `json:"mode"`
}

func (a *storedTopicReviewedAnswer) toTopicReviewedAnswer() *TopicReviewedAnswer {
	return &TopicReviewedAnswer{
		AnswerID:      a.AnswerID,
		DatasetArn:    a.DatasetArn,
		Question:      a.Question,
		Mode:          a.Mode,
		PrimaryVisual: a.PrimaryVisual,
		Template:      a.Template,
	}
}

// storedTopic is the persisted representation of a QuickSight topic.
// CustomInstructions/PublishOption/DataSetsV2/DataSetRelations are V2-only
// fields; DataSets/UserExperienceVersion are V1-only -- see topics_v2.go.
type storedTopic struct {
	CreatedTime           time.Time                              `json:"createdTime"`
	LastUpdatedTime       time.Time                              `json:"lastUpdatedTime"`
	RefreshSchedules      map[string]*storedTopicRefreshSchedule `json:"refreshSchedules"`
	Refreshes             map[string]*storedTopicRefresh         `json:"refreshes"`
	ReviewedAnswers       map[string]*storedTopicReviewedAnswer  `json:"reviewedAnswers"`
	TopicID               string                                 `json:"topicId"`
	Arn                   string                                 `json:"arn"`
	Name                  string                                 `json:"name"`
	Description           string                                 `json:"description,omitempty"`
	UserExperienceVersion string                                 `json:"userExperienceVersion,omitempty"`
	CustomInstructions    string                                 `json:"customInstructions,omitempty"`
	PublishOption         string                                 `json:"publishOption,omitempty"`
	DataSets              []map[string]any                       `json:"dataSets,omitempty"`
	DataSetsV2            []map[string]any                       `json:"dataSetsV2,omitempty"`
	DataSetRelations      []map[string]any                       `json:"dataSetRelations,omitempty"`
	Permissions           []ResourcePermission                   `json:"permissions,omitempty"`
}

func (t *storedTopic) toTopic() *Topic {
	return &Topic{
		CreatedTime:           t.CreatedTime,
		LastUpdatedTime:       t.LastUpdatedTime,
		TopicID:               t.TopicID,
		Arn:                   t.Arn,
		Name:                  t.Name,
		Description:           t.Description,
		UserExperienceVersion: t.UserExperienceVersion,
		CustomInstructions:    t.CustomInstructions,
		PublishOption:         t.PublishOption,
		DataSets:              t.DataSets,
		DataSetsV2:            t.DataSetsV2,
		DataSetRelations:      t.DataSetRelations,
		Permissions:           clonePermissions(t.Permissions),
	}
}

func topicKey(accountID, topicID string) string {
	return accountID + "/" + topicID
}

// ---- Topics ----

func (b *InMemoryBackend) CreateTopic(
	accountID, topicID, name, description, userExperienceVersion string,
	dataSets []map[string]any,
	permissions []ResourcePermission,
	tags map[string]string,
) (*Topic, error) {
	if topicID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTopic")
	defer b.mu.Unlock()

	key := topicKey(accountID, topicID)
	if b.topics.Has(key) {
		return nil, ErrTopicAlreadyExists
	}

	now := time.Now().UTC()
	t := &storedTopic{
		CreatedTime:           now,
		LastUpdatedTime:       now,
		TopicID:               topicID,
		Arn:                   b.buildARN("topic", topicID),
		Name:                  name,
		Description:           description,
		UserExperienceVersion: userExperienceVersion,
		DataSets:              dataSets,
		Permissions:           clonePermissions(permissions),
		RefreshSchedules:      make(map[string]*storedTopicRefreshSchedule),
		Refreshes:             make(map[string]*storedTopicRefresh),
		ReviewedAnswers:       make(map[string]*storedTopicReviewedAnswer),
	}
	b.topics.Put(t)

	if len(tags) > 0 {
		b.tags[t.Arn] = maps.Clone(tags)
	}

	return t.toTopic(), nil
}

func (b *InMemoryBackend) DescribeTopic(accountID, topicID string) (*Topic, error) {
	b.mu.RLock("DescribeTopic")
	defer b.mu.RUnlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, ErrTopicNotFound
	}

	return t.toTopic(), nil
}

func (b *InMemoryBackend) UpdateTopic(
	accountID, topicID, name, description, userExperienceVersion string,
	dataSets []map[string]any,
) (*Topic, error) {
	b.mu.Lock("UpdateTopic")
	defer b.mu.Unlock()

	key := topicKey(accountID, topicID)
	t, ok := b.topics.Get(key)
	if !ok {
		return nil, ErrTopicNotFound
	}

	if name != "" {
		t.Name = name
	}
	if description != "" {
		t.Description = description
	}
	if userExperienceVersion != "" {
		t.UserExperienceVersion = userExperienceVersion
	}
	if dataSets != nil {
		t.DataSets = dataSets
	}
	t.LastUpdatedTime = time.Now().UTC()

	return t.toTopic(), nil
}

func (b *InMemoryBackend) DeleteTopic(accountID, topicID string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	key := topicKey(accountID, topicID)
	t, ok := b.topics.Get(key)
	if !ok {
		return ErrTopicNotFound
	}

	delete(b.tags, t.Arn)
	b.topics.Delete(key)

	return nil
}

func (b *InMemoryBackend) allTopicsLocked(_ string) []*storedTopic {
	all := b.topics.All()
	sort.Slice(all, func(i, j int) bool { return all[i].TopicID < all[j].TopicID })

	return all
}

func (b *InMemoryBackend) ListTopics(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Topic, string, error) {
	b.mu.RLock("ListTopics")
	defer b.mu.RUnlock()

	all := b.allTopicsLocked(accountID)

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		start = len(all)
		for i, t := range all {
			if t.TopicID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].TopicID
	} else {
		end = len(all)
	}

	result := make([]*Topic, 0, end-start)
	for _, t := range all[start:end] {
		result = append(result, t.toTopic())
	}

	return result, next, nil
}

// SearchTopics searches topics by name (filter Name == filterTopicName); any
// other filter Name (QUICKSIGHT_USER, QUICKSIGHT_VIEWER_OR_OWNER, etc.) is an
// ownership-related filter that this in-memory backend doesn't track
// principals for and is treated as a pass-through match, mirroring
// SearchDashboards/SearchAnalyses.
//
//nolint:dupl // search functions share structure but operate on different stored types
func (b *InMemoryBackend) SearchTopics(
	_ string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*Topic, string, error) {
	b.mu.RLock("SearchTopics")
	defer b.mu.RUnlock()

	var filtered []*storedTopic
	for _, t := range b.topics.All() {
		if matchesAllNameFilters(t.Name, filters, filterTopicName) {
			filtered = append(filtered, t)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].TopicID < filtered[j].TopicID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, t := range filtered {
			if t.TopicID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(filtered) {
		next = filtered[end].TopicID
	} else {
		end = len(filtered)
	}

	result := make([]*Topic, 0, end-start)
	for _, t := range filtered[start:end] {
		result = append(result, t.toTopic())
	}

	return result, next, nil
}

// PredictQAResults answers a natural-language query against the account's
// registered Topics. This emulator cannot run real Q&A/NLP inference, but it
// grounds its answer in real backend state: if any Topic's Name appears in
// the query text, it returns a GENERATED_ANSWER result referencing that real
// Topic (its actual TopicId/Name), rather than fabricating a plausible-looking
// answer out of nothing. Otherwise it returns the explicit NO_ANSWER result
// type, which is itself a valid QAResultType per the AWS API (not an error).
func (b *InMemoryBackend) PredictQAResults(accountID, queryText string) (*QAResult, error) {
	if queryText == "" {
		return nil, ErrValidation
	}

	b.mu.RLock("PredictQAResults")
	defer b.mu.RUnlock()

	lowerQuery := strings.ToLower(queryText)

	for _, t := range b.allTopicsLocked(accountID) {
		if t.Name != "" && strings.Contains(lowerQuery, strings.ToLower(t.Name)) {
			return &QAResult{
				ResultType:   qaResultTypeGeneratedAnswer,
				AnswerID:     uuid.New().String(),
				AnswerStatus: generatedAnswerStatusRetrieved,
				QuestionID:   uuid.New().String(),
				QuestionText: queryText,
				TopicID:      t.TopicID,
				TopicName:    t.Name,
			}, nil
		}
	}

	return &QAResult{ResultType: qaResultTypeNoAnswer}, nil
}

// ---- Topic permissions ----

func (b *InMemoryBackend) DescribeTopicPermissions(accountID, topicID string) (*Topic, []ResourcePermission, error) {
	b.mu.RLock("DescribeTopicPermissions")
	defer b.mu.RUnlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, nil, ErrTopicNotFound
	}

	return t.toTopic(), clonePermissions(t.Permissions), nil
}

func (b *InMemoryBackend) UpdateTopicPermissions(
	accountID, topicID string,
	grant, revoke []ResourcePermission,
) (*Topic, []ResourcePermission, error) {
	b.mu.Lock("UpdateTopicPermissions")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, nil, ErrTopicNotFound
	}

	t.Permissions = applyGrantRevoke(t.Permissions, grant, revoke)
	t.LastUpdatedTime = time.Now().UTC()

	return t.toTopic(), clonePermissions(t.Permissions), nil
}

// ---- Topic refresh ----

// DescribeTopicRefresh reports the status of a topic refresh execution. Because
// this batch of operations does not include a "start topic refresh" API, refresh
// records are synthesized lazily on first describe (mirroring the fact that
// QuickSight refreshes run automatically in the background): the first describe
// for a given RefreshId creates a COMPLETED record, and subsequent describes for
// the same RefreshId return that same stored record.
func (b *InMemoryBackend) DescribeTopicRefresh(accountID, topicID, refreshID string) (*TopicRefreshDetails, error) {
	if refreshID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("DescribeTopicRefresh")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, ErrTopicNotFound
	}

	r, exists := t.Refreshes[refreshID]
	if !exists {
		r = &storedTopicRefresh{
			CreatedTime:   time.Now().UTC(),
			RefreshID:     refreshID,
			RefreshStatus: statusCompleted,
		}
		t.Refreshes[refreshID] = r
	}

	return r.toTopicRefreshDetails(), nil
}

// ---- Topic refresh schedules ----

func (b *InMemoryBackend) CreateTopicRefreshSchedule(
	accountID, topicID, datasetID, datasetArn, refreshType string,
	isEnabled bool,
	scheduleConfig map[string]any,
) (*TopicRefreshSchedule, error) {
	if datasetID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTopicRefreshSchedule")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, ErrTopicNotFound
	}

	if _, exists := t.RefreshSchedules[datasetID]; exists {
		return nil, ErrTopicRefreshScheduleAlreadyExists
	}

	if refreshType == "" {
		refreshType = defaultTopicRefreshType
	}

	s := &storedTopicRefreshSchedule{
		DatasetID:      datasetID,
		DatasetArn:     datasetArn,
		RefreshType:    refreshType,
		IsEnabled:      isEnabled,
		ScheduleConfig: scheduleConfig,
	}
	t.RefreshSchedules[datasetID] = s

	return s.toTopicRefreshSchedule(), nil
}

func (b *InMemoryBackend) DescribeTopicRefreshSchedule(
	accountID, topicID, datasetID string,
) (*TopicRefreshSchedule, error) {
	b.mu.RLock("DescribeTopicRefreshSchedule")
	defer b.mu.RUnlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, ErrTopicNotFound
	}

	s, ok := t.RefreshSchedules[datasetID]
	if !ok {
		return nil, ErrTopicRefreshScheduleNotFound
	}

	return s.toTopicRefreshSchedule(), nil
}

// UpdateTopicRefreshSchedule updates an existing schedule. Pointer/nil-able
// parameters are only applied when non-nil/non-empty so a partial (or empty)
// update body leaves existing fields untouched, matching the CreateFolder/
// UpdateFolder convention used elsewhere in this package.
func (b *InMemoryBackend) UpdateTopicRefreshSchedule(
	accountID, topicID, datasetID, refreshType string,
	isEnabled *bool,
	scheduleConfig map[string]any,
) (*TopicRefreshSchedule, error) {
	b.mu.Lock("UpdateTopicRefreshSchedule")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, ErrTopicNotFound
	}

	s, ok := t.RefreshSchedules[datasetID]
	if !ok {
		return nil, ErrTopicRefreshScheduleNotFound
	}

	if refreshType != "" {
		s.RefreshType = refreshType
	}
	if isEnabled != nil {
		s.IsEnabled = *isEnabled
	}
	if scheduleConfig != nil {
		s.ScheduleConfig = scheduleConfig
	}

	return s.toTopicRefreshSchedule(), nil
}

func (b *InMemoryBackend) DeleteTopicRefreshSchedule(
	accountID, topicID, datasetID string,
) (*TopicRefreshSchedule, error) {
	b.mu.Lock("DeleteTopicRefreshSchedule")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, ErrTopicNotFound
	}

	s, exists := t.RefreshSchedules[datasetID]
	if !exists {
		return nil, ErrTopicRefreshScheduleNotFound
	}
	delete(t.RefreshSchedules, datasetID)

	return s.toTopicRefreshSchedule(), nil
}

func (b *InMemoryBackend) ListTopicRefreshSchedules(accountID, topicID string) ([]*TopicRefreshSchedule, error) {
	b.mu.RLock("ListTopicRefreshSchedules")
	defer b.mu.RUnlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, ErrTopicNotFound
	}

	ids := make([]string, 0, len(t.RefreshSchedules))
	for id := range t.RefreshSchedules {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	result := make([]*TopicRefreshSchedule, 0, len(ids))
	for _, id := range ids {
		result = append(result, t.RefreshSchedules[id].toTopicRefreshSchedule())
	}

	return result, nil
}

// ---- Topic reviewed answers ----

func (b *InMemoryBackend) BatchCreateTopicReviewedAnswer(
	accountID, topicID string,
	answers []map[string]any,
) ([]*TopicReviewedAnswer, []TopicAnswerError, error) {
	b.mu.Lock("BatchCreateTopicReviewedAnswer")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, nil, ErrTopicNotFound
	}

	succeeded := make([]*TopicReviewedAnswer, 0, len(answers))
	var failed []TopicAnswerError

	for _, raw := range answers {
		answerID, _ := raw["AnswerId"].(string)
		question, _ := raw["Question"].(string)
		datasetArn, _ := raw["DatasetArn"].(string)

		if answerID == "" || question == "" || datasetArn == "" {
			failed = append(failed, TopicAnswerError{
				AnswerID: answerID,
				Message:  "AnswerId, Question, and DatasetArn are required",
			})

			continue
		}

		mode, _ := raw["Mode"].(string)
		primaryVisual, _ := raw["PrimaryVisual"].(map[string]any)
		template, _ := raw["Template"].(map[string]any)

		a := &storedTopicReviewedAnswer{
			AnswerID:      answerID,
			DatasetArn:    datasetArn,
			Question:      question,
			Mode:          mode,
			PrimaryVisual: primaryVisual,
			Template:      template,
		}
		t.ReviewedAnswers[answerID] = a
		succeeded = append(succeeded, a.toTopicReviewedAnswer())
	}

	return succeeded, failed, nil
}

func (b *InMemoryBackend) BatchDeleteTopicReviewedAnswer(
	accountID, topicID string,
	answerIDs []string,
) ([]string, []TopicAnswerError, error) {
	b.mu.Lock("BatchDeleteTopicReviewedAnswer")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, nil, ErrTopicNotFound
	}

	succeeded := make([]string, 0, len(answerIDs))
	var failed []TopicAnswerError

	for _, id := range answerIDs {
		if _, exists := t.ReviewedAnswers[id]; !exists {
			failed = append(failed, TopicAnswerError{AnswerID: id, Message: "reviewed answer not found"})

			continue
		}
		delete(t.ReviewedAnswers, id)
		succeeded = append(succeeded, id)
	}

	return succeeded, failed, nil
}

func (b *InMemoryBackend) ListTopicReviewedAnswers(accountID, topicID string) ([]*TopicReviewedAnswer, error) {
	b.mu.RLock("ListTopicReviewedAnswers")
	defer b.mu.RUnlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, ErrTopicNotFound
	}

	ids := make([]string, 0, len(t.ReviewedAnswers))
	for id := range t.ReviewedAnswers {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	result := make([]*TopicReviewedAnswer, 0, len(ids))
	for _, id := range ids {
		result = append(result, t.ReviewedAnswers[id].toTopicReviewedAnswer())
	}

	return result, nil
}
