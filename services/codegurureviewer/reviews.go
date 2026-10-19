package codegurureviewer

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

var reviewNameRe = regexp.MustCompile(`^[a-zA-Z0-9\-_$:.]+$`)

// CreateCodeReviewInput is the CreateCodeReview request.
type CreateCodeReviewInput struct {
	Type                     *CodeReviewType `json:"Type"`
	Name                     string          `json:"Name"`
	RepositoryAssociationArn string          `json:"RepositoryAssociationArn"`
	ClientRequestToken       string          `json:"ClientRequestToken,omitempty"`
}

// CodeReviewOutput is the Create/DescribeCodeReview response.
type CodeReviewOutput struct {
	CodeReview *CodeReview `json:"CodeReview"`
}

// ListCodeReviewsInput is the ListCodeReviews request.
type ListCodeReviewsInput struct {
	Type            string   `json:"-" query:"Type"`
	NextToken       string   `json:"-" query:"NextToken"`
	ProviderTypes   []string `json:"-" query:"ProviderTypes"`
	States          []string `json:"-" query:"States"`
	RepositoryNames []string `json:"-" query:"RepositoryNames"`
	MaxResults      int32    `json:"-" query:"MaxResults"`
}

// ListCodeReviewsOutput is the ListCodeReviews response.
type ListCodeReviewsOutput struct {
	NextToken           string `json:",omitempty"`
	CodeReviewSummaries []CodeReviewSummary
}

func reviewStates() []string { return []string{jobPending, jobCompleted, jobFailed, "Deleting"} }

func allAnalysisTypes() []string { return []string{analysisSecurity, analysisCodeQuality} }

// view returns the review at now: Pending until its window passes, then the outcome of the analysis.
func (r *reviewRecord) view(now time.Time) reviewView {
	if now.Before(r.SettleAt.Time) {
		return reviewView{state: jobPending, updated: r.Updated}
	}

	return reviewView{state: r.FinalState, reason: r.StateReason, metrics: r.Metrics, updated: r.SettleAt}
}

type reviewView struct {
	metrics *Metrics
	updated awstime.EpochTime
	state   string
	reason  string
}

func (r *reviewRecord) wire(now time.Time) *CodeReview {
	v := r.view(now)

	return &CodeReview{
		AnalysisTypes: slices.Clone(r.AnalysisTypes), AssociationArn: r.AssociationARN, CodeReviewArn: r.ARN,
		ConfigFileState: r.ConfigFile, CreatedTimeStamp: r.Created, LastUpdatedTimeStamp: v.updated,
		Metrics: v.metrics, Name: r.Name, Owner: r.Owner, ProviderType: r.ProviderType,
		PullRequestID: r.PullRequestID, RepositoryName: r.RepositoryName, SourceCodeType: r.SourceCodeType,
		State: v.state, StateReason: v.reason, Type: r.Type,
	}
}

func (r *reviewRecord) summary(now time.Time) CodeReviewSummary {
	v := r.view(now)

	return CodeReviewSummary{
		CodeReviewArn: r.ARN, CreatedTimeStamp: r.Created, LastUpdatedTimeStamp: v.updated,
		MetricsSummary: v.metrics, Name: r.Name, Owner: r.Owner, ProviderType: r.ProviderType,
		PullRequestID: r.PullRequestID, RepositoryName: r.RepositoryName, SourceCodeType: r.SourceCodeType,
		State: v.state, Type: r.Type,
	}
}

// reviewSource resolves which code a review analyzes and the SourceCodeType echoed back.
func reviewSource(a *associationRecord, ra *RepositoryAnalysis) (*SourceCodeType, error) {
	if a.ProviderType == providerS3 {
		s := ra.SourceCodeType
		if s == nil || s.S3BucketRepository == nil {
			return nil, validationErr("SourceCodeType.S3BucketRepository is required for S3 repositories")
		}

		repo := s.S3BucketRepository
		if repo.Name != a.Name || repo.Details == nil || a.S3 == nil || repo.Details.BucketName != a.S3.BucketName {
			return nil, validationErr(
				"S3BucketRepository must name the associated repository %q and its bucket",
				a.Name,
			)
		}

		return &SourceCodeType{S3BucketRepository: repo}, nil
	}

	head := ra.RepositoryHead
	if head == nil && ra.SourceCodeType != nil {
		head = ra.SourceCodeType.RepositoryHead
	}

	if head == nil || head.BranchName == "" {
		return nil, validationErr("RepositoryHead.BranchName is required")
	}

	return &SourceCodeType{RepositoryHead: head}, nil
}

func validateCreateReview(in *CreateCodeReviewInput) error {
	if len(in.Name) == 0 || len(in.Name) > maxNameLen || !reviewNameRe.MatchString(in.Name) {
		return validationErr("Name must be 1-%d characters of letters, digits and -_$:.", maxNameLen)
	}

	if len(in.ClientRequestToken) > maxTokenLen {
		return validationErr("ClientRequestToken must be at most %d characters", maxTokenLen)
	}

	if in.Type == nil || in.Type.RepositoryAnalysis == nil {
		return validationErr("Type.RepositoryAnalysis is required: only RepositoryAnalysis code reviews can be created")
	}

	return validateEnums("AnalysisTypes", in.Type.AnalysisTypes, allAnalysisTypes())
}

// CreateCodeReview analyzes the associated repository's source with the static rule set and records the findings.
// The review reads Pending until the lifecycle window passes, then Completed (or Failed when the source is unreadable).
func (b *InMemoryBackend) CreateCodeReview(ctx context.Context, in *CreateCodeReviewInput) (*reviewRecord, error) {
	if err := validateCreateReview(in); err != nil {
		return nil, err
	}

	if !validAssociationARN(in.RepositoryAssociationArn) {
		return nil, validationErr("RepositoryAssociationArn %q is not a valid ARN", in.RepositoryAssociationArn)
	}

	b.mu.RLock("CreateCodeReview")
	a, ok := b.associations.Get(in.RepositoryAssociationArn)

	var assoc *associationRecord
	if ok {
		assoc = a.view(b.now())
	}

	b.mu.RUnlock()

	if assoc == nil {
		return nil, notFoundErr("repository association %q does not exist", in.RepositoryAssociationArn)
	}

	if assoc.State != stateAssociated {
		return nil, conflictErr(
			"repository association is %s; a code review needs an Associated repository",
			assoc.State,
		)
	}

	src, err := reviewSource(assoc, in.Type.RepositoryAnalysis)
	if err != nil {
		return nil, err
	}

	types := in.Type.AnalysisTypes
	if len(types) == 0 {
		types = allAnalysisTypes()
	}

	_, region := b.scope(ctx)
	rec := &reviewRecord{
		AssociationARN: assoc.ARN, Name: in.Name, Owner: assoc.Owner, ProviderType: assoc.ProviderType,
		RepositoryName: assoc.Name, Type: typeRepositoryAnalysis, AnalysisTypes: slices.Clone(types),
		SourceCodeType: src, FinalState: jobCompleted, ConfigFile: configAbsent,
	}

	b.runAnalysis(ctx, region, assoc, rec)

	return b.storeReview(rec)
}

// runAnalysis fetches the sources and fills the review's outcome; an unreadable source fails the review.
func (b *InMemoryBackend) runAnalysis(ctx context.Context, region string, a *associationRecord, rec *reviewRecord) {
	files, err := b.fetchSources(ctx, region, a, rec.SourceCodeType)
	if err != nil {
		rec.FinalState, rec.StateReason, rec.ConfigFile = jobFailed, err.Error(), ""

		return
	}

	if hasConfigFile(files) {
		rec.ConfigFile = configPresent
	}

	findings, loc := analyze(files, rec.AnalysisTypes)

	rec.Recommendations = make([]Recommendation, 0, len(findings))

	for i := range findings {
		f := &findings[i]
		rec.Recommendations = append(rec.Recommendations, Recommendation{
			Description:            f.description,
			FilePath:               f.path,
			StartLine:              lineNumber(f.start),
			EndLine:                lineNumber(f.end),
			RecommendationCategory: f.rule.Category,
			RecommendationID:       recommendationID(f),
			Severity:               f.rule.Severity,
			RuleMetadata: &RuleMetadata{
				LongDescription: f.rule.Long, RuleID: f.rule.ID, RuleName: f.rule.Name,
				ShortDescription: f.rule.Short, RuleTags: slices.Clone(f.rule.Tags),
			},
		})
	}

	rec.Metrics = &Metrics{FindingsCount: int64(len(findings)), MeteredLinesOfCodeCount: loc}
}

func (b *InMemoryBackend) storeReview(rec *reviewRecord) (*reviewRecord, error) {
	b.mu.Lock("CreateCodeReview")
	defer b.mu.Unlock()

	for _, r := range b.reviews.All() {
		if r.Name == rec.Name {
			return nil, conflictErr("a code review named %q already exists", rec.Name)
		}
	}

	now := b.now()
	rec.ARN = rec.AssociationARN + ":codereview:" + uuid.NewString()
	rec.Created = awstime.NewEpochTime(now)
	rec.Updated = rec.Created
	rec.SettleAt = awstime.NewEpochTime(now.Add(b.dwellLocked()))
	b.reviews.Put(rec)

	cp := *rec

	return &cp, nil
}

func validReviewARN(v string) bool {
	return validAssociationARN(v) && strings.Contains(v, ":codereview:")
}

func (b *InMemoryBackend) reviewLocked(arn string) (*reviewRecord, error) {
	if !validReviewARN(arn) {
		return nil, validationErr("CodeReviewArn %q is not a valid code review ARN", arn)
	}

	r, ok := b.reviews.Get(arn)
	if !ok {
		return nil, resourceNotFoundErr("code review %q does not exist", arn)
	}

	return r, nil
}

// GetCodeReview returns a review by ARN.
func (b *InMemoryBackend) GetCodeReview(arn string) (*reviewRecord, error) {
	b.mu.RLock("GetCodeReview")
	defer b.mu.RUnlock()

	r, err := b.reviewLocked(arn)
	if err != nil {
		return nil, err
	}

	cp := *r

	return &cp, nil
}

// wireReview renders a review as seen now.
func (b *InMemoryBackend) wireReview(r *reviewRecord) *CodeReview {
	b.mu.RLock("wireReview")
	defer b.mu.RUnlock()

	return r.wire(b.now())
}

// ListCodeReviews lists reviews of one type, newest first.
func (b *InMemoryBackend) ListCodeReviews(in *ListCodeReviewsInput) ([]CodeReviewSummary, string, error) {
	if in.Type != typePullRequest && in.Type != typeRepositoryAnalysis {
		return nil, "", validationErr("Type must be %s or %s", typePullRequest, typeRepositoryAnalysis)
	}

	if err := errors2(
		validateEnums("ProviderTypes", in.ProviderTypes, providerTypes()),
		validateEnums("States", in.States, reviewStates()),
	); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListCodeReviews")

	now := b.now()

	var recs []*reviewRecord

	for _, r := range b.reviews.All() {
		if r.Type == in.Type && matchAny(in.ProviderTypes, r.ProviderType) && matchAny(in.States, r.view(now).state) &&
			matchAny(in.RepositoryNames, r.RepositoryName) {
			recs = append(recs, r)
		}
	}

	slices.SortFunc(recs, func(x, y *reviewRecord) int {
		if c := y.Created.Compare(x.Created.Time); c != 0 {
			return c
		}

		return strings.Compare(x.ARN, y.ARN)
	})

	pg, err := restroute.Paginate(recs, in.NextToken, int(in.MaxResults), defaultPageSize, maxPageSize)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", validationErr("%v", err)
	}

	out := make([]CodeReviewSummary, 0, len(pg.Data))
	for _, r := range pg.Data {
		out = append(out, r.summary(now))
	}

	b.mu.RUnlock()

	return out, pg.Next, nil
}

// ListRecommendations pages through a completed review's findings; a review still running has none yet.
func (b *InMemoryBackend) ListRecommendations(arn, token string, maxResults int32) ([]Recommendation, string, error) {
	b.mu.RLock("ListRecommendations")
	defer b.mu.RUnlock()

	r, err := b.reviewLocked(arn)
	if err != nil {
		return nil, "", err
	}

	var all []Recommendation
	if r.view(b.now()).state == jobCompleted {
		all = r.Recommendations
	}

	pg, err := restroute.Paginate(all, token, int(maxResults), defaultPageSize, maxPageSize)
	if err != nil {
		return nil, "", validationErr("%v", err)
	}

	return slices.Clone(pg.Data), pg.Next, nil
}

// lineNumber converts a line index; files are capped at maxFileBytes so it always fits.
func lineNumber(n int) int32 { return int32(n) } //nolint:gosec // bounded by maxFileBytes
