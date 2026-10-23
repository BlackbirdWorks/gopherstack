package connect

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	maxVocabularyContent = 60000
	maxVocabularyName    = 140
	vocabActive          = "ACTIVE"
	vocabCreating        = "CREATION_IN_PROGRESS"
	vocabFailed          = "CREATION_FAILED"
	vocabDeleting        = "DELETE_IN_PROGRESS"
	vocabHeader          = "phrase\tipa\tsoundslike\tdisplayas"
	vocabColumns         = 4
	vocabHeaderLines     = 1
	vocabFirstData       = 2
)

var (
	vocabNamePattern   = regexp.MustCompile(`^[0-9a-zA-Z._-]+$`)
	vocabPhrasePattern = regexp.MustCompile(`^[\p{L}\p{N}'.-]+$`)
)

var vocabLanguages = []string{ //nolint:gochecknoglobals // immutable lookup table
	"ar-AE", "de-CH", "de-DE", "en-AB", "en-AU", "en-GB", "en-IE", "en-IN", "en-US", "en-WL", "es-ES", "es-US",
	"fr-CA", "fr-FR", "hi-IN", "it-IT", "ja-JP", "ko-KR", "pt-BR", "pt-PT", "zh-CN", "en-NZ", "en-ZA", "ca-ES",
	"da-DK", "fi-FI", "id-ID", "ms-MY", "nl-NL", "no-NO", "pl-PL", "sv-SE", "tl-PH",
}

// Vocabulary is a custom vocabulary.
type Vocabulary struct {
	LastModifiedTime awstime.EpochTime `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Arn              string            `json:"Arn"`
	Content          string            `json:"Content,omitempty"`
	FailureReason    string            `json:"FailureReason,omitempty"`
	ID               string            `json:"Id"`
	LanguageCode     string            `json:"LanguageCode"`
	Name             string            `json:"Name"`
	State            string            `json:"State"`
}

// VocabularySummary is the list shape of a Vocabulary.
type VocabularySummary struct {
	LastModifiedTime awstime.EpochTime `json:"LastModifiedTime"`
	Arn              string            `json:"Arn"`
	FailureReason    string            `json:"FailureReason,omitempty"`
	ID               string            `json:"Id"`
	LanguageCode     string            `json:"LanguageCode"`
	Name             string            `json:"Name"`
	State            string            `json:"State"`
}

// DefaultVocabulary is the vocabulary used for a language.
type DefaultVocabulary struct {
	InstanceID     string `json:"InstanceId"`
	LanguageCode   string `json:"LanguageCode"`
	VocabularyID   string `json:"VocabularyId"`
	VocabularyName string `json:"VocabularyName"`
}

type vocabularyRec struct {
	InstanceID string     `json:"instanceId"`
	Vocabulary Vocabulary `json:"vocabulary"`
}

type defaultVocabRec struct {
	InstanceID   string `json:"instanceId"`
	LanguageCode string `json:"languageCode"`
	VocabularyID string `json:"vocabularyId"`
}

// CreateVocabularyInput is the CreateVocabulary request.
type CreateVocabularyInput struct {
	Tags           map[string]string
	ClientToken    string
	Content        string
	LanguageCode   string
	VocabularyName string
	instPath
}

// SearchVocabulariesInput is the SearchVocabularies request.
type SearchVocabulariesInput struct {
	LanguageCode   string
	NameStartsWith string
	NextToken      string
	State          string
	instPath
	MaxResults int32
}

// AssociateDefaultVocabularyInput is the AssociateDefaultVocabulary request.
type AssociateDefaultVocabularyInput struct {
	LanguageCode string `json:"-"            path:"LanguageCode"`
	VocabularyID string `json:"VocabularyId"`
	instPath
}

// ListDefaultVocabulariesInput is the ListDefaultVocabularies request.
type ListDefaultVocabulariesInput struct {
	LanguageCode string
	NextToken    string
	instPath
	MaxResults int32
}

// VocabularyOut is the wire shape of a vocabulary.
type VocabularyOut struct {
	State         string `json:"State"`
	VocabularyArn string `json:"VocabularyArn"`
	VocabularyID  string `json:"VocabularyId"`
}

// SearchVocabulariesOut is the SearchVocabularies response body.
type SearchVocabulariesOut struct {
	NextToken             string              `json:"NextToken,omitempty"`
	VocabularySummaryList []VocabularySummary `json:"VocabularySummaryList"`
}

// ListDefaultVocabulariesOut is the ListDefaultVocabularies response body.
type ListDefaultVocabulariesOut struct {
	NextToken             string              `json:"NextToken,omitempty"`
	DefaultVocabularyList []DefaultVocabulary `json:"DefaultVocabularyList"`
}

// vocabularyProblem validates tab-separated custom vocabulary content, returning why it is rejected.
func vocabularyProblem(content string) string {
	lines := strings.Split(strings.TrimSpace(strings.ReplaceAll(content, "\r\n", "\n")), "\n")
	if strings.ToLower(strings.TrimSpace(lines[0])) != vocabHeader {
		return "the first line must be the header Phrase, IPA, SoundsLike and DisplayAs separated by tabs"
	}

	if len(lines) <= vocabHeaderLines {
		return "the vocabulary has no phrases"
	}

	for i, line := range lines[1:] {
		cols := strings.Split(line, "\t")
		if len(cols) != vocabColumns {
			return "line " + strconv.Itoa(i+vocabFirstData) + " must have four tab-separated columns"
		}

		if !vocabPhrasePattern.MatchString(strings.TrimSpace(cols[0])) {
			return "line " + strconv.Itoa(i+vocabFirstData) + " has an invalid Phrase"
		}
	}

	return ""
}

func (b *Backend) viewVocabulary(v *Vocabulary) Vocabulary {
	out := *v
	out.Tags = cloneTags(v.Tags)

	if out.State == vocabCreating && b.now().Sub(out.LastModifiedTime.Time) >= b.delay {
		out.State, out.FailureReason = vocabActive, ""
		if p := vocabularyProblem(out.Content); p != "" {
			out.State, out.FailureReason = vocabFailed, p
		}
	}

	return out
}

func vocabSummary(v *Vocabulary) VocabularySummary {
	return VocabularySummary{
		LastModifiedTime: v.LastModifiedTime, Arn: v.Arn, FailureReason: v.FailureReason, ID: v.ID,
		LanguageCode: v.LanguageCode, Name: v.Name, State: v.State,
	}
}

// CreateVocabulary creates a custom vocabulary; invalid content fails asynchronously like Transcribe.
func (b *Backend) CreateVocabulary(ctx context.Context, in *CreateVocabularyInput) (*VocabularyOut, error) {
	if err := firstError(
		checkName("VocabularyName", in.VocabularyName, maxVocabularyName),
		oneOf("LanguageCode", in.LanguageCode, vocabLanguages...),
		checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	if !vocabNamePattern.MatchString(in.VocabularyName) || in.Content == "" || len(in.Content) > maxVocabularyContent {
		return nil, invalidParamErr("VocabularyName or Content is not valid")
	}

	return writeInstance(b, "CreateVocabulary", in.InstanceID, func(inst string) (*VocabularyOut, error) {
		for _, r := range b.vocabularies.in(inst) {
			if r.Vocabulary.Name == in.VocabularyName {
				return nil, inUseErr("Vocabulary %q already exists", in.VocabularyName)
			}
		}

		id := newID()
		v := Vocabulary{
			LastModifiedTime: awstime.NewEpochTime(b.now()), Tags: cloneTags(in.Tags),
			Arn:     b.buildARN(ctx, "instance/"+inst+"/vocabulary/"+id),
			Content: in.Content, ID: id, LanguageCode: in.LanguageCode, Name: in.VocabularyName, State: vocabCreating,
		}
		b.vocabularies.put(&vocabularyRec{InstanceID: inst, Vocabulary: v})

		return &VocabularyOut{State: vocabCreating, VocabularyArn: v.Arn, VocabularyID: id}, nil
	})
}

// DescribeVocabulary returns a vocabulary.
func (b *Backend) DescribeVocabulary(inst, id string) (*Vocabulary, error) {
	return readInstance(b, "DescribeVocabulary", inst, func(inst string) (*Vocabulary, error) {
		r, err := b.vocabularies.get(inst, id)
		if err != nil {
			return nil, err
		}

		out := b.viewVocabulary(&r.Vocabulary)

		return &out, nil
	})
}

// SearchVocabularies pages through vocabularies filtered by language, state and name prefix.
func (b *Backend) SearchVocabularies(in *SearchVocabulariesInput) (*SearchVocabulariesOut, error) {
	if in.LanguageCode != "" {
		if err := oneOf("LanguageCode", in.LanguageCode, vocabLanguages...); err != nil {
			return nil, err
		}
	}

	if in.State != "" {
		if err := oneOf("State", in.State, vocabCreating, vocabActive, vocabFailed, vocabDeleting); err != nil {
			return nil, err
		}
	}

	items, next, err := listIn(b, "SearchVocabularies", in.InstanceID, b.vocabularies,
		func(r *vocabularyRec) (VocabularySummary, bool) {
			v := b.viewVocabulary(&r.Vocabulary)
			ok := (in.LanguageCode == "" || v.LanguageCode == in.LanguageCode) &&
				(in.State == "" || v.State == in.State) &&
				strings.HasPrefix(strings.ToLower(v.Name), strings.ToLower(in.NameStartsWith))

			return vocabSummary(&v), ok
		}, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &SearchVocabulariesOut{NextToken: next, VocabularySummaryList: orEmpty(items)}, nil
}

// DeleteVocabulary removes a vocabulary that is not the default of a language.
func (b *Backend) DeleteVocabulary(inst, id string) (*VocabularyOut, error) {
	return writeInstance(b, "DeleteVocabulary", inst, func(inst string) (*VocabularyOut, error) {
		r, err := b.vocabularies.get(inst, id)
		if err != nil {
			return nil, err
		}

		for _, d := range b.defaultVocabs.in(inst) {
			if d.VocabularyID == id {
				return nil, inUseErr("Vocabulary %q is the default for %s", id, d.LanguageCode)
			}
		}

		b.vocabularies.table.Delete(inst + "/" + id)

		return &VocabularyOut{State: vocabDeleting, VocabularyArn: r.Vocabulary.Arn, VocabularyID: id}, nil
	})
}

// AssociateDefaultVocabulary sets the default vocabulary of a language; an empty VocabularyId resets it.
func (b *Backend) AssociateDefaultVocabulary(in *AssociateDefaultVocabularyInput) (*empty, error) {
	if err := oneOf("LanguageCode", in.LanguageCode, vocabLanguages...); err != nil {
		return nil, err
	}

	return writeInstance(b, "AssociateDefaultVocabulary", in.InstanceID, func(inst string) (*empty, error) {
		if in.VocabularyID == "" {
			b.defaultVocabs.table.Delete(inst + "/" + in.LanguageCode)

			return &empty{}, nil
		}

		r, err := b.vocabularies.get(inst, in.VocabularyID)
		if err != nil {
			return nil, err
		}

		if r.Vocabulary.LanguageCode != in.LanguageCode {
			return nil, invalidRequestErr("vocabulary %q is not a %s vocabulary", in.VocabularyID, in.LanguageCode)
		}

		if v := b.viewVocabulary(&r.Vocabulary); v.State != vocabActive {
			return nil, invalidRequestErr("vocabulary %q is %s", in.VocabularyID, v.State)
		}

		b.defaultVocabs.put(
			&defaultVocabRec{InstanceID: inst, LanguageCode: in.LanguageCode, VocabularyID: in.VocabularyID},
		)

		return &empty{}, nil
	})
}

// ListDefaultVocabularies pages through per-language default vocabularies.
func (b *Backend) ListDefaultVocabularies(in *ListDefaultVocabulariesInput) (*ListDefaultVocabulariesOut, error) {
	if in.LanguageCode != "" {
		if err := oneOf("LanguageCode", in.LanguageCode, vocabLanguages...); err != nil {
			return nil, err
		}
	}

	items, next, err := listIn(b, "ListDefaultVocabularies", in.InstanceID, b.defaultVocabs,
		func(d *defaultVocabRec) (DefaultVocabulary, bool) {
			out := DefaultVocabulary{
				InstanceID:   d.InstanceID,
				LanguageCode: d.LanguageCode,
				VocabularyID: d.VocabularyID,
			}
			if v, ok := b.vocabularies.table.Get(d.InstanceID + "/" + d.VocabularyID); ok {
				out.VocabularyName = v.Vocabulary.Name
			}

			return out, in.LanguageCode == "" || d.LanguageCode == in.LanguageCode
		}, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &ListDefaultVocabulariesOut{NextToken: next, DefaultVocabularyList: orEmpty(items)}, nil
}
