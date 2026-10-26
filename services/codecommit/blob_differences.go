package codecommit

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	blobDiffDefaultContext = 3
	blobDiffMaxContext     = 20
	blobDiffDefaultMax     = 100
	blobIDLength           = 40

	diffChangeContext = "CONTEXT"
	diffChangeAdd     = "ADD"
	diffChangeDelete  = "DELETE"
)

// DiffChange is types.DiffChange.
type DiffChange struct {
	BeforeLineNumber *int32 `json:"beforeLineNumber,omitempty"`
	AfterLineNumber  *int32 `json:"afterLineNumber,omitempty"`
	Type             string `json:"type"`
	Content          string `json:"content"`
}

// DiffHunk is types.DiffHunk.
type DiffHunk struct {
	Changes         []DiffChange `json:"changes"`
	BeforeStartLine int32        `json:"beforeStartLine"`
	BeforeLineCount int32        `json:"beforeLineCount"`
	AfterStartLine  int32        `json:"afterStartLine"`
	AfterLineCount  int32        `json:"afterLineCount"`
}

// BlobDiff is the GetBlobDifferences result: one page of hunks plus blob metadata.
type BlobDiff struct {
	Hunks          page.Page[DiffHunk]
	BeforeBlobSize int64
	AfterBlobSize  int64
	IsBinary       bool
}

// BlobDiffOptions are the optional GetBlobDifferences parameters; a nil
// ContextLines or MaxResults selects the default.
type BlobDiffOptions struct {
	ContextLines     *int
	MaxResults       *int
	NextToken        string
	IgnoreWhitespace bool
}

func validBlobID(id string) bool {
	if len(id) != blobIDLength {
		return false
	}

	for _, r := range id {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}

	return true
}

// GetBlobDifferences diffs two blobs line by line; an empty beforeBlobID diffs against an empty blob.
func (b *InMemoryBackend) GetBlobDifferences(
	repoName, beforeBlobID, afterBlobID string,
	opts BlobDiffOptions,
) (*BlobDiff, error) {
	ctxLines := blobDiffDefaultContext
	if opts.ContextLines != nil {
		ctxLines = *opts.ContextLines
	}

	if ctxLines < 0 || ctxLines > blobDiffMaxContext {
		return nil, fmt.Errorf("%w: contextLines must be between 0 and %d", errInvalidRequest, blobDiffMaxContext)
	}

	if opts.MaxResults != nil && *opts.MaxResults < 1 {
		return nil, fmt.Errorf("%w: MaxResults must be at least 1", ErrInvalidMaxResults)
	}

	for _, id := range []string{beforeBlobID, afterBlobID} {
		if id != "" && !validBlobID(id) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidBlobID, id)
		}
	}

	if err := page.ValidateToken(opts.NextToken); err != nil {
		return nil, fmt.Errorf("%w: invalid NextToken", ErrInvalidContinuationToken)
	}

	before, after, err := b.blobPair(repoName, beforeBlobID, afterBlobID)
	if err != nil {
		return nil, err
	}

	out := &BlobDiff{BeforeBlobSize: int64(len(before)), AfterBlobSize: int64(len(after))}

	if isBinaryContent(before) || isBinaryContent(after) {
		out.IsBinary = true
		out.Hunks = page.New[DiffHunk](nil, opts.NextToken, 0, blobDiffDefaultMax)

		return out, nil
	}

	hunks := buildDiffHunks(splitLines(before), splitLines(after), ctxLines, opts.IgnoreWhitespace)

	maxResults := 0
	if opts.MaxResults != nil {
		maxResults = *opts.MaxResults
	}

	out.Hunks = page.New(hunks, opts.NextToken, maxResults, blobDiffDefaultMax)

	return out, nil
}

func (b *InMemoryBackend) blobPair(repoName, beforeBlobID, afterBlobID string) ([]byte, []byte, error) {
	b.mu.RLock("GetBlobDifferences")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, nil, fmt.Errorf("%w: %s does not exist", ErrNotFound, repoName)
	}

	var before []byte

	if beforeBlobID != "" {
		content, ok := b.blobContentLocked(repoName, beforeBlobID)
		if !ok {
			return nil, nil, fmt.Errorf("%w: blob %s not found", ErrBlobNotFound, beforeBlobID)
		}

		before = content
	}

	after, ok := b.blobContentLocked(repoName, afterBlobID)
	if !ok {
		return nil, nil, fmt.Errorf("%w: blob %s not found", ErrBlobNotFound, afterBlobID)
	}

	return append([]byte(nil), before...), append([]byte(nil), after...), nil
}

func diffKeys(lines []string, ignoreWS bool) []string {
	keys := make([]string, len(lines))

	for i, l := range lines {
		l = strings.TrimSuffix(l, "\n")
		if ignoreWS {
			l = strings.Map(func(r rune) rune {
				if unicode.IsSpace(r) {
					return -1
				}

				return r
			}, l)
		}

		keys[i] = l
	}

	return keys
}

func buildDiffHunks(before, after []string, ctxLines int, ignoreWS bool) []DiffHunk {
	regs := diffRegions(diffKeys(before, ignoreWS), diffKeys(after, ignoreWS))

	var hunks []DiffHunk

	for start := 0; start < len(regs); {
		end := start + 1
		for end < len(regs) && regs[end].bs-regs[end-1].be <= 2*ctxLines {
			end++
		}

		hunks = append(hunks, buildHunk(before, after, regs[start:end], ctxLines))
		start = end
	}

	return hunks
}

func buildHunk(before, after []string, regs []region, ctxLines int) DiffHunk {
	startB := max(regs[0].bs-ctxLines, 0)
	startA := regs[0].os - (regs[0].bs - startB)
	endB := min(regs[len(regs)-1].be+ctxLines, len(before))
	bp, ap := startB, startA
	changes := []DiffChange{}

	contextUntil := func(limit int) {
		for ; bp < limit; bp, ap = bp+1, ap+1 {
			changes = append(changes, DiffChange{
				Type: diffChangeContext, Content: strings.TrimSuffix(after[ap], "\n"),
				BeforeLineNumber: lineNo(bp), AfterLineNumber: lineNo(ap),
			})
		}
	}

	for _, r := range regs {
		contextUntil(r.bs)

		for ; bp < r.be; bp++ {
			changes = append(changes, DiffChange{
				Type: diffChangeDelete, Content: strings.TrimSuffix(before[bp], "\n"), BeforeLineNumber: lineNo(bp),
			})
		}

		for ; ap < r.oe; ap++ {
			changes = append(changes, DiffChange{
				Type: diffChangeAdd, Content: strings.TrimSuffix(after[ap], "\n"), AfterLineNumber: lineNo(ap),
			})
		}
	}

	contextUntil(endB)

	return DiffHunk{
		Changes:         changes,
		BeforeStartLine: int32(startB + 1),  //nolint:gosec // line counts are bounded by blob size
		BeforeLineCount: int32(bp - startB), //nolint:gosec // as above
		AfterStartLine:  int32(startA + 1),  //nolint:gosec // as above
		AfterLineCount:  int32(ap - startA), //nolint:gosec // as above
	}
}

func lineNo(zeroBased int) *int32 {
	n := int32(zeroBased + 1) //nolint:gosec // bounded by blob size

	return &n
}
