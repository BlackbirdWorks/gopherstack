package fis

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
)

func notFoundLabel(err error) string {
	switch {
	case errors.Is(err, ErrTemplateNotFound):
		return "Experiment template"
	case errors.Is(err, ErrExperimentNotFound):
		return "Experiment"
	case errors.Is(err, ErrActionNotFound):
		return "Action"
	case errors.Is(err, ErrTargetResourceTypeNotFound):
		return "Target resource type"
	case errors.Is(err, ErrSafetyLeverNotFound):
		return "Safety lever"
	case errors.Is(err, ErrTargetAccountConfigNotFound):
		return "Target account configuration"
	case errors.Is(err, ErrResourceNotFound):
		return "Resource"
	}

	return ""
}

// errorMessage turns a sentinel-prefixed backend error into the sentence a
// client sees: "ExperimentNotFound: x" becomes "Experiment x does not exist" and a
// leading "<Sentinel>: " is dropped from every other error.
func errorMessage(err error) string {
	msg := err.Error()

	if label := notFoundLabel(err); label != "" {
		if sentinel, rest, ok := strings.Cut(msg, ": "); ok && !strings.Contains(sentinel, " ") {
			return label + " " + rest + " does not exist"
		}

		return msg
	}

	if before, after, ok := strings.Cut(msg, ": "); ok && !strings.ContainsAny(before, " ") {
		return after
	}

	return msg
}

func isListOp(op string) bool {
	switch op {
	case opListActions, opListExperimentResolvedTargets, opListExperimentTargetAccountConfigurations,
		opListExperimentTemplates, opListExperiments, opListTargetAccountConfigurations,
		opListTargetResourceTypes:
		return true
	}

	return false
}

var (
	errBadMaxResults = errors.New("maxResults must be between 1 and 100")
	errBadNextToken  = errors.New("nextToken is invalid")
)

// checkPageQuery rejects a maxResults outside 1-100 or an undecodable
// nextToken the way the service does, instead of silently clamping.
func checkPageQuery(q url.Values) error {
	if v := q.Get("maxResults"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > absoluteMaxResults {
			return errBadMaxResults
		}
	}

	if tok := q.Get("nextToken"); tok != "" {
		if n, err := decodePageToken(tok); err != nil || n < 0 {
			return errBadNextToken
		}
	}

	return nil
}
