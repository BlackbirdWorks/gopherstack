package azureblob

import (
	"net/http"
	"strings"
	"time"
)

// Precond carries the conditional headers and lease id of one request.
// Zero time and empty string mean "header absent". Source holds the
// x-ms-source-if-* conditions of copy-style operations.
type Precond struct {
	Source             *Precond
	IfModifiedSince    time.Time
	IfUnmodifiedSince  time.Time
	IfMatch            string
	IfNoneMatch        string
	IfTags             string
	LeaseID            string
}

type condKind int

const (
	// condRead: a failed If-None-Match or If-Modified-Since is 304.
	condRead condKind = iota
	// condWrite: every failed condition is 412.
	condWrite
	// condCreate: like condWrite, but If-None-Match: * on an existing blob is 409.
	condCreate
)

func firstPre(pre []Precond) Precond {
	if len(pre) > 0 {
		return pre[0]
	}

	return Precond{}
}

func errConditionNotMet() *StorageError {
	return storageErr(http.StatusPreconditionFailed, "ConditionNotMet",
		"The condition specified using HTTP conditional header(s) is not met.")
}

func etagMatches(header, etag string) bool {
	for part := range strings.SplitSeq(header, ",") {
		part = strings.TrimSpace(part)
		if part == "*" || strings.TrimPrefix(part, "W/") == etag {
			return true
		}
	}

	return false
}

// check evaluates the conditions against a blob (exists=false: the blob is
// absent, which only If-None-Match can satisfy).
func (p Precond) check(kind condKind, exists bool, etag string, lm time.Time) error {
	if !exists {
		if p.IfMatch != "" || !p.IfModifiedSince.IsZero() || !p.IfUnmodifiedSince.IsZero() || p.IfTags != "" {
			return errConditionNotMet()
		}

		return nil
	}

	lm = lm.Truncate(time.Second)

	if p.IfMatch != "" {
		if !etagMatches(p.IfMatch, etag) {
			return errConditionNotMet()
		}
	} else if !p.IfUnmodifiedSince.IsZero() && lm.After(p.IfUnmodifiedSince) {
		return errConditionNotMet()
	}

	if p.IfTags != "" {
		if err := evalTagCondition(p.IfTags); err != nil {
			return err
		}
	}

	return p.checkNegative(kind, etag, lm)
}

func (p Precond) checkNegative(kind condKind, etag string, lm time.Time) error {
	notModified := func() error {
		if kind != condRead {
			return errConditionNotMet()
		}

		e := storageErr(http.StatusNotModified, "ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met.")
		e.Header = http.Header{"Etag": {etag}, "Last-Modified": {lm.Format(http.TimeFormat)}}

		return e
	}

	if p.IfNoneMatch != "" {
		if !etagMatches(p.IfNoneMatch, etag) {
			return nil
		}

		if kind == condCreate && strings.TrimSpace(p.IfNoneMatch) == "*" {
			return storageErr(http.StatusConflict, "BlobAlreadyExists", "The specified blob already exists.")
		}

		return notModified()
	}

	if !p.IfModifiedSince.IsZero() && !lm.After(p.IfModifiedSince) {
		return notModified()
	}

	return nil
}

// checkContainer evaluates the time conditions, the only ones containers take.
func (p Precond) checkContainer(lm time.Time) error {
	lm = lm.Truncate(time.Second)

	if (!p.IfModifiedSince.IsZero() && !lm.After(p.IfModifiedSince)) ||
		(!p.IfUnmodifiedSince.IsZero() && lm.After(p.IfUnmodifiedSince)) {
		return errConditionNotMet()
	}

	return nil
}

// evalTagCondition validates an x-ms-if-tags expression. Blob index tags are
// not modeled, so every blob has none and a well-formed expression is false.
func evalTagCondition(expr string) error {
	for _, clause := range splitAnd(expr) {
		if !validTagClause(strings.TrimSpace(clause)) {
			return storageErr(http.StatusBadRequest, "InvalidHeaderValue",
				"The value for one of the HTTP headers is not in the correct format (x-ms-if-tags).")
		}
	}

	return errConditionNotMet()
}

func splitAnd(expr string) []string {
	parts := strings.Split(expr, " AND ")
	if len(parts) == 1 {
		parts = strings.Split(expr, " and ")
	}

	return parts
}

func validTagClause(c string) bool {
	if len(c) < len(`"a"='b'`) || c[0] != '"' {
		return false
	}

	end := strings.Index(c[1:], `"`)
	if end <= 0 {
		return false
	}

	rest := strings.TrimSpace(c[end+2:])

	for _, op := range []string{"<>", ">=", "<=", "=", ">", "<"} {
		if val, ok := strings.CutPrefix(rest, op); ok {
			val = strings.TrimSpace(val)

			return len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\''
		}
	}

	return false
}
