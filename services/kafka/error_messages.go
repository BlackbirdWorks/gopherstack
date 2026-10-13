package kafka

import (
	"errors"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var errCodeTail = regexp.MustCompile(`: [A-Za-z]+(Exception)$`)

type describedError struct {
	error

	noun string
}

func (e describedError) Unwrap() error { return e.error }

func (e describedError) Error() string {
	msg := errCodeTail.ReplaceAllString(e.error.Error(), "")
	if !strings.HasSuffix(msg, "Exception") || strings.Contains(msg, " ") {
		return msg
	}

	switch {
	case errors.Is(e.error, ErrTopicExists), errors.Is(e.error, ErrTopicNotFound):
		return msg
	case errors.Is(e.error, awserr.ErrNotFound):
		return "The requested " + e.noun + " was not found."
	case errors.Is(e.error, awserr.ErrAlreadyExists):
		return "A " + e.noun + " with the same name already exists."
	}

	return msg
}

func resourceNoun(path string) string {
	switch {
	case strings.Contains(path, "/topics"):
		return "topic"
	case strings.HasPrefix(path, "/v1/configurations"):
		return "configuration"
	case strings.Contains(path, "/replicators"):
		return "replicator"
	case strings.Contains(path, "vpc-connection"):
		return "VPC connection"
	default:
		return "cluster"
	}
}
