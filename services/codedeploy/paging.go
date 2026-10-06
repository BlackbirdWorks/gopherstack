package codedeploy

import "fmt"

// rejectNextToken fails any non-empty token: lists are never truncated, so none is ever issued.
func rejectNextToken(token string) error {
	if token == "" {
		return nil
	}

	return fmt.Errorf("%w: nextToken %q was not issued by a previous call", ErrInvalidNextToken, token)
}
