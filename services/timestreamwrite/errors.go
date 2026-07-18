package timestreamwrite

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrDatabaseNotFound is returned when the requested database does not exist.
	ErrDatabaseNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrTableNotFound is returned when the requested table does not exist.
	ErrTableNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrDatabaseAlreadyExists is returned when a database with the same name already exists.
	ErrDatabaseAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrTableAlreadyExists is returned when a table with the same name already exists.
	ErrTableAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrBatchLoadTaskNotFound is returned when the requested batch load task does not exist.
	ErrBatchLoadTaskNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrInvalidBatchLoadStatus is returned when a task cannot be resumed from its current status.
	ErrInvalidBatchLoadStatus = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrResourceNotFound is returned when tagging an ARN that is not registered in the backend.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
)

// RejectedRecord describes a single record that could not be written due to a version conflict.
type RejectedRecord struct {
	Reason          string `json:"Reason"`
	ExistingVersion int64  `json:"ExistingVersion,omitempty"`
	RecordIndex     int    `json:"RecordIndex"`
}

// RejectedRecordsError is returned by WriteRecords when one or more records are
// rejected due to version conflicts.
type RejectedRecordsError struct {
	RejectedRecords []RejectedRecord
}

func (e *RejectedRecordsError) Error() string {
	return fmt.Sprintf(
		"RejectedRecordsException: %d record(s) rejected due to version conflict",
		len(e.RejectedRecords),
	)
}

// Is satisfies errors.Is so that errors.Is(err, ErrRejectedRecords) returns true.
func (e *RejectedRecordsError) Is(target error) bool {
	_, ok := target.(*RejectedRecordsError)

	return ok
}

// ErrRejectedRecords is the sentinel used with errors.Is for RejectedRecordsError.
var ErrRejectedRecords = &RejectedRecordsError{}
