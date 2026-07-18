package mediastoredata

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// maxPathLength is the maximum allowed byte-length of an object path,
// matching the AWS MediaStore Data limit.
const maxPathLength = 900

var (
	// ErrNotFound is returned when a requested object does not exist.
	ErrNotFound = awserr.New("ObjectNotFoundException", awserr.ErrNotFound)

	// ErrInvalidPath is returned when a path fails validation.
	ErrInvalidPath = awserr.New("InvalidPathException", awserr.ErrInvalidParameter)

	// ErrInvalidStorageClass is returned when an unknown storage class is supplied.
	ErrInvalidStorageClass = awserr.New("InvalidStorageClassException", awserr.ErrInvalidParameter)
)

// isValidStorageClass reports whether sc is a known MediaStore Data storage
// class. Real AWS Elemental MediaStore Data has exactly one StorageClass
// value ("TEMPORAL" -- see aws-sdk-go-v2/service/mediastoredata/types.
// StorageClass, whose only enum member is StorageClassTemporal). "STANDARD"
// is NOT a storage class: it is a value of the unrelated
// x-amz-upload-availability header (UploadAvailability), and must not be
// accepted here.
func isValidStorageClass(sc string) bool {
	return sc == "TEMPORAL"
}

// normalizePath normalises an object path (strips leading slash).
func normalizePath(p string) string {
	return strings.TrimPrefix(p, "/")
}

// ValidatePath checks that path is a legal MediaStore object path.
func ValidatePath(p string) error {
	key := normalizePath(p)
	if key == "" {
		return fmt.Errorf("%w: path cannot be empty", ErrInvalidPath)
	}
	if len(key) > maxPathLength {
		return fmt.Errorf("%w: path exceeds %d characters", ErrInvalidPath, maxPathLength)
	}
	if strings.Contains(key, "..") {
		return fmt.Errorf("%w: path cannot contain '..'", ErrInvalidPath)
	}
	if strings.ContainsRune(key, 0) {
		return fmt.Errorf("%w: path contains null byte", ErrInvalidPath)
	}

	return nil
}
