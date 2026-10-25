package azurefile

import (
	"errors"
	"net/http"
)

// ErrSnapshotNull is returned by Restore when a snapshot holds a null entry.
var ErrSnapshotNull = errors.New("azurefile: restore snapshot: null entry")

// apiError is an Azure Files error: HTTP status plus x-ms-error-code.
type apiError struct {
	Code    string
	Message string
	Status  int
}

func (e *apiError) Error() string { return e.Code + ": " + e.Message }

func newErr(status int, code, msg string) *apiError {
	return &apiError{Status: status, Code: code, Message: msg}
}

func errShareNotFound() *apiError {
	return newErr(http.StatusNotFound, "ShareNotFound", "The specified share does not exist.")
}

func errResourceNotFound() *apiError {
	return newErr(http.StatusNotFound, "ResourceNotFound", "The specified resource does not exist.")
}

func errParentNotFound() *apiError {
	return newErr(http.StatusNotFound, "ParentNotFound", "The specified parent path does not exist.")
}

func errAlreadyExists() *apiError {
	return newErr(http.StatusConflict, "ResourceAlreadyExists", "The specified resource already exists.")
}

func errTypeMismatch() *apiError {
	return newErr(http.StatusBadRequest, "ResourceTypeMismatch",
		"The specified resource type does not match the type of the existing resource.")
}

func errInvalidHeader(name string) *apiError {
	return newErr(http.StatusBadRequest, "InvalidHeaderValue",
		"The value for one of the HTTP headers is not in the correct format: "+name)
}

func errInvalidQuery(name string) *apiError {
	return newErr(http.StatusBadRequest, "InvalidQueryParameterValue",
		"Value for one of the query parameters specified in the request URI is invalid: "+name)
}

func errMissingHeader(name string) *apiError {
	return newErr(http.StatusBadRequest, "MissingRequiredHeader",
		"An HTTP header that's mandatory for this request is not specified: "+name)
}

func errInvalidName() *apiError {
	return newErr(
		http.StatusBadRequest,
		"InvalidResourceName",
		"The specified resource name contains invalid characters.",
	)
}

func errInvalidXML() *apiError {
	return newErr(http.StatusBadRequest, "InvalidXmlDocument", "XML specified is not syntactically valid.")
}

func errReadOnlyShareSnapshot() *apiError {
	return newErr(http.StatusConflict, "ShareSnapshotOperationNotSupported",
		"The operation is not supported on a share snapshot.")
}

func errInvalidPath() *apiError {
	return newErr(http.StatusBadRequest, "InvalidFileOrDirectoryPathName",
		"The specified file or directory path name is not valid.")
}
