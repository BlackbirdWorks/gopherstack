package sagemaker

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrValidation is returned for invalid input parameters.
var ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
