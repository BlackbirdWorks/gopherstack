package apigateway

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	ErrRestAPINotFound                     = errors.New("NotFoundException")
	ErrResourceNotFound                    = errors.New("NotFoundException")
	ErrMethodNotFound                      = errors.New("NotFoundException")
	ErrMethodResponseNotFound              = errors.New("NotFoundException")
	ErrIntegrationResponseNotFound         = errors.New("NotFoundException")
	ErrDeploymentNotFound                  = errors.New("NotFoundException")
	ErrAuthorizerNotFound                  = errors.New("NotFoundException")
	ErrValidatorNotFound                   = errors.New("NotFoundException")
	ErrAPIKeyNotFound                      = errors.New("NotFoundException")
	ErrBasePathMappingNotFound             = errors.New("NotFoundException")
	ErrDocumentationPartNotFound           = errors.New("NotFoundException")
	ErrDocumentationVersionNotFound        = errors.New("NotFoundException")
	ErrDomainNameNotFound                  = errors.New("NotFoundException")
	ErrDomainNameAccessAssociationNotFound = errors.New("NotFoundException")
	ErrModelNotFound                       = errors.New("NotFoundException")
	ErrUsagePlanNotFound                   = errors.New("NotFoundException")
	ErrUsagePlanKeyNotFound                = errors.New("NotFoundException")
	ErrStageNotFound                       = errors.New("NotFoundException")
	ErrNotFound                            = errors.New("NotFoundException")
	ErrAlreadyExists                       = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	ErrInvalidParameter                    = errors.New("BadRequestException")

	// ErrQuotaExceeded is returned by the data plane when an API key has exhausted
	// its usage-plan quota for the current period (AWS maps this to HTTP 429).
	ErrQuotaExceeded = errors.New("LimitExceededException")
	// ErrThrottled is returned by the data plane when an API key exceeds the
	// usage-plan rate/burst throttle (AWS maps this to HTTP 429).
	ErrThrottled = errors.New("TooManyRequestsException")
)
