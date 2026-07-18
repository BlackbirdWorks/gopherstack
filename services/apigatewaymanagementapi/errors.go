package apigatewaymanagementapi

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrConnectionNotFound is returned when the requested connection does not exist.
	ErrConnectionNotFound = awserr.New("GoneException", awserr.ErrNotFound)
	// ErrPayloadTooLarge is returned when the payload exceeds the maximum allowed size.
	ErrPayloadTooLarge = errors.New("payload too large")
	// ErrConnectionExists is returned when attempting to create a duplicate connection.
	ErrConnectionExists = errors.New("connection already exists")
	// ErrLimitExceeded is returned when a frame cannot be queued for delivery
	// because the WebSocket connection's client-side buffer is full. Real AWS
	// documents this exact condition as a LimitExceededException.
	ErrLimitExceeded = errors.New("websocket client-side buffer is full")
)
