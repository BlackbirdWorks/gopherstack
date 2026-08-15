package redshift_test

import (
	"testing"

	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	redshiftserverlesssdk "github.com/aws/aws-sdk-go-v2/service/redshiftserverless"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// redshift client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &redshiftsdk.Client{}, h.GetSupportedOperations(), []string{})
}

// TestSDKCompleteness_Serverless is the redshiftserverless counterpart of
// TestSDKCompleteness above. It also pins the module: nothing else in this
// package imports aws-sdk-go-v2/service/redshiftserverless, so without this
// import `go mod tidy` would strip the go.mod requirement gopherstack-0w2p
// added, leaving the wire-shape comments throughout this package citing a
// version the module graph no longer pins.
func TestSDKCompleteness_Serverless(t *testing.T) {
	t.Parallel()

	// gopherstack-v4wu: UpdateSnapshot, GetTrack/ListTracks,
	// UpdateLakehouseConfiguration, and GetIdentityCenterAuthToken are now
	// implemented (see handler_serverless.go/serverless_tracks.go/
	// serverless_lakehouse.go/serverless_workgroups.go). The reservation
	// family (CreateReservation/GetReservation/GetReservationOffering/
	// ListReservationOfferings/ListReservations) remains a deliberate,
	// documented gap -- see PARITY.md's "Redshift Serverless" family note for
	// why: ReservationOffering carries AWS-set commercial pricing
	// (HourlyCharge/UpfrontCharge/CurrencyCode) with no fixed, enumerable
	// catalog in the SDK model to model honestly against (unlike classic
	// Redshift's ReservedNode, whose offerings key off a small, real,
	// AWS-documented node-type catalog), and this family has zero existing
	// backend state to hang a reservation's identity on.
	notImplemented := []string{
		"CreateReservation",
		"GetReservation",
		"GetReservationOffering",
		"ListReservationOfferings",
		"ListReservations",
	}

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewServerlessHandler(backend)
	sdkcheck.CheckCompleteness(t, &redshiftserverlesssdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
