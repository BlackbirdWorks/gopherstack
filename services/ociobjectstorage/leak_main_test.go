package ociobjectstorage_test

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

func TestMain(m *testing.M) {
	// The OCI SDK client arms a per-request timeout timer that outlives unread response bodies.
	testleak.VerifyTestMain(m, goleak.IgnoreTopFunction("net/http.setRequestCancel.func4"))
}
