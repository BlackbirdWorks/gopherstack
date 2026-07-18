package amplify_test

import "github.com/blackbirdworks/gopherstack/services/amplify"

// newTestBackend creates a fresh in-memory Amplify backend for tests.
func newTestBackend() *amplify.InMemoryBackend {
	return amplify.NewInMemoryBackend("000000000000", "us-east-1")
}
