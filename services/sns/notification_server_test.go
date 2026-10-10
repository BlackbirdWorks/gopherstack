package sns_test

import (
	"net/http"
	"net/http/httptest"
)

func skipConfirmation(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Sns-Message-Type") == "SubscriptionConfirmation" {
			w.WriteHeader(http.StatusOK)

			return
		}

		h.ServeHTTP(w, r)
	})
}

func newNotificationServer(h http.Handler) *httptest.Server {
	return httptest.NewServer(skipConfirmation(h))
}

func newNotificationTLSServer(h http.Handler) *httptest.Server {
	return httptest.NewTLSServer(skipConfirmation(h))
}
