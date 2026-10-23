package gcpkms_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name:     "create key ring",
			Method:   http.MethodPost,
			Path:     "/v1/projects/p/locations/global/keyRings?keyRingId=kr",
			Body:     `{}`,
			Status:   200,
			Contains: []string{`"name":"projects/p/locations/global/keyRings/kr"`},
		},
		{
			Name: "create crypto key", Method: http.MethodPost,
			Path: "/v1/projects/p/locations/global/keyRings/kr/cryptoKeys?cryptoKeyId=k",
			Body: `{"purpose":"ENCRYPT_DECRYPT"}`, Status: 200, Contains: []string{`"purpose":"ENCRYPT_DECRYPT"`},
		},
		{
			Name: "encrypt", Method: http.MethodPost,
			Path: "/v1/projects/p/locations/global/keyRings/kr/cryptoKeys/k:encrypt",
			Body: `{"plaintext":"aGk="}`, Status: 200, Contains: []string{`"ciphertext"`},
		},
		{
			Name: "get missing key", Method: http.MethodGet,
			Path: "/v1/projects/p/locations/global/keyRings/kr/cryptoKeys/nope", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}
