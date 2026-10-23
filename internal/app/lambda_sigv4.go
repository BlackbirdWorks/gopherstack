package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// wireLambdaSigV4 passes --sigv4-secret to Lambda so AWS_IAM function URLs verify against it.
func wireLambdaSigV4(lambdaReg service.Registerable, secret string) {
	h, ok := lambdaReg.(*lambdabackend.Handler)
	if !ok {
		return
	}

	if bk, bkOk := h.Backend.(*lambdabackend.InMemoryBackend); bkOk {
		bk.SetSigV4Secret(secret)
	}
}
