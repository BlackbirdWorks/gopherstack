package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// wireEKSIRSAToSTS lets STS verify web-identity tokens signed by EKS cluster issuers.
func wireEKSIRSAToSTS(eksReg, stsReg service.Registerable) {
	eksH, eksOk := eksReg.(*eksbackend.Handler)
	stsH, stsOk := stsReg.(*stsbackend.Handler)

	if !eksOk || !stsOk {
		return
	}

	if stsBk, ok := stsH.Backend.(*stsbackend.InMemoryBackend); ok {
		stsBk.SetIssuerKeyResolver(eksH)
	}
}
