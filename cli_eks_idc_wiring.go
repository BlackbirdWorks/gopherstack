package main

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	ssoadminbackend "github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

const eksIdcApplicationProviderArn = "arn:aws:sso::aws:applicationProvider/eks-argocd"

// eksIdcApplications creates the IAM Identity Center application behind an EKS Argo CD capability.
type eksIdcApplications struct{ handler *ssoadminbackend.Handler }

func (a eksIdcApplications) CreateManagedApplication(idcRegion, instanceArn, name string) (string, error) {
	app, err := a.handler.BackendFor(idcRegion).CreateApplication(
		instanceArn, eksIdcApplicationProviderArn, name, "Managed by Amazon EKS", "ENABLED", nil, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create identity center application: %w", err)
	}

	return app.ApplicationArn, nil
}

func (a eksIdcApplications) DeleteManagedApplication(idcRegion, applicationArn string) error {
	return a.handler.BackendFor(idcRegion).DeleteApplication(applicationArn)
}

// wireEKSIdcApplications lets Argo CD capabilities report their Identity Center managed application ARN.
func wireEKSIdcApplications(byName map[string]service.Registerable) {
	sso, ok := byName["SsoAdmin"].(*ssoadminbackend.Handler)
	if !ok {
		return
	}

	if h, hok := byName["EKS"].(*eksbackend.Handler); hok {
		h.Backend.SetIdcApplicationManager(eksIdcApplications{handler: sso})
	}
}
