package main

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	wafv2backend "github.com/blackbirdworks/gopherstack/services/wafv2"
)

// appsyncWebACLResolver adapts WAFv2 associations to appsync.WebACLResolver.
type appsyncWebACLResolver struct {
	backend wafv2backend.StorageBackend
}

func (r appsyncWebACLResolver) WebACLARN(ctx context.Context, resourceARN string) string {
	acl, err := r.backend.GetWebACLForResource(ctx, resourceARN)
	if err != nil || acl == nil {
		return ""
	}

	return acl.ARN
}

// wireAppSyncWAF gives AppSync the WAFv2 association lookup behind wafWebAclArn.
func wireAppSyncWAF(byName map[string]service.Registerable) {
	asH, ok := byName["AppSync"].(*appsyncbackend.Handler)
	if !ok {
		return
	}

	wafH, ok := byName["Wafv2"].(*wafv2backend.Handler)
	if !ok {
		return
	}

	asH.SetWebACLResolver(appsyncWebACLResolver{backend: wafH.Backend})
}
