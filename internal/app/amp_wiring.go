package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ampbackend "github.com/blackbirdworks/gopherstack/services/amp"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
)

// ampClusterResolver answers "does this EKS cluster exist" for scraper source validation.
type ampClusterResolver struct{ eks *eksbackend.Handler }

func (r ampClusterResolver) ClusterExists(region, name string) bool {
	b := r.eks.BackendFor(region)
	if b == nil {
		return false
	}

	_, err := b.DescribeCluster(name)

	return err == nil
}

// wireAMPScraperSources lets Managed Service for Prometheus scrapers validate their EKS source cluster.
func wireAMPScraperSources(byName map[string]service.Registerable) {
	ampH, aok := byName["AMP"].(*ampbackend.Handler)
	eksH, eok := byName["EKS"].(*eksbackend.Handler)

	if aok && eok {
		ampH.Backend.SetClusterResolver(ampClusterResolver{eks: eksH})
	}
}
