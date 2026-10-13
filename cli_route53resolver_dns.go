package main

import (
	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// wireRoute53ResolverDNS applies Resolver rules, DNS Firewall and query logging
// to queries reaching the embedded DNS server.
func wireRoute53ResolverDNS(reg service.Registerable, dnsSrv *gopherDNS.Server) {
	if reg == nil || dnsSrv == nil {
		return
	}

	h, ok := reg.(*route53resolverbackend.Handler)
	if !ok {
		return
	}

	if bk, isMem := h.Backend.(*route53resolverbackend.InMemoryBackend); isMem {
		dnsSrv.SetInterceptor(bk.NewDNSResolver(dnsSrv.Owns))
	}
}
