package app

import (
	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
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

// wireRoute53ResolverQueryLogs lets query log configs deliver to S3 buckets and Firehose delivery streams.
func wireRoute53ResolverQueryLogs(reg, s3Reg, firehoseReg service.Registerable) {
	h, ok := reg.(*route53resolverbackend.Handler)
	if !ok {
		return
	}

	bk, ok := h.Backend.(*route53resolverbackend.InMemoryBackend)
	if !ok {
		return
	}

	var (
		objects route53resolverbackend.QueryLogObjectStore
		streams route53resolverbackend.QueryLogStreams
	)

	if s3H, s3Ok := s3Reg.(*s3backend.S3Handler); s3Ok && s3H != nil {
		objects = codebuildS3{backend: s3H.Backend}
	}

	if fhH, fhOk := firehoseReg.(*firehosebackend.Handler); fhOk && fhH != nil {
		streams = fhH.Backend
	}

	bk.SetQueryLogDestinations(objects, streams)
}
