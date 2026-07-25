package cloudfront

import "github.com/google/uuid"

// Behavior-enum and header-name literals shared by the seed tables below. Named
// here (rather than repeated inline) purely to keep this file's string literals
// under goconst's duplication threshold -- these are not part of the public API.
const (
	behaviorNone      = "none"
	behaviorAll       = "all"
	behaviorWhitelist = "whitelist"

	headerOrigin = "Origin"
	headerHost   = "Host"

	// managedCachingOptimizedTTL and managedElementalMediaPackageTTL are the
	// DefaultTTL (86400s = 1 day) shared by several managed cache policies; named to
	// avoid repeating the same magic number across seed table entries.
	managedDefaultTTLOneDay     = 86400
	managedAmplifyMaxTTLSeconds = 600
	managedAmplifyTTLSeconds    = 2
	oneYearInSeconds            = 31536000
)

// This file seeds the AWS-provided "managed" cache policies, origin request
// policies, and response headers policies that exist by default in every real
// CloudFront account (see "Use managed cache policies" / "Use managed origin
// request policies" / "Use managed response headers policies" in the AWS
// CloudFront Developer Guide). Every name and ID below was verified against
// the live AWS documentation pages, not invented -- these IDs are permanent,
// well-known, and identical across every AWS account and region.
//
// Managed policies are read-only: UpdateXPolicy/DeleteXPolicy on one of these
// IDs returns IllegalUpdate/IllegalDelete (see cache_policies.go,
// origin_request_policies.go, response_headers_policies.go). ListXPolicies
// supports a Type=managed|custom query filter (see handler_cache_policies.go
// et al.) that partitions on the Managed field this seeding sets.
//
// A deliberately partial set is seeded: the four Amplify-internal cache
// policies (Amplify-Default, Amplify-DefaultNoCookies,
// Amplify-ImageOptimization, Amplify-StaticContent) are documented as
// "only used by Amplify... we don't recommend that you use these policies for
// your distributions" and are omitted, since they're not meant to be attached
// by hand the way the general-purpose managed policies are.

// managedCachePolicySeed describes one seeded managed cache policy.
type managedCachePolicySeed struct {
	id                  string
	name                string
	headerBehavior      string
	cookieBehavior      string
	queryStringBehavior string
	headers             []string
	queryStrings        []string
	minTTL              int64
	maxTTL              int64
	defaultTTL          int64
	gzip                bool
	brotli              bool
}

//nolint:gochecknoglobals // static seed table, read-only after init, mirrors errCodeMapping style
var managedCachePolicySeeds = []managedCachePolicySeed{
	{
		id: "658327ea-f89d-4fab-a63d-7e88639e58f6", name: "Managed-CachingOptimized",
		minTTL: 1, maxTTL: maxCachePolicyTTL, defaultTTL: managedDefaultTTLOneDay,
		headerBehavior: behaviorNone, cookieBehavior: behaviorNone, queryStringBehavior: behaviorNone,
		gzip: true, brotli: true,
	},
	{
		id: "4135ea2d-6df8-44a3-9df3-4b5a84be39ad", name: "Managed-CachingDisabled",
		minTTL: 0, maxTTL: 0, defaultTTL: 0,
		headerBehavior: behaviorNone, cookieBehavior: behaviorNone, queryStringBehavior: behaviorNone,
	},
	{
		id: "b2884449-e4de-46a7-ac36-70bc7f1ddd6d", name: "Managed-CachingOptimizedForUncompressedObjects",
		minTTL: 1, maxTTL: maxCachePolicyTTL, defaultTTL: managedDefaultTTLOneDay,
		headerBehavior: behaviorNone, cookieBehavior: behaviorNone, queryStringBehavior: behaviorNone,
	},
	{
		id: "08627262-05a9-4f76-9ded-b50ca2e3a84f", name: "Managed-Elemental-MediaPackage",
		minTTL: 0, maxTTL: maxCachePolicyTTL, defaultTTL: managedDefaultTTLOneDay,
		headerBehavior: behaviorWhitelist, headers: []string{headerOrigin},
		cookieBehavior:      behaviorNone,
		queryStringBehavior: behaviorWhitelist,
		queryStrings:        []string{"aws.manifestfilter", "start", "end", "m"},
		gzip:                true,
	},
	{
		id: "2e54312d-136d-493c-8eb9-b001f22f67d2", name: "Managed-Amplify",
		minTTL: managedAmplifyTTLSeconds, maxTTL: managedAmplifyMaxTTLSeconds, defaultTTL: managedAmplifyTTLSeconds,
		headerBehavior: behaviorWhitelist,
		headers:        []string{"Authorization", "CloudFront-Viewer-Country", headerHost},
		cookieBehavior: behaviorAll, queryStringBehavior: behaviorAll,
		gzip: true, brotli: true,
	},
	{
		id:             "83da9c7e-98b4-4e11-a168-04f0df8e2c65",
		name:           "Managed-UseOriginCacheControlHeaders",
		minTTL:         0,
		maxTTL:         maxCachePolicyTTL,
		defaultTTL:     0,
		headerBehavior: behaviorWhitelist,
		headers: []string{
			headerHost,
			headerOrigin,
			"X-HTTP-Method-Override",
			"X-HTTP-Method",
			"X-Method-Override",
		},
		cookieBehavior:      behaviorAll,
		queryStringBehavior: behaviorNone,
		gzip:                true,
		brotli:              true,
	},
	{
		id:             "4cc15a8a-d715-48a4-82b8-cc0b614638fe",
		name:           "Managed-UseOriginCacheControlHeaders-QueryStrings",
		minTTL:         0,
		maxTTL:         maxCachePolicyTTL,
		defaultTTL:     0,
		headerBehavior: behaviorWhitelist,
		headers: []string{
			headerHost,
			headerOrigin,
			"X-HTTP-Method-Override",
			"X-HTTP-Method",
			"X-Method-Override",
		},
		cookieBehavior:      behaviorAll,
		queryStringBehavior: behaviorAll,
		gzip:                true,
		brotli:              true,
	},
}

// managedOriginRequestPolicySeed describes one seeded managed origin request policy.
type managedOriginRequestPolicySeed struct {
	id                  string
	name                string
	headerBehavior      string
	cookieBehavior      string
	queryStringBehavior string
	headers             []string
}

//nolint:gochecknoglobals // static seed table, read-only after init
var managedOriginRequestPolicySeeds = []managedOriginRequestPolicySeed{
	{
		id: "216adef6-5c7f-47e4-b989-5492eafa07d3", name: "Managed-AllViewer",
		headerBehavior: "allViewer", cookieBehavior: behaviorAll, queryStringBehavior: behaviorAll,
	},
	{
		id: "33f36d7e-f396-46d9-90e0-52428a34d9dc", name: "Managed-AllViewerAndCloudFrontHeaders-2022-06",
		headerBehavior: "allViewerAndWhitelistCloudFront",
		headers: []string{
			"CloudFront-Forwarded-Proto", "CloudFront-Is-Android-Viewer", "CloudFront-Is-Desktop-Viewer",
			"CloudFront-Is-IOS-Viewer", "CloudFront-Is-Mobile-Viewer", "CloudFront-Is-SmartTV-Viewer",
			"CloudFront-Is-Tablet-Viewer", "CloudFront-Viewer-Address", "CloudFront-Viewer-ASN",
			"CloudFront-Viewer-City", "CloudFront-Viewer-Country", "CloudFront-Viewer-Country-Name",
			"CloudFront-Viewer-Country-Region", "CloudFront-Viewer-Country-Region-Name",
			"CloudFront-Viewer-Http-Version", "CloudFront-Viewer-Latitude", "CloudFront-Viewer-Longitude",
			"CloudFront-Viewer-Metro-Code", "CloudFront-Viewer-Postal-Code", "CloudFront-Viewer-Time-Zone",
			"CloudFront-Viewer-TLS",
		},
		cookieBehavior: behaviorAll, queryStringBehavior: behaviorAll,
	},
	{
		id: "b689b0a8-53d0-40ab-baf2-68738e2966ac", name: "Managed-AllViewerExceptHostHeader",
		headerBehavior: "allExcept", headers: []string{headerHost},
		cookieBehavior: behaviorAll, queryStringBehavior: behaviorAll,
	},
	{
		id: "59781a5b-3903-41f3-afcb-af62929ccde1", name: "Managed-CORS-CustomOrigin",
		headerBehavior: behaviorWhitelist, headers: []string{headerOrigin},
		cookieBehavior: behaviorNone, queryStringBehavior: behaviorNone,
	},
	{
		id: "88a5eaf4-2fd4-4709-b370-b4c650ea3fcf", name: "Managed-CORS-S3Origin",
		headerBehavior: behaviorWhitelist,
		headers:        []string{headerOrigin, "Access-Control-Request-Headers", "Access-Control-Request-Method"},
		cookieBehavior: behaviorNone, queryStringBehavior: behaviorNone,
	},
	{
		id: "775133bc-15f2-49f9-abea-afb2e0bf67d2", name: "Managed-Elemental-MediaTailor-PersonalizedManifests",
		headerBehavior: behaviorWhitelist,
		headers: []string{
			headerOrigin, "Access-Control-Request-Headers", "Access-Control-Request-Method",
			"User-Agent", "X-Forwarded-For",
		},
		cookieBehavior: behaviorNone, queryStringBehavior: behaviorAll,
	},
	{
		id: "bf0718e1-ba1e-49d1-88b1-f726733018ae", name: "Managed-HostHeaderOnly",
		headerBehavior: behaviorWhitelist, headers: []string{headerHost},
		cookieBehavior: behaviorNone, queryStringBehavior: behaviorNone,
	},
	{
		id: "acba4595-bd28-49b8-b9fe-13317c0390fa", name: "Managed-UserAgentRefererHeaders",
		headerBehavior: behaviorWhitelist, headers: []string{"User-Agent", "Referer"},
		cookieBehavior: behaviorNone, queryStringBehavior: behaviorNone,
	},
}

// managedSecurityHeaders is the fixed security-header set shared by
// Managed-SecurityHeadersPolicy, Managed-CORS-and-SecurityHeadersPolicy, and
// Managed-CORS-with-preflight-and-SecurityHeadersPolicy.
func managedSecurityHeaders() *RHPSecurityHeaders {
	return &RHPSecurityHeaders{
		ReferrerPolicy:                 "strict-origin-when-cross-origin",
		StrictTransportSecuritySeconds: oneYearInSeconds,
		ContentTypeOptionsOverride:     true,
		FrameOptionsValue:              "SAMEORIGIN",
		XSSProtection:                  "1; mode=block",
	}
}

// managedResponseHeadersPolicySeed describes one seeded managed response headers policy.
type managedResponseHeadersPolicySeed struct {
	cors     *RHPCorsConfig
	id       string
	name     string
	security bool
}

//nolint:gochecknoglobals // static seed table, read-only after init
var managedResponseHeadersPolicySeeds = []managedResponseHeadersPolicySeed{
	{
		id: "60669652-455b-4ae9-85a4-c4c02393f86c", name: "Managed-SimpleCORS",
		cors: &RHPCorsConfig{AccessControlAllowOrigins: []string{"*"}},
	},
	{
		id: "5cc3b908-e619-4b99-88e5-2cf7f45965bd", name: "Managed-CORS-With-Preflight",
		cors: &RHPCorsConfig{
			AccessControlAllowOrigins:  []string{"*"},
			AccessControlAllowMethods:  []string{"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"},
			AccessControlExposeHeaders: []string{"*"},
		},
	},
	{
		id: "67f7725c-6f97-4210-82d7-5512b31e9d03", name: "Managed-SecurityHeadersPolicy",
		security: true,
	},
	{
		id: "e61eb60c-9c35-4d20-a928-2b84e02af89c", name: "Managed-CORS-and-SecurityHeadersPolicy",
		cors:     &RHPCorsConfig{AccessControlAllowOrigins: []string{"*"}},
		security: true,
	},
	{
		id: "eaab4381-ed33-4a86-88ca-d9558dc6cd63", name: "Managed-CORS-with-preflight-and-SecurityHeadersPolicy",
		cors: &RHPCorsConfig{
			AccessControlAllowOrigins:  []string{"*"},
			AccessControlAllowMethods:  []string{"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"},
			AccessControlExposeHeaders: []string{"*"},
		},
		security: true,
	},
}

// seedManagedPoliciesLocked populates the AWS-managed cache/origin-request/
// response-headers policies. Must be called with the backend already
// exclusive-owned: either during NewInMemoryBackend construction (no
// concurrent access is possible yet) or with b.mu already held (Reset).
func (b *InMemoryBackend) seedManagedPoliciesLocked() {
	for _, s := range managedCachePolicySeeds {
		p := &CachePolicy{
			ID: s.id, Name: s.name, ETag: uuid.NewString(),
			DefaultTTL: s.defaultTTL, MaxTTL: s.maxTTL, MinTTL: s.minTTL,
			Managed: true,
			Params: &CachePolicyParams{
				EnableAcceptEncodingGzip:   s.gzip,
				EnableAcceptEncodingBrotli: s.brotli,
				HeadersConfig: CachePolicyHeadersConfig{
					HeaderBehavior: s.headerBehavior,
					Headers:        s.headers,
				},
				CookiesConfig: CachePolicyCookiesConfig{CookieBehavior: s.cookieBehavior},
				QueryStringsConfig: CachePolicyQueryStringsConfig{
					QueryStringBehavior: s.queryStringBehavior, QueryStrings: s.queryStrings,
				},
			},
		}
		b.cachePolicies.Put(p)
		b.cachePolicyByName[s.name] = s.id
	}

	for _, s := range managedOriginRequestPolicySeeds {
		p := &OriginRequestPolicy{
			ID: s.id, Name: s.name, ETag: uuid.NewString(), Managed: true,
			HeadersConfig:      &ORPHeadersConfig{HeaderBehavior: s.headerBehavior, Headers: s.headers},
			CookiesConfig:      &ORPCookiesConfig{CookieBehavior: s.cookieBehavior},
			QueryStringsConfig: &ORPQueryStringsConfig{QueryStringBehavior: s.queryStringBehavior},
		}
		b.originRequestPolicies.Put(p)
		b.originRequestPolicyByName[s.name] = s.id
	}

	for _, s := range managedResponseHeadersPolicySeeds {
		p := &ResponseHeadersPolicy{
			ID: s.id, Name: s.name, ETag: uuid.NewString(), Managed: true,
			CorsConfig: s.cors,
		}
		if s.security {
			p.SecurityHeaders = managedSecurityHeaders()
		}
		b.responseHeadersPolicies.Put(p)
		b.responseHeadersPolicyByName[s.name] = s.id
	}
}
