package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/internal/routegates"
)

const (
	noauthLimit     = 20
	getQueryLimit   = 10
	probeLimit      = 4
	restNoauthLimit = 12
)

type collector struct {
	seen map[row]bool
	out  []row
}

func (c *collector) add(r row) {
	if !strings.Contains(r.note, ":noauth") && !strings.HasPrefix(r.note, "special") {
		ua := fmt.Sprintf(uaFmt, r.pkg())

		switch {
		case r.headers == "":
			r.headers = ua
		case !strings.Contains(r.headers, "User-Agent"):
			r.headers += ";" + ua
		}
	}

	if !c.seen[r] {
		c.seen[r] = true
		c.out = append(c.out, r)
	}
}

func expandRows(sdk []row) []row {
	var order []string

	groups := map[string][]row{}

	for _, r := range sdk {
		p := r.pkg()
		if _, ok := groups[p]; !ok {
			order = append(order, p)
		}

		groups[p] = append(groups[p], r)
	}

	c := &collector{seen: map[row]bool{}}

	for _, p := range order {
		for i, r := range groups[p] {
			c.add(r)
			c.addProbes(i, r)
		}

		if p == "s3" {
			c.addS3Hosts(groups[p])
		}
	}

	c.addSpecials()
	c.addAppStream(groups["appstream"])
	c.addS3Lookalikes()
	c.addAltGateRows()

	return c.out
}

func (c *collector) variant(r row, mutate func(*row), suffix string) {
	r.note += ":" + suffix
	mutate(&r)
	c.add(r)
}

func (c *collector) addProbes(i int, r row) {
	isQuery := strings.HasPrefix(r.body, "Action=")
	isJSON := r.target != ""

	if i < noauthLimit && (isQuery || isJSON) {
		c.variant(r, func(x *row) { x.auth = "" }, "noauth")
	}

	if i < getQueryLimit && isQuery {
		c.variant(r, func(x *row) {
			x.method, x.uri, x.ctype, x.target, x.body = methodGet, "/?"+r.body, "", "", ""
		}, "getq")
	}

	if i < probeLimit {
		c.variant(r, func(x *row) {
			x.auth = "s3"
			if r.auth != scopeExecuteAPI {
				x.auth = scopeExecuteAPI
			}
		}, "wrongscope")
		c.variant(r, func(x *row) { x.host = r.auth + ".us-east-1.amazonaws.com" }, "awshost")
		c.variant(r, func(x *row) {
			sep := "?"
			if strings.Contains(r.uri, "?") {
				sep = "&"
			}

			x.uri = r.uri + sep + "X-Amz-Credential=AKIA%2F20260101%2Fus-east-1%2F" + r.auth +
				"%2Faws4_request&X-Amz-Signature=00"
			x.auth = ""
		}, "presigned")
	}

	if i < restNoauthLimit && !isQuery && !isJSON {
		c.variant(r, func(x *row) { x.auth = "" }, "noauth")
	}
}

func (c *collector) addS3Hosts(rows []row) {
	for _, r := range rows {
		pp, q, _ := strings.Cut(r.uri, "?")
		if q != "" {
			q = r.uri[len(pp):]
		}

		segs := strings.Split(pp, "/")
		if len(segs) < 2 || segs[1] == "" {
			continue
		}

		rest := "/"
		if len(segs) > bucketSegs {
			rest = "/" + strings.Join(segs[2:], "/")
		}

		c.variant(r, func(x *row) { x.host, x.uri = "mybucket.s3.localhost:4566", rest+q }, "vhost")
		c.variant(r, func(x *row) { x.host, x.uri = "mybucket.s3.us-west-2.amazonaws.com", rest+q }, "vhostaws")
		c.variant(r, func(x *row) { x.host, x.uri, x.auth = "mybucket.s3.localhost:4566", rest+q, "" }, "vhostnoauth")
		c.variant(r, func(x *row) {
			x.host, x.uri, x.auth = "s3.localhost:4566", "/mybucket"+strings.TrimRight(rest, "/")+q, ""
		}, "pathnoauth")
	}
}

func (c *collector) addSpecials() {
	specials := [][3]string{
		{methodGet, host, "/"}, {methodGet, host, "/health"}, {methodGet, host, "/_localstack/health"},
		{methodGet, host, "/_localstack/info"}, {methodGet, host, "/_gopherstack/health"},
		{
			methodGet,
			host,
			"/_gopherstack/chaos/faults",
		}, {methodGet, host, "/dashboard"}, {methodGet, host, "/dashboard/"},
		{methodGet, host, "/dashboard/s3"}, {methodGet, host, "/dashboard/api/v1/x"}, {methodGet, host, "/metrics"},
		{methodGet, host, "/favicon.ico"}, {methodGet, host, "/robots.txt"}, {methodGet, host, "/nonexistent/path"},
		{methodGet, host, "/mybucket"}, {methodGet, host, keyPath}, {"PUT", host, "/mybucket"},
		{"HEAD", host, keyPath}, {"OPTIONS", host, keyPath}, {"OPTIONS", host, "/"},
		{methodPost, host, "/"}, {methodGet, host, "/_aws/ses"}, {methodGet, host, "/_aws/sqs/messages"},
		{"DELETE", host, "/_aws/ses"}, {methodGet, host, "/_aws/cloudwatch/metrics/raw"},
		{methodPost, host, "/_localstack/state/reset"},
		{methodGet, host, "/_aws/sqs/messages/us-east-1/000000000000/q"},
		{methodGet, host, "/swagger"}, {methodGet, host, "/tags/arn%3Aaws%3Aservice%3Aus-east-1%3A000000000000%3Ax"},
		{methodGet, host, "/resourcepolicy/x"}, {methodGet, host, "/flows"}, {methodGet, host, "/agents"},
		{methodGet, host, "/prompts"}, {methodGet, host, "/2015-03-31/functions/"}, {methodGet, host, "/restapis"},
		{
			methodGet,
			host,
			"/v2/apis",
		}, {methodGet, host, "/2013-04-01/hostedzone"}, {methodGet, "s3.localhost:4566", "/"},
		{methodGet, "s3.us-east-1.amazonaws.com", "/"}, {methodGet, "mybucket.s3.amazonaws.com", "/key"},
		{methodGet, host, "/mybucket?versioning"},
	}

	for _, s := range specials {
		c.add(row{method: s[0], host: s[1], uri: s[2], note: "special"})

		for _, sg := range []string{"s3", "sts", "iam", "dynamodb", scopeExecuteAPI, "es", "foo"} {
			c.add(row{method: s[0], host: s[1], uri: s[2], auth: sg, note: "special:" + sg})
		}
	}

	for _, pre := range []string{"AmazonSSM", "Foo", "", ".", "Kinesis_20131202"} {
		c.add(row{methodPost, host, "/", "", json11, pre + ".Bogus", "{}", "", "special:target"})
		c.add(row{methodPost, host, "/", "dynamodb", json10, pre + ".Bogus", "{}", "", "special:target"})
	}

	const form = "application/x-www-form-urlencoded"

	for _, q := range [][2]string{
		{"", "Action=Bogus&Version=2010-03-31"}, {"", "Action=Bogus"}, {"sns", "Action=Bogus&Version=1999"}, {"", ""},
	} {
		c.add(row{methodPost, host, "/", q[0], form, "", q[1], "", "special:query"})
	}
}

func (c *collector) addAppStream(rows []row) {
	seen := map[string]bool{}

	for _, r := range rows {
		if r.headers != cborHeader {
			continue
		}

		op := r.uri[strings.LastIndex(r.uri, "/")+1:]
		if seen[op] {
			continue
		}

		seen[op] = true

		c.add(row{
			methodPost, host, "/", "appstream", json11,
			"PhotonAdminProxyService." + op, "{}", "", "appstream:" + op,
		})
	}
}

const noteS3Lookalike = "special:s3lookalike"

// lookalikeBuckets returns bucket names that collide with other services' first path segments.
func lookalikeBuckets() []string {
	return []string{
		"tags", "2015-03-31", "restapis", "functions", "model", "things", "policies", "proxy", "api", "_admin",
		"apikeys", "domainnames", "usageplans", "clientcertificates", "vpclinks", "sdktypes", "rules", "cluster",
		"controlpanel", "routingcontrol", "safetyrule", "zonalshifts", "cells", "identities", "invocations", "mcp",
		"memories", "runtimes", "target-policies", "layers", "event-source-mappings", "2018-10-31", "2020-04-22",
	}
}

// addS3Lookalikes adds S3 object requests whose bucket name equals another service's path prefix.
func (c *collector) addS3Lookalikes() {
	names := map[string]bool{}

	for _, b := range lookalikeBuckets() {
		names[b] = true
	}

	for _, prefixes := range routegates.PathGates() {
		for _, p := range prefixes {
			if seg := strings.Trim(p, "/"); seg != "" && !strings.Contains(seg, "/") {
				names[seg] = true
			}
		}
	}

	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}

	sort.Strings(sorted)

	for _, b := range sorted {
		obj := "/" + b + "/key.txt"
		c.add(row{methodGet, host, obj, "s3", "", "", "", "", noteS3Lookalike})
		c.add(row{"PUT", host, obj, "s3", "application/octet-stream", "", "body", "", noteS3Lookalike})
		c.add(row{methodGet, host, obj, "", "", "", "", fmt.Sprintf(uaFmt, "s3"), noteS3Lookalike})
		c.add(row{"HEAD", host, "/" + b, "s3", "", "", "", "", noteS3Lookalike})
	}
}

const noteAlt = "special:altgate"

// addAltGateRows exercises each OR-gate alternative alone, absent, and via presigned query auth.
func (c *collector) addAltGateRows() {
	const (
		lambdaID   = "abcdef0123456789abcdef0123456789"
		msdUA      = "User-Agent:aws-sdk-go-v2/1.41.0 api/mediastoredata#1.0.0"
		amzUAPrefx = "X-Amz-User-Agent:aws-sdk-js/3.0 "
	)

	presigned := func(uri, scope string) string {
		sep := "?"
		if strings.Contains(uri, "?") {
			sep = "&"
		}

		return uri + sep + "X-Amz-Credential=AKIA%2F20260101%2Fus-east-1%2F" + scope + "%2Faws4_request&X-Amz-Signature=00"
	}

	add := func(method, h, uri, auth, target, headers string) {
		c.add(row{method, h, uri, auth, json11, target, "{}", headers, noteAlt})
	}

	for _, m := range []string{methodGet, methodPost} {
		for _, scope := range []string{"memorydb", "route53-recovery-cluster", "cognito-idp", "lambda", "s3", ""} {
			for _, uri := range []string{"/", "/route53-recovery-cluster/abc/us-east-1/", "/x"} {
				if m != methodPost && scope == "route53-recovery-cluster" {
					continue
				}

				add(m, host, uri, scope, "", "")
				add(m, host, presigned(uri, scope), "", "", "")
			}
		}
	}

	for _, t := range []string{
		"AmazonMemoryDB.DescribeClusters", "ToggleCustomerAPI.GetRoutingControlState", "AWSLambda.ListFunctions",
		"AWSCognitoIdentityProviderService.InitiateAuth", "AWSLambda20150331.ListFunctions", "AmazonMemoryDB",
	} {
		add(methodPost, host, "/", "", t, "")
		add(methodPost, host, "/", "s3", t, "")
	}

	for _, ua := range []string{
		msdUA, "User-Agent:MediaStoreData", "User-Agent:x MEDIASTORE-DATA y", "User-Agent:aws-sdk-go-v2 api/mediastore#1",
		amzUAPrefx + "mediastore-data", amzUAPrefx + "MediaStoreData", "User-Agent:\u0130mediastoredata",
	} {
		for _, uri := range []string{"/", "/obj", "/a/b?x=1"} {
			for _, m := range []string{methodGet, "PUT", "DELETE", "HEAD"} {
				add(m, host, uri, "mediastore", "", ua)
				add(m, host, uri, "", "", ua)
			}
		}
	}

	oauth := []string{
		"/oauth2/token", "/oauth2/authorize?client_id=c", "/oauth2/userInfo", "/oauth2/revoke", "/oauth2/idpresponse",
		"/login?client_id=c", "/logout?client_id=c", "/saml2/idpresponse", "/saml2/logout", "/saml2/metadata",
		"/us-east-1_abc/saml2/metadata", "/us-east-1_abc/.well-known/jwks.json",
		"/us-east-1_abc/.well-known/openid-configuration", "/.well-known/jwks.json",
		"/.well-known/openid-configuration", "//oauth2/token", "/a/b/.well-known/jwks.json",
	}
	for _, uri := range oauth {
		for _, m := range []string{methodGet, methodPost, "OPTIONS"} {
			add(m, host, uri, "", "", "")
			add(m, host, uri, "cognito-idp", "", "")
			add(m, "auth.localhost:4566", uri, "", "", "")
		}
	}

	for _, h := range []string{
		lambdaID + ".lambda-url.us-east-1.on.aws", lambdaID + ".lambda-url.eu-west-1.localhost:4566",
		strings.ToUpper(lambdaID) + ".LAMBDA-URL.us-east-1.ON.AWS", lambdaID + ".lambda-url.us-east-1.on.aws:443",
		lambdaID + ".lambda-url.us-east-1.example.com", "lambda-url.us-east-1.on.aws", "x.lambda-url..on.aws",
	} {
		for _, uri := range []string{"/", "/a/b?x=1", "/2015-03-31/functions/"} {
			add(methodGet, h, uri, "", "", "")
			add(methodPost, h, uri, "s3", "", "")
		}
	}

	for _, uri := range []string{
		"/2015-03-31/functions/", "/2016-08-19/account-settings", "/2025-12-01/durable-executions",
		"/2026-07-09/resource-policy/arn", "/2020-04-22/code-signing-configs", "/2018-10-31/layers",
		"/2017-03-31/tags/arn", "/2099-01-01/functions", "/2015-03-31/event-source-mappings",
	} {
		add(methodGet, host, uri, "", "", "")
		add(methodGet, host, uri, "lambda", "", "")
		add(methodGet, host, uri, "s3", "", "")
	}

	c.addAltGateRowsBatch2(add)
}

const formCT = "application/x-www-form-urlencoded"

func (c *collector) addAltGateRowsBatch2(add func(method, h, uri, auth, target, headers string)) {
	for _, t := range []string{
		"AmazonEventBridge.ListRules", "AWSEvents.ListRules", "AWSSchemas.ListRegistries", "ResourceGroups.ListGroups",
		"AmazonPersonalize.ListDatasets", "AmazonPersonalizeRuntime.GetRecommendations", "AmazonSSM.ListDocuments",
		"EC2WindowsMessageDeliveryService.Poll", "AmazonEC2ContainerRegistry_V20150921.DescribeRepositories",
		"AWSScheduler.ListSchedules", "AmazonSQS.ListQueues",
	} {
		add(methodPost, host, "/", "", t, "")
		add(methodPost, host, "/", "s3", t, "")
		add(methodPost, host, "/q/000000000000/name", "sqs", t, "")
	}

	for _, uri := range []string{
		"/v1/registries", "/v1/discover", "//v1/policy", "/v1/discoverers/id/d1/start", "/v1/registries/name/r/schemas",
		"/groups", "/get-group", "/update-group-query", "/list-group-resources", "/resources/search",
		"/resources/arn:aws:resource-groups:us-east-1:000000000000:group%2Fg/tags", "/ungroup-resources",
		"/recommendations", "/personalize-ranking", "/action-recommendations", "/ssmmessages/v1/data-channel/x",
		"/v2/", "/v2", "/v2/_catalog", "/v2/repo/manifests/latest", "/v2/repo/blobs/sha256:x", "/v2/repo/tags/list",
		"/schedules", "/schedules/x", "/schedule-groups/g", "/tags/arn:aws:scheduler:us-east-1:000000000000:schedule%2Fx",
		"/tags/arn:aws:sns:us-east-1:000000000000:t", "/SimpleNotificationService-abc.pem",
	} {
		for _, m := range []string{methodGet, methodPost, "PUT", "DELETE"} {
			add(m, host, uri, "", "", "")
			add(m, host, uri, "s3", "", "")
		}
	}

	for _, q := range []string{
		"Action=ListQueues&Version=2012-11-05", "Action=GetCallerIdentity&Version=2011-06-15",
		"Action=ListTopics&Version=2010-03-31", "Action=Bogus", "",
	} {
		for _, ct := range []string{formCT, "APPLICATION/X-WWW-FORM-URLENCODED", "application/json", ""} {
			for _, auth := range []string{"sts", "sns", "sqs", ""} {
				c.add(row{methodPost, host, "/", auth, ct, "", q, "", noteAlt})
			}
		}
	}

	for _, q := range []string{
		"?Action=ConfirmSubscription&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%3At&Token=x",
		"?Act%69on=ConfirmSubscription&TopicArn=x", "?Action=ListTopics", "?x=1", "?X-Amz-Credential=a%2Fb",
	} {
		add(methodGet, host, "/"+q, "", "", "")
		add(methodGet, host, "/bucket/key"+q, "s3", "", "")
	}
}

