package main

import "strings"

const (
	noteAlt       = "special:altgate"
	formCT        = "application/x-www-form-urlencoded"
	lambdaURLID   = "abcdef0123456789abcdef0123456789"
	scopeR53RC    = "route53-recovery-cluster"
	r53rcRootPath = "/route53-recovery-cluster/abc/us-east-1/"
)

type altAdder func(method, h, uri, auth, target, headers string)

func presignedURI(uri, scope string) string {
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}

	return uri + sep + "X-Amz-Credential=AKIA%2F20260101%2Fus-east-1%2F" + scope + "%2Faws4_request&X-Amz-Signature=00"
}

// addAltGateRows exercises each OR-gate alternative alone, absent, and via presigned query auth.
func (c *collector) addAltGateRows() {
	add := func(method, h, uri, auth, target, headers string) {
		c.add(row{method, h, uri, auth, json11, target, "{}", headers, noteAlt})
	}

	c.addScopeRows(add)
	c.addTargetRows(add)
	c.addUserAgentRows(add)
	c.addCognitoRows(add)
	c.addLambdaRows(add)
	c.addPathRows(add)
	c.addFormRows()
	c.addQueryRows(add)
}

func (c *collector) addScopeRows(add altAdder) {
	scopes := []string{"memorydb", scopeR53RC, "cognito-idp", "lambda", "s3", ""}

	for _, m := range []string{methodGet, methodPost} {
		for _, scope := range scopes {
			if m != methodPost && scope == scopeR53RC {
				continue
			}

			for _, uri := range []string{"/", r53rcRootPath, "/x"} {
				add(m, host, uri, scope, "", "")
				add(m, host, presignedURI(uri, scope), "", "", "")
			}
		}
	}
}

func (c *collector) addTargetRows(add altAdder) {
	targets := []string{
		"AmazonMemoryDB.DescribeClusters", "ToggleCustomerAPI.GetRoutingControlState", "AWSLambda.ListFunctions",
		"AWSCognitoIdentityProviderService.InitiateAuth", "AWSLambda20150331.ListFunctions", "AmazonMemoryDB",
		"AmazonEventBridge.ListRules", "AWSEvents.ListRules", "AWSSchemas.ListRegistries", "ResourceGroups.ListGroups",
		"AmazonPersonalize.ListDatasets", "AmazonPersonalizeRuntime.GetRecommendations", "AmazonSSM.ListDocuments",
		"EC2WindowsMessageDeliveryService.Poll", "AmazonEC2ContainerRegistry_V20150921.DescribeRepositories",
		"AWSScheduler.ListSchedules", "AmazonSQS.ListQueues",
	}

	for _, t := range targets {
		add(methodPost, host, "/", "", t, "")
		add(methodPost, host, "/", "s3", t, "")
		add(methodPost, host, "/q/000000000000/name", "sqs", t, "")
	}
}

func (c *collector) addUserAgentRows(add altAdder) {
	agents := []string{
		"User-Agent:aws-sdk-go-v2/1.41.0 api/mediastoredata#1.0.0", "User-Agent:MediaStoreData",
		"User-Agent:x MEDIASTORE-DATA y", "User-Agent:aws-sdk-go-v2 api/mediastore#1",
		"X-Amz-User-Agent:aws-sdk-js/3.0 mediastore-data", "X-Amz-User-Agent:aws-sdk-js/3.0 MediaStoreData",
		"User-Agent:İmediastoredata",
	}

	for _, ua := range agents {
		for _, uri := range []string{"/", "/obj", "/a/b?x=1"} {
			for _, m := range []string{methodGet, methodPut, methodDelete, methodHead} {
				add(m, host, uri, "mediastore", "", ua)
				add(m, host, uri, "", "", ua)
			}
		}
	}
}

func (c *collector) addCognitoRows(add altAdder) {
	paths := []string{
		"/oauth2/token", "/oauth2/authorize?client_id=c", "/oauth2/userInfo", "/oauth2/revoke", "/oauth2/idpresponse",
		"/login?client_id=c", "/logout?client_id=c", "/saml2/idpresponse", "/saml2/logout", "/saml2/metadata",
		"/us-east-1_abc/saml2/metadata", "/us-east-1_abc/.well-known/jwks.json",
		"/us-east-1_abc/.well-known/openid-configuration", "/.well-known/jwks.json",
		"/.well-known/openid-configuration", "//oauth2/token", "/a/b/.well-known/jwks.json",
	}

	for _, uri := range paths {
		for _, m := range []string{methodGet, methodPost, methodOptions} {
			add(m, host, uri, "", "", "")
			add(m, host, uri, "cognito-idp", "", "")
			add(m, "auth.localhost:4566", uri, "", "", "")
		}
	}
}

func (c *collector) addLambdaRows(add altAdder) {
	hosts := []string{
		lambdaURLID + ".lambda-url.us-east-1.on.aws", lambdaURLID + ".lambda-url.eu-west-1.localhost:4566",
		strings.ToUpper(lambdaURLID) + ".LAMBDA-URL.us-east-1.ON.AWS", lambdaURLID + ".lambda-url.us-east-1.on.aws:443",
		lambdaURLID + ".lambda-url.us-east-1.example.com", "lambda-url.us-east-1.on.aws", "x.lambda-url..on.aws",
	}

	for _, h := range hosts {
		for _, uri := range []string{"/", "/a/b?x=1", lambdaFunctionsPath} {
			add(methodGet, h, uri, "", "", "")
			add(methodPost, h, uri, "s3", "", "")
		}
	}

	paths := []string{
		lambdaFunctionsPath, "/2016-08-19/account-settings", "/2025-12-01/durable-executions",
		"/2026-07-09/resource-policy/arn", "/2020-04-22/code-signing-configs", "/2018-10-31/layers",
		"/2017-03-31/tags/arn", "/2099-01-01/functions", "/2015-03-31/event-source-mappings",
	}

	for _, uri := range paths {
		add(methodGet, host, uri, "", "", "")
		add(methodGet, host, uri, "lambda", "", "")
		add(methodGet, host, uri, "s3", "", "")
	}
}

func (c *collector) addPathRows(add altAdder) {
	paths := []string{
		"/v1/registries", "/v1/discover", "//v1/policy", "/v1/discoverers/id/d1/start", "/v1/registries/name/r/schemas",
		"/groups", "/get-group", "/update-group-query", "/list-group-resources", "/resources/search",
		"/resources/arn:aws:resource-groups:us-east-1:000000000000:group%2Fg/tags", "/ungroup-resources",
		"/recommendations", "/personalize-ranking", "/action-recommendations", "/ssmmessages/v1/data-channel/x",
		"/v2/", "/v2", "/v2/_catalog", "/v2/repo/manifests/latest", "/v2/repo/blobs/sha256:x", "/v2/repo/tags/list",
		"/schedules", "/schedules/x", "/schedule-groups/g",
		"/tags/arn:aws:scheduler:us-east-1:000000000000:schedule%2Fx",
		"/tags/arn:aws:sns:us-east-1:000000000000:t", "/SimpleNotificationService-abc.pem",
	}

	for _, uri := range paths {
		for _, m := range []string{methodGet, methodPost, methodPut, methodDelete} {
			add(m, host, uri, "", "", "")
			add(m, host, uri, "s3", "", "")
		}
	}
}

func (c *collector) addFormRows() {
	bodies := []string{
		"Action=ListQueues&Version=2012-11-05", "Action=GetCallerIdentity&Version=2011-06-15",
		"Action=ListTopics&Version=2010-03-31", "Action=Bogus", "",
	}

	for _, q := range bodies {
		for _, ct := range []string{formCT, "APPLICATION/X-WWW-FORM-URLENCODED", "application/json", ""} {
			for _, auth := range []string{"sts", "sns", "sqs", ""} {
				c.add(row{methodPost, host, "/", auth, ct, "", q, "", noteAlt})
			}
		}
	}
}

func (c *collector) addQueryRows(add altAdder) {
	queries := []string{
		"?Action=ConfirmSubscription&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%3At&Token=x",
		"?Act%69on=ConfirmSubscription&TopicArn=x", "?Action=ListTopics", "?x=1", "?X-Amz-Credential=a%2Fb",
	}

	for _, q := range queries {
		add(methodGet, host, "/"+q, "", "", "")
		add(methodGet, host, "/bucket/key"+q, "s3", "", "")
	}
}
