package main

import (
	"fmt"
	"strings"
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
