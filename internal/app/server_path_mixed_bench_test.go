package app

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

type mixedCase struct {
	name, method, path, signSvc, ctype, target, body string
}

func mixedCases() []mixedCase {
	const (
		json10 = "application/x-amz-json-1.0"
		form   = "application/x-www-form-urlencoded"
	)

	return []mixedCase{
		{"ddb_getitem", "POST", "/", "dynamodb", json10, "DynamoDB_20120810.GetItem",
			`{"TableName":"bench-server-table","Key":{"id":{"S":"k"}}}`},
		{"sts_query", "POST", "/", "sts", form, "", "Action=GetCallerIdentity&Version=2011-06-15"},
		{"sns_query", "POST", "/", "sns", form, "", "Action=ListTopics&Version=2010-03-31"},
		{"sqs_query", "POST", "/", "sqs", form, "", "Action=ListQueues&Version=2012-11-05"},
		{"ec2_describe_instances", "POST", "/", "ec2", form, "", "Action=DescribeInstances&Version=2016-11-15"},
		{"s3_getobject", "GET", "/bench-server-bucket/obj.txt", "s3", "", "", ""},
		{"apigw_restjson", "GET", "/restapis", "apigateway", "", "", ""},
	}
}

func (tc mixedCase) newRequest(tb testing.TB) *http.Request {
	tb.Helper()

	req, err := http.NewRequestWithContext(
		tb.Context(), tc.method, benchEndpoint+tc.path, strings.NewReader(tc.body),
	)
	if err != nil {
		tb.Fatal(err)
	}

	req.Header.Set("Authorization", strings.Replace(benchAuth, "%s", tc.signSvc, 1))
	req.Header.Set("X-Amz-Date", "20260101T000000Z")

	if tc.ctype != "" {
		req.Header.Set("Content-Type", tc.ctype)
	}

	if tc.target != "" {
		req.Header.Set("X-Amz-Target", tc.target)
	}

	return req
}

// BenchmarkServerPathMixed round-robins requests across many services so
// routing cost across the whole matcher set is visible.
func BenchmarkServerPathMixed(b *testing.B) {
	e, byName := benchServer(b)
	serverFixtures(b, byName)

	cases := mixedCases()

	b.ReportAllocs()

	for i := range b.N {
		w := &discardWriter{h: make(http.Header)}
		e.ServeHTTP(w, cases[i%len(cases)].newRequest(b))

		if w.code >= http.StatusInternalServerError {
			b.Fatalf("status %d", w.code)
		}
	}
}

// BenchmarkServerPathMixedParallel is the concurrent variant of BenchmarkServerPathMixed.
func BenchmarkServerPathMixedParallel(b *testing.B) {
	e, byName := benchServer(b)
	serverFixtures(b, byName)

	cases := mixedCases()

	var next atomic.Uint64

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := next.Add(1)
			w := &discardWriter{h: make(http.Header)}
			e.ServeHTTP(w, cases[i%uint64(len(cases))].newRequest(b))

			if w.code >= http.StatusInternalServerError {
				b.Errorf("status %d", w.code)

				return
			}
		}
	})
}
