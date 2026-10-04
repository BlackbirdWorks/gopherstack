package apigateway_test

import (
	"testing"
)

func BenchmarkProxyAWSProxy(b *testing.B) {
	const uri = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/" +
		"arn:aws:lambda:us-east-1:000000000000:function:f/invocations"

	h, e, apiID := setupProxyAPIViaHandler(b, "AWS_PROXY", uri)
	h.SetLambdaInvoker(&proxyMockInvoker{})

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		proxyReq(b, h, e, apiID, "/items", `{"key":"val"}`)
	}
}
