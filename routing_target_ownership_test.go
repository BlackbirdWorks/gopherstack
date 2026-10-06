package main

import (
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestRoutingTargetOwnership(t *testing.T) {
	t.Parallel()

	reg, _ := routingFixture(t)
	router := service.NewServiceRouter(reg).WithTargetGates(routeTargetGates()).WithPathGates(routePathGates())

	const (
		local     = "localhost:4566"
		queryHost = "query.timestream.us-east-1.amazonaws.com"
		json10    = "application/x-amz-json-1.0"
		json11    = "application/x-amz-json-1.1"
		tsTarget  = "Timestream_20181101.DescribeEndpoints"
		tsQuery   = "TimestreamQuery"
		cwTarget  = "GraniteServiceVersion20100801.ListMetrics"
		tsWrite   = "TimestreamWrite"
		kinesisUA = "User-Agent:aws-sdk-go-v2/1.41.0 api/kinesis#1.0.0"
		queryUA   = "User-Agent:aws-sdk-go-v2/1.41.0 api/timestreamquery#1.0.0"
		writeUA   = "User-Agent:aws-sdk-go-v2/1.41.0 api/timestreamwrite#1.0.0"
	)

	type tc struct{ name, host, authSvc, ctype, target, headers, want string }

	kin := func(op string) tc {
		return tc{"kinesis_" + op, local, "kinesis", json11, "Kinesis_20131202." + op, kinesisUA, "Kinesis"}
	}

	tests := []tc{
		kin("TagResource"),
		kin("UntagResource"),
		kin("ListTagsForResource"),
		{"describe_endpoints_query_sdk", local, "timestream", json10, tsTarget, queryUA, tsQuery},
		{"describe_endpoints_query_host", queryHost, "timestream", json10, tsTarget, "", tsQuery},
		{"describe_endpoints_write_sdk", local, "timestream", json10, tsTarget, writeUA, tsWrite},
		{"describe_endpoints_unmarked", local, "", json10, tsTarget, "", tsWrite},
		{"cloudwatch_json_target", local, "monitoring", json10, cwTarget, "", "CloudWatch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := routingCase{
				method: "POST", host: tt.host, uri: "/", authSvc: tt.authSvc, ctype: tt.ctype,
				target: tt.target, body: "{}", headers: tt.headers,
			}

			for range 3 {
				c := echo.NewContext(rc.request(t), httptest.NewRecorder())
				assert.Equal(t, tt.want, entryName(router.Lookup(c)))
				assert.Equal(t, tt.want, scanSelect(routingEntries, c))
			}
		})
	}
}
