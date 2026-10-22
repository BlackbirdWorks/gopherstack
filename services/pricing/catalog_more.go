package pricing

import "fmt"

const (
	svcSQS         = "AmazonSQS"
	svcSNS         = "AmazonSNS"
	svcCloudFront  = "AmazonCloudFront"
	svcElastiCache = "AmazonElastiCache"
	svcECS         = "AmazonECS"

	perMillion        = 1_000_000
	famDataTransfer   = "Data Transfer"
	famAPIRequest     = "API Request"
	famFargate        = "Compute"
	sqsStandardUSD    = 0.0000004
	sqsFIFOUSD        = 0.0000005
	cloudFrontGBUSD   = 0.085
	fargateVCPUUSD    = 0.04048
	fargateGBUSD      = 0.004445
	cacheEngineRedis  = "Redis"
	cacheEngineMemcch = "Memcached"
)

// simpleSpec builds a single-dimension, single-attribute-set spec.
func simpleSpec(svc, family string, r region, attrs map[string]string, unit, desc string, price float64) spec {
	return spec{
		service: svc, family: family, region: r, attrs: attrs,
		dims: []dim{zeroDim(unit, desc, price)},
	}
}

func sqsSpecs() []spec {
	out := make([]spec, 0, len(regions))

	for _, r := range regions {
		for _, q := range []struct{ kind, usage string }{{storageStandard, "Requests"}, {"FIFO", "Requests-FIFO"}} {
			price := sqsStandardUSD * r.factor
			if q.kind == "FIFO" {
				price = sqsFIFOUSD * r.factor
			}

			out = append(out, simpleSpec(svcSQS, famAPIRequest, r,
				map[string]string{"queueType": q.kind, attrUsage: q.usage, attrOperation: ""},
				"Requests", fmt.Sprintf("$%.7f per %s queue request", price, q.kind), price))
		}
	}

	return out
}

func snsSpecs() []spec {
	out := make([]spec, 0, len(regions))

	for _, r := range regions {
		items := []struct {
			family, usage, unit, desc string
			price                     float64
		}{
			{famAPIRequest, "Requests-Tier1", "Requests", "per publish request", 0.5 / perMillion},
			{"Message Delivery", "DeliveryAttempts-HTTP", "Notifications", "per HTTP/S notification", 0.6 / perMillion},
			{"Message Delivery", "DeliveryAttempts-SQS", "Notifications", "per SQS notification", 0},
		}
		for _, it := range items {
			price := it.price * r.factor
			out = append(out, simpleSpec(svcSNS, it.family, r,
				map[string]string{attrUsage: it.usage, attrOperation: ""},
				it.unit, fmt.Sprintf("$%.8f %s", price, it.desc), price))
		}
	}

	return out
}

func cloudFrontSpecs() []spec {
	out := make([]spec, 0, len(regions))

	for _, r := range regions {
		out = append(out,
			simpleSpec(
				svcCloudFront,
				famDataTransfer,
				r,
				map[string]string{
					attrUsage:      "DataTransfer-Out-Bytes",
					attrOperation:  "",
					"transferType": "CloudFront to Internet",
				},
				"GB",
				fmt.Sprintf("$%.4f per GB - first 10 TB / month data transfer out", cloudFrontGBUSD*r.factor),
				cloudFrontGBUSD*r.factor,
			),
			simpleSpec(
				svcCloudFront,
				famAPIRequest,
				r,
				map[string]string{attrUsage: "Requests-Tier2-HTTPS", attrOperation: "HTTPS"},
				"Requests",
				fmt.Sprintf("$%.8f per 10,000 HTTPS requests", 0.01/perThousand*r.factor),
				0.01/perThousand*r.factor,
			),
		)
	}

	return out
}

func elastiCacheSpecs() []spec {
	nodes := []struct {
		name, vcpu, memory string
		hourly             float64
	}{
		{"cache.t3.micro", "2", "0.5 GiB", 0.017}, {"cache.t3.medium", "2", "3.09 GiB", 0.068},
		{"cache.m5.large", "2", "6.38 GiB", 0.156}, {"cache.r5.large", "2", "13.07 GiB", 0.216},
	}
	out := make([]spec, 0, 2*len(nodes)*len(regions))

	for _, r := range regions {
		for _, n := range nodes {
			for _, engine := range []string{cacheEngineRedis, cacheEngineMemcch} {
				price := n.hourly * r.factor
				out = append(out, simpleSpec(svcElastiCache, "Cache Instance", r, map[string]string{
					attrInstanceType: n.name, attrVCPU: n.vcpu, attrMemory: n.memory, "cacheEngine": engine,
					attrUsage: "NodeUsage:" + n.name, attrOperation: "CreateCacheCluster",
				}, "Hrs", fmt.Sprintf("$%.3f per %s %s node hour", price, n.name, engine), price))
			}
		}
	}

	return out
}

func ecsSpecs() []spec {
	out := make([]spec, 0, len(regions))

	for _, r := range regions {
		vcpu, gb := fargateVCPUUSD*r.factor, fargateGBUSD*r.factor
		out = append(out,
			simpleSpec(
				svcECS,
				famFargate,
				r,
				map[string]string{
					"launchType":  "Fargate",
					attrUsage:     "Fargate-vCPU-Hours:perCPU",
					attrOperation: "FargateTask",
				},
				"vCPU-Hours",
				fmt.Sprintf("$%.5f per vCPU per hour (Fargate)", vcpu),
				vcpu,
			),
			simpleSpec(svcECS, famFargate, r,
				map[string]string{"launchType": "Fargate", attrUsage: "Fargate-GB-Hours", attrOperation: "FargateTask"},
				"GB-Hours", fmt.Sprintf("$%.6f per GB per hour (Fargate)", gb), gb),
		)
	}

	return out
}
