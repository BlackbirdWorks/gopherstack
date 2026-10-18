package pricing

import "fmt"

const (
	svcEC2         = "AmazonEC2"
	svcS3          = "AmazonS3"
	svcLambda      = "AWSLambda"
	svcDynamoDB    = "AmazonDynamoDB"
	svcRDS         = "AmazonRDS"
	mem4           = "4 GiB"
	mem16          = "16 GiB"
	volGeneral     = "General Purpose"
	osWindows      = "Windows"
	attrOperation  = "operation"
	attrUsage      = "usagetype"
	attrVolumeType = "volumeType"
	attrGroup      = "group"
	unitGBMo       = "GB-Mo"
	opInvoke       = "Invoke"
	perThousand    = 1000
	famProvisioned = "Provisioned IOPS"
	famServerless  = "Serverless"
	s3RequestSpecs = 2
	multiAZFactor  = 2
	tier2Factor    = 0.96
	tier3Factor    = 0.91
	rdsStorageUSD  = 0.115
	lambdaSpecsPer = 3
	dynamoItems    = 5

	lambdaRequestUSD = 0.0000002
	lambdaX86USD     = 0.0000166667
	lambdaARMUSD     = 0.0000133334
)

// Prices are an illustrative us-east-1 snapshot scaled by a per-region factor, not live AWS rates.

type instanceType struct {
	name, vcpu, memory string
	hourly             float64
}

const windowsFactor = 1.9

func zeroDim(unit, desc string, price float64) dim {
	return dim{unit: unit, desc: desc, begin: "0", end: infinity, usd: price}
}

func ec2Specs() []spec {
	types := []instanceType{
		{"t3.micro", "2", "1 GiB", 0.0104}, {"t3.small", "2", "2 GiB", 0.0208}, {"t3.medium", "2", mem4, 0.0416},
		{"m5.large", "2", "8 GiB", 0.096}, {"m5.xlarge", "4", mem16, 0.192},
		{"c5.large", "2", mem4, 0.085}, {"r5.large", "2", mem16, 0.126},
	}
	volumes := []struct {
		api, name string
		gbMonth   float64
	}{
		{"gp3", volGeneral, 0.08}, {"gp2", volGeneral, 0.10}, {"io1", famProvisioned, 0.125},
		{"st1", "Throughput Optimized HDD", 0.045}, {"sc1", "Cold HDD", 0.015},
	}

	out := make([]spec, 0, (2*len(types)+len(volumes))*len(regions))

	for _, r := range regions {
		for _, it := range types {
			for _, os := range []string{"Linux", osWindows} {
				price := it.hourly * r.factor
				if os == osWindows {
					price *= windowsFactor
				}

				out = append(out, spec{
					service:  svcEC2,
					family:   "Compute Instance",
					region:   r,
					reserved: true,
					attrs: map[string]string{
						"instanceType": it.name, "vcpu": it.vcpu, "memory": it.memory, "operatingSystem": os,
						"tenancy": "Shared", "preInstalledSw": "NA", "capacitystatus": "Used",
						attrOperation: map[string]string{"Linux": "RunInstances", osWindows: "RunInstances:0002"}[os],
						attrUsage:     fmt.Sprintf("BoxUsage:%s", it.name), "instanceFamily": "General purpose",
					},
					dims: []dim{
						zeroDim(
							"Hrs",
							fmt.Sprintf("$%.4f per On Demand %s %s Instance Hour", price, os, it.name),
							price,
						),
					},
				})
			}
		}

		for _, v := range volumes {
			price := v.gbMonth * r.factor
			out = append(out, spec{
				service: svcEC2,
				family:  "Storage",
				region:  r,
				attrs: map[string]string{
					"volumeApiName": v.api, attrVolumeType: v.name, "storageMedia": "SSD-backed",
					attrUsage: "EBS:VolumeUsage." + v.api, attrOperation: "",
				},
				dims: []dim{
					zeroDim(unitGBMo, fmt.Sprintf("$%.3f per GB-month of %s provisioned storage", price, v.api), price),
				},
			})
		}
	}

	return out
}

func s3Specs() []spec {
	classes := []struct {
		volume, usage string
		gbMonth       float64
	}{
		{
			"Standard",
			"TimedStorage-ByteHrs",
			0.023,
		},
		{"Standard - Infrequent Access", "TimedStorage-SIA-ByteHrs", 0.0125},
		{"One Zone - Infrequent Access", "TimedStorage-ZIA-ByteHrs", 0.01},
		{"Glacier Flexible Retrieval", "TimedStorage-GlacierByteHrs", 0.0036},
		{"Intelligent-Tiering Frequent Access", "TimedStorage-INT-FA-ByteHrs", 0.023},
	}

	out := make([]spec, 0, (len(classes)+s3RequestSpecs)*len(regions))

	for _, r := range regions {
		for _, c := range classes {
			price := c.gbMonth * r.factor
			dims := []dim{
				zeroDim(
					unitGBMo,
					fmt.Sprintf("$%.4f per GB - first 50 TB / month of %s storage", price, c.volume),
					price,
				),
			}

			if c.volume == "Standard" {
				dims = []dim{
					{
						unit:  unitGBMo,
						desc:  fmt.Sprintf("$%.3f per GB - first 50 TB / month of storage used", price),
						begin: "0",
						end:   "51200",
						usd:   price,
					},
					{
						unit:  unitGBMo,
						desc:  fmt.Sprintf("$%.3f per GB - next 450 TB / month of storage used", price*tier2Factor),
						begin: "51200",
						end:   "512000",
						usd:   price * tier2Factor,
					},
					{
						unit:  unitGBMo,
						desc:  fmt.Sprintf("$%.3f per GB - storage used / month over 500 TB", price*tier3Factor),
						begin: "512000",
						end:   infinity,
						usd:   price * tier3Factor,
					},
				}
			}

			out = append(out, spec{
				service: svcS3, family: "Storage", region: r,
				attrs: map[string]string{
					"storageClass": c.volume, attrVolumeType: c.volume, attrUsage: c.usage, attrOperation: "",
					"durability": "99.999999999%", "availability": "99.9%",
				},
				dims: dims,
			})
		}

		for _, q := range []struct {
			group, op string
			perK      float64
		}{
			{"S3-API-Tier1", "PutObject", 0.005}, {"S3-API-Tier2", "GetObject", 0.0004},
		} {
			price := q.perK / perThousand * r.factor
			out = append(out, spec{
				service: svcS3, family: "API Request", region: r,
				attrs: map[string]string{attrGroup: q.group, attrOperation: q.op, attrUsage: "Requests-" + q.group[7:]},
				dims:  []dim{zeroDim("Requests", fmt.Sprintf("$%.7f per request - %s", price, q.group), price)},
			})
		}
	}

	return out
}

func lambdaSpecs() []spec {
	out := make([]spec, 0, lambdaSpecsPer*len(regions))

	for _, r := range regions {
		out = append(out,
			spec{
				service: svcLambda,
				family:  famServerless,
				region:  r,
				attrs: map[string]string{
					attrGroup:     "AWS-Lambda-Requests",
					attrUsage:     "Request",
					attrOperation: opInvoke,
				},
				dims: []dim{zeroDim("Requests", "AWS Lambda - Total Requests", lambdaRequestUSD*r.factor)},
			},
			spec{
				service: svcLambda,
				family:  famServerless,
				region:  r,
				attrs: map[string]string{
					attrGroup:      "AWS-Lambda-Duration",
					attrUsage:      "Lambda-GB-Second",
					attrOperation:  opInvoke,
					"architecture": "x86_64",
				},
				dims: []dim{zeroDim("Lambda-GB-Second", "AWS Lambda - Total Compute - x86", lambdaX86USD*r.factor)},
			},
			spec{
				service: svcLambda,
				family:  famServerless,
				region:  r,
				attrs: map[string]string{
					attrGroup:      "AWS-Lambda-Duration-ARM",
					attrUsage:      "Lambda-GB-Second-ARM",
					attrOperation:  opInvoke,
					"architecture": "arm64",
				},
				dims: []dim{zeroDim("Lambda-GB-Second", "AWS Lambda - Total Compute - ARM", lambdaARMUSD*r.factor)},
			},
		)
	}

	return out
}

func dynamoSpecs() []spec {
	out := make([]spec, 0, len(regions)*dynamoItems)

	for _, r := range regions {
		items := []struct {
			family, group, usage, unit, desc string
			price                            float64
		}{
			{
				"Amazon DynamoDB PayPerRequest Throughput",
				"DDB-WriteUnits",
				"WriteRequestUnits",
				"WriteRequestUnits",
				"per million write request units",
				0.00000125,
			},
			{
				"Amazon DynamoDB PayPerRequest Throughput",
				"DDB-ReadUnits",
				"ReadRequestUnits",
				"ReadRequestUnits",
				"per million read request units",
				0.00000025,
			},
			{
				famProvisioned,
				"DDB-WriteUnits",
				"WriteCapacityUnit-Hrs",
				"WriteCapacityUnit-Hrs",
				"per WCU-hour (provisioned)",
				0.00065,
			},
			{
				famProvisioned,
				"DDB-ReadUnits",
				"ReadCapacityUnit-Hrs",
				"ReadCapacityUnit-Hrs",
				"per RCU-hour (provisioned)",
				0.00013,
			},
			{"Database Storage", "DDB-StorageUsage", "TimedStorage-ByteHrs", unitGBMo, "per GB-month of storage", 0.25},
		}
		for _, it := range items {
			price := it.price * r.factor
			out = append(out, spec{
				service: svcDynamoDB,
				family:  it.family,
				region:  r,
				attrs: map[string]string{
					attrGroup:     it.group,
					attrUsage:     it.usage,
					attrOperation: "PayPerRequestThroughput",
				},
				dims: []dim{zeroDim(it.unit, fmt.Sprintf("$%.8f %s", price, it.desc), price)},
			})
		}
	}

	return out
}

func rdsSpecs() []spec {
	classes := []struct {
		name, vcpu, memory string
		hourly             float64
	}{
		{"db.t3.micro", "2", "1 GiB", 0.017}, {"db.t3.medium", "2", mem4, 0.068},
		{"db.m5.large", "2", "8 GiB", 0.171}, {"db.r5.large", "2", mem16, 0.24},
	}

	out := make([]spec, 0, (4*len(classes)+1)*len(regions))

	for _, r := range regions {
		for _, c := range classes {
			for _, engine := range []string{"MySQL", "PostgreSQL"} {
				for _, deploy := range []string{"Single-AZ", "Multi-AZ"} {
					price := c.hourly * r.factor
					if deploy == "Multi-AZ" {
						price *= multiAZFactor
					}

					out = append(out, spec{
						service:  svcRDS,
						family:   "Database Instance",
						region:   r,
						reserved: true,
						attrs: map[string]string{
							"instanceType": c.name, "vcpu": c.vcpu, "memory": c.memory, "databaseEngine": engine,
							"deploymentOption": deploy, attrUsage: "InstanceUsage:" + c.name,
							attrOperation: "CreateDBInstance:0002",
						},
						dims: []dim{
							zeroDim(
								"Hrs",
								fmt.Sprintf(
									"$%.3f per RDS %s %s %s Instance Hour (or partial hour)",
									price,
									c.name,
									deploy,
									engine,
								),
								price,
							),
						},
					})
				}
			}
		}

		out = append(out, spec{
			service: svcRDS,
			family:  "Database Storage",
			region:  r,
			attrs:   map[string]string{attrVolumeType: volGeneral, attrUsage: "RDS:GP2-Storage", attrOperation: ""},
			dims: []dim{
				zeroDim(
					unitGBMo,
					fmt.Sprintf("$%.3f per GB-month of General Purpose storage", rdsStorageUSD*r.factor),
					rdsStorageUSD*r.factor,
				),
			},
		})
	}

	return out
}
