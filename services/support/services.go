package support

const (
	categoryPerformanceLow     = "performance"
	categoryGeneralGuidanceLow = "general-guidance"
	categoryPerformance        = "Performance"
	categoryGeneralGuidance    = "General Guidance"
)

// DescribeServices returns AWS services, optionally filtered by service codes.
func (b *InMemoryBackend) DescribeServices(serviceCodeList []string, language string) []Service {
	all := staticServices(language)
	if len(serviceCodeList) == 0 {
		return all
	}

	filter := make(map[string]bool, len(serviceCodeList))
	for _, c := range serviceCodeList {
		filter[c] = true
	}

	out := make([]Service, 0, len(all))
	for _, svc := range all {
		if filter[svc.Code] {
			out = append(out, svc)
		}
	}

	return out
}

// staticServices returns a small static list of common AWS services.
func staticServices(language string) []Service {
	all := []Service{
		{
			Code: "amazon-s3",
			Name: "Amazon Simple Storage Service (Amazon S3)",
			Categories: []ServiceCategory{
				{Code: "data-management", Name: "Data Management"},
				{Code: categoryPerformanceLow, Name: categoryPerformance},
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
			},
		},
		{
			Code: "amazon-ec2",
			Name: "Amazon Elastic Compute Cloud (Amazon EC2)",
			Categories: []ServiceCategory{
				{Code: "instance-issue", Name: "Instance Issue"},
				{Code: categoryPerformanceLow, Name: categoryPerformance},
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
			},
		},
		{
			Code: "amazon-dynamodb",
			Name: "Amazon DynamoDB",
			Categories: []ServiceCategory{
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
				{Code: categoryPerformanceLow, Name: categoryPerformance},
			},
		},
		{
			Code: "amazon-rds",
			Name: "Amazon Relational Database Service (Amazon RDS)",
			Categories: []ServiceCategory{
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
				{Code: "connectivity", Name: "Connectivity"},
			},
		},
		{
			Code: "amazon-cloudfront",
			Name: "Amazon CloudFront",
			Categories: []ServiceCategory{
				{Code: categoryGeneralGuidanceLow, Name: categoryGeneralGuidance},
				{Code: categoryPerformanceLow, Name: categoryPerformance},
			},
		},
	}
	if language == "ja" {
		all[0].Name = "Amazon Simple Storage Service (Amazon S3)"
		all[0].Categories[2].Name = japaneseGeneralGuidance
		all[1].Categories[2].Name = japaneseGeneralGuidance
	}

	return all
}
