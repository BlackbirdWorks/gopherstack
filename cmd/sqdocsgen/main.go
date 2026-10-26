// Command sqdocsgen regenerates services/servicequotas/docs_quotas.json.gz from the AWS General Reference
// "endpoints and quotas" pages. Only adjustable quotas are emitted: the docs show a quota code solely
// through the Service Quotas console link on adjustable rows.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const fetchTimeout = time.Minute

var errFetch = errors.New("fetch failed")

const base = "https://docs.aws.amazon.com/general/latest/gr/"

// pages maps a General Reference page to the Service Quotas service codes taken from it.
// An empty name means "use the page title".
var pages = map[string]map[string]string{ //nolint:gochecknoglobals // generator input table
	"access-analyzer": {
		"access-analyzer": "",
	},
	"acm":                  {"acm": ""},
	"pca":                  {"acm-pca": "AWS Private Certificate Authority"},
	"amplify":              {"amplify": ""},
	"apigateway":           {"apigateway": ""},
	"appconfig":            {"appconfig": ""},
	"appmesh":              {"appmesh": ""},
	"apprunner":            {"apprunner": ""},
	"appsync":              {"appsync": ""},
	"as-app":               {"application-autoscaling": ""},
	"as":                   {"autoscaling": ""},
	"athena":               {"athena": ""},
	"bk":                   {"backup": ""},
	"bedrock":              {"bedrock": ""},
	"billing":              {"ce": "AWS Cost Explorer"},
	"cfn":                  {"cloudformation": ""},
	"cf_region":            {"cloudfront": ""},
	"clean-rooms":          {"cleanrooms": ""},
	"cloudhsm":             {"cloudhsm": ""},
	"cloud_map":            {"AWSCloudMap": ""},
	"codeartifact":         {"codeartifact": ""},
	"codebuild":            {"codebuild": ""},
	"codecommit":           {"codecommit": ""},
	"codedeploy":           {"codedeploy": ""},
	"codeguru-reviewer":    {"codeguru-reviewer": ""},
	"codepipeline":         {"codepipeline": ""},
	"cognito":              {"cognito-identity": "Amazon Cognito Identity", "cognito-idp": "Amazon Cognito User Pools"},
	"comprehend":           {"comprehend": ""},
	"connect_region":       {"connect": ""},
	"cw_region":            {"monitoring": ""},
	"cwe_region":           {"events": ""},
	"cwl_region":           {"logs": ""},
	"cwnm_region":          {"networkmonitor": ""},
	"cw_rum_region":        {"rum": ""},
	"ddb":                  {"dynamodb": "Amazon DynamoDB", "dax": "Amazon DynamoDB Accelerator (DAX)"},
	"dlm":                  {"dlm": ""},
	"dms":                  {"dms": ""},
	"dsql":                 {"dsql": ""},
	"ds_region":            {"ds": ""},
	"ecr":                  {"ecr": ""},
	"ecr-public":           {"ecr-public": ""},
	"ecs-service":          {"ecs": "Amazon Elastic Container Service"},
	"eks":                  {"eks": ""},
	"elasticache-service":  {"elasticache": ""},
	"elasticbeanstalk":     {"elasticbeanstalk": ""},
	"elasticfilesystem":    {"elasticfilesystem": ""},
	"elb":                  {"elasticloadbalancing": ""},
	"emr":                  {"elasticmapreduce": ""},
	"eventbridgescheduler": {"scheduler": ""},
	"fh":                   {"firehose": ""},
	"forecast":             {"forecast": ""},
	"fsxn":                 {"fsx": ""},
	"global_accelerator":   {"globalaccelerator": ""},
	"gluedatabrew":         {"databrew": ""},
	"glue":                 {"glue": ""},
	"grafana-service":      {"grafana": ""},
	"iam-service":          {"iam": ""},
	"iot-core":             {"iotcore": "AWS IoT Core"},
	"iot-lorawan":          {"iotwireless": ""},
	"ak":                   {"kinesis": "Amazon Kinesis Data Streams"},
	"akv":                  {"kinesisvideo": ""},
	"ka":                   {"kinesisanalytics": ""},
	"kms":                  {"kms": ""},
	"lake-formation":       {"lakeformation": ""},
	"lambda-service":       {"lambda": ""},
	"lightsail":            {"lightsail": ""},
	"macie":                {"macie2": ""},
	"managedblockchain":    {"managedblockchain": ""},
	"mediaconvert":         {"mediaconvert": ""},
	"medialive_region":     {"medialive": ""},
	"mediapackage":         {"mediapackage": ""},
	"mediatailor":          {"mediatailor": ""},
	"mgn":                  {"mgn": ""},
	"msk":                  {"kafka": ""},
	"amazon-mq":            {"mq": ""},
	"mwaa":                 {"airflow": ""},
	"neptune":              {"neptune": ""},
	"network-firewall":     {"network-firewall": ""},
	"amazondocdb":          {"docdb": ""},
	"healthomics-quotas":   {"omics": "AWS HealthOmics"},
	"opensearch-service":   {"es": ""},
	"ao":                   {"organizations": ""},
	"outposts_region":      {"outposts": ""},
	"personalize":          {"personalize": ""},
	"pinpoint":             {"pinpoint": ""},
	"pol":                  {"polly": ""},
	"prometheus-service":   {"aps": ""},
	"quicksight": {
		"quicksight": "",
	},
	"r53":                 {"route53": "Amazon Route 53", "route53resolver": "Amazon Route 53 Resolver"},
	"ram":                 {"ram": ""},
	"rds-service":         {"rds": ""},
	"resiliencehub":       {"resiliencehub": ""},
	"arg":                 {"resource-groups": ""},
	"s3":                  {"s3": ""},
	"sagemaker":           {"sagemaker": ""},
	"serverlessrepo":      {"serverlessrepo": ""},
	"servicecatalog":      {"servicecatalog": ""},
	"ses":                 {"ses": ""},
	"shield":              {"shield": ""},
	"sso":                 {"sso": ""},
	"step-functions":      {"states": ""},
	"swf-service":         {"swf": ""},
	"transcribe":          {"transcribe": ""},
	"transfer-service":    {"transfer": ""},
	"translate-service":   {"translate": ""},
	"verifiedpermissions": {"verifiedpermissions": ""},
	"vpc-lattice-service": {"vpc-lattice": ""},
	"vpc-service":         {"vpc": ""},
	"waf":                 {"wafv2": ""},
	"xray":                {"xray": ""},
	"sqs-service":         {"sqs": ""},
	"sns":                 {"sns": ""},
	"ec2-service":         {"ec2": ""},
}

func fetch(url string) (string, error) {
	c := httpclient.New(fetchTimeout)

	resp, err := c.Get(url) //nolint:noctx // one-shot generator
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%w: %s: %s", errFetch, url, resp.Status)
	}

	b, err := io.ReadAll(resp.Body)

	return string(b), err
}

func main() {
	out := flag.String("out", "services/servicequotas/docs_quotas.json.gz", "output file")
	flag.Parse()

	var svcs []service

	slugs := make([]string, 0, len(pages))
	for slug := range pages {
		slugs = append(slugs, slug)
	}

	slices.Sort(slugs)

	for _, slug := range slugs {
		url := base + slug + ".html"

		page, err := fetch(url)
		if err != nil {
			fmt.Fprintln(os.Stderr, "skip:", err)

			continue
		}

		found := parseQuotas(page)

		for code, name := range pages[slug] {
			qs := found[code]
			if len(qs) == 0 {
				continue
			}

			if name == "" {
				name = pageTitle(page)
			}

			slices.SortFunc(qs, func(a, b quota) int { return strings.Compare(a.Code, b.Code) })
			svcs = append(svcs, service{Code: code, Name: name, Source: url, Quotas: qs})
		}
	}

	slices.SortFunc(svcs, func(a, b service) int { return strings.Compare(a.Code, b.Code) })

	b, err := json.MarshalIndent(svcs, "", " ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var buf bytes.Buffer

	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	_, _ = zw.Write(append(b, '\n'))
	_ = zw.Close()

	if err = os.WriteFile(*out, buf.Bytes(), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	n := 0
	for _, s := range svcs {
		n += len(s.Quotas)
	}

	fmt.Fprintf(os.Stdout, "%d services, %d quotas\n", len(svcs), n)
}
