// Command sfnschemagen writes services/stepfunctions/sdkwire/registry_gen.go,
// the table of AWS SDK operation schemas the Step Functions SDK integration
// serves without linking the SDK service clients.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"go/format"
	"log"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire/ir"
)

const (
	sdkModule    = "github.com/aws/aws-sdk-go-v2/service/"
	outFile      = "services/stepfunctions/sdkwire/registry_gen.go"
	legacyDir    = "services/stepfunctions/sdkwire/legacy"
	dispatchFile = "services/stepfunctions/sdk_dispatch_gen.go"
	clientsFile  = "services/stepfunctions/sdkwire/clients_whitebox_test.go"

	protoQuery    = "query"
	protoEC2      = "ec2query"
	protoJSON11   = "awsjson11"
	protoRestJSON = "restjson1"
	protoRestXML  = "restxml"

	prefixQuery    = "awsAwsquery_"
	prefixEC2      = "awsEc2query_"
	prefixJSON11   = "awsAwsjson11_"
	prefixRestJSON = "awsRestjson1_"
	prefixRestXML  = "awsRestxml_"

	fileMode = 0o600
)

func fatalf(format string, args ...any) {
	log.Fatalf(format, args...)
}

func warnf(format string, args ...any) {
	log.Printf(format, args...)
}

func serviceKeys() []string {
	return []string{
		"acm", "amplify", "apigateway", "apigatewayv2", "appconfig", "appsync", "athena", "autoscaling", "batch",
		"bedrock", "bedrockruntime", "cloudformation", "cloudwatch", "cloudwatchlogs", "codebuild", "codedeploy",
		"codepipeline", "cognitoidentityprovider", "comprehend", "dynamodb", "ec2", "ecr", "ecs", "efs", "eks",
		"elasticache", "elasticloadbalancingv2", "emr", "eventbridge", "firehose", "glue", "iam", "iot", "kafka",
		"kinesis", "kms", "lambda", "mq", "opensearch", "organizations", "pipes", "rds", "redshift", "redshiftdata",
		"rekognition", "route53", "s3", "sagemaker", "scheduler", "secretsmanager", "ses", "sesv2", "sfn", "sns",
		"sqs", "ssm", "sts", "textract", "transcribe", "translate",
	}
}

var (
	reProtocol = regexp.MustCompile(`options\.Protocol = (\w+)\.(New\w*)\(schemas\.(\w+)\)`)
	reSigning  = regexp.MustCompile(`SetSigV4SigningName\(&props, "([^"]+)"\)`)
	reOpSchema = regexp.MustCompile(
		`smithy\.NewOperationSchema\(schemas\.(\w+), (schemas\.\w+|nil), (schemas\.\w+|nil)\)`,
	)
	reToken  = regexp.MustCompile(`if input\.(\w+) == nil \{\s+t, err := m\.tokenProvider\.GetIdempotencyToken`)
	reImport = regexp.MustCompile(`(?m)^\s*(?:(\w+) )?"(github\.com/aws/smithy-go/transport/http/protocol/\w+)"`)
)

type op struct {
	name, in, out, token string
}

type svc struct {
	key, pkg, protoAlias, protoCtor, serviceSchema, signing, version string
	ops                                                              []op
	acceptJSON                                                       bool
}

func main() {
	var out []svc

	protoImports := map[string]string{}

	for _, key := range serviceKeys() {
		dir := moduleDir(key)
		if _, err := os.Stat(filepath.Join(dir, "schemas")); err != nil {
			continue
		}

		s, imports := scan(key, dir)
		s.version = moduleVersion(key)
		out = append(out, s)

		maps.Copy(protoImports, imports)
	}

	legacy := writeLegacyModels()

	write(out, protoImports, legacy)
	writeDispatch()
	writeClientsTest(out, legacy)
}

type legacySvc struct {
	key, signing, userAgentAPI, ctor string
}

// legacyServices are served from models extracted from their pinned
// (pre-schema) SDK clients.
func legacyServices() map[string]legacyProtocol {
	query := legacyProtocol{protoQuery, prefixQuery, "queryProtocol"}
	json11 := legacyProtocol{protoJSON11, prefixJSON11, "awsjson11Protocol"}
	restJSON := legacyProtocol{protoRestJSON, prefixRestJSON, "restjson1Protocol"}

	return map[string]legacyProtocol{
		"autoscaling": query, "cloudformation": query, "elasticache": query, "elasticloadbalancingv2": query,
		"iam": query, "rds": query, "redshift": query, "ses": query, "sns": query, "sts": query,
		"ssm": json11, "redshiftdata": json11, "rekognition": json11,
		"ec2":       {protoEC2, prefixEC2, "ec2queryProtocol"},
		"scheduler": restJSON, "efs": restJSON,
		"route53": {protoRestXML, prefixRestXML, "restxmlProtocol"},
	}
}

func writeLegacyModels() []legacySvc {
	var out []legacySvc

	for _, key := range serviceKeys() {
		proto, ok := legacyServices()[key]
		if !ok {
			continue
		}

		dir := moduleDir(key)
		if _, err := os.Stat(filepath.Join(dir, "schemas")); err == nil {
			log.Fatalf("%s now has schemas; drop it from legacyServices", key)
		}

		model := extract(dir, proto.name, proto.prefix, protocolVersion(dir, proto))
		model.Target = jsonTarget(dir, proto)
		writeModel(key, model)

		signing := ""
		if sm := reSigning.FindStringSubmatch(mustRead(filepath.Join(dir, "auth.go"))); sm != nil {
			signing = sm[1]
		}

		out = append(out, legacySvc{
			key: key, signing: signing, ctor: proto.ctor,
			userAgentAPI: "api/" + key + "#" + strings.TrimPrefix(moduleVersion(key), "v"),
		})
	}

	return out
}

var reJSONTarget = regexp.MustCompile(`SetHeader\("X-Amz-Target"\)\.String\("([A-Za-z0-9_]+)\.`)

func jsonTarget(dir string, proto legacyProtocol) string {
	if !strings.HasPrefix(proto.name, "awsjson") {
		return ""
	}

	m := reJSONTarget.FindStringSubmatch(mustRead(filepath.Join(dir, "serializers.go")))
	if m == nil {
		log.Fatalf("%s: no X-Amz-Target", dir)
	}

	return m[1]
}

func protocolVersion(dir string, proto legacyProtocol) string {
	if strings.HasPrefix(proto.name, "awsjson") || strings.HasPrefix(proto.name, "rest") {
		return ""
	}

	return queryVersion(dir)
}

var reQueryVersion = regexp.MustCompile(`Key\("Version"\)\.String\("([^"]+)"\)`)

func queryVersion(dir string) string {
	m := reQueryVersion.FindStringSubmatch(mustRead(filepath.Join(dir, "serializers.go")))
	if m == nil {
		log.Fatalf("%s: no query Version", dir)
	}

	return m[1]
}

func writeModel(key string, model *ir.Model) {
	raw, err := json.Marshal(model)
	if err != nil {
		log.Fatal(err)
	}

	var buf bytes.Buffer

	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	_, _ = zw.Write(raw)
	_ = zw.Close()

	if werr := os.WriteFile(filepath.Join(legacyDir, key+".json.gz"), buf.Bytes(), fileMode); werr != nil {
		log.Fatal(werr)
	}
}

func moduleDir(key string) string {
	b, err := exec.CommandContext( //nolint:gosec // fixed go tool; module path from the generator's own table
		context.Background(), "go", "list", "-m", "-f", "{{.Dir}}", sdkModule+key).Output()
	if err != nil {
		log.Fatalf("go list %s: %v", key, err)
	}

	return strings.TrimSpace(string(b))
}

func moduleVersion(key string) string {
	b, err := exec.CommandContext( //nolint:gosec // fixed go tool; module path from the generator's own table
		context.Background(), "go", "list", "-m", "-f", "{{.Version}}", sdkModule+key).Output()
	if err != nil {
		log.Fatalf("go list %s: %v", key, err)
	}

	return strings.TrimSpace(string(b))
}

func scan(key, dir string) (svc, map[string]string) {
	client := mustRead(filepath.Join(dir, "api_client.go"))

	m := reProtocol.FindStringSubmatch(client)
	if m == nil {
		log.Fatalf("%s: no protocol", key)
	}

	imports := map[string]string{}

	for _, im := range reImport.FindAllStringSubmatch(client, -1) {
		alias := im[1]
		if alias == "" {
			alias = im[2][strings.LastIndex(im[2], "/")+1:]
		}

		imports[alias] = im[2]
	}

	s := svc{key: key, pkg: key + "schemas", protoAlias: m[1], protoCtor: m[2], serviceSchema: m[3]}

	if sm := reSigning.FindStringSubmatch(mustRead(filepath.Join(dir, "auth.go"))); sm != nil {
		s.signing = sm[1]
	}

	files, _ := filepath.Glob(filepath.Join(dir, "api_op_*.go"))
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}

		src := mustRead(f)
		s.acceptJSON = s.acceptJSON || strings.Contains(src, "addAcceptHeader(")

		om := reOpSchema.FindStringSubmatch(src)
		if om == nil {
			log.Fatalf("%s: no op schema in %s", key, f)
		}

		o := op{name: om[1], in: om[2], out: om[3]}
		if tm := reToken.FindStringSubmatch(src); tm != nil {
			o.token = tm[1]
		}

		s.ops = append(s.ops, o)
	}

	sort.Slice(s.ops, func(i, j int) bool { return s.ops[i].name < s.ops[j].name })

	return s, map[string]string{m[1]: imports[m[1]]}
}

func mustRead(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		log.Fatal(err)
	}

	return string(b)
}

func schemaRef(pkg, ref string) string {
	if ref == "nil" {
		return "nil"
	}

	return pkg + "." + strings.TrimPrefix(ref, "schemas.")
}

func write(svcs []svc, protoImports map[string]string, legacy []legacySvc) {
	var b bytes.Buffer

	b.WriteString("// Code generated by cmd/sfnschemagen; DO NOT EDIT.\n\npackage sdkwire\n\nimport (\n")
	b.WriteString("\t\"github.com/aws/smithy-go\"\n")

	aliases := make([]string, 0, len(protoImports))
	for a := range protoImports {
		aliases = append(aliases, a)
	}

	sort.Strings(aliases)

	for _, a := range aliases {
		fmt.Fprintf(&b, "\t%s %q\n", a, protoImports[a])
	}

	for _, s := range svcs {
		fmt.Fprintf(&b, "\t%s %q\n", s.pkg, sdkModule+s.key+"/schemas")
	}

	b.WriteString(")\n\nfunc init() {\n")

	for _, s := range svcs {
		fmt.Fprintf(&b, "\tregister(%q, func() *Service {\n\t\treturn &Service{\n", s.key)
		fmt.Fprintf(
			&b,
			"\t\t\tProtocol: %s.%s(%s.%s),\n\t\t\tSigningName: %q,\n\t\t\tUserAgentAPI: %q,\n",
			s.protoAlias,
			s.protoCtor,
			s.pkg,
			s.serviceSchema,
			s.signing,
			"api/"+s.key+"#"+strings.TrimPrefix(s.version, "v"),
		)
		if s.acceptJSON {
			b.WriteString("\t\t\tHeaders: map[string]string{\"Accept\": \"application/json\"},\n")
		}

		b.WriteString("\t\t\tOps: map[string]*Op{\n")

		for _, o := range s.ops {
			fmt.Fprintf(&b, "\t\t\t\t%q: {Schema: smithy.NewOperationSchema(%s.%s, %s, %s)",
				o.name, s.pkg, o.name, schemaRef(s.pkg, o.in), schemaRef(s.pkg, o.out))

			if o.token != "" {
				fmt.Fprintf(&b, ", Token: %q", o.token)
			}

			b.WriteString("},\n")
		}

		b.WriteString("\t\t\t},\n\t\t}\n\t})\n")
	}

	for _, l := range legacy {
		fmt.Fprintf(&b, "\tregister(%q, func() *Service {\n\t\treturn legacyService(%q, %q, %q, %s)\n\t})\n",
			l.key, l.key, l.signing, l.userAgentAPI, l.ctor)
	}

	b.WriteString("}\n")

	src, err := format.Source(b.Bytes())
	if err != nil {
		log.Fatalf("format: %v\n%s", err, b.String())
	}

	if werr := os.WriteFile(outFile, src, fileMode); werr != nil {
		log.Fatal(werr)
	}
}

// dispatchServices keep their generated SDK client; the Step Functions
// integration reaches their operations through a static method table.
func dispatchServices() []string {
	return []string{"s3"}
}

func writeDispatch() {
	var b bytes.Buffer

	b.WriteString(
		"// Code generated by cmd/sfnschemagen; DO NOT EDIT.\n\npackage stepfunctions\n\nimport (\n\t\"context\"\n\n",
	)

	for _, key := range dispatchServices() {
		fmt.Fprintf(&b, "\tx%s %q\n", key, sdkModule+key)
	}

	b.WriteString(")\n\n")
	b.WriteString("type sdkOpCall func(ctx context.Context, client any, params any) (any, error)\n\n")
	b.WriteString("func sdkOpTable(service string) map[string]sdkOpCall {\n\tswitch service {\n")

	for _, key := range dispatchServices() {
		fmt.Fprintf(&b, "\tcase %q:\n\t\treturn %sOps()\n", key, key)
	}

	b.WriteString("\tdefault:\n\t\treturn nil\n\t}\n}\n")

	for _, key := range dispatchServices() {
		fmt.Fprintf(&b, "\nfunc %sOps() map[string]sdkOpCall {\n\treturn map[string]sdkOpCall{\n", key)

		files, _ := filepath.Glob(filepath.Join(moduleDir(key), "api_op_*.go"))
		sort.Strings(files)

		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}

			op := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(f), "api_op_"), ".go")
			fmt.Fprintf(&b, "\t\t%q: func(ctx context.Context, client any, params any) (any, error) {\n",
				strings.ToLower(op[:1])+op[1:])
			fmt.Fprintf(
				&b,
				"\t\t\tc, _ := client.(*x%s.Client)\n\t\t\treturn invokeTyped(ctx, c.%s, params)\n\t\t},\n",
				key,
				op,
			)
		}

		b.WriteString("\t}\n}\n")
	}

	src, err := format.Source(b.Bytes())
	if err != nil {
		log.Fatalf("format dispatch: %v", err)
	}

	if werr := os.WriteFile(dispatchFile, src, fileMode); werr != nil {
		log.Fatal(werr)
	}
}

// writeClientsTest lists the real SDK client of every registered service for
// the parity tests.
func writeClientsTest(schema []svc, legacy []legacySvc) {
	keys := make([]string, 0, len(schema)+len(legacy))
	for _, s := range schema {
		keys = append(keys, s.key)
	}

	for _, l := range legacy {
		keys = append(keys, l.key)
	}

	sort.Strings(keys)

	var b bytes.Buffer

	b.WriteString("// Code generated by cmd/sfnschemagen; DO NOT EDIT.\n\npackage sdkwire\n\nimport (\n")
	b.WriteString("\t\"github.com/aws/aws-sdk-go-v2/aws\"\n")

	for _, k := range keys {
		fmt.Fprintf(&b, "\tsdk%s %q\n", k, sdkModule+k)
	}

	b.WriteString(")\n\n// sdkClients builds the real SDK client of each registered service.\n")
	b.WriteString("func sdkClients() map[string]func(aws.Config) any {\n\treturn map[string]func(aws.Config) any{\n")

	for _, k := range keys {
		fmt.Fprintf(&b, "\t\t%q: func(c aws.Config) any { return sdk%s.NewFromConfig(c) },\n", k, k)
	}

	b.WriteString("\t}\n}\n")

	src, err := format.Source(b.Bytes())
	if err != nil {
		fatalf("format clients: %v", err)
	}

	if werr := os.WriteFile(clientsFile, src, fileMode); werr != nil {
		fatalf("%v", werr)
	}
}
