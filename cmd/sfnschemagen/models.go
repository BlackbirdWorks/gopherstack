package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const modelsDir = "services/stepfunctions/sdkwire/models"

// modeledServices have their SDK schemas package linked by nothing else, so
// they are served from an embedded copy of the schema graph instead of
// linking the package and running its init functions.
func modeledServices() map[string]bool {
	keys := []string{
		"apigatewayv2", "appconfig", "athena", "batch", "bedrockruntime", "comprehend", "emr", "firehose",
		"kafka", "mq", "opensearch", "organizations", "pipes", "sagemaker", "sesv2", "textract", "translate",
	}
	m := make(map[string]bool, len(keys))

	for _, k := range keys {
		m[k] = true
	}

	return m
}

// modeledProtocols maps the SDK's protocol constructor to the sdkwire helper.
func modeledProtocols() map[string]string {
	return map[string]string{
		"awsjson.New11": "awsjson11Protocol",
		"restjson1.New": "restjson1Protocol",
		"rpcv2.NewCBOR": "rpcv2CBORProtocol",
	}
}

var reShapeVar = regexp.MustCompile(`(?m)^var (\w+) = smithy\.NewSchema\(`)

func shapeVars(dir string) []string {
	files, _ := filepath.Glob(filepath.Join(dir, "schemas", "*.go"))

	var names []string

	for _, f := range files {
		for _, m := range reShapeVar.FindAllStringSubmatch(mustRead(f), -1) {
			names = append(names, m[1])
		}
	}

	sort.Strings(names)

	return names
}

// copySchemas copies the SDK schemas package, adding SchemaPool to expose its
// unexported shape variables.
func copySchemas(dir, dst string) error {
	if err := os.MkdirAll(dst, 0o750); err != nil {
		return err
	}

	files, _ := filepath.Glob(filepath.Join(dir, "schemas", "*.go"))
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dst, filepath.Base(f)), []byte(mustRead(f)), fileMode); err != nil {
			return err
		}
	}

	return os.WriteFile(filepath.Join(dst, "schema_pool_gen.go"), []byte(poolFile(dir)), fileMode)
}

// poolFile lists every shape variable of the schemas package in dir.
func poolFile(dir string) string {
	var b strings.Builder

	b.WriteString("package schemas\n\nimport smithy \"github.com/aws/smithy-go\"\n\n")
	b.WriteString("func SchemaPool() []*smithy.Schema {\n\treturn []*smithy.Schema{\n")

	for _, v := range shapeVars(dir) {
		fmt.Fprintf(&b, "\t\t%s,\n", v)
	}

	b.WriteString("\t}\n}\n")

	return b.String()
}

// writeSchemaModels runs a throwaway program that links the SDK schemas
// packages, encodes each graph, and verifies the decoded copy against it.
func writeSchemaModels(svcs []svc) {
	var b bytes.Buffer

	copies := map[string]string{}

	b.WriteString("package main\n\nimport (\n\t\"fmt\"\n\t\"os\"\n\t\"path/filepath\"\n\n")
	b.WriteString("\t\"github.com/aws/smithy-go\"\n")
	b.WriteString("\t\"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire/schemair\"\n")

	for _, s := range svcs {
		fmt.Fprintf(&b, "\t%s %q\n", s.pkg, "@TMP@/"+s.key)
	}

	b.WriteString(")\n\nfunc dump(\n\tkey string, svc *smithy.ServiceSchema,\n")
	b.WriteString("\tops map[string]schemair.OpRef, pool []*smithy.Schema,\n) {\n")
	b.WriteString("\tm, err := schemair.Encode(svc, ops, pool)\n\tif err == nil {\n\t\terr = m.Check(svc, ops)\n\t}\n")
	b.WriteString("\tvar raw []byte\n\tif err == nil {\n\t\traw, err = m.Write()\n\t}\n")
	b.WriteString("\tif err == nil {\n\t\tout := filepath.Join(os.Args[1], key+\".json.gz\")\n")
	b.WriteString("\t\terr = os.WriteFile(out, raw, 0o600)\n\t}\n")
	b.WriteString("\tif err != nil {\n\t\tfmt.Fprintln(os.Stderr, key+\":\", err)\n\t\tos.Exit(1)\n\t}\n}\n")
	b.WriteString("\nfunc main() {\n")

	for _, s := range svcs {
		dir := moduleDir(s.key)

		fmt.Fprintf(&b, "\tdump(%q, %s.%s, map[string]schemair.OpRef{\n", s.key, s.pkg, s.serviceSchema)

		for _, o := range s.ops {
			fmt.Fprintf(&b, "\t\t%q: {Op: %s.%s, In: %s, Out: %s, Token: %q},\n",
				o.name, s.pkg, o.name, schemaRef(s.pkg, o.in), schemaRef(s.pkg, o.out), o.token)
		}

		fmt.Fprintf(&b, "\t}, %s.SchemaPool())\n", s.pkg)

		copies[s.key] = dir
	}

	b.WriteString("}\n")

	if err := runDump(b.Bytes(), copies); err != nil {
		fatalf("schema models: %v", err)
	}
}

func runDump(src []byte, copies map[string]string) error {
	tmp, err := os.MkdirTemp(filepath.Join("cmd", "sfnschemagen"), "dump-")
	if err != nil {
		return err
	}

	defer os.RemoveAll(tmp)

	src = bytes.ReplaceAll(src, []byte("@TMP@"),
		[]byte("github.com/blackbirdworks/gopherstack/"+filepath.ToSlash(tmp)))

	if werr := os.WriteFile(filepath.Join(tmp, "main.go"), src, fileMode); werr != nil {
		return werr
	}

	for key, dir := range copies {
		if cerr := copySchemas(dir, filepath.Join(tmp, key)); cerr != nil {
			return cerr
		}
	}

	abs, err := filepath.Abs(modelsDir)
	if err != nil {
		return err
	}

	cmd := exec.CommandContext( //nolint:gosec // fixed go tool; temp dir from MkdirTemp
		context.Background(), "go", "run", "./"+filepath.ToSlash(tmp), abs)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr

	if rerr := cmd.Run(); rerr != nil {
		return rerr
	}

	return nil
}

func modeledLines(svcs []svc) string {
	var b strings.Builder

	for _, s := range svcs {
		proto, ok := modeledProtocols()[s.protoAlias+"."+s.protoCtor]
		if !ok {
			fatalf("%s: no modeled protocol for %s.%s", s.key, s.protoAlias, s.protoCtor)
		}

		if s.acceptJSON {
			fatalf("%s: modeled services do not send an Accept header", s.key)
		}

		fmt.Fprintf(&b, "\tregister(%q, func() *Service {\n\t\treturn modeledService(%q, %q, %q, %s)\n\t})\n",
			s.key, s.key, s.signing, "api/"+s.key+"#"+strings.TrimPrefix(s.version, "v"), proto)
	}

	return b.String()
}
