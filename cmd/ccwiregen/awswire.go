package main

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

var (
	errUnion      = errors.New("operation reaches a union type the JSON bridge cannot decode; list it in unions")
	errNoPaginate = errors.New("paginator token fields not found")

	reTokenIn  = regexp.MustCompile(`params\.(\w+) = p\.nextToken`)
	reTokenOut = regexp.MustCompile(`p\.nextToken = result\.(\w+)`)
	reLimit    = regexp.MustCompile(`params\.(\w+) = limit`)
	reTypesRef = regexp.MustCompile(`\btypes\.(\w+)`)
)

// wireModel lists the per-service packages under internal/awswire: the operations and paginators the main binary
// calls without linking the generated SDK clients. A service block is its module name followed by "unions:",
// "ops:" and "pages:" word lists; indented lines continue the previous list.
const wireModel = `
cloudwatchlogs
	ops: CreateExportTask CreateLogGroup CreateLogStream DeleteLogGroup DeleteRetentionPolicy
		DescribeLogGroups DescribeLogStreams FilterLogEvents GetLogEvents ListTagsForResource
		PutLogEvents PutResourcePolicy PutRetentionPolicy PutSubscriptionFilter
	pages: DescribeLogGroups

eventbridge
	ops: CreateApiDestination CreateConnection CreateEventBus DeauthorizeConnection DeleteEventBus
		DescribeEventBus DescribeRule ListEventBuses ListRules ListTagsForResource
		ListTargetsByRule PutEvents PutRule PutTargets TagResource UntagResource UpdateEventBus

iam
	ops: AddRoleToInstanceProfile AddUserToGroup AttachGroupPolicy AttachRolePolicy
		AttachUserPolicy CreateAccessKey CreateGroup CreateInstanceProfile CreatePolicy
		CreatePolicyVersion CreateRole CreateUser DeletePolicyVersion DeleteRole DeleteUser
		DetachRolePolicy GetGroup GetGroupPolicy GetInstanceProfile GetPolicy GetPolicyVersion
		GetRole GetRolePolicy GetUser GetUserPolicy ListAccessKeys ListAttachedGroupPolicies
		ListAttachedRolePolicies ListAttachedUserPolicies ListGroupPolicies ListGroupsForUser
		ListGroups ListInstanceProfiles ListPolicies ListPolicyVersions ListRolePolicies ListRoles
		ListUserPolicies ListUsers PutGroupPolicy PutRolePolicy PutUserPermissionsBoundary
		PutUserPolicy TagRole TagUser UntagRole UpdateAssumeRolePolicy UpdateRole
	pages: ListAttachedGroupPolicies ListAttachedRolePolicies ListAttachedUserPolicies ListGroupPolicies
		ListGroupsForUser ListGroups ListInstanceProfiles ListPolicies ListRolePolicies ListRoles ListUserPolicies
		ListUsers

kinesis
	ops: AddTagsToStream CreateStream DecreaseStreamRetentionPeriod DeleteStream DescribeStream
		DescribeStreamSummary GetRecords GetShardIterator IncreaseStreamRetentionPeriod ListShards
		ListStreams ListTagsForStream PutRecord PutRecords RemoveTagsFromStream
	pages: ListStreams

kms
	ops: CreateAlias CreateKey Decrypt DescribeKey DisableKey DisableKeyRotation EnableKey
		EnableKeyRotation GetKeyPolicy GetKeyRotationStatus ListAliases ListKeys ListResourceTags
		PutKeyPolicy ScheduleKeyDeletion TagResource UntagResource UpdateAlias
		UpdateKeyDescription
	pages: ListAliases ListKeys

lambda
	ops: AddPermission CreateAlias CreateEventSourceMapping CreateFunction DeleteFunction GetAlias
		GetFunctionConfiguration GetFunction GetProvisionedConcurrencyConfig Invoke ListAliases
		ListEventSourceMappings ListFunctions ListVersionsByFunction PublishVersion
		PutFunctionConcurrency PutProvisionedConcurrencyConfig TagResource UntagResource
		UpdateAlias UpdateEventSourceMapping UpdateFunctionCode UpdateFunctionConfiguration
	pages: ListAliases ListEventSourceMappings ListFunctions ListVersionsByFunction

secretsmanager
	ops: CreateSecret DeleteSecret DescribeSecret GetResourcePolicy GetSecretValue ListSecrets
		ListSecretVersionIds PutResourcePolicy PutSecretValue TagResource UntagResource
		UpdateSecret UpdateSecretVersionStage
	pages: ListSecrets ListSecretVersionIds

sns
	ops: CreatePlatformApplication CreateTopic DeleteTopic GetSubscriptionAttributes
		GetTopicAttributes ListSubscriptionsByTopic ListTagsForResource ListTopics Publish
		SetSubscriptionAttributes SetTopicAttributes Subscribe TagResource UntagResource
	pages: ListSubscriptionsByTopic ListTopics

sqs
	ops: CreateQueue DeleteMessage DeleteQueue GetQueueAttributes GetQueueUrl ListQueues
		ListQueueTags ReceiveMessage SendMessage SendMessageBatch SetQueueAttributes TagQueue
	pages: ListQueues

ssm
	ops: AddTagsToResource CancelCommand CreateDocument DeleteParameter
		DescribeAutomationExecutions DescribeInstanceInformation DescribeParameters
		GetAutomationExecution GetCommandInvocation GetParameter GetParameters ListCommands
		ListTagsForResource PutParameter RemoveTagsFromResource SendCommand
		StartAutomationExecution
	pages: DescribeParameters

sfn
	ops: CreateStateMachine DeleteStateMachine DescribeExecution DescribeStateMachine
		ListExecutions ListStateMachines ListTagsForResource StartExecution StartSyncExecution
		StopExecution TagResource UntagResource UpdateStateMachine
	pages: ListStateMachines

cloudwatch
	unions: EvaluationCriteria EvaluationWindow
	ops: DeleteAlarms DescribeAlarms ListMetrics ListTagsForResource PutMetricAlarm PutMetricData
		SetAlarmState TagResource UntagResource
	pages: DescribeAlarms
`

func wireSpecs() []spec {
	out := make([]spec, 0, strings.Count(wireModel, "\n\n")+1)

	for blk := range strings.SplitSeq(strings.TrimSpace(wireModel), "\n\n") {
		var (
			cur *[]string
			sp  = spec{alias: typesAlias, pkg: true}
		)

		for line := range strings.SplitSeq(blk, "\n") {
			trimmed := strings.TrimSpace(line)

			switch {
			case !strings.HasPrefix(line, "\t"):
				sp.module = trimmed
			case strings.HasPrefix(trimmed, "unions:"):
				cur, trimmed = &sp.unions, strings.TrimPrefix(trimmed, "unions:")
			case strings.HasPrefix(trimmed, "ops:"):
				cur, trimmed = &sp.ops, strings.TrimPrefix(trimmed, "ops:")
			case strings.HasPrefix(trimmed, "pages:"):
				cur, trimmed = &sp.pages, strings.TrimPrefix(trimmed, "pages:")
			}

			if line != "" && strings.HasPrefix(line, "\t") {
				*cur = append(*cur, strings.Fields(trimmed)...)
			}
		}

		out = append(out, sp)
	}

	return out
}

// typeDecl is one declaration of a service's types package with the imports of its file.
type typeDecl struct {
	expr    ast.Expr
	imports map[string]string
}

// loadTypeDecls indexes the declarations of a service's types package by name.
func loadTypeDecls(dir string) (map[string]typeDecl, error) {
	typesDir := filepath.Join(dir, "types")

	entries, err := os.ReadDir(typesDir)
	if err != nil {
		return nil, err
	}

	decls := map[string]typeDecl{}

	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}

		f, perr := parser.ParseFile(token.NewFileSet(), filepath.Join(typesDir, e.Name()), nil, 0)
		if perr != nil {
			return nil, perr
		}

		imports := fileImportsOf(f)

		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}

			for _, sp := range gd.Specs {
				if ts, isType := sp.(*ast.TypeSpec); isType {
					decls[ts.Name.Name] = typeDecl{expr: ts.Type, imports: imports}
				}
			}
		}
	}

	return decls, nil
}

func fileImportsOf(f *ast.File) map[string]string {
	out := map[string]string{}

	for _, im := range f.Imports {
		p := strings.Trim(im.Path.Value, `"`)

		name := filepath.Base(p)
		if im.Name != nil {
			name = im.Name.Name
		}

		out[name] = p
	}

	return out
}

// checkNoUnions fails when generated members reach an interface type of the service's types package that the spec
// does not drop: those decode from the JSON bridge only through the SDK's own deserializers.
func checkNoUnions(decls map[string]typeDecl, s spec, generated string) error {
	seen := map[string]bool{}

	var queue []string
	for _, m := range reTypesRef.FindAllStringSubmatch(generated, -1) {
		queue = append(queue, m[1])
	}

	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]

		if seen[n] {
			continue
		}

		seen[n] = true

		if _, isUnion := decls[n].expr.(*ast.InterfaceType); isUnion && !slices.Contains(s.unions, n) {
			return fmt.Errorf("%w: %s.%s", errUnion, s.module, n)
		}

		if st, isStruct := decls[n].expr.(*ast.StructType); isStruct {
			queue = append(queue, referencedDecls(st, decls)...)
		}
	}

	return nil
}

func referencedDecls(st *ast.StructType, decls map[string]typeDecl) []string {
	var out []string

	ast.Inspect(st, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok {
			if _, isDecl := decls[id.Name]; isDecl {
				out = append(out, id.Name)
			}
		}

		return true
	})

	return out
}

// writePaginator emits the SDK-shaped paginator of op, reading its token members from the SDK source.
func writePaginator(body *bytes.Buffer, dir, op string) error {
	src, err := os.ReadFile(filepath.Join(dir, "api_op_"+op+".go"))
	if err != nil {
		return err
	}

	in, out := reTokenIn.FindSubmatch(src), reTokenOut.FindSubmatch(src)
	if in == nil || out == nil {
		return fmt.Errorf("%w: %s", errNoPaginate, op)
	}

	clearLimit := ""
	if m := reLimit.FindSubmatch(src); m != nil {
		clearLimit = fmt.Sprintf(" i.%s = nil\n", m[1])
	}

	fmt.Fprintf(body, `// %[1]sPaginator pages through %[1]s.
type %[1]sPaginator = awswire.Paginator[%[1]sInput, %[1]sOutput]

// New%[1]sPaginator returns a paginator over %[1]s.
func New%[1]sPaginator(c *Client, in *%[1]sInput) *%[1]sPaginator {
	return awswire.NewPaginator(in, c.%[1]s,
		func(i *%[1]sInput, t *string) { i.%[2]s = t
%[4]s},
		func(o *%[1]sOutput) *string { return o.%[3]s })
}

`, op, in[1], out[1], clearLimit)

	return nil
}

// unionCtx rewrites listed union types to JSON-shaped maps and copies the structs that hold them, since the
// SDK's own structs cannot decode unions from the JSON bridge.
type unionCtx struct {
	decls  map[string]typeDecl
	cloned map[string]bool
	need   []string
	done   map[string]bool
	unions []string
}

func newUnionCtx(decls map[string]typeDecl, unions []string) *unionCtx {
	u := &unionCtx{decls: decls, cloned: map[string]bool{}, done: map[string]bool{}, unions: unions}

	for changed := true; changed; {
		changed = false

		for name, d := range decls {
			st, ok := d.expr.(*ast.StructType)
			if !ok || u.cloned[name] {
				continue
			}

			for _, ref := range referencedDecls(st, decls) {
				if u.cloned[ref] || slices.Contains(unions, ref) {
					u.cloned[name], changed = true, true

					break
				}
			}
		}
	}

	return u
}

// mapType rewrites an expression of a field type: unions become map[string]any and copied structs lose their
// types. qualifier. inside is true for expressions taken from the types package itself (unqualified idents).
func (u *unionCtx) mapType(e ast.Expr, inside bool) ast.Expr {
	switch t := e.(type) {
	case *ast.StarExpr:
		t.X = u.mapType(t.X, inside)
	case *ast.ArrayType:
		t.Elt = u.mapType(t.Elt, inside)
	case *ast.MapType:
		t.Value = u.mapType(t.Value, inside)
	case *ast.SelectorExpr:
		if id, ok := t.X.(*ast.Ident); ok && id.Name == typesAlias && !inside {
			return u.named(t.Sel.Name, e)
		}
	case *ast.Ident:
		if _, isDecl := u.decls[t.Name]; isDecl && inside {
			return u.named(t.Name, &ast.SelectorExpr{X: ast.NewIdent(typesAlias), Sel: ast.NewIdent(t.Name)})
		}
	}

	return e
}

func (u *unionCtx) named(name string, qualified ast.Expr) ast.Expr {
	switch {
	case slices.Contains(u.unions, name):
		return &ast.MapType{Key: ast.NewIdent("string"), Value: ast.NewIdent("any")}
	case u.cloned[name]:
		u.need = append(u.need, name)

		return ast.NewIdent(name)
	default:
		return qualified
	}
}

// emit writes the copies of every struct the generated code needs, and the ones those need in turn.
func (u *unionCtx) emit(body *bytes.Buffer, imports map[string]string) error {
	for len(u.need) > 0 {
		name := u.need[0]
		u.need = u.need[1:]

		if u.done[name] {
			continue
		}

		u.done[name] = true

		d := u.decls[name]
		st, _ := d.expr.(*ast.StructType)

		var fields []*ast.Field

		for _, fld := range st.Fields.List {
			if len(fld.Names) == 0 || !fld.Names[0].IsExported() {
				continue
			}

			fld.Doc, fld.Comment = nil, nil
			rewriteSelectors(fld.Type, typesAlias, d.imports, imports)
			fld.Type = u.mapType(fld.Type, true)
			fields = append(fields, fld)
		}

		decl := &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{
			Name: ast.NewIdent(name), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}},
		}}}

		var b bytes.Buffer
		if err := printer.Fprint(&b, token.NewFileSet(), decl); err != nil {
			return err
		}

		body.WriteString(b.String() + "\n\n")
	}

	return nil
}
