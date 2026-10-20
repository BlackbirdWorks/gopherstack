package guards_test

import (
	"go/ast"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	sdkCaseKeyRe = regexp.MustCompile(`(?:case|object\.Key\()\s*"(\w+)"`)
)

// requestOrStoredRe skips request-only structs (Go decodes keys
// case-insensitively, so input works) and unexported persistence rows.
var requestOrStoredRe = regexp.MustCompile(`(?i)(input|request)$|^stored`)

// wireKeyCaseAllowlist lists "<file>: <Struct>" entries whose tags differ in
// case from an SDK key by design.
const reasonInternal = "internal backend model; the handler converts it to a separate wire type"

func wireKeyCaseAllowlist() map[string]string {
	return map[string]string{
		"services/appconfig/models.go: HostedConfigurationVersion":           reasonInternal,
		"services/applicationautoscaling/models.go: NotScaledReason":         reasonInternal,
		"services/applicationautoscaling/models.go: ScalingActivity":         reasonInternal,
		"services/awsconfig/models.go: ConfigurationRecorder":                reasonInternal,
		"services/codecommit/models.go: Repository":                          reasonInternal,
		"services/dynamodb/store.go: Backup":                                 reasonInternal,
		"services/dynamodb/store.go: KinesisDestinationEntry":                reasonInternal,
		"services/dynamodb/store.go: Table":                                  reasonInternal,
		"services/emr/models.go: NotebookExecution":                          reasonInternal,
		"services/fis/models.go: Experiment":                                 reasonInternal,
		"services/fis/models.go: ExperimentAction":                           reasonInternal,
		"services/fis/models.go: ExperimentTemplateAction":                   reasonInternal,
		"services/glacier/models.go: VaultLock":                              reasonInternal,
		"services/glue/models.go: ConnectionTypeInfo":                        reasonInternal,
		"services/glue/models.go: CrawlHistoryEntry":                         reasonInternal,
		"services/organizations/models.go: HandshakeParty":                   reasonInternal,
		"services/quicksight/types.go: ResourcePermission":                   reasonInternal,
		"services/s3tables/models.go: Table":                                 reasonInternal,
		"services/sagemaker/models.go: EndpointConfig":                       reasonInternal,
		"services/sagemaker/models.go: Model":                                reasonInternal,
		"services/scheduler/models.go: EcsAwsvpcConfiguration":               reasonInternal,
		"services/sesv2/contacts.go: TopicPreference":                        reasonInternal,
		"services/sesv2/email_templates.go: EmailTemplateContent":            reasonInternal,
		"services/support/models.go: Case":                                   reasonInternal,
		"services/swf/models.go: WorkflowExecution":                          reasonInternal,
		"services/verifiedpermissions/models.go: Policy":                     reasonInternal,
		"services/verifiedpermissions/models.go: PolicyStoreAlias":           reasonInternal,
		"services/verifiedpermissions/models.go: PolicyTemplate":             reasonInternal,
		"services/sagemaker/training_plan.go: pendingTrainingPlanExtension":  reasonInternal,
		"services/awsconfig/models.go: wireConfigurationRecorder":            "shadows the embedded field; must use its key",
		"services/cloudformation/stack_policy_eval.go: stackPolicyStatement": "IAM policy document, not an SDK shape",
		"services/glue/models.go: JobRun":                                    "WorkflowRunId is internal-only",
		"services/opensearch/handler_domain_options.go: ebsOptionsJSON":      "SDK EBSOptions lacks KmsKeyId",
		"services/stepfunctions/handler_tags.go: sfnTagEntry":                "SDK Tag uses lowercase key/value",
		"services/acm/handler_certificates.go: listCertificatesIncludes":     "request-only filter",
		"services/firehose/handler_delivery_streams.go: aosDeliveryField":    "request-only; decode is case-insensitive",
		"services/firehose/handler_destination_updates.go: aosUpdateField":   "request-only; decode is case-insensitive",
		"services/ssm/models_instances.go: InstancePropertyFilter":           "request-only filter",
	}
}

func addKeys(keys map[string]bool, src []byte) {
	for _, m := range sdkCaseKeyRe.FindAllStringSubmatch(string(src), -1) {
		keys[m[1]] = true
	}
}

// caseMismatches returns json keys in dir that equal an SDK key only
// case-insensitively and are not themselves an SDK key. Only wire structs are
// checked: those with most tags matching an SDK key exactly, plus the structs they
// directly reference. Internal models tagged consistently in another case are skipped.
func caseMismatches(t *testing.T, dir string, sdkKeys map[string]bool) []string {
	t.Helper()

	folded := map[string]string{}
	for k := range sdkKeys {
		folded[strings.ToLower(k)] = k
	}

	pg := parseGuardedDir(t, dir)
	specs := structSpecs(pg)
	wire := withChildren(specs, exactMatchRoots(specs, sdkKeys))

	var out []string

	for name, sp := range specs {
		if wire[name] {
			out = append(out, structCaseMismatches(sp.file, sp.ts, sp.st, sdkKeys, folded)...)
		}
	}

	sort.Strings(out)

	return out
}

type structSpec struct {
	ts   *ast.TypeSpec
	st   *ast.StructType
	file string
}

func structSpecs(pg parsedGo) map[string]structSpec {
	out := map[string]structSpec{}

	for i, f := range pg.files {
		ast.Inspect(f, func(n ast.Node) bool {
			if ts, ok := n.(*ast.TypeSpec); ok {
				if st, isSt := ts.Type.(*ast.StructType); isSt {
					out[ts.Name.Name] = structSpec{ts: ts, st: st, file: filepath.ToSlash(pg.names[i])}
				}
			}

			return true
		})
	}

	return out
}

func exactMatchRoots(specs map[string]structSpec, sdkKeys map[string]bool) map[string]bool {
	roots := map[string]bool{}

	for name, sp := range specs {
		exact, tagged := 0, 0

		for _, fld := range sp.st.Fields.List {
			if fld.Tag == nil || len(fld.Names) == 0 {
				continue
			}

			tagged++

			if sdkKeys[jsonKey(fld, fld.Names[0].Name)] {
				exact++
			}
		}

		if exact*2 > tagged {
			roots[name] = true
		}
	}

	return roots
}

func structCaseMismatches(
	file string, ts *ast.TypeSpec, st *ast.StructType, sdkKeys map[string]bool, folded map[string]string,
) []string {
	var out []string

	if requestOrStoredRe.MatchString(ts.Name.Name) {
		return nil
	}

	for _, fld := range st.Fields.List {
		for _, nm := range fld.Names {
			if fld.Tag == nil || !strings.Contains(fld.Tag.Value, `json:"`) {
				continue
			}

			key := jsonKey(fld, nm.Name)
			want, hit := folded[strings.ToLower(key)]

			if key == "-" || sdkKeys[key] || !hit {
				continue
			}

			if _, skip := wireKeyCaseAllowlist()[file+": "+ts.Name.Name]; skip {
				continue
			}

			out = append(out, file+": "+ts.Name.Name+"."+nm.Name+` json:"`+key+`" but the SDK uses "`+want+`"`)
		}
	}

	return out
}

// TestWireKeysMatchSDKCase fails when a service struct's json key differs
// from the pinned SDK wire key only in case: decode still works (Go matches
// case-insensitively) but the SDK's case-sensitive deserializer drops the
// field from responses.
func TestWireKeysMatchSDKCase(t *testing.T) {
	t.Parallel()

	sdk := loadSDK(t).keys
	require.NotEmpty(t, sdk)

	entries, err := os.ReadDir("services")
	require.NoError(t, err)

	var offenders []string

	for _, e := range entries {
		if !e.IsDir() || skipGuardDir(e.Name()) {
			continue
		}

		dir := filepath.Join("services", e.Name())

		pkgs := dirSDKPackages(t, dir)
		if _, ok := sdk[e.Name()]; ok {
			pkgs = map[string]bool{e.Name(): true}
		}

		for pkg := range pkgs {
			if keys, ok := sdk[pkg]; ok {
				offenders = append(offenders, caseMismatches(t, dir, keys)...)
			}
		}
	}

	sort.Strings(offenders)
	require.Empty(t, compact(offenders), "json keys differ from the SDK wire key only in case")
}

func compact(in []string) []string {
	var out []string

	for i, s := range in {
		if i == 0 || s != in[i-1] {
			out = append(out, s)
		}
	}

	return out
}

// TestWireKeyCaseGuardDetectsMiscase is the positive control: the EventBridge
// ECS NetworkConfiguration shape with "AwsvpcConfiguration" must be flagged.
func TestWireKeyCaseGuardDetectsMiscase(t *testing.T) {
	t.Parallel()

	sdk := loadSDK(t).keys
	require.Contains(t, sdk, "eventbridge")

	got := caseMismatches(t, "test/guards/testdata/wire_key_case/miscased", sdk["eventbridge"])
	require.Len(t, got, 1)
	require.Contains(t, got[0], "networkConfiguration.AwsvpcConfiguration")
}

func withChildren(specs map[string]structSpec, roots map[string]bool) map[string]bool {
	out := map[string]bool{}

	for name := range roots {
		out[name] = true

		for _, fld := range specs[name].st.Fields.List {
			if tn := typeName(fld.Type); tn != "" {
				out[tn] = true
			}
		}
	}

	return out
}
