package codebuild

import (
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// batchNode is one resolved node of a buildspec `batch:` section: a
// build-list or build-graph entry (see the CodeBuild "Batch build buildspec
// reference"). build-matrix is recognized but rejected with a clear error
// (see PARITY.md); no other batch type is recognized.
type batchNode struct {
	Env           *batchNodeEnv
	Identifier    string
	Buildspec     string
	DependsOn     []string
	IgnoreFailure bool
}

// batchNodeEnv is a batch node's `env:` override, layered onto the batch's
// own (possibly StartBuildBatch-overridden) environment.
type batchNodeEnv struct {
	Variables      map[string]string
	ComputeType    string
	Image          string
	Type           string
	PrivilegedMode bool
}

// batchDefinition is a buildspec's parsed `batch:` section, in the buildspec's
// own definition order -- BuildGroups must reflect that order deterministically.
type batchDefinition struct {
	byID  map[string]batchNode
	nodes []batchNode
}

func (d batchDefinition) node(id string) batchNode { return d.byID[id] }

type batchSpecFile struct {
	Batch *batchSectionYAML `yaml:"batch"`
}

type batchSectionYAML struct {
	BuildMatrix map[string]any  `yaml:"build-matrix"`
	BuildList   []batchNodeYAML `yaml:"build-list"`
	BuildGraph  []batchNodeYAML `yaml:"build-graph"`
}

type batchNodeYAML struct {
	Env           *batchNodeEnvYAML `yaml:"env"`
	Identifier    string            `yaml:"identifier"`
	Buildspec     string            `yaml:"buildspec"`
	DependsOn     []string          `yaml:"depend-on"`
	IgnoreFailure bool              `yaml:"ignore-failure"`
}

type batchNodeEnvYAML struct {
	Variables      map[string]string `yaml:"variables"`
	ComputeType    string            `yaml:"compute-type"`
	Image          string            `yaml:"image"`
	Type           string            `yaml:"type"`
	PrivilegedMode bool              `yaml:"privileged-mode"`
}

// errNoBatchConfig is StartBuildBatch's real error (InvalidInputException,
// via ErrValidation) when the buildspec has no `batch:` section, or the
// section declares no build-list/build-graph nodes. AWS does not publish its
// server-side message text in the client SDK (deserializers.go's generic
// awsAwsjson11_deserializeOpErrorStartBuildBatch reads whatever __type/message
// the server sent, it doesn't enumerate one), so only the exception type is
// SDK-verified; the message text below is descriptive, not a verified literal.
var errNoBatchConfig = fmt.Errorf(
	"%w: the buildspec file for this build does not contain a batch configuration",
	ErrValidation,
)

// parseBatchDefinition parses a resolved buildspec's `batch:` section.
func parseBatchDefinition(buildspec string) (batchDefinition, error) {
	var file batchSpecFile
	if err := yaml.Unmarshal([]byte(buildspec), &file); err != nil {
		//nolint:errorlint // %v is deliberate: a yaml.v3 parse error, never wrapped/compared via errors.Is
		return batchDefinition{}, fmt.Errorf("%w: invalid buildspec YAML: %v", ErrValidation, err)
	}

	if file.Batch == nil {
		return batchDefinition{}, errNoBatchConfig
	}

	rawNodes, err := selectBatchNodes(*file.Batch)
	if err != nil {
		return batchDefinition{}, err
	}

	return buildBatchDefinition(rawNodes)
}

// selectBatchNodes picks the one batch strategy a buildspec declares.
// build-list, build-graph, and build-matrix are mutually exclusive in real
// CodeBuild buildspecs.
func selectBatchNodes(sec batchSectionYAML) ([]batchNodeYAML, error) {
	present := 0
	if len(sec.BuildList) > 0 {
		present++
	}

	if len(sec.BuildGraph) > 0 {
		present++
	}

	if len(sec.BuildMatrix) > 0 {
		present++
	}

	switch {
	case present == 0:
		return nil, errNoBatchConfig
	case present > 1:
		return nil, fmt.Errorf(
			"%w: a batch definition may declare only one of build-list, build-graph, or build-matrix",
			ErrValidation,
		)
	case len(sec.BuildMatrix) > 0:
		return nil, fmt.Errorf("%w: build-matrix batch definitions are not supported by this emulator", ErrValidation)
	case len(sec.BuildList) > 0:
		return sec.BuildList, nil
	default:
		return sec.BuildGraph, nil
	}
}

// buildBatchDefinition validates and indexes a batch strategy's raw nodes:
// every identifier is unique and non-empty, every depend-on target exists,
// and the dependency graph has no cycle (see checkBatchDependencyCycles).
func buildBatchDefinition(raw []batchNodeYAML) (batchDefinition, error) {
	def := batchDefinition{
		nodes: make([]batchNode, 0, len(raw)),
		byID:  make(map[string]batchNode, len(raw)),
	}

	for _, n := range raw {
		if n.Identifier == "" {
			return batchDefinition{}, fmt.Errorf(
				"%w: every batch build-list/build-graph entry requires an identifier", ErrValidation,
			)
		}

		if _, dup := def.byID[n.Identifier]; dup {
			return batchDefinition{}, fmt.Errorf("%w: duplicate batch identifier %q", ErrValidation, n.Identifier)
		}

		node := batchNode{
			Identifier:    n.Identifier,
			Buildspec:     n.Buildspec,
			DependsOn:     n.DependsOn,
			IgnoreFailure: n.IgnoreFailure,
			Env:           toBatchNodeEnv(n.Env),
		}
		def.nodes = append(def.nodes, node)
		def.byID[n.Identifier] = node
	}

	for _, n := range def.nodes {
		for _, dep := range n.DependsOn {
			if _, ok := def.byID[dep]; !ok {
				return batchDefinition{}, fmt.Errorf(
					"%w: batch identifier %q depends on unknown identifier %q", ErrValidation, n.Identifier, dep,
				)
			}
		}
	}

	if err := checkBatchDependencyCycles(def); err != nil {
		return batchDefinition{}, err
	}

	return def, nil
}

func toBatchNodeEnv(y *batchNodeEnvYAML) *batchNodeEnv {
	if y == nil {
		return nil
	}

	return &batchNodeEnv{
		ComputeType:    y.ComputeType,
		Image:          y.Image,
		Type:           y.Type,
		PrivilegedMode: y.PrivilegedMode,
		Variables:      y.Variables,
	}
}

// checkBatchDependencyCycles rejects a build-graph with a dependency cycle.
// Real AWS validates this at StartBuildBatch time; without the check here, a
// cycle would leave every node in it permanently unstartable and the batch
// stuck IN_PROGRESS forever.
func checkBatchDependencyCycles(def batchDefinition) error {
	const (
		unvisited = iota
		visiting
		visited
	)

	state := make(map[string]int, len(def.nodes))

	var visit func(id string) error
	visit = func(id string) error {
		switch state[id] {
		case visited:
			return nil
		case visiting:
			return fmt.Errorf("%w: batch definition has a dependency cycle involving %q", ErrValidation, id)
		}

		state[id] = visiting

		for _, dep := range def.byID[id].DependsOn {
			if err := visit(dep); err != nil {
				return err
			}
		}

		state[id] = visited

		return nil
	}

	for _, n := range def.nodes {
		if err := visit(n.Identifier); err != nil {
			return err
		}
	}

	return nil
}

// envVarsFromMap converts a batchspec `env.variables` mapping into the
// deterministic []EnvironmentVariable shape the rest of this package uses
// (map iteration order is random, so results must be sorted by key).
func envVarsFromMap(m map[string]string) []EnvironmentVariable {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]EnvironmentVariable, 0, len(m))
	for _, k := range keys {
		out = append(out, EnvironmentVariable{Name: k, Value: m[k]})
	}

	return out
}

// applyNodeEnvOverrides layers a batch node's own `env:` block onto the
// batch-level environment it would otherwise inherit.
func applyNodeEnvOverrides(env ProjectEnvironment, node batchNode) ProjectEnvironment {
	if node.Env == nil {
		return env
	}

	if node.Env.ComputeType != "" {
		env.ComputeType = node.Env.ComputeType
	}

	if node.Env.Image != "" {
		env.Image = node.Env.Image
	}

	if node.Env.Type != "" {
		env.Type = node.Env.Type
	}

	if node.Env.PrivilegedMode {
		env.PrivilegedMode = true
	}

	if vars := envVarsFromMap(node.Env.Variables); len(vars) > 0 {
		env.EnvironmentVariables = mergeEnvVarOverrides(env.EnvironmentVariables, vars)
	}

	return env
}
