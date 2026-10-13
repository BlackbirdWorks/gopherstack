package codebuild

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"

	"gopkg.in/yaml.v3"
)

const maxBatchMatrixBuilds = 100

type batchMatrixYAML struct {
	Static  batchMatrixStaticYAML  `yaml:"static"`
	Dynamic batchMatrixDynamicYAML `yaml:"dynamic"`
}

type batchMatrixStaticYAML struct {
	Env           *batchNodeEnvYAML `yaml:"env"`
	IgnoreFailure bool              `yaml:"ignore-failure"`
}

type batchMatrixDynamicYAML struct {
	Env       *batchMatrixDynamicEnvYAML `yaml:"env"`
	Buildspec string                     `yaml:"buildspec"`
}

type batchMatrixDynamicEnvYAML struct {
	Variables      map[string]flexList `yaml:"variables"`
	Image          flexList            `yaml:"image"`
	ComputeType    flexList            `yaml:"compute-type"`
	Type           flexList            `yaml:"type"`
	PrivilegedMode flexList            `yaml:"privileged-mode"`
}

// flexList decodes a YAML scalar or sequence into a string slice.
type flexList []string

func (l *flexList) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		*l = flexList{n.Value}

		return nil
	}

	var out []string
	if err := n.Decode(&out); err != nil {
		return fmt.Errorf("decode list: %w", err)
	}

	*l = out

	return nil
}

// matrixAxis is one dimension of the build-matrix cross product.
type matrixAxis struct {
	apply  func(env *batchNodeEnvYAML, val string)
	values []string
}

func matrixAxes(env *batchMatrixDynamicEnvYAML) []matrixAxis {
	if env == nil {
		return nil
	}

	var axes []matrixAxis

	add := func(vals flexList, apply func(*batchNodeEnvYAML, string)) {
		if len(vals) > 0 {
			axes = append(axes, matrixAxis{values: vals, apply: apply})
		}
	}

	add(env.Image, func(e *batchNodeEnvYAML, v string) { e.Image = v })
	add(env.ComputeType, func(e *batchNodeEnvYAML, v string) { e.ComputeType = v })
	add(env.Type, func(e *batchNodeEnvYAML, v string) { e.Type = v })
	add(env.PrivilegedMode, func(e *batchNodeEnvYAML, v string) {
		e.PrivilegedMode, _ = strconv.ParseBool(v)
	})

	names := make([]string, 0, len(env.Variables))
	for k := range env.Variables {
		names = append(names, k)
	}

	sort.Strings(names)

	for _, name := range names {
		add(env.Variables[name], func(e *batchNodeEnvYAML, v string) {
			if e.Variables == nil {
				e.Variables = map[string]string{}
			}

			e.Variables[name] = v
		})
	}

	return axes
}

func cloneNodeEnvYAML(src *batchNodeEnvYAML) *batchNodeEnvYAML {
	out := &batchNodeEnvYAML{}
	if src == nil {
		return out
	}

	*out = *src
	if src.Variables != nil {
		out.Variables = make(map[string]string, len(src.Variables))
		maps.Copy(out.Variables, src.Variables)
	}

	return out
}

// expandBatchMatrix expands a build-matrix into one node per combination of
// its dynamic env values, layered over the static env.
func expandBatchMatrix(m batchMatrixYAML) ([]batchNodeYAML, error) {
	axes := matrixAxes(m.Dynamic.Env)

	total := 1
	for _, a := range axes {
		total *= len(a.values)
		if total > maxBatchMatrixBuilds {
			return nil, fmt.Errorf(
				"%w: build-matrix expands to more than %d builds", ErrValidation, maxBatchMatrixBuilds,
			)
		}
	}

	nodes := make([]batchNodeYAML, 0, total)

	for i := range total {
		env := cloneNodeEnvYAML(m.Static.Env)
		rem := i

		for _, a := range slices.Backward(axes) {
			a.apply(env, a.values[rem%len(a.values)])
			rem /= len(a.values)
		}

		nodes = append(nodes, batchNodeYAML{
			Identifier:    "build" + strconv.Itoa(i+1),
			Buildspec:     m.Dynamic.Buildspec,
			Env:           env,
			IgnoreFailure: m.Static.IgnoreFailure,
		})
	}

	return nodes, nil
}
