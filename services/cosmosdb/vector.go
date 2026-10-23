package cosmosdb

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
)

const (
	maxVectorDimensions = 4096
	maxVectorPaths      = 10

	vectorFnCosine     = "cosine"
	vectorFnDotProduct = "dotproduct"
	vectorFnEuclidean  = "euclidean"

	vectorIdxFlat          = "flat"
	vectorIdxQuantizedFlat = "quantizedFlat"
	vectorIdxDiskANN       = "diskANN"

	optDistanceFunction = "distanceFunction"
	optDataType         = "dataType"

	vectorOptionsArg = 3
	bruteForceArg    = 2
)

type vectorEmbedding struct {
	Path             string `json:"path"`
	DataType         string `json:"dataType"`
	DistanceFunction string `json:"distanceFunction"`
	Dimensions       int    `json:"dimensions"`
}

type vectorEmbeddingPolicy struct {
	VectorEmbeddings []vectorEmbedding `json:"vectorEmbeddings"`
}

func validVectorDataType(s string) bool {
	return slices.Contains([]string{"float16", "float32", "uint8", "int8"}, s)
}

func validVectorFunction(s string) bool {
	return slices.Contains([]string{vectorFnCosine, vectorFnDotProduct, vectorFnEuclidean}, s)
}

func validVectorIndexType(s string) bool {
	return slices.Contains([]string{vectorIdxFlat, vectorIdxQuantizedFlat, vectorIdxDiskANN}, s)
}

func parseVectorPolicy(raw json.RawMessage) (vectorEmbeddingPolicy, error) {
	var pol vectorEmbeddingPolicy
	if isJSONNull(raw) {
		return pol, nil
	}

	if err := json.Unmarshal(raw, &pol); err != nil {
		return pol, fmt.Errorf("%w: vectorEmbeddingPolicy is malformed", ErrInvalidContainerPolicy)
	}

	return pol, nil
}

func validateVectorPolicy(raw json.RawMessage) error {
	pol, err := parseVectorPolicy(raw)
	if err != nil {
		return err
	}

	if len(pol.VectorEmbeddings) > maxVectorPaths {
		return fmt.Errorf("%w: at most %d vector embeddings are allowed", ErrInvalidContainerPolicy, maxVectorPaths)
	}

	seen := make(map[string]struct{}, len(pol.VectorEmbeddings))

	for _, e := range pol.VectorEmbeddings {
		if err = validateVectorEmbedding(e, seen); err != nil {
			return err
		}
	}

	return nil
}

func validateVectorEmbedding(e vectorEmbedding, seen map[string]struct{}) error {
	switch {
	case !strings.HasPrefix(e.Path, "/") || len(e.Path) == 1:
		return fmt.Errorf("%w: vector embedding path %q must start with '/'", ErrInvalidContainerPolicy, e.Path)
	case !validVectorDataType(e.DataType):
		return fmt.Errorf("%w: vector dataType %q must be float16, float32, uint8 or int8",
			ErrInvalidContainerPolicy, e.DataType)
	case !validVectorFunction(e.DistanceFunction):
		return fmt.Errorf("%w: vector distanceFunction %q must be cosine, dotproduct or euclidean",
			ErrInvalidContainerPolicy, e.DistanceFunction)
	case e.Dimensions < 1 || e.Dimensions > maxVectorDimensions:
		return fmt.Errorf(
			"%w: vector dimensions must be between 1 and %d",
			ErrInvalidContainerPolicy,
			maxVectorDimensions,
		)
	}

	if _, dup := seen[e.Path]; dup {
		return fmt.Errorf("%w: duplicate vector embedding path %q", ErrInvalidContainerPolicy, e.Path)
	}

	seen[e.Path] = struct{}{}

	return nil
}

// validateVectorIndexes checks the indexing policy's vectorIndexes against the container's embedding policy.
func validateVectorIndexes(pol indexingPolicy, vp vectorEmbeddingPolicy) error {
	seen := make(map[string]struct{}, len(pol.VectorIndexes))

	for _, vi := range pol.VectorIndexes {
		if !validVectorIndexType(vi.Type) {
			return fmt.Errorf("%w: vector index type %q must be flat, quantizedFlat or diskANN",
				ErrInvalidContainerPolicy, vi.Type)
		}

		if !slices.ContainsFunc(vp.VectorEmbeddings, func(e vectorEmbedding) bool { return e.Path == vi.Path }) {
			return fmt.Errorf("%w: vector index path %q is not in the vectorEmbeddingPolicy",
				ErrInvalidContainerPolicy, vi.Path)
		}

		if _, dup := seen[vi.Path]; dup {
			return fmt.Errorf("%w: duplicate vector index path %q", ErrInvalidContainerPolicy, vi.Path)
		}

		seen[vi.Path] = struct{}{}
	}

	return nil
}

// floatVector converts a JSON array of numbers to float64s; ok is false for anything else, empty or oversized.
func floatVector(v any) ([]float64, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 || len(arr) > maxVectorDimensions {
		return nil, false
	}

	out := make([]float64, len(arr))

	for i, e := range arr {
		n, isNum := toNumber(e)
		if !isNum {
			return nil, false
		}

		out[i] = numFloat(n)
	}

	return out, true
}

func vectorScore(fn string, a, b []float64) (float64, bool) {
	var dot, na, nb, sq float64

	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
		sq += (a[i] - b[i]) * (a[i] - b[i])
	}

	switch fn {
	case vectorFnDotProduct:
		return dot, true
	case vectorFnEuclidean:
		return math.Sqrt(sq), true
	default:
		if na == 0 || nb == 0 {
			return 0, false
		}

		return dot / (math.Sqrt(na) * math.Sqrt(nb)), true
	}
}

// evalVectorDistance is VectorDistance(v1, v2 [, bruteForce [, {distanceFunction, dataType}]]): exact evaluation
// over the stored vectors whatever the index type.
func (ec *evalCtx) evalVectorDistance(n *exprCall, args []any) any {
	a, okA := floatVector(args[0])
	b, okB := floatVector(args[1])

	if !okA || !okB || len(a) != len(b) {
		return undef{}
	}

	if len(args) > bruteForceArg {
		if _, isBool := args[bruteForceArg].(bool); !isBool {
			return undef{}
		}
	}

	fn := ec.vectorMetric(n)
	if len(args) > vectorOptionsArg {
		var ok bool
		if fn, ok = vectorOptions(args[vectorOptionsArg], fn); !ok {
			return undef{}
		}
	}

	if !ec.coll.vectorDimsOK(n, len(a)) {
		return undef{}
	}

	score, ok := vectorScore(fn, a, b)
	if !ok {
		return undef{}
	}

	return floatResult(score)
}

// vectorOptions applies the options object, returning the distance function to use.
func vectorOptions(v any, def string) (string, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return def, false
	}

	if dt, has := m[optDataType]; has {
		s, isStr := dt.(string)
		if !isStr || !validVectorDataType(s) {
			return def, false
		}
	}

	if df, has := m[optDistanceFunction]; has {
		s, isStr := df.(string)
		if !isStr || !validVectorFunction(s) {
			return def, false
		}

		return s, true
	}

	return def, true
}

// vectorPathOf is the container path of the first VectorDistance argument that is one.
func vectorPathOf(n *exprCall) (string, bool) {
	for _, a := range n.args[:min(len(n.args), arityPair)] {
		if p, ok := rootPath(a); ok && p != "" {
			return p, true
		}
	}

	return "", false
}

// vectorMetric is the distance function VectorDistance uses: the options object when it names one statically,
// else the embedding policy's for the path, else cosine.
func (ec *evalCtx) vectorMetric(n *exprCall) string {
	if fn, ok := ec.optionMetric(n); ok {
		return fn
	}

	if path, ok := vectorPathOf(n); ok {
		if e, found := ec.coll.embedding(path); found {
			return e.DistanceFunction
		}
	}

	return vectorFnCosine
}

// optionMetric is the distance function the options object names statically.
func (ec *evalCtx) optionMetric(n *exprCall) (string, bool) {
	if len(n.args) <= vectorOptionsArg {
		return "", false
	}

	obj, ok := n.args[vectorOptionsArg].(*exprObjectLit)
	if !ok {
		return "", false
	}

	i := slices.Index(obj.keys, optDistanceFunction)
	if i < 0 {
		return "", false
	}

	s, isStr := ec.staticValue(obj.vals[i]).(string)

	return s, isStr && validVectorFunction(s)
}

// staticValue is a literal or bound parameter's value; anything else is undefined.
func (ec *evalCtx) staticValue(x sqlExpr) any {
	switch n := x.(type) {
	case *exprLit:
		return n.v
	case *exprParam:
		return ec.params[n.name]
	default:
		return undef{}
	}
}

// bruteForceSet reports whether the call passes a static bruteForce of true.
func (ec *evalCtx) bruteForceSet(n *exprCall) bool {
	if len(n.args) <= bruteForceArg {
		return false
	}

	b, _ := ec.staticValue(n.args[bruteForceArg]).(bool)

	return b
}
