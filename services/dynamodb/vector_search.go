package dynamodb

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

const float32Bytes = 4

type vectorHit struct {
	item  map[string]any
	score float64
}

type vectorQuery struct {
	filter     *ParsedCondition
	eav        map[string]any
	names      map[string]string
	projection string
	condition  string
	vector     []float64
	topK       int
}

func newVectorQuery(input *dynamodb.SearchVectorsInput) (*vectorQuery, error) {
	vec := make([]float64, 0, len(input.SearchVector))

	for _, av := range input.SearchVector {
		n, ok := av.(*types.AttributeValueMemberN)
		if !ok {
			return nil, NewValidationException("SearchVector elements must be numbers")
		}

		f, err := strconv.ParseFloat(n.Value, 32)
		if err != nil {
			return nil, NewValidationException("SearchVector element is not a valid 32-bit float: " + n.Value)
		}

		vec = append(vec, f)
	}

	if aws.ToInt32(input.TopK) < 1 {
		return nil, NewValidationException("TopK must be at least 1")
	}

	q := &vectorQuery{
		vector:     vec,
		topK:       int(aws.ToInt32(input.TopK)),
		names:      input.ExpressionAttributeNames,
		eav:        models.FromSDKItem(input.ExpressionAttributeValues),
		projection: aws.ToString(input.ProjectionExpression),
		condition:  aws.ToString(input.SearchConditionExpression),
	}

	if q.condition != "" {
		if err := checkUndefinedExpressionAttributeNames(
			q.names,
			"SearchConditionExpression",
			q.condition,
		); err != nil {
			return nil, err
		}

		if err := checkUndefinedExpressionAttributeValues(q.eav, "SearchConditionExpression", q.condition); err != nil {
			return nil, err
		}

		parsed, err := ParseConditionStr(q.condition)
		if err != nil {
			return nil, NewValidationException("Invalid SearchConditionExpression: " + err.Error())
		}

		q.filter = parsed
	}

	return q, nil
}

func findVectorIndex(table *Table, name string) (models.VectorIndexDescription, bool) {
	for _, d := range table.VectorIndexes {
		if d.IndexName == name {
			return d, true
		}
	}

	return models.VectorIndexDescription{}, false
}

// bind checks the query against the index it targets.
func (q *vectorQuery) bind(desc models.VectorIndexDescription) error {
	if desc.IndexStatus != "" && desc.IndexStatus != string(types.IndexStatusActive) {
		return NewValidationException("Vector index " + desc.IndexName + " is not in the ACTIVE state")
	}

	if int64(len(q.vector)) != desc.Dimensions {
		return NewValidationException(fmt.Sprintf(
			"SearchVector has %d dimensions but vector index %s has %d",
			len(q.vector),
			desc.IndexName,
			desc.Dimensions,
		))
	}

	if desc.DistanceFunction == string(types.VectorDistanceFunctionCosine) && norm(q.vector) == 0 {
		return NewValidationException("SearchVector must not be a zero vector for the COSINE distance function")
	}

	return q.requireHashAttributes(desc)
}

// requireHashAttributes enforces that every HASH search-schema attribute is referenced by the condition.
func (q *vectorQuery) requireHashAttributes(desc models.VectorIndexDescription) error {
	for _, el := range desc.SearchSchema {
		if el.SearchSchemaElementType != string(types.SearchSchemaElementTypeHash) {
			continue
		}

		if !q.references(el.AttributeName) {
			return NewValidationException(
				"SearchConditionExpression must provide a value for HASH attribute " + el.AttributeName)
		}
	}

	return nil
}

func (q *vectorQuery) references(attr string) bool {
	if q.condition == "" {
		return false
	}

	if regexp.MustCompile(`(^|[^#:\w])` + regexp.QuoteMeta(attr) + `($|[^\w])`).MatchString(q.condition) {
		return true
	}

	for alias, name := range q.names {
		if name == attr && regexp.MustCompile(regexp.QuoteMeta(alias)+`($|[^\w])`).MatchString(q.condition) {
			return true
		}
	}

	return false
}

// rank scores every eligible item and returns the topK best, most similar first.
func (q *vectorQuery) rank(table *Table, desc models.VectorIndexDescription) []vectorHit {
	attr := desc.VectorAttribute.AttributeName
	hits := make([]vectorHit, 0, len(table.Items))

	for _, item := range table.Items {
		if isItemExpired(item, table.TTLAttribute) {
			continue
		}

		vec, ok := itemVector(item, attr, int(desc.Dimensions))
		if !ok {
			continue
		}

		score, scored := vectorScore(desc.DistanceFunction, q.vector, vec)
		if !scored {
			continue
		}

		if q.filter != nil && !q.filter.Evaluate(item, q.eav, q.names) {
			continue
		}

		hits = append(hits, vectorHit{item: item, score: score})
	}

	descending := desc.DistanceFunction == string(types.VectorDistanceFunctionDotProduct)

	sort.SliceStable(hits, func(i, j int) bool {
		if descending {
			return hits[i].score > hits[j].score
		}

		return hits[i].score < hits[j].score
	})

	if len(hits) > q.topK {
		hits = hits[:q.topK]
	}

	return hits
}

// project returns the index-projected attributes of item, narrowed by ProjectionExpression.
func (q *vectorQuery) project(
	table *Table,
	desc models.VectorIndexDescription,
	item map[string]any,
) (map[string]any, error) {
	base := projectedIndexItem(table, desc, item)

	if q.projection == "" {
		return base, nil
	}

	projector, err := ParseProjector(q.projection, q.names)
	if err != nil {
		return nil, NewValidationException("Invalid ProjectionExpression: " + err.Error())
	}

	return projector.Project(base), nil
}

func projectedIndexItem(table *Table, desc models.VectorIndexDescription, item map[string]any) map[string]any {
	if desc.Projection != nil && desc.Projection.ProjectionType == string(types.ProjectionTypeAll) {
		return deepCopyItem(item)
	}

	keep := []string{desc.VectorAttribute.AttributeName}
	for _, k := range table.KeySchema {
		keep = append(keep, k.AttributeName)
	}

	for _, el := range desc.SearchSchema {
		keep = append(keep, el.AttributeName)
	}

	if desc.Projection != nil {
		keep = append(keep, desc.Projection.NonKeyAttributes...)
	}

	out := make(map[string]any, len(keep))

	for _, name := range keep {
		if v, ok := item[name]; ok {
			out[name] = deepCopyValue(v)
		}
	}

	return out
}

func deepCopyValue(v any) any {
	return deepCopyItem(map[string]any{"v": v})["v"]
}

// itemVector parses item[attr] as a list of exactly dims numbers.
func itemVector(item map[string]any, attr string, dims int) ([]float64, bool) {
	av, ok := item[attr].(map[string]any)
	if !ok {
		return nil, false
	}

	list, ok := av["L"].([]any)
	if !ok || len(list) != dims {
		return nil, false
	}

	vec := make([]float64, dims)

	for i, el := range list {
		m, isMap := el.(map[string]any)
		if !isMap {
			return nil, false
		}

		s, isStr := m["N"].(string)
		if !isStr {
			return nil, false
		}

		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, false
		}

		vec[i] = f
	}

	return vec, true
}

func norm(v []float64) float64 {
	var sum float64
	for _, x := range v {
		sum += x * x
	}

	return math.Sqrt(sum)
}

// vectorScore returns the score documented on SearchVectors for fn: cosine
// distance in [0,2], Euclidean distance, or raw dot product.
func vectorScore(fn string, query, vec []float64) (float64, bool) {
	var dot, dist float64

	for i := range query {
		dot += query[i] * vec[i]

		d := query[i] - vec[i]
		dist += d * d
	}

	switch fn {
	case string(types.VectorDistanceFunctionDotProduct):
		return dot, true
	case string(types.VectorDistanceFunctionEuclidean):
		return math.Sqrt(dist), true
	case string(types.VectorDistanceFunctionCosine):
		denom := norm(query) * norm(vec)
		if denom == 0 {
			return 0, false
		}

		return 1 - dot/denom, true
	default:
		return 0, false
	}
}
