package glue

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	ddbSampleItems    = 1000
	ddbInputFormat    = "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat"
	ddbOutputFormat   = "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat"
	ddbSerde          = "org.apache.hadoop.hive.dynamodb.DynamoDBSerDe"
	ddbClassification = "dynamodb"
	kindArray         = "array"
	kindStruct        = "struct"
	typeBinary        = "binary"
)

var errDDBUnavailable = errors.New("DynamoDB is not available to crawl")

// CrawlerDynamoDB is the DynamoDB side a crawler needs to infer table schemas.
type CrawlerDynamoDB interface {
	DescribeTable(ctx context.Context, in *sdk_ddb.DescribeTableInput) (*sdk_ddb.DescribeTableOutput, error)
	Scan(ctx context.Context, in *sdk_ddb.ScanInput) (*sdk_ddb.ScanOutput, error)
}

// SetDynamoDBBackend wires DynamoDB so crawlers can infer schemas from the
// emulator's own tables.
func (b *InMemoryBackend) SetDynamoDBBackend(d CrawlerDynamoDB) {
	b.mu.Lock("SetDynamoDBBackend")
	defer b.mu.Unlock()

	b.crawlerDDB = d
}

func (b *InMemoryBackend) currentCrawlerDDB() CrawlerDynamoDB {
	b.mu.RLock("currentCrawlerDDB")
	defer b.mu.RUnlock()

	return b.crawlerDDB
}

// ddbType is an inferred attribute type: a scalar, array or struct.
type ddbType struct {
	fields map[string]*ddbType
	elem   *ddbType
	kind   string
}

func scalarKind(k string) *ddbType { return &ddbType{kind: k} }

func (t *ddbType) render() string {
	switch t.kind {
	case kindArray:
		if t.elem == nil {
			return "array<string>"
		}

		return "array<" + t.elem.render() + ">"
	case kindStruct:
		names := make([]string, 0, len(t.fields))
		for n := range t.fields {
			names = append(names, n)
		}

		sort.Strings(names)

		parts := make([]string, len(names))
		for i, n := range names {
			parts[i] = n + ":" + t.fields[n].render()
		}

		return "struct<" + strings.Join(parts, ",") + ">"
	default:
		return t.kind
	}
}

func mergeDDBType(a, b *ddbType) *ddbType {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case a.kind != b.kind:
		if isNumericKind(a.kind) && isNumericKind(b.kind) {
			return scalarKind(avroDouble)
		}

		return scalarKind(avroString)
	case a.kind == kindArray:
		return &ddbType{kind: kindArray, elem: mergeDDBType(a.elem, b.elem)}
	case a.kind == kindStruct:
		out := &ddbType{kind: kindStruct, fields: maps.Clone(a.fields)}
		for n, t := range b.fields {
			out.fields[n] = mergeDDBType(out.fields[n], t)
		}

		return out
	default:
		return a
	}
}

func isNumericKind(k string) bool { return k == typeBigint || k == avroDouble }

func numberKind(n string) string {
	if _, err := strconv.ParseInt(n, 10, 64); err == nil {
		return typeBigint
	}

	return avroDouble
}

func inferDDBValue(v ddbtypes.AttributeValue) *ddbType {
	switch x := v.(type) {
	case *ddbtypes.AttributeValueMemberS:
		return scalarKind(avroString)
	case *ddbtypes.AttributeValueMemberN:
		return scalarKind(numberKind(x.Value))
	case *ddbtypes.AttributeValueMemberB:
		return scalarKind(typeBinary)
	case *ddbtypes.AttributeValueMemberBOOL:
		return scalarKind(avroBoolean)
	case *ddbtypes.AttributeValueMemberSS:
		return &ddbType{kind: kindArray, elem: scalarKind(avroString)}
	case *ddbtypes.AttributeValueMemberNS:
		var elem *ddbType
		for _, n := range x.Value {
			elem = mergeDDBType(elem, scalarKind(numberKind(n)))
		}

		return &ddbType{kind: kindArray, elem: elem}
	case *ddbtypes.AttributeValueMemberBS:
		return &ddbType{kind: kindArray, elem: scalarKind(typeBinary)}
	case *ddbtypes.AttributeValueMemberL:
		var elem *ddbType
		for _, e := range x.Value {
			elem = mergeDDBType(elem, inferDDBValue(e))
		}

		return &ddbType{kind: kindArray, elem: elem}
	case *ddbtypes.AttributeValueMemberM:
		st := &ddbType{kind: kindStruct, fields: map[string]*ddbType{}}
		for n, e := range x.Value {
			if t := inferDDBValue(e); t != nil {
				st.fields[n] = t
			}
		}

		return st
	default:
		return nil
	}
}

func ddbKeyTypes(attrs []ddbtypes.AttributeDefinition) map[string]*ddbType {
	out := map[string]*ddbType{}

	for _, a := range attrs {
		switch a.AttributeType {
		case ddbtypes.ScalarAttributeTypeN:
			out[aws.ToString(a.AttributeName)] = scalarKind(typeBigint)
		case ddbtypes.ScalarAttributeTypeB:
			out[aws.ToString(a.AttributeName)] = scalarKind(typeBinary)
		default:
			out[aws.ToString(a.AttributeName)] = scalarKind(avroString)
		}
	}

	return out
}

func (r *crawlRun) scanDDBTypes(
	ctx context.Context, table string, t DynamoDBTarget, types map[string]*ddbType,
) error {
	var start map[string]ddbtypes.AttributeValue

	seen := 0

	for {
		in := &sdk_ddb.ScanInput{TableName: aws.String(table), ExclusiveStartKey: start}
		if !t.ScanAll {
			in.Limit = aws.Int32(ddbSampleItems)
		}

		out, err := r.ddb.Scan(ctx, in)
		if err != nil {
			return fmt.Errorf("scan dynamodb table %s: %w", table, err)
		}

		for _, item := range out.Items {
			seen++

			for n, v := range item {
				if ty := inferDDBValue(v); ty != nil {
					types[n] = mergeDDBType(types[n], ty)
				}
			}
		}

		if len(out.LastEvaluatedKey) == 0 || (!t.ScanAll && seen >= ddbSampleItems) {
			return nil
		}

		start = out.LastEvaluatedKey
	}
}

func (r *crawlRun) describeDDBTable(ctx context.Context, t DynamoDBTarget) ([]Column, map[string]string, error) {
	desc, err := r.ddb.DescribeTable(ctx, &sdk_ddb.DescribeTableInput{TableName: aws.String(t.Path)})
	if err != nil {
		return nil, nil, fmt.Errorf("describe dynamodb table %s: %w", t.Path, err)
	}

	tbl := desc.Table
	types := ddbKeyTypes(tbl.AttributeDefinitions)

	if err = r.scanDDBTypes(ctx, t.Path, t, types); err != nil {
		return nil, nil, err
	}

	keys := make([]string, 0, len(tbl.KeySchema))
	for _, k := range tbl.KeySchema {
		keys = append(keys, aws.ToString(k.AttributeName))
	}

	names := make([]string, 0, len(types))
	for n := range types {
		if !slices.Contains(keys, n) {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	cols := make([]Column, 0, len(types))
	for _, n := range append(keys, names...) {
		if ty, ok := types[n]; ok {
			cols = append(cols, Column{Name: n, Type: ty.render()})
		}
	}

	params := map[string]string{
		"recordCount": strconv.FormatInt(aws.ToInt64(tbl.ItemCount), 10),
		"sizeKey":     strconv.FormatInt(aws.ToInt64(tbl.TableSizeBytes), 10),
	}

	return cols, params, nil
}

func (r *crawlRun) crawlDynamoDB(t DynamoDBTarget) error {
	if r.ddb == nil {
		return fmt.Errorf("%w: %q", errDDBUnavailable, t.Path)
	}

	ctx, cancel := context.WithTimeout(awsmeta.WithRegion(context.Background(), r.region), crawlerS3Timeout)
	defer cancel()

	cols, params, err := r.describeDDBTable(ctx, t)
	if err != nil {
		return err
	}

	params[paramClassification] = ddbClassification
	params[paramTypeOfData] = typeOfDataTable
	params[crawlerManagedParam] = r.c.Name

	r.upsert(&Table{
		Name: crawlTableName(r.c.TablePrefix, t.Path), DatabaseName: strs.Fold(r.c.DatabaseName),
		Parameters: params,
		StorageDescriptor: StorageDescriptor{
			Columns: cols, Location: t.Path, InputFormat: ddbInputFormat, OutputFormat: ddbOutputFormat,
			SerdeInfo:  &SerDeInfo{SerializationLibrary: ddbSerde},
			Parameters: map[string]string{paramClassification: ddbClassification, crawlerManagedParam: r.c.Name},
		},
	})

	return nil
}
