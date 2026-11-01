package glue

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	mongoTimeout        = 30 * time.Second
	mongoSampleDocs     = 1000
	mongoClassification = "mongodb"
	mongoConnType       = "MONGODB"
	mongoKindStruct     = "struct"
	mongoKindArray      = "array"
)

var (
	errMongoConnection = errors.New("MongoDB target needs a connection of type MONGODB")
	errMongoNoURL      = errors.New("connection has no CONNECTION_URL")
	errMongoPath       = errors.New("MongoDB path must be database/collection")
)

type mongoType struct {
	fields map[string]*mongoType
	elem   *mongoType
	kind   string
	order  []string
}

func (t *mongoType) render() string {
	switch t.kind {
	case mongoKindStruct:
		parts := make([]string, 0, len(t.order))
		for _, n := range t.order {
			parts = append(parts, n+":"+t.fields[n].render())
		}

		return "struct<" + strings.Join(parts, ",") + ">"
	case mongoKindArray:
		if t.elem == nil {
			return "array<string>"
		}

		return "array<" + t.elem.render() + ">"
	default:
		return t.kind
	}
}

func isMongoNumeric(k string) bool { return k == avroInt || k == typeBigint || k == avroDouble }

func mergeMongoType(a, b *mongoType) *mongoType {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case a.kind != b.kind:
		return mergeMongoScalars(a, b)
	case a.kind == mongoKindStruct:
		for _, n := range b.order {
			if _, ok := a.fields[n]; !ok {
				a.order = append(a.order, n)
			}

			a.fields[n] = mergeMongoType(a.fields[n], b.fields[n])
		}
	case a.kind == mongoKindArray:
		a.elem = mergeMongoType(a.elem, b.elem)
	}

	return a
}

func mergeMongoScalars(a, b *mongoType) *mongoType {
	if !isMongoNumeric(a.kind) || !isMongoNumeric(b.kind) {
		return &mongoType{kind: avroString}
	}

	if a.kind == avroDouble || b.kind == avroDouble {
		return &mongoType{kind: avroDouble}
	}

	return &mongoType{kind: typeBigint}
}

func inferMongoDoc(pairs []bson.E) *mongoType {
	t := &mongoType{kind: mongoKindStruct, fields: map[string]*mongoType{}}

	for _, e := range pairs {
		if ft := inferMongoValue(e.Value); ft != nil {
			t.order = append(t.order, e.Key)
			t.fields[e.Key] = ft
		}
	}

	return t
}

func inferMongoValue(v any) *mongoType {
	switch x := v.(type) {
	case nil:
		return nil
	case bson.D, bson.M, bson.A, bson.ObjectID:
		return inferMongoComposite(x)
	default:
		return &mongoType{kind: mongoScalarKind(v)}
	}
}

func mongoScalarKind(v any) string {
	switch v.(type) {
	case int32:
		return avroInt
	case int, int64:
		return typeBigint
	case float64:
		return avroDouble
	case bool:
		return avroBoolean
	case bson.DateTime, time.Time, bson.Timestamp:
		return typeTimestamp
	case bson.Binary:
		return typeBinary
	default:
		return avroString
	}
}

func inferMongoComposite(v any) *mongoType {
	switch x := v.(type) {
	case bson.D:
		return inferMongoDoc(x)
	case bson.M:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}

		slices.Sort(keys)

		pairs := make([]bson.E, len(keys))
		for i, k := range keys {
			pairs[i] = bson.E{Key: k, Value: x[k]}
		}

		return inferMongoDoc(pairs)
	case bson.A:
		arr := &mongoType{kind: mongoKindArray}
		for _, el := range x {
			arr.elem = mergeMongoType(arr.elem, inferMongoValue(el))
		}

		return arr
	default:
		return &mongoType{
			kind: mongoKindStruct, order: []string{"oid"},
			fields: map[string]*mongoType{"oid": {kind: avroString}},
		}
	}
}

func isMongoSystemDB(name string) bool {
	switch name {
	case "admin", "local", "config":
		return true
	default:
		return false
	}
}

func mongoPattern(p string) *regexp.Regexp {
	parts := strings.Split(p, "%")
	for i := range parts {
		parts[i] = regexp.QuoteMeta(parts[i])
	}

	return regexp.MustCompile("^" + strings.Join(parts, ".*") + "$")
}

func (r *crawlRun) mongoClient(ctx context.Context, conn *Connection) (*mongo.Client, error) {
	if !strings.EqualFold(conn.ConnectionType, mongoConnType) {
		return nil, fmt.Errorf("%w: %q is %s", errMongoConnection, conn.Name, conn.ConnectionType)
	}

	props := conn.ConnectionProperties

	uri := props["CONNECTION_URL"]
	if uri == "" {
		return nil, errMongoNoURL
	}

	user, pass := props["USERNAME"], props["PASSWORD"]

	if props["SECRET_ID"] != "" {
		var err error
		if user, pass, err = r.resolveSecretCredentials(ctx, props["SECRET_ID"]); err != nil {
			return nil, err
		}
	}

	opts := options.Client().SetServerSelectionTimeout(mongoTimeout).ApplyURI(uri)
	if user != "" {
		opts.SetAuth(options.Credential{Username: user, Password: pass})
	}

	return mongo.Connect(opts)
}

func (r *crawlRun) crawlMongoDB(t MongoDBTarget) error {
	conn, ok := r.conns[t.ConnectionName]
	if !ok {
		return fmt.Errorf("%w: %q", errJDBCNoConnection, t.ConnectionName)
	}

	dbPat, collPat, ok := strings.Cut(strings.Trim(t.Path, "/"), "/")
	if !ok || dbPat == "" || collPat == "" {
		return fmt.Errorf("%w: %q", errMongoPath, t.Path)
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()

	client, err := r.mongoClient(ctx, conn)
	if err != nil {
		return err
	}

	defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()

	dbs, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("connect to MongoDB data store: %w", err)
	}

	dbRe, collRe := mongoPattern(dbPat), mongoPattern(collPat)

	for _, dbName := range dbs {
		if isMongoSystemDB(dbName) || !dbRe.MatchString(dbName) {
			continue
		}

		if err = r.crawlMongoDatabase(ctx, client.Database(dbName), collRe, t); err != nil {
			return err
		}
	}

	return nil
}

func (r *crawlRun) crawlMongoDatabase(
	ctx context.Context, db *mongo.Database, collRe *regexp.Regexp, t MongoDBTarget,
) error {
	names, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("list MongoDB collections of %s: %w", db.Name(), err)
	}

	slices.Sort(names)

	for _, name := range names {
		if strings.HasPrefix(name, "system.") || !collRe.MatchString(name) {
			continue
		}

		if err = r.crawlMongoCollection(ctx, db, name, t); err != nil {
			return err
		}
	}

	return nil
}

func (r *crawlRun) crawlMongoCollection(ctx context.Context, db *mongo.Database, name string, t MongoDBTarget) error {
	coll := db.Collection(name)

	findOpts := options.Find()
	if !t.ScanAll {
		findOpts.SetLimit(mongoSampleDocs)
	}

	cur, err := coll.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		return fmt.Errorf("read MongoDB collection %s.%s: %w", db.Name(), name, err)
	}
	defer cur.Close(ctx)

	var merged *mongoType

	for cur.Next(ctx) {
		var doc bson.D
		if err = cur.Decode(&doc); err != nil {
			return fmt.Errorf("decode MongoDB document of %s.%s: %w", db.Name(), name, err)
		}

		merged = mergeMongoType(merged, inferMongoDoc(doc))
	}

	if err = cur.Err(); err != nil {
		return fmt.Errorf("read MongoDB collection %s.%s: %w", db.Name(), name, err)
	}

	if merged == nil || len(merged.order) == 0 {
		return nil
	}

	cols := make([]Column, 0, len(merged.order))
	for _, n := range merged.order {
		cols = append(cols, Column{Name: n, Type: merged.fields[n].render()})
	}

	count, _ := coll.EstimatedDocumentCount(ctx)
	params := map[string]string{
		paramClassification: mongoClassification, paramTypeOfData: typeOfDataTable, crawlerManagedParam: r.c.Name,
		paramConnectionName: t.ConnectionName, "recordCount": strconv.FormatInt(count, 10),
	}

	r.upsert(&Table{
		Name:         crawlTableName(r.c.TablePrefix, db.Name()+"_"+name),
		DatabaseName: strs.Fold(r.c.DatabaseName), Parameters: params,
		StorageDescriptor: StorageDescriptor{
			Columns: cols, Location: db.Name() + "." + name,
			Parameters: map[string]string{paramConnectionName: t.ConnectionName, crawlerManagedParam: r.c.Name},
		},
	})

	return nil
}
