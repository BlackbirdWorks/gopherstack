package glue

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const icebergMetadataDir = "metadata"

var (
	errIcebergNoMetadata = errors.New("no metadata.json file")
	errIcebergNoSchema   = errors.New("iceberg metadata carries no current schema")

	icebergVersionRe = regexp.MustCompile(`^(?:v)?(\d+)(?:-[0-9a-fA-F-]+)?\.metadata\.json$`)
)

func isIcebergMetadata(file string) bool { return icebergVersionRe.MatchString(file) }

func (r *crawlRun) crawlIceberg(t IcebergTarget) error {
	ctx, cancel, err := r.lakeContext()
	if err != nil {
		return err
	}
	defer cancel()

	for _, p := range t.Paths {
		bucket, prefix, ok := splitCrawlS3URI(p)
		if !ok {
			return fmt.Errorf("%w %q", errInvalidCrawlPath, p)
		}

		keys, lerr := r.b.listKeys(ctx, bucket, prefix)
		if lerr != nil {
			return lerr
		}

		for _, root := range lakeRoots(keys, prefix, icebergMetadataDir, isIcebergMetadata,
			int(t.MaximumTraversalDepth), t.Exclusions) {
			if err = r.crawlIcebergTable(ctx, bucket, root, keys); err != nil {
				return fmt.Errorf("iceberg table s3://%s/%s: %w", bucket, root, err)
			}
		}
	}

	return nil
}

func latestIcebergMetadata(ctx context.Context, r *crawlRun, bucket, root string, keys []string) (string, error) {
	metaDir := joinKey(root, icebergMetadataDir)
	best, bestVer := "", int64(-1)

	for _, k := range keys {
		dir, file := splitKey(k)
		if dir != metaDir {
			continue
		}

		m := icebergVersionRe.FindStringSubmatch(file)
		if m == nil {
			continue
		}

		if v, _ := strconv.ParseInt(m[1], 10, 64); v > bestVer || (v == bestVer && k > best) {
			best, bestVer = k, v
		}
	}

	if hint, err := r.b.readObject(ctx, bucket, metaDir+"/version-hint.text"); err == nil {
		want := strings.TrimSpace(string(hint))
		for _, k := range keys {
			dir, file := splitKey(k)
			if m := icebergVersionRe.FindStringSubmatch(file); dir == metaDir && m != nil && m[1] == want {
				return k, nil
			}
		}
	}

	if best == "" {
		return "", errIcebergNoMetadata
	}

	return best, nil
}

func (r *crawlRun) crawlIcebergTable(ctx context.Context, bucket, root string, keys []string) error {
	metaKey, err := latestIcebergMetadata(ctx, r, bucket, root, keys)
	if err != nil {
		return err
	}

	body, err := r.b.readObject(ctx, bucket, metaKey)
	if err != nil {
		return err
	}

	var meta struct {
		Schema          *icebergSchema  `json:"schema"`
		Location        string          `json:"location"`
		Schemas         []icebergSchema `json:"schemas"`
		CurrentSchemaID int             `json:"current-schema-id"`
	}

	if err = decodeJSON(body, &meta); err != nil {
		return fmt.Errorf("parse %s: %w", metaKey, err)
	}

	schema := meta.Schema

	for i := range meta.Schemas {
		if meta.Schemas[i].ID == meta.CurrentSchemaID {
			schema = &meta.Schemas[i]
		}
	}

	if schema == nil {
		return errIcebergNoSchema
	}

	location := meta.Location
	if location == "" {
		location = "s3://" + bucket + "/" + root
	}

	cols := make([]Column, len(schema.Fields))
	for i, f := range schema.Fields {
		cols[i] = Column{Name: f.Name, Type: icebergType(f.Type), Parameters: map[string]string{
			"iceberg.field.id": strconv.Itoa(f.ID), "iceberg.field.optional": strconv.FormatBool(!f.Required),
			"iceberg.field.current": paramTrue,
		}}
	}

	params := map[string]string{
		"table_type": "ICEBERG", "metadata_location": "s3://" + bucket + "/" + metaKey,
		crawlerManagedParam: r.c.Name,
	}

	r.upsert(&Table{
		Name:         crawlTableName(r.c.TablePrefix, lastPathSegment(root)),
		DatabaseName: r.c.DatabaseName,
		Parameters:   params,
		StorageDescriptor: StorageDescriptor{
			Columns:    cols,
			Location:   location,
			Parameters: map[string]string{crawlerManagedParam: r.c.Name},
		},
	})

	return nil
}

type icebergSchema struct {
	Fields []struct {
		Type     any    `json:"type"`
		Name     string `json:"name"`
		ID       int    `json:"id"`
		Required bool   `json:"required"`
	} `json:"fields"`
	ID int `json:"schema-id"`
}

func icebergType(t any) string {
	switch v := t.(type) {
	case string:
		return icebergPrimitive(v)
	case map[string]any:
		switch v["type"] {
		case "struct":
			fields, _ := v["fields"].([]any)
			parts := make([]string, 0, len(fields))

			for _, f := range fields {
				fm, _ := f.(map[string]any)
				name, _ := fm["name"].(string)
				parts = append(parts, name+":"+icebergType(fm["type"]))
			}

			return "struct<" + strings.Join(parts, ",") + ">"
		case "list":
			return "array<" + icebergType(v["element"]) + ">"
		case avroMap:
			return "map<" + icebergType(v["key"]) + "," + icebergType(v["value"]) + ">"
		}
	}

	return avroString
}

func icebergPrimitive(t string) string {
	switch {
	case t == avroLong:
		return typeBigint
	case t == typeTimestamp, t == "timestamptz", t == "timestamp_ns", t == "timestamptz_ns":
		return typeTimestamp
	case t == typeBinary, strings.HasPrefix(t, "fixed"):
		return typeBinary
	case strings.HasPrefix(t, "decimal"):
		return t
	case t == avroBoolean, t == avroInt, t == avroFloat, t == avroDouble, t == typeDate:
		return t
	default:
		return avroString
	}
}
