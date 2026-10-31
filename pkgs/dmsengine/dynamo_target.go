package dmsengine

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var errNoDynamoKey = errors.New("DynamoDB target requires a source primary key")

type dynamoTarget struct{ store DynamoStore }

func newDynamoTarget(store DynamoStore) (Target, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: DynamoDB", ErrNotConfigured)
	}

	return &dynamoTarget{store: store}, nil
}

func (t *dynamoTarget) Ping(context.Context) error { return nil }

func (t *dynamoTarget) Close() error { return nil }

func (t *dynamoTarget) EndLoad(context.Context, TargetTable) error { return nil }

func (t *dynamoTarget) Commit(context.Context) error { return nil }

func isNumeric(k Kind) bool { return k == KindInt || k == KindDecimal || k == KindFloat }

func (t *dynamoTarget) Prepare(ctx context.Context, tt TargetTable, mode PrepMode) error {
	if len(tt.PK) == 0 || len(tt.PK) > 2 {
		return fmt.Errorf("%w: %s", errNoDynamoKey, tt.Key())
	}

	keys := make([]DynamoKey, len(tt.PK))

	for i, name := range tt.PK {
		for _, c := range tt.Columns {
			if c.Name == name {
				keys[i] = DynamoKey{Name: name, Number: isNumeric(c.Kind)}
			}
		}
	}

	if mode != PrepDoNothing {
		_ = t.store.DropTable(ctx, tt.Name)
	}

	err := t.store.CreateTable(ctx, tt.Name, keys)
	if err != nil && mode == PrepDoNothing && strings.Contains(strings.ToLower(err.Error()), "exist") {
		return nil
	}

	return err
}

func dynamoValue(v any, k Kind) (DynamoValue, bool) {
	switch x := v.(type) {
	case nil:
		return DynamoValue{}, false
	case []byte:
		return DynamoValue{Type: "B", B: x}, true
	case string:
		if isNumeric(k) {
			return DynamoValue{Type: "N", S: x}, true
		}

		return DynamoValue{Type: "S", S: x}, true
	}

	return DynamoValue{Type: "S", S: fmt.Sprint(v)}, true
}

func (t *dynamoTarget) item(tt TargetTable, row Row, keyOnly bool) map[string]DynamoValue {
	out := map[string]DynamoValue{}

	for i, c := range tt.Columns {
		if keyOnly && !slicesContain(tt.PK, c.Name) {
			continue
		}

		if v, ok := dynamoValue(row[i], c.Kind); ok {
			out[c.Name] = v
		}
	}

	return out
}

func (t *dynamoTarget) Load(ctx context.Context, tt TargetTable, rows []Row) error {
	for _, r := range rows {
		if err := t.store.PutItem(ctx, tt.Name, t.item(tt, r, false)); err != nil {
			return err
		}
	}

	return nil
}

func (t *dynamoTarget) Apply(ctx context.Context, tt TargetTable, c Change) error {
	if c.Op == OpDelete {
		return t.store.DeleteItem(ctx, tt.Name, t.item(tt, c.Before, true))
	}

	if c.Op == OpUpdate && c.Before != nil && !sameKey(tt, c.Before, c.After) {
		if err := t.store.DeleteItem(ctx, tt.Name, t.item(tt, c.Before, true)); err != nil {
			return err
		}
	}

	return t.store.PutItem(ctx, tt.Name, t.item(tt, c.After, false))
}

func sameKey(tt TargetTable, a, b Row) bool {
	for i, c := range tt.Columns {
		if slicesContain(tt.PK, c.Name) && fmt.Sprint(a[i]) != fmt.Sprint(b[i]) {
			return false
		}
	}

	return true
}
