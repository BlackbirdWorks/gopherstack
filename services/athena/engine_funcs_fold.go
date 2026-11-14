package athena

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
)

// udfFold runs a reduce_agg fold: args are the collected array, the initial state and the fold SQL
// (parameters ?1 and ?2). It exists because SQLite forbids aggregates inside the correlated fold subquery.
func udfFold(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	text, ok := asString(args[2])
	if !ok {
		return nil, fmt.Errorf("%w: fold query", errBadUnit)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	var out any
	if err = db.QueryRowContext(context.Background(), text, args[0], args[1]).Scan(&out); err != nil {
		return nil, err
	}

	if b, isBytes := out.([]byte); isBytes {
		return string(b), nil
	}

	return out, nil
}
