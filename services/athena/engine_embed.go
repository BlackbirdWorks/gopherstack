package athena

import "database/sql"

// TranslateTrino rewrites Presto/Trino SQL onto the SQLite engine. resolve maps a
// table reference to the SQLite schema and table it was loaded into; unresolved
// references are returned by name.
func TranslateTrino(
	query string, resolve func(parts []string) (schema, name string, ok bool),
) (string, []string, error) {
	return translateSQL(query, func(parts []string) resolvedRef {
		schema, name, ok := resolve(parts)

		return resolvedRef{schema: schema, name: name, found: ok}
	})
}

// OpenEngineDB opens a private in-memory SQLite database with the Trino function set registered.
func OpenEngineDB() (*sql.DB, error) {
	registerEngineFuncs()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)

	return db, nil
}
