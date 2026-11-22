package athena

import "database/sql"

// TranslateTrino rewrites Presto/Trino SQL onto the SQLite engine. resolve maps a
// table reference to the SQLite schema and table it was loaded into; unresolved
// references are returned by name.
func TranslateTrino(
	query string, resolve func(parts []string) (schema, name string, ok bool),
) (string, []string, error) {
	return translateSQLFor(query, wrapResolve(resolve), false, nil)
}

// TranslateEmbedded is TranslateTrino for a service that embeds the engine: names in owned are left
// for the host to resolve, and functions Athena adds beyond the shared set stay unresolved.
func TranslateEmbedded(
	query string, resolve func(parts []string) (schema, name string, ok bool), owned map[string]struct{},
) (string, []string, error) {
	return translateSQLFor(query, wrapResolve(resolve), true, owned)
}

func wrapResolve(resolve func(parts []string) (schema, name string, ok bool)) func([]string) resolvedRef {
	return func(parts []string) resolvedRef {
		schema, name, ok := resolve(parts)

		return resolvedRef{schema: schema, name: name, found: ok}
	}
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
