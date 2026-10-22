package azuresql

// Exported for the external tests.

func CreateDatabaseSQL(name, collation string) (string, error) {
	return createDatabaseSQL(name, collation)
}

func DropDatabaseSQL(name string) (string, error) { return dropDatabaseSQL(name) }

func LoginSQL(login, password string) string { return loginSQL(login, password) }
