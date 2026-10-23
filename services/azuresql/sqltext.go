package azuresql

import (
	"fmt"
	"strings"
)

// nLiteral renders s as a T-SQL Unicode string literal. Identifiers and passwords are then quoted server-side
// with QUOTENAME, so no client-built text is ever used as an identifier.
func nLiteral(s string) string { return "N'" + strings.ReplaceAll(s, "'", "''") + "'" }

func loginSQL(login, password string) string {
	return "DECLARE @l sysname = " + nLiteral(login) + ", @p nvarchar(128) = " + nLiteral(password) +
		", @s nvarchar(max); " +
		"IF SUSER_ID(@l) IS NULL SET @s = N'CREATE LOGIN ' + QUOTENAME(@l) + N' WITH PASSWORD = ' + " +
		"QUOTENAME(@p, '''') + N', CHECK_POLICY = OFF' " +
		"ELSE SET @s = N'ALTER LOGIN ' + QUOTENAME(@l) + N' WITH PASSWORD = ' + QUOTENAME(@p, ''''); " +
		"EXEC(@s); " +
		"SET @s = N'ALTER SERVER ROLE sysadmin ADD MEMBER ' + QUOTENAME(@l); EXEC(@s);"
}

func createDatabaseSQL(name, collation string) (string, error) {
	if !validIdent(name) {
		return "", fmt.Errorf("%w: database name", ErrInvalid)
	}

	clause := ""

	if collation != "" {
		if !collationRe.MatchString(collation) {
			return "", fmt.Errorf("%w: collation %q", ErrInvalid, collation)
		}

		clause = " + N' COLLATE " + collation + "'"
	}

	return "DECLARE @n sysname = " + nLiteral(name) + ", @s nvarchar(max); " +
		"IF DB_ID(@n) IS NULL BEGIN SET @s = N'CREATE DATABASE ' + QUOTENAME(@n)" + clause + "; EXEC(@s); END", nil
}

func dropDatabaseSQL(name string) (string, error) {
	if !validIdent(name) {
		return "", fmt.Errorf("%w: database name", ErrInvalid)
	}

	return "DECLARE @n sysname = " + nLiteral(name) + ", @s nvarchar(max); " +
		"IF DB_ID(@n) IS NOT NULL BEGIN " +
		"SET @s = N'ALTER DATABASE ' + QUOTENAME(@n) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE'; EXEC(@s); " +
		"SET @s = N'DROP DATABASE ' + QUOTENAME(@n); EXEC(@s); END", nil
}

const listDatabasesSQL = "SET NOCOUNT ON; SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name"
