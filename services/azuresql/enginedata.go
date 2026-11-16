package azuresql

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxBackupBytes      = 64 << 20
	maxSnapshotBytes    = 256 << 20
	uploadChunkBytes    = 72 << 10
	backupDir           = "/var/opt/mssql/data/"
	tokenBytes          = 8
	backupFileSuffix    = ".bak"
	encodedFileSuffix   = ".b64"
	fileSizeScript      = `stat -c %s "$1"`
	fileReadScript      = `base64 -w0 "$1"`
	fileAppendScript    = `printf %s "$2" >> "$1"`
	fileDecodeScript    = `base64 -d "$1" > "$2" && sha256sum "$2"`
	fileRemoveScript    = `rm -f "$1" "$2"`
	backupOptionsClause = " WITH INIT, COPY_ONLY, COMPRESSION, FORMAT"
)

// DataEngine is an Engine that can export and import the content of a database, so snapshots keep engine data.
type DataEngine interface {
	Engine
	// BackupDatabase returns a backup of the database; it fails when the backup would exceed maxBytes.
	BackupDatabase(ctx context.Context, name string, maxBytes int64) ([]byte, error)
	// RestoreDatabase replaces the database with the content of a backup taken by BackupDatabase.
	RestoreDatabase(ctx context.Context, name string, data []byte) error
}

func backupSQL(name, path string) (string, error) {
	if !validIdent(name) {
		return "", fmt.Errorf("%w: database name", ErrInvalid)
	}

	return "DECLARE @n sysname = " + nLiteral(name) + ", @s nvarchar(max); " +
		"SET @s = N'BACKUP DATABASE ' + QUOTENAME(@n) + N' TO DISK = ' + QUOTENAME(" + nLiteral(path) +
		", '''') + N'" + backupOptionsClause + "'; EXEC(@s);", nil
}

func restoreSQL(name, path string) (string, error) {
	if !validIdent(name) {
		return "", fmt.Errorf("%w: database name", ErrInvalid)
	}

	return "DECLARE @n sysname = " + nLiteral(name) + ", @s nvarchar(max); " +
		"SET @s = N'ALTER DATABASE ' + QUOTENAME(@n) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE'; " +
		"IF DB_ID(@n) IS NOT NULL EXEC(@s); " +
		"SET @s = N'RESTORE DATABASE ' + QUOTENAME(@n) + N' FROM DISK = ' + QUOTENAME(" + nLiteral(path) +
		", '''') + N' WITH REPLACE'; EXEC(@s); " +
		"SET @s = N'ALTER DATABASE ' + QUOTENAME(@n) + N' SET MULTI_USER'; EXEC(@s);", nil
}

func multiUserSQL(name string) (string, error) {
	if !validIdent(name) {
		return "", fmt.Errorf("%w: database name", ErrInvalid)
	}

	return "DECLARE @n sysname = " + nLiteral(name) + ", @s nvarchar(max); " +
		"SET @s = N'ALTER DATABASE ' + QUOTENAME(@n) + N' SET MULTI_USER'; " +
		"IF DB_ID(@n) IS NOT NULL EXEC(@s);", nil
}

func scratchPath(suffix string) (string, error) {
	buf := make([]byte, tokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("scratch name: %w", err)
	}

	return backupDir + "gopherstack-" + hex.EncodeToString(buf) + suffix, nil
}

func (e *dockerEngine) sh(ctx context.Context, script string, args ...string) (string, error) {
	cmd := append([]string{"sh", "-c", script, "sh"}, args...)

	res, err := e.rt.Exec(ctx, e.container(), cmd)
	if err != nil {
		return "", fmt.Errorf("container exec: %w", err)
	}

	if res.ExitCode != 0 {
		return "", fmt.Errorf("%w: command exit %d: %s", ErrInvalid, res.ExitCode, trimOutput(res.Stderr))
	}

	return res.Stdout, nil
}

// BackupDatabase implements DataEngine.
func (e *dockerEngine) BackupDatabase(ctx context.Context, name string, maxBytes int64) ([]byte, error) {
	path, err := scratchPath(backupFileSuffix)
	if err != nil {
		return nil, err
	}

	text, err := backupSQL(name, path)
	if err != nil {
		return nil, err
	}

	defer e.cleanup(ctx, path, path)

	if _, err = e.run(ctx, text); err != nil {
		return nil, err
	}

	sizeText, err := e.sh(ctx, fileSizeScript, path)
	if err != nil {
		return nil, err
	}

	size, err := strconv.ParseInt(strings.TrimSpace(sizeText), 10, 64)
	if err != nil || size < 0 {
		return nil, fmt.Errorf("%w: backup size %q", ErrInvalid, trimOutput(sizeText))
	}

	if size > maxBytes {
		return nil, fmt.Errorf("%w: backup of %d bytes exceeds %d", ErrInvalid, size, maxBytes)
	}

	enc, err := e.sh(ctx, fileReadScript, path)
	if err != nil {
		return nil, err
	}

	return decodeBase64(enc, maxBytes)
}

// RestoreDatabase implements DataEngine.
func (e *dockerEngine) RestoreDatabase(ctx context.Context, name string, data []byte) error {
	if int64(len(data)) > maxBackupBytes {
		return fmt.Errorf("%w: backup of %d bytes exceeds %d", ErrInvalid, len(data), maxBackupBytes)
	}

	bak, err := scratchPath(backupFileSuffix)
	if err != nil {
		return err
	}

	enc := bak + encodedFileSuffix

	text, err := restoreSQL(name, bak)
	if err != nil {
		return err
	}

	defer e.cleanup(ctx, bak, enc)

	if err = e.upload(ctx, enc, encodeBase64(data)); err != nil {
		return err
	}

	sum, err := e.sh(ctx, fileDecodeScript, enc, bak)
	if err != nil {
		return err
	}

	want := sha256.Sum256(data)
	if f := strings.Fields(sum); len(f) == 0 || f[0] != hex.EncodeToString(want[:]) {
		return fmt.Errorf("%w: uploaded backup failed its integrity check", ErrInvalid)
	}

	if _, err = e.run(ctx, text); err != nil {
		e.releaseSingleUser(ctx, name)

		return err
	}

	return nil
}

func (e *dockerEngine) releaseSingleUser(ctx context.Context, name string) {
	ctx = context.WithoutCancel(ctx)

	text, err := multiUserSQL(name)
	if err == nil {
		_, err = e.run(ctx, text)
	}

	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "azuresql: restoring multi-user mode failed", "database", name, "error", err)
	}
}

func (e *dockerEngine) upload(ctx context.Context, path, encoded string) error {
	if _, err := e.sh(ctx, `: > "$1"`, path); err != nil {
		return err
	}

	for len(encoded) > 0 {
		n := min(len(encoded), uploadChunkBytes)
		if _, err := e.sh(ctx, fileAppendScript, path, encoded[:n]); err != nil {
			return err
		}

		encoded = encoded[n:]
	}

	return nil
}

func (e *dockerEngine) cleanup(ctx context.Context, a, b string) {
	ctx = context.WithoutCancel(ctx)

	if _, err := e.sh(ctx, fileRemoveScript, a, b); err != nil {
		logger.Load(ctx).WarnContext(ctx, "azuresql: scratch file removal failed", "error", err)
	}
}

func decodeBase64(s string, maxBytes int64) ([]byte, error) {
	out, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("%w: backup encoding", ErrInvalid)
	}

	if int64(len(out)) > maxBytes {
		return nil, fmt.Errorf("%w: backup exceeds %d bytes", ErrInvalid, maxBytes)
	}

	return out, nil
}

func encodeBase64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
