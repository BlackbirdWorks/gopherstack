package dmsengine

import (
	"io"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

type parquetColumn = parquetio.Column

func writeParquetFile(w io.Writer, cols []parquetColumn, rows [][]any) error {
	return parquetio.Write(w, cols, rows, "")
}
