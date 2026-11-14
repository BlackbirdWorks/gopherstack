package ocidatabase_test

import (
	"archive/zip"
	"bytes"
	"crypto/x509"
	"io"
	"net/http"
	"sort"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"software.sslmate.com/src/go-pkcs12"
)

const walletPassword = "Wallet#1234"

func (e *env) wallet(t *testing.T, id *string, password string) (map[string][]byte, error) {
	t.Helper()

	resp, err := e.client.GenerateAutonomousDatabaseWallet(
		t.Context(),
		database.GenerateAutonomousDatabaseWalletRequest{
			AutonomousDatabaseId: id,
			Password:             new(password),
		},
	)
	if err != nil {
		return nil, err
	}

	defer resp.Content.Close()

	data, err := io.ReadAll(resp.Content)
	require.NoError(t, err)
	require.NotNil(t, resp.LastModified)

	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	files := map[string][]byte{}

	for _, f := range zr.File {
		rc, oerr := f.Open()
		require.NoError(t, oerr)

		b, rerr := io.ReadAll(rc)
		require.NoError(t, rerr)
		require.NoError(t, rc.Close())

		files[f.Name] = b
	}

	return files, nil
}

func TestWalletContents(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.available(t, createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) {
		d.IsMtlsConnectionRequired = new(true)
	}})

	files, err := e.wallet(t, db.Id, walletPassword)
	require.NoError(t, err)

	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}

	sort.Strings(names)
	assert.Equal(t, []string{"README", "ewallet.p12", "ojdbc.properties", "sqlnet.ora", "tnsnames.ora"}, names)

	tns := string(files["tnsnames.ora"])
	for _, g := range []string{"high", "medium", "low", "tp", "tpurgent"} {
		assert.Contains(t, tns, "mydb_"+g+" = (description=")
	}

	assert.Contains(t, tns, "(protocol=tcps)(port=1522)")
	assert.Contains(t, string(files["sqlnet.ora"]), "WALLET_LOCATION")
	assert.Contains(t, string(files["sqlnet.ora"]), "SSL_SERVER_DN_MATCH=yes")
	assert.Contains(t, string(files["ojdbc.properties"]), "oracle.net.wallet_location")

	key, leaf, cas, err := pkcs12.DecodeChain(files["ewallet.p12"], walletPassword)
	require.NoError(t, err)
	assert.NotNil(t, key)
	assert.Equal(t, "mydb", leaf.Subject.CommonName)
	assert.Contains(t, leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	assert.Len(t, cas, 2)

	_, _, _, err = pkcs12.DecodeChain(files["ewallet.p12"], "wrong-Pass1")
	require.Error(t, err, "the wallet is encrypted with its password")

	for name, data := range files {
		assert.NotContains(t, string(data), walletPassword, name)
		assert.NotContains(t, string(data), testPassword, name)
	}
}

func TestWalletTLSProfilesWhenMtlsOptional(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.available(t, createOpts{})

	files, err := e.wallet(t, db.Id, walletPassword)
	require.NoError(t, err)
	assert.Contains(t, string(files["tnsnames.ora"]), "mydb_high_tls = ")
	assert.Contains(t, string(files["tnsnames.ora"]), "(port=1521)")
}

func TestWalletValidationAndState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantHTTP int
	}{
		{name: "ok", password: "Wallet#1234", wantHTTP: 0},
		{name: "letters-and-digit", password: "abcdefg1", wantHTTP: 0},
		{name: "letters-and-special", password: "abcdefg!", wantHTTP: 0},
		{name: "too-short", password: "ab1!", wantHTTP: http.StatusBadRequest},
		{name: "letters-only", password: "abcdefghij", wantHTTP: http.StatusBadRequest},
		{name: "digits-only", password: "12345678", wantHTTP: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.available(t, createOpts{})

			_, err := e.wallet(t, db.Id, tc.password)
			if tc.wantHTTP == 0 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tc.wantHTTP, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	t.Run("stopped-ok-terminated-not", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		db := e.available(t, createOpts{})

		_, err := e.client.StopAutonomousDatabase(
			t.Context(),
			database.StopAutonomousDatabaseRequest{AutonomousDatabaseId: db.Id},
		)
		require.NoError(t, err)
		e.settle()

		_, err = e.wallet(t, db.Id, walletPassword)
		require.NoError(t, err)

		_, err = e.client.DeleteAutonomousDatabase(
			t.Context(),
			database.DeleteAutonomousDatabaseRequest{AutonomousDatabaseId: db.Id},
		)
		require.NoError(t, err)
		e.settle()

		_, err = e.wallet(t, db.Id, walletPassword)
		assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
	})

	t.Run("missing-database", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		_, err := e.wallet(t, new("ocid1.autonomousdatabase.oc1..nope"), walletPassword)
		assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
	})
}

func TestWalletRotate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.available(t, createOpts{})

	status, err := e.client.GetAutonomousDatabaseWallet(
		t.Context(),
		database.GetAutonomousDatabaseWalletRequest{AutonomousDatabaseId: db.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, database.AutonomousDatabaseWalletLifecycleStateActive, status.LifecycleState)
	assert.Nil(t, status.TimeRotated)

	resp, err := e.client.UpdateAutonomousDatabaseWallet(t.Context(), database.UpdateAutonomousDatabaseWalletRequest{
		AutonomousDatabaseId: db.Id,
		ShouldRotate:         new(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", string(e.wr(t, resp.OpcWorkRequestId).Status))

	status, err = e.client.GetAutonomousDatabaseWallet(
		t.Context(),
		database.GetAutonomousDatabaseWalletRequest{AutonomousDatabaseId: db.Id},
	)
	require.NoError(t, err)
	assert.NotNil(t, status.TimeRotated)

	_, err = e.client.UpdateAutonomousDatabaseWallet(t.Context(), database.UpdateAutonomousDatabaseWalletRequest{
		AutonomousDatabaseId: db.Id,
		GracePeriod:          new(-1),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}
