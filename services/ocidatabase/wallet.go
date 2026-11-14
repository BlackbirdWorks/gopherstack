package ocidatabase

import (
	"archive/zip"
	"bytes"
	"fmt"
	"time"

	"software.sslmate.com/src/go-pkcs12"
)

const (
	walletFileMode = 0o600
	hoursToGrace   = time.Hour
)

const (
	sqlnetOra = "WALLET_LOCATION = (SOURCE = (METHOD = file) (METHOD_DATA = (DIRECTORY=\"?/network/admin\")))\n" +
		"SSL_SERVER_DN_MATCH=yes\n"
	ojdbcProps = "# Connection property while using Oracle wallets.\n" +
		"oracle.net.wallet_location=(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=${TNS_ADMIN})))\n"
	walletReadme = "Gopherstack Autonomous Database wallet\n\n" +
		"ewallet.p12 holds the client certificate and key for mutual TLS and the emulator CA certificates, " +
		"encrypted with the wallet password.\ntnsnames.ora lists the connection profiles; sqlnet.ora and " +
		"ojdbc.properties point clients at the wallet directory.\n" +
		"cwallet.sso (the auto-login wallet), keystore.jks and truststore.jks are not generated.\n"
)

// WalletStatus is the wire form of AutonomousDatabaseWallet.
type WalletStatus struct {
	TimeRotated    *time.Time `json:"timeRotated,omitempty"`
	LifecycleState string     `json:"lifecycleState"`
}

// GenerateWallet builds the wallet zip of a database, encrypted with in.Password.
func (b *InMemoryBackend) GenerateWallet(id string, in walletIn) ([]byte, time.Time, error) {
	if in.Password == nil {
		return nil, time.Time{}, errMissing("password is required")
	}

	if aerr := checkWalletPassword(*in.Password); aerr != nil {
		return nil, time.Time{}, aerr
	}

	if in.GenerateType != nil {
		if aerr := oneOf("generateType", *in.GenerateType, "ALL", "SINGLE"); aerr != nil {
			return nil, time.Time{}, aerr
		}
	}

	b.mu.Lock("GenerateWallet")
	defer b.mu.Unlock()

	db, aerr := b.guard(id, "", stateAvailable, stateStopped)
	if aerr != nil {
		return nil, time.Time{}, aerr
	}

	ep, ok := b.endpointOf(db)
	if !ok {
		return nil, time.Time{}, errState("The database has no listeners to describe in a wallet yet")
	}

	sec, err := b.securityLocked(id)
	if err != nil {
		return nil, time.Time{}, err
	}

	now := b.nowUTC()

	data, err := buildWallet(db, ep, sec, *in.Password, now)
	if err != nil {
		return nil, time.Time{}, err
	}

	return data, now, nil
}

func buildWallet(db *ADB, ep endpoint, sec *security, password string, now time.Time) ([]byte, error) {
	client, err := sec.issueClient(db.DBName)
	if err != nil {
		return nil, err
	}

	leaf, err := parseLeaf(client)
	if err != nil {
		return nil, err
	}

	p12, err := pkcs12.Modern.Encode(client.PrivateKey, leaf, sec.trustCerts(), password)
	if err != nil {
		return nil, fmt.Errorf("encode ewallet.p12: %w", err)
	}

	files := []struct {
		name string
		data []byte
	}{
		{"README", []byte(walletReadme)},
		{"ewallet.p12", p12},
		{"ojdbc.properties", []byte(ojdbcProps)},
		{"sqlnet.ora", []byte(sqlnetOra)},
		{"tnsnames.ora", []byte(ep.tnsnames(db))},
	}

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for _, f := range files {
		h := &zip.FileHeader{Name: f.name, Method: zip.Deflate, Modified: now}
		h.SetMode(walletFileMode)

		w, werr := zw.CreateHeader(h)
		if werr != nil {
			return nil, fmt.Errorf("zip %s: %w", f.name, werr)
		}

		if _, werr = w.Write(f.data); werr != nil {
			return nil, fmt.Errorf("zip %s: %w", f.name, werr)
		}
	}

	if err = zw.Close(); err != nil {
		return nil, fmt.Errorf("close wallet zip: %w", err)
	}

	return buf.Bytes(), nil
}

// GetWallet returns the wallet status of a database.
func (b *InMemoryBackend) GetWallet(id string) (WalletStatus, error) {
	b.mu.RLock("GetWallet")
	defer b.mu.RUnlock()

	db, aerr := b.activeDB(id)
	if aerr != nil {
		return WalletStatus{}, aerr
	}

	return WalletStatus{LifecycleState: "ACTIVE", TimeRotated: db.WalletRotated}, nil
}

// UpdateWallet rotates the client trust of a database: wallets issued before stop working once the grace
// period (in hours) is over.
func (b *InMemoryBackend) UpdateWallet(id string, in walletUpdateIn) (string, error) {
	b.mu.Lock("UpdateWallet")
	defer b.mu.Unlock()

	db, aerr := b.guard(id, "", stateAvailable, stateStopped)
	if aerr != nil {
		return "", aerr
	}

	if in.GracePeriod != nil && *in.GracePeriod < 0 {
		return "", errParam("gracePeriod must not be negative")
	}

	if deref(in.ShouldRotate, false) {
		sec, err := b.securityLocked(id)
		if err != nil {
			return "", err
		}

		if err = sec.rotate(time.Duration(deref(in.GracePeriod, 0)) * hoursToGrace); err != nil {
			return "", err
		}

		now := b.nowUTC()
		db.WalletRotated = &now
	}

	return b.instant(db, "UPDATE_AUTONOMOUS_DATABASE_WALLET").ID, nil
}
