package mysqlengine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/dbrepl"
)

const (
	replPassLen = 30
	netSuffix   = 8
	superRO     = "SET GLOBAL super_read_only=ON"
)

// ErrNoReplication is returned when the container runtime cannot manage the network replication needs.
var ErrNoReplication = errors.New("mysqlengine: the container runtime cannot run replication")

// ReplSource tells a replica how to reach its source.
type ReplSource struct {
	Network   string
	NetworkID string
	Host      string
	Password  string
	Port      int
}

type sourceNet struct {
	id        string
	name      string
	connected atomic.Bool
}

func replicationFlags(serverID uint32) []string {
	return []string{
		"--server-id=" + strconv.FormatUint(uint64(max(serverID, 1)), 10),
		"--gtid-mode=ON", "--enforce-gtid-consistency=ON",
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

func (s *Server) replicaPass() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.replPass == "" {
		s.replPass = randomHex(replPassLen)[:replPassLen]
	}

	return s.replPass
}

// ReplicationSource creates the replication account and puts the server on a network its replicas can join.
func (s *Server) ReplicationSource(ctx context.Context) (ReplSource, error) {
	nr, ok := s.e.cfg.Runtime.(container.NetworkRuntime)
	if !ok {
		return ReplSource{}, ErrNoReplication
	}

	pass := s.replicaPass()

	if _, err := s.Statement(ctx, dbrepl.MySQLReplicaUserSQL(s.e.cfg.ReplUser, pass)); err != nil {
		return ReplSource{}, err
	}

	n, err := s.sourceNetwork(ctx, nr)
	if err != nil {
		return ReplSource{}, err
	}

	return ReplSource{Network: n.name, NetworkID: n.id, Host: s.ContainerName(), Port: mysqlPort, Password: pass}, nil
}

func (s *Server) sourceNetwork(ctx context.Context, nr container.NetworkRuntime) (*sourceNet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.net == nil {
		name := s.e.cfg.Prefix + "-net-" + sanitize(s.spec.ID) + "-" + randomHex(netSuffix)[:netSuffix]

		id, err := nr.CreateNetwork(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("create replication network: %w", err)
		}

		s.net = &sourceNet{id: id, name: name}
		s.netID = id
	}

	if !s.net.connected.Load() {
		if err := nr.ConnectNetwork(ctx, s.net.id, s.ctrID); err != nil {
			return nil, fmt.Errorf("connect source to replication network: %w", err)
		}

		s.net.connected.Store(true)
	}

	return s.net, nil
}

// releaseNetwork removes the replication network; one still in use by another server is removed by whichever
// closes last.
func (s *Server) releaseNetwork(ctx context.Context) {
	nr, ok := s.e.cfg.Runtime.(container.NetworkRuntime)
	if !ok {
		return
	}

	s.mu.Lock()
	id := s.netID
	s.mu.Unlock()

	if id != "" {
		_ = nr.RemoveNetwork(ctx, id)
	}
}

// ReplicaSeed returns what a replica of this server starts from: a dump of dbs with the GTID set, or just the
// executed GTID set when dbs is empty.
func (s *Server) ReplicaSeed(ctx context.Context, dbs []string) ([]byte, error) {
	if len(dbs) == 0 {
		out, err := s.Statement(ctx, "SELECT @@GLOBAL.gtid_executed")
		if err != nil {
			return nil, err
		}

		gtid := strings.ReplaceAll(strings.Join(strings.Fields(out), ""), `\n`, "")
		if gtid == "" || gtid == "NULL" {
			return nil, nil
		}

		return []byte("SET @@GLOBAL.GTID_PURGED='" + gtid + "';\n"), nil
	}

	cmd := append([]string{
		mysqldumpBin, "-u" + RootUser, argSingleTx, "--skip-dump-date",
		argRoutines, "--events", "--triggers", "--set-gtid-purged=ON", argAddDrop,
		"-r", dumpMount + "/seed.sql", argDatabases,
	}, dbs...)

	if _, err := s.RunAuth(ctx, cmd); err != nil {
		return nil, err
	}

	return s.readDump("seed.sql")
}

func (s *Server) resetStmt() string {
	if strings.HasSuffix(s.spec.Image, ":8.4") {
		return "RESET BINARY LOGS AND GTIDS"
	}

	return "RESET MASTER"
}

// AttachReplica makes the server follow src; seed, when present, is loaded first.
func (s *Server) AttachReplica(ctx context.Context, src ReplSource, seed []byte) error {
	s.mu.Lock()
	s.netID = src.NetworkID
	s.mu.Unlock()

	change := dbrepl.MySQLChangeSourceSQL(src.Host, src.Port, s.e.cfg.ReplUser, src.Password)

	if out, err := s.Statement(ctx, "SHOW REPLICA STATUS"); err == nil && strings.TrimSpace(out) != "" {
		_, err = s.Statement(ctx, "STOP REPLICA; "+change)

		return s.guardReplica(ctx, err)
	}

	if _, err := s.Statement(ctx, s.resetStmt()); err != nil {
		return err
	}

	if len(seed) > 0 {
		if err := s.RestoreInto(ctx, "", seed); err != nil {
			return err
		}
	}

	_, err := s.Statement(ctx, change)

	return s.guardReplica(ctx, err)
}

func (s *Server) guardReplica(ctx context.Context, prev error) error {
	if prev != nil {
		return prev
	}

	if _, err := s.Statement(ctx, superRO); err != nil {
		return err
	}

	s.mu.Lock()
	s.attached = true
	s.mu.Unlock()

	return nil
}

// Promote detaches a replica from its source and makes it writable.
func (s *Server) Promote(ctx context.Context) error {
	if _, err := s.Statement(ctx,
		"STOP REPLICA; RESET REPLICA ALL; SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF"); err != nil {
		return err
	}

	s.mu.Lock()
	s.promoted = true
	s.mu.Unlock()

	return nil
}

// SetReplication pauses (false) or resumes (true) replication.
func (s *Server) SetReplication(ctx context.Context, on bool) error {
	stmt := "STOP REPLICA"
	if on {
		stmt = "START REPLICA"
	}

	_, err := s.Statement(ctx, stmt)

	return err
}

// ReplicationStatus reports the replica's lag and thread state.
func (s *Server) ReplicationStatus(ctx context.Context) (dbrepl.Status, error) {
	db, err := s.open("")
	if err != nil {
		return dbrepl.Status{}, err
	}
	defer db.Close()

	return dbrepl.QueryStatus(ctx, db, false)
}

// reguard re-applies the read-only guard a replica loses on container restart.
func (s *Server) reguard(ctx context.Context) {
	s.mu.Lock()
	replica := s.attached && !s.promoted
	s.mu.Unlock()

	if replica {
		if _, err := s.Statement(ctx, superRO); err != nil {
			s.e.cfg.Logger.WarnContext(ctx, "mysqlengine: replica read-only guard failed",
				"server", s.spec.ID, "error", err)
		}
	}
}
