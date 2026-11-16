package mysqlengine

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// ChannelSpec configures an inbound replication channel on a server.
type ChannelSpec struct {
	Name     string
	Host     string
	User     string
	Password string
	// SSL requires an encrypted connection to the source.
	SSL   bool
	Port  int
	Delay int
}

// ChannelState is the health of one replication channel.
type ChannelState struct {
	IOError   string
	SQLError  string
	Running   bool
	IORunning bool
	SQLActive bool
	Lag       int
}

// JoinNetwork attaches the server to the replication network of a source so it can reach it by container name.
func (s *Server) JoinNetwork(ctx context.Context, src ReplSource) error {
	nr, ok := s.e.cfg.Runtime.(container.NetworkRuntime)
	if !ok {
		return ErrNoReplication
	}

	if src.NetworkID == "" {
		return nil
	}

	s.mu.Lock()
	joined := s.netID == src.NetworkID
	s.netID = src.NetworkID
	id := s.ctrID
	s.mu.Unlock()

	if joined {
		return nil
	}

	if err := nr.ConnectNetwork(ctx, src.NetworkID, id); err != nil {
		return fmt.Errorf("connect to replication network: %w", err)
	}

	return nil
}

// AddChannel (re)configures a channel from the source in spec and starts it when start is set. GTID auto-positioning
// is used, so the source must run with GTIDs.
func (s *Server) AddChannel(ctx context.Context, c ChannelSpec, start bool) error {
	ssl := 0
	if c.SSL {
		ssl = 1
	}

	if err := s.exec(
		ctx,
		"STOP REPLICA FOR CHANNEL ?",
		c.Name,
	); err != nil &&
		!strings.Contains(err.Error(), "does not exist") {
		return err
	}

	stmt := "CHANGE REPLICATION SOURCE TO SOURCE_HOST=?, SOURCE_PORT=" + strconv.Itoa(c.Port) +
		", SOURCE_USER=?, SOURCE_PASSWORD=?, SOURCE_AUTO_POSITION=1, SOURCE_DELAY=" + strconv.Itoa(c.Delay) +
		", SOURCE_SSL=" + strconv.Itoa(ssl) + ", GET_SOURCE_PUBLIC_KEY=1 FOR CHANNEL ?"
	if err := s.exec(ctx, stmt, c.Host, c.User, c.Password, c.Name); err != nil {
		return err
	}

	if !start {
		return nil
	}

	return s.exec(ctx, "START REPLICA FOR CHANNEL ?", c.Name)
}

// SetChannel starts (true) or stops (false) a channel.
func (s *Server) SetChannel(ctx context.Context, name string, on bool) error {
	stmt := "STOP REPLICA FOR CHANNEL ?"
	if on {
		stmt = "START REPLICA FOR CHANNEL ?"
	}

	return s.exec(ctx, stmt, name)
}

// RemoveChannel stops a channel and forgets its source; an unknown channel is not an error.
func (s *Server) RemoveChannel(ctx context.Context, name string) error {
	for _, stmt := range []string{"STOP REPLICA FOR CHANNEL ?", "RESET REPLICA ALL FOR CHANNEL ?"} {
		if err := s.exec(ctx, stmt, name); err != nil && !strings.Contains(err.Error(), "does not exist") {
			return err
		}
	}

	return nil
}

// ChannelStatus reads SHOW REPLICA STATUS of one channel.
func (s *Server) ChannelStatus(ctx context.Context, name string) (ChannelState, error) {
	rows, err := s.Query(
		ctx,
		"",
		"SHOW REPLICA STATUS FOR CHANNEL '"+strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(name)+"'",
	)
	if err != nil {
		return ChannelState{}, err
	}

	if len(rows) <= 1 {
		return ChannelState{}, nil
	}

	col := map[string]string{}
	for i, h := range rows[0] {
		col[h] = rows[1][i]
	}

	st := ChannelState{
		IORunning: col["Replica_IO_Running"] == "Yes", SQLActive: col["Replica_SQL_Running"] == "Yes",
		IOError: col["Last_IO_Error"], SQLError: col["Last_SQL_Error"],
	}
	st.Running = st.IORunning && st.SQLActive
	st.Lag, _ = strconv.Atoi(col["Seconds_Behind_Source"])

	return st, nil
}
