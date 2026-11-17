package gcpsql

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	quotaPoll = 5 * time.Second
	gib       = int64(1) << 30
)

// DiskUser is implemented by engines that can report their data size and stop accepting writes, which is how a
// full disk shows to clients.
type DiskUser interface {
	DiskUsage(ctx context.Context) (int64, error)
	SetReadOnly(ctx context.Context, on bool) error
}

// SetQuotaInterval sets how often disk usage is compared with the provisioned size.
func (b *InMemoryBackend) SetQuotaInterval(d time.Duration) {
	b.mu.Lock("SetQuotaInterval")
	defer b.mu.Unlock()

	if d > 0 {
		b.quotaEvery = d
	}
}

// diskCapacity is the number of bytes the instance may hold; zero means unlimited because the disk grows.
func diskCapacity(settings map[string]any) int64 {
	size, _ := strconv.ParseInt(settingString(settings, "dataDiskSizeGb"), 10, 64)
	if size <= 0 {
		return 0
	}

	if grows, _ := settings["storageAutoResize"].(bool); grows {
		limit, _ := strconv.ParseInt(settingString(settings, "storageAutoResizeLimit"), 10, 64)

		return max(limit, 0) * gib
	}

	return size * gib
}

// checkDiskShrink rejects a smaller dataDiskSizeGb: a Cloud SQL disk can only grow.
func checkDiskShrink(cur, next map[string]any) error {
	was, _ := strconv.ParseInt(settingString(cur, "dataDiskSizeGb"), 10, 64)
	now, _ := strconv.ParseInt(settingString(next, "dataDiskSizeGb"), 10, 64)

	if was > 0 && now > 0 && now < was {
		return newErr(http.StatusBadRequest, "invalid",
			"Invalid request: the disk size can be increased but not decreased (%dGB to %dGB).", was, now)
	}

	return nil
}

// startQuotaLocked launches the usage check of rec once.
func (b *InMemoryBackend) startQuotaLocked(rec *instRec) {
	if rec.quotaStop != nil {
		return
	}

	ctx, cancel := context.WithCancel(b.runCtx)
	rec.quotaStop = cancel
	every := b.quotaEvery

	b.quotaWG.Add(1)

	go b.watchQuota(ctx, rec, every)
}

func (b *InMemoryBackend) watchQuota(ctx context.Context, rec *instRec, every time.Duration) {
	defer b.quotaWG.Done()

	tick := time.NewTicker(every)
	defer tick.Stop()

	var (
		last Engine
		full bool
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			last, full = b.checkQuota(ctx, rec, last, full)
		}
	}
}

// checkQuota compares usage with capacity and flips the engine's read-only mode when the answer changes. It
// returns the engine acted on and whether it is now full.
func (b *InMemoryBackend) checkQuota(ctx context.Context, rec *instRec, last Engine, full bool) (Engine, bool) {
	b.mu.RLock("checkQuota")
	eng, capacity := rec.eng, diskCapacity(rec.Inst.Settings)
	b.mu.RUnlock()

	du, ok := eng.(DiskUser)
	if !ok {
		return nil, false
	}

	if eng != last {
		full = false
	}

	if capacity == 0 && !full && !autoResizes(rec.Inst.Settings) {
		return eng, false
	}

	used, err := du.DiskUsage(ctx)
	if err != nil {
		return eng, full
	}

	if grown := b.growDisk(rec, eng, used); grown {
		b.mu.RLock("checkQuota")
		capacity = diskCapacity(rec.Inst.Settings)
		b.mu.RUnlock()
	}

	if want := capacity > 0 && used > capacity; want != full {
		if err = du.SetReadOnly(ctx, want); err != nil {
			logger.Load(ctx).WarnContext(ctx, "gcpsql: could not change disk-full mode",
				"instance", rec.Inst.Name, "error", err)

			return eng, full
		}

		return eng, want
	}

	return eng, full
}

func autoResizes(settings map[string]any) bool {
	on, _ := settings["storageAutoResize"].(bool)

	return on
}

// growDisk raises dataDiskSizeGb to the next whole GiB above usage while storageAutoResize allows it; the increment
// Cloud SQL uses is not documented.
func (b *InMemoryBackend) growDisk(rec *instRec, eng Engine, used int64) bool {
	b.mu.Lock("growDisk")
	defer b.mu.Unlock()

	if rec.gone || rec.eng != eng || !autoResizes(rec.Inst.Settings) {
		return false
	}

	size, _ := strconv.ParseInt(settingString(rec.Inst.Settings, "dataDiskSizeGb"), 10, 64)
	if size <= 0 || used <= size*gib {
		return false
	}

	want := (used + gib - 1) / gib
	if limit, _ := strconv.ParseInt(settingString(rec.Inst.Settings, "storageAutoResizeLimit"), 10, 64); limit > 0 {
		want = min(want, limit)
	}

	if want <= size {
		return false
	}

	rec.Inst.Settings["dataDiskSizeGb"] = strconv.FormatInt(want, 10)

	return true
}

func stopQuota(cancel context.CancelFunc) {
	if cancel != nil {
		cancel()
	}
}

// DiskUsage implements DiskUser.
func (e *dockerEngine) DiskUsage(ctx context.Context) (int64, error) {
	out, err := e.psql(ctx, "SELECT COALESCE(sum(pg_database_size(datname)), 0)::bigint FROM pg_database")
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(strings.TrimSpace(out), 10, 64) //nolint:wrapcheck // psql output
}

// SetReadOnly implements DiskUser.
func (e *dockerEngine) SetReadOnly(ctx context.Context, on bool) error {
	stmt := "ALTER SYSTEM RESET default_transaction_read_only"
	if on {
		stmt = "ALTER SYSTEM SET default_transaction_read_only = on"
	}

	if _, err := e.psql(ctx, stmt); err != nil {
		return err
	}

	_, err := e.psql(ctx, stmtReloadConf)

	return err
}

// DiskUsage implements DiskUser.
func (e *mysqlEngine) DiskUsage(ctx context.Context) (int64, error) {
	rows, err := e.srv.QueryMax(
		ctx,
		"",
		"SELECT COALESCE(SUM(data_length + index_length), 0) FROM information_schema.tables",
		0,
	)
	if err != nil || len(rows) < 2 || len(rows[1]) == 0 {
		return 0, mapMySQLErr(err)
	}

	return strconv.ParseInt(rows[1][0], 10, 64) //nolint:wrapcheck // engine output
}

// SetReadOnly implements DiskUser.
func (e *mysqlEngine) SetReadOnly(ctx context.Context, on bool) error {
	v := "OFF"
	if on {
		v = "ON"
	}

	if err := e.exec(ctx, "SET GLOBAL super_read_only = "+v); err != nil {
		return err
	}

	return e.exec(ctx, "SET GLOBAL read_only = "+v)
}
