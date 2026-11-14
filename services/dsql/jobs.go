package dsql

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/blackbirdworks/gopherstack/pkgs/pgengine"
)

const (
	jobChannel    = "gs_dsql_jobs"
	uniqueFailure = "Found duplicate key while validating index for UCVs"
	pgUniqueCode  = "23505"
	rejectedDDL   = "only a single CREATE INDEX statement is allowed"
)

var (
	indexDDLRE   = regexp.MustCompile(`(?is)^\s*create\s+(unique\s+)?index\s`)
	quotedPartRE = regexp.MustCompile(`'[^']*'|"[^"]*"`)
)

// validIndexDDL admits exactly one CREATE [UNIQUE] INDEX statement. Backslash, dollar quoting and comments are
// refused outright because they can hide a separator from the quote-aware scan below.
func validIndexDDL(ddl string) bool {
	if !indexDDLRE.MatchString(ddl) || strings.ContainsAny(ddl, `\$`) ||
		strings.Contains(ddl, "--") || strings.Contains(ddl, "/*") {
		return false
	}

	return !strings.Contains(quotedPartRE.ReplaceAllString(ddl, ""), ";")
}

type indexJob struct {
	id    string
	ddl   string
	owner string
}

// runJobs builds submitted CREATE INDEX ASYNC jobs until ctx ends, waking on the notification the shim raises.
func (i *dockerInstance) runJobs(ctx context.Context) {
	conn, err := i.superConn(ctx)
	if err != nil {
		if ctx.Err() == nil {
			i.logWarn(ctx, "dsql: job worker cannot connect", "cluster", i.ref.ID)
		}

		return
	}
	defer conn.Close(context.WithoutCancel(ctx))

	if _, err = conn.Exec(ctx, "LISTEN "+pgengine.Ident(jobChannel)); err != nil {
		return
	}

	for {
		i.drainJobs(ctx, conn)

		if _, err = conn.WaitForNotification(ctx); err != nil {
			return
		}
	}
}

func (i *dockerInstance) drainJobs(ctx context.Context, conn *pgx.Conn) {
	for ctx.Err() == nil {
		job, ok := claimJob(ctx, conn)
		if !ok {
			return
		}

		i.buildIndex(ctx, conn, job)
	}
}

func claimJob(ctx context.Context, conn *pgx.Conn) (indexJob, bool) {
	var j indexJob

	const q = `UPDATE sys.gs_jobs SET status = 'processing', start_time = now(), update_time = now()
		WHERE job_id = (SELECT job_id FROM sys.gs_jobs WHERE status = 'submitted' ORDER BY seq LIMIT 1
			FOR UPDATE SKIP LOCKED)
		RETURNING job_id, ddl, owner`
	if err := conn.QueryRow(ctx, q).Scan(&j.id, &j.ddl, &j.owner); err != nil {
		return indexJob{}, false
	}

	return j, true
}

// buildIndex runs the DDL as the submitting role on its own connection and records the outcome.
func (i *dockerInstance) buildIndex(ctx context.Context, status *pgx.Conn, j indexJob) {
	details := i.runDDL(ctx, j)
	state := "completed"

	if details != "" {
		state = "failed"
	}

	rctx := context.WithoutCancel(ctx)

	if state == "completed" {
		_, _ = status.Exec(
			rctx,
			"UPDATE sys.gs_jobs SET object_id = to_regclass(object_name)::oid WHERE job_id = $1",
			j.id,
		)
	}

	const q = "UPDATE sys.gs_jobs SET status = $2, details = NULLIF($3, ''), update_time = now() WHERE job_id = $1"
	if _, err := status.Exec(rctx, q, j.id, state, details); err != nil {
		i.logWarn(ctx, "dsql: job result not recorded", "cluster", i.ref.ID, "job", j.id)
	}
}

func (i *dockerInstance) runDDL(ctx context.Context, j indexJob) string {
	if !validIndexDDL(j.ddl) {
		return rejectedDDL
	}

	conn, err := i.roleConn(ctx, j.owner)
	if err != nil {
		return "index build could not start"
	}
	defer conn.Close(context.WithoutCancel(ctx))

	if _, err = conn.Exec(ctx, j.ddl); err != nil {
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
			if pgErr.Code == pgUniqueCode {
				return uniqueFailure
			}

			return pgErr.Message
		}

		return "index build failed"
	}

	return ""
}
