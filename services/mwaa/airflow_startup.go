package mwaa

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	startupLogTail = 2000
	exitMarkerFile = "exited"
	startupLogFile = "startup.log"
	bootScriptHead = "trap 'rc=$?; [ $rc -ne 0 ] && cat " + airflowStateDir + "/" + startupLogFile +
		" 2>/dev/null; echo $rc > " + airflowStateDir + "/" + exitMarkerFile + "' EXIT\n"
	userCreateStep = "airflow db migrate && airflow users create --username " + airflowAdminUser +
		` --password "$MWAA_ADMIN_PASSWORD" --firstname MWAA --lastname Admin --role Admin` +
		" --email admin@example.com || exit 1\n"
	startupFunction = `startup() {
  [ -s ` + mwaaMountDir + `/startup.sh ] || return 0
  local log=` + airflowStateDir + "/" + startupLogFile + `
  echo "[mwaa-startup] running startup script"
  { source ` + mwaaMountDir + `/startup.sh; } > "$log" 2>&1
  local rc=$?
  [ $rc -eq 0 ] || exit $rc
  cat "$log"
  echo "[mwaa-startup] startup script finished"
}
`
)

var errAirflowExited = errors.New("airflow container exited during startup")

// startScript wraps Airflow's boot: the S3 startup script is sourced first (its exports are kept), then
// requirements, then the admin user (2.x), then Airflow itself.
func startScript(version string) string {
	step := ""
	if !isAirflowV3(version) {
		step = userCreateStep
	}

	return bootScriptHead + startupFunction + "startup\n" +
		"[ -s " + mwaaMountDir + "/requirements.txt ] && pip install --no-cache-dir -r " +
		mwaaMountDir + "/requirements.txt\n" +
		step + "unset MWAA_ADMIN_PASSWORD\nexec airflow standalone\n"
}

// containerExitError reports a boot script that exited before handing over to Airflow.
func containerExitError(stageDir string) error {
	raw, err := os.ReadFile(filepath.Join(stageDir, "state", exitMarkerFile))
	if err != nil {
		return nil //nolint:nilerr // no marker means the container is still booting
	}

	code := strings.TrimSpace(string(raw))
	if code == "" {
		return nil
	}

	log, _ := os.ReadFile(filepath.Join(stageDir, "state", startupLogFile))
	if len(log) > startupLogTail {
		log = log[len(log)-startupLogTail:]
	}

	return fmt.Errorf("%w: exit status %s: %s", errAirflowExited, code, strings.TrimSpace(string(log)))
}
