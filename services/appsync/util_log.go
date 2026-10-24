package appsync

import "strings"

// utilLog implements $util.log.info/error: both append to the evaluation logs
// (EvaluateMappingTemplate returns them); with extra arguments the first is a
// "{}" placeholder format string.
func utilLog() map[string]utilFn {
	logFn := func(u *utilEnv, a []any) (any, error) {
		u.logs = append(u.logs, formatLog(a))

		return "", nil
	}

	return map[string]utilFn{"log.info": logFn, "log.error": logFn}
}

func formatLog(a []any) string {
	if len(a) == 0 {
		return ""
	}

	if len(a) == 1 {
		return vrender(a[0])
	}

	parts := strings.Split(vrender(a[0]), "{}")

	var sb strings.Builder

	for i, p := range parts {
		sb.WriteString(p)

		if i < len(parts)-1 {
			if i+1 < len(a) {
				sb.WriteString(vrender(a[i+1]))
			} else {
				sb.WriteString("{}")
			}
		}
	}

	return sb.String()
}
