package ssm

import (
	"strconv"
	"strings"
)

// Known Run Command documents whose script bodies we can plausibly emulate.
const (
	docRunShellScript      = "AWS-RunShellScript"
	docRunPowerShellScript = "AWS-RunPowerShellScript"
)

// renderCommandOutput produces plausible StandardOutputContent /
// StandardErrorContent and a terminal status for a SendCommand invocation,
// based on the document being run and its parameters.
//
// For the two ubiquitous script documents (AWS-RunShellScript /
// AWS-RunPowerShellScript) it interprets the `commands` parameter: echo-style
// statements are evaluated to their emitted text and an explicit non-zero
// `exit N` marks the invocation Failed. For any other (unknown) document it
// returns the AWS-accurate result: the command is accepted and succeeds, but no
// script output is modelled, so output is empty.
func renderCommandOutput(
	docName string,
	params map[string][]string,
) (string, string, string) {
	switch docName {
	case docRunShellScript:
		return renderScriptOutput(params["commands"], shellEcho)
	case docRunPowerShellScript:
		return renderScriptOutput(params["commands"], powerShellEcho)
	default:
		return "", "", commandStatusSuccess
	}
}

// echoEvaluator interprets a single script line, returning the text it emits to
// stdout (with the trailing newline already stripped) and whether the line was
// recognised as an output-producing statement.
type echoEvaluator func(line string) (out string, matched bool)

// renderScriptOutput walks the script lines, accumulating emitted stdout and
// honouring an explicit non-zero `exit` as a failure. It returns the rendered
// stdout, stderr and terminal status.
func renderScriptOutput(
	commands []string,
	eval echoEvaluator,
) (string, string, string) {
	var out strings.Builder

	for _, raw := range commands {
		for line := range strings.SplitSeq(raw, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			if code, ok := parseExit(line); ok {
				if code != 0 {
					stderr := "failed to run commands: exit status " + strconv.Itoa(code)

					return out.String(), stderr, commandStatusFailed
				}

				continue
			}

			if text, matched := eval(line); matched {
				out.WriteString(text)
				out.WriteString("\n")
			}
		}
	}

	return out.String(), "", commandStatusSuccess
}

// parseExit recognises a POSIX/PowerShell `exit N` statement and returns its
// numeric code.
func parseExit(line string) (int, bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 || fields[0] != "exit" {
		return 0, false
	}

	if len(fields) == 1 {
		return 0, true
	}

	code, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, false
	}

	return code, true
}

// shellEcho evaluates a POSIX shell `echo` statement.
func shellEcho(line string) (string, bool) {
	arg, ok := stripPrefixWord(line, "echo")
	if !ok {
		return "", false
	}

	return unquoteShell(arg), true
}

// powerShellEcho evaluates the common PowerShell output cmdlets. `echo` is an
// alias for Write-Output; Write-Host writes to the host stream which the SSM
// agent captures on stdout.
func powerShellEcho(line string) (string, bool) {
	for _, kw := range []string{"Write-Output", "Write-Host", "echo"} {
		if arg, ok := stripPrefixWordFold(line, kw); ok {
			return unquoteShell(arg), true
		}
	}

	return "", false
}

// stripPrefixWord returns the remainder of line after a leading word, requiring
// the word to be followed by whitespace.
func stripPrefixWord(line, word string) (string, bool) {
	if !strings.HasPrefix(line, word) {
		return "", false
	}

	rest := line[len(word):]
	if rest == "" || (rest[0] != ' ' && rest[0] != '\t') {
		return "", false
	}

	return strings.TrimSpace(rest), true
}

// stripPrefixWordFold is stripPrefixWord with case-insensitive matching, used
// for PowerShell cmdlets which are conventionally case-insensitive.
func stripPrefixWordFold(line, word string) (string, bool) {
	if len(line) < len(word) || !strings.EqualFold(line[:len(word)], word) {
		return "", false
	}

	rest := line[len(word):]
	if rest == "" || (rest[0] != ' ' && rest[0] != '\t') {
		return "", false
	}

	return strings.TrimSpace(rest), true
}

// unquoteShell removes a single matching pair of surrounding single or double
// quotes from an echo argument.
func unquoteShell(s string) string {
	const minQuotedLen = 2
	if len(s) >= minQuotedLen {
		first := s[0]
		last := s[len(s)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			return s[1 : len(s)-1]
		}
	}

	return s
}
