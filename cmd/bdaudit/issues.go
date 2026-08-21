package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const statusClosed = "closed"

const depTypeDiscoveredFrom = "discovered-from"

const (
	scannerInitialBufSize = 64 * 1024
	scannerMaxLineSize    = 16 * 1024 * 1024
)

type dependency struct {
	IssueID     string `json:"issue_id"`
	DependsOnID string `json:"depends_on_id"`
	Type        string `json:"type"`
}

type issue struct {
	ID           string       `json:"id"`
	Title        string       `json:"title"`
	Description  string       `json:"description"`
	Status       string       `json:"status"`
	CloseReason  string       `json:"close_reason"`
	IssueType    string       `json:"issue_type"`
	Dependencies []dependency `json:"dependencies"`
	Priority     int          `json:"priority"`
}

func (i issue) closed() bool { return i.Status == statusClosed }

// loadIssues reads bd's committed JSONL export -- one issue per line, later
// lines for the same id win, matching the export being a full snapshot
// rather than an append log.
func loadIssues(path string) (map[string]issue, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	issues := map[string]issue{}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, scannerInitialBufSize), scannerMaxLineSize)

	line := 0
	for scanner.Scan() {
		line++

		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}

		var iss issue
		if unmarshalErr := json.Unmarshal([]byte(text), &iss); unmarshalErr != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, unmarshalErr)
		}
		if iss.ID == "" {
			continue
		}

		issues[iss.ID] = iss
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, fmt.Errorf("scan %s: %w", path, scanErr)
	}

	return issues, nil
}
