package rds

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// LogFileFilter narrows the results returned by DescribeDBLogFiles, matching the
// RDS DescribeDBLogFiles request filters.
type LogFileFilter struct {
	// FilenameContains, when non-empty, keeps only log files whose name contains it.
	FilenameContains string
	// FileLastWritten, when > 0, keeps only files written at or after this epoch-ms time.
	FileLastWritten int64
	// FileSize, when > 0, keeps only files at least this many bytes.
	FileSize int64
}

// LogFilePortion is a chunk of a DB log file returned by DownloadDBLogFilePortion.
type LogFilePortion struct {
	LogFileData           string
	Marker                string
	AdditionalDataPending bool
}

// DescribeDBLogFiles returns the log files for the given instance, filtered by the
// supplied LogFileFilter. The instance is seeded with a small set of realistic log
// files on first access.
func (b *InMemoryBackend) DescribeDBLogFiles(instanceID string, filter LogFileFilter) ([]DBLogFile, error) {
	b.mu.Lock("DescribeDBLogFiles")
	defer b.mu.Unlock()
	inst, exists := b.instances.Get(instanceID)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}
	b.ensureInstanceLogsLocked(instanceID, inst.Engine)

	result := make([]DBLogFile, 0, len(b.instanceLogFiles[instanceID]))
	for _, f := range b.instanceLogFiles[instanceID] {
		if filter.FilenameContains != "" && !strings.Contains(f.LogFileName, filter.FilenameContains) {
			continue
		}
		if filter.FileLastWritten > 0 && f.LastWritten < filter.FileLastWritten {
			continue
		}
		if filter.FileSize > 0 && f.Size < filter.FileSize {
			continue
		}
		result = append(result, f)
	}

	return result, nil
}

// DownloadDBLogFilePortion returns a portion of the named log file for the given
// instance, honoring the supplied marker and line count. Marker is "0" or "" for
// the start of the file; the returned marker is the next line offset to read from.
func (b *InMemoryBackend) DownloadDBLogFilePortion(
	instanceID, logFileName, marker string,
	numberOfLines int,
) (LogFilePortion, error) {
	b.mu.Lock("DownloadDBLogFilePortion")
	defer b.mu.Unlock()
	inst, exists := b.instances.Get(instanceID)
	if !exists {
		return LogFilePortion{}, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}
	b.ensureInstanceLogsLocked(instanceID, inst.Engine)

	content, ok := b.instanceLogContent[instanceID][logFileName]
	if !ok {
		return LogFilePortion{}, fmt.Errorf(
			"%w: DBLogFileNotFoundFault: log file %s not found for instance %s",
			ErrInvalidParameter,
			logFileName,
			instanceID,
		)
	}

	lines := strings.Split(content, "\n")
	// Drop a trailing empty element produced by a final newline.
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	start := 0
	if marker != "" && marker != "0" {
		if n, err := strconv.Atoi(marker); err == nil && n >= 0 {
			start = n
		}
	}
	if start > len(lines) {
		start = len(lines)
	}

	end := len(lines)
	if numberOfLines > 0 && start+numberOfLines < end {
		end = start + numberOfLines
	}

	portion := strings.Join(lines[start:end], "\n")
	if end > start {
		portion += "\n"
	}

	return LogFilePortion{
		LogFileData:           portion,
		Marker:                strconv.Itoa(end),
		AdditionalDataPending: end < len(lines),
	}, nil
}

// ensureInstanceLogsLocked seeds a deterministic set of log files and their content
// for an instance the first time its logs are requested. Caller must hold b.mu.
func (b *InMemoryBackend) ensureInstanceLogsLocked(instanceID, engine string) {
	if _, ok := b.instanceLogFiles[instanceID]; ok {
		return
	}

	now := time.Now().UTC()
	prefix := "error/postgresql.log"
	switch {
	case strings.Contains(strings.ToLower(engine), "mysql"), strings.Contains(strings.ToLower(engine), "maria"):
		prefix = "error/mysql-error.log"
	case strings.Contains(strings.ToLower(engine), "oracle"):
		prefix = "trace/alert_ORCL.log"
	case strings.Contains(strings.ToLower(engine), "sqlserver"):
		prefix = "log/ERROR"
	}

	files := make([]DBLogFile, 0, seededLogFileCount)
	content := make(map[string]string)
	for i := range seededLogFileCount {
		ts := now.Add(time.Duration(-i) * time.Hour)
		name := prefix
		if i > 0 {
			name = fmt.Sprintf("%s.%d", prefix, i)
		}
		readyPID := seededLogBasePID + i
		checkpointStartPID := readyPID + 1
		checkpointDonePID := checkpointStartPID + 1
		body := fmt.Sprintf(
			"%s UTC [%d]: LOG:  database system is ready to accept connections on %s\n"+
				"%s UTC [%d]: LOG:  checkpoint starting: time\n"+
				"%s UTC [%d]: LOG:  checkpoint complete\n",
			ts.Format("2006-01-02 15:04:05"), readyPID, instanceID,
			ts.Add(time.Minute).Format("2006-01-02 15:04:05"), checkpointStartPID,
			ts.Add(time.Minute+time.Minute).Format("2006-01-02 15:04:05"), checkpointDonePID,
		)
		files = append(files, DBLogFile{
			LogFileName: name,
			LastWritten: ts.UnixMilli(),
			Size:        int64(len(body)),
		})
		content[name] = body
	}

	b.instanceLogFiles[instanceID] = files
	b.instanceLogContent[instanceID] = content
}
