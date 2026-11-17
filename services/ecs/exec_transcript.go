package ecs

import (
	"context"
	"path"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	execTranscriptPrefix = "ecs-execute-command-"
	maxTranscriptLines   = 10000
	maxTranscriptLineLen = 8192
)

// ExecTranscriptStore writes ECS Exec session transcripts to S3.
type ExecTranscriptStore interface {
	PutObject(ctx context.Context, bucket, key string, body []byte) error
}

// SetExecTranscriptStore wires the S3 bucket sink for executeCommandConfiguration S3 logging.
func (b *InMemoryBackend) SetExecTranscriptStore(s ExecTranscriptStore) {
	b.mu.Lock("SetExecTranscriptStore")
	defer b.mu.Unlock()

	b.execTranscripts = s
}

// execLogPlan is where one session's transcript goes once the session ends.
type execLogPlan struct {
	cwl      CWLogsBackend
	s3       ExecTranscriptStore
	group    string
	bucket   string
	prefix   string
	kmsKeyID string
	cwEnc    bool
	s3Enc    bool
}

func (p *execLogPlan) empty() bool {
	return p == nil || (p.group == "" && p.bucket == "")
}

// execLogPlanLocked resolves executeCommandConfiguration for the exec'd container. DEFAULT writes to the
// container's awslogs group; NONE and a DEFAULT without awslogs write nothing. The caller holds b.mu.
func (b *InMemoryBackend) execLogPlanLocked(
	cfg *ClusterConfiguration,
	td *TaskDefinition,
	container string,
) *execLogPlan {
	if cfg == nil || cfg.ExecuteCommandConfiguration == nil {
		return b.defaultExecPlanLocked(td, container, "")
	}

	ec := cfg.ExecuteCommandConfiguration

	switch ec.Logging {
	case "NONE":
		return nil
	case "OVERRIDE":
		lc := ec.LogConfiguration
		if lc == nil {
			return nil
		}

		return &execLogPlan{
			cwl: b.cwLogs, s3: b.execTranscripts, group: lc.CloudWatchLogGroupName, bucket: lc.S3BucketName,
			prefix: lc.S3KeyPrefix, kmsKeyID: ec.KmsKeyID, cwEnc: lc.CloudWatchEncryptionEnabled,
			s3Enc: lc.S3EncryptionEnabled,
		}
	default:
		return b.defaultExecPlanLocked(td, container, ec.KmsKeyID)
	}
}

func (b *InMemoryBackend) defaultExecPlanLocked(td *TaskDefinition, container, kms string) *execLogPlan {
	if td == nil {
		return nil
	}

	for _, cd := range td.ContainerDefinitions {
		if cd.Name != container {
			continue
		}

		if group, _, ok := awslogsTarget(cd, ""); ok {
			return &execLogPlan{cwl: b.cwLogs, group: group, kmsKeyID: kms}
		}
	}

	return nil
}

// deliver writes the transcript; it runs after the session ended, outside every backend lock.
func (p *execLogPlan) deliver(ctx context.Context, sessionID string, transcript []byte) {
	if p.empty() {
		return
	}

	log := logger.Load(ctx)
	name := execTranscriptPrefix + sessionID

	if p.kmsKeyID != "" {
		log.InfoContext(ctx, "ecs exec: would encrypt the session with the KMS key; not performed",
			"kmsKeyId", p.kmsKeyID, "cloudWatchEncryption", p.cwEnc, "s3Encryption", p.s3Enc)
	}

	if p.group != "" && p.cwl != nil {
		if err := p.cwl.EnsureLogGroupAndStream(p.group, name); err != nil {
			log.WarnContext(ctx, "ecs exec: transcript log stream failed", "group", p.group, "error", err)
		} else if err = p.cwl.PutLogLines(p.group, name, transcriptLines(transcript)); err != nil {
			log.WarnContext(ctx, "ecs exec: transcript delivery to CloudWatch Logs failed", "error", err)
		}
	}

	if p.bucket != "" && p.s3 != nil {
		key := path.Join(strings.Trim(p.prefix, "/"), name+".log")
		if err := p.s3.PutObject(ctx, p.bucket, key, transcript); err != nil {
			log.WarnContext(ctx, "ecs exec: transcript delivery to S3 failed", "bucket", p.bucket, "error", err)
		}
	}
}

func transcriptLines(transcript []byte) []string {
	var out []string

	for line := range strings.SplitSeq(strings.ReplaceAll(string(transcript), "\r", ""), "\n") {
		if line == "" {
			continue
		}

		if len(line) > maxTranscriptLineLen {
			line = line[:maxTranscriptLineLen]
		}

		if out = append(out, line); len(out) >= maxTranscriptLines {
			break
		}
	}

	return out
}
