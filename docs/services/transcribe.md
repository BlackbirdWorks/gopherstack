# Transcribe

Stub Transcribe implementation for managing transcription jobs.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `StartTranscriptionJob` | Start a new transcription job |
| `GetTranscriptionJob` | Get job status and result |
| `ListTranscriptionJobs` | List transcription jobs |

## AWS CLI Examples

```bash
# Start a transcription job
aws --endpoint-url http://localhost:8000 transcribe start-transcription-job \
    --transcription-job-name my-job \
    --language-code en-US \
    --media-format mp3 \
    --media '{"MediaFileUri":"s3://my-bucket/audio.mp3"}'

# Get job status
aws --endpoint-url http://localhost:8000 transcribe get-transcription-job \
    --transcription-job-name my-job

# List jobs
aws --endpoint-url http://localhost:8000 transcribe list-transcription-jobs
```

## Known Limitations

- No actual audio transcription is performed. Jobs are immediately marked as `COMPLETED` with an empty transcript.
- Vocabulary customization, speaker diarization, and medical transcription are not implemented.
- Transcription results are stub data; no real transcript text is generated.
