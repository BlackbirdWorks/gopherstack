# Transcribe

In-memory Transcribe: transcription jobs, medical and call analytics jobs, vocabularies, vocabulary filters and language models. By default jobs finish with a valid but empty transcript. With the Docker engine a Vosk speech model transcribes the media.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws transcribe start-transcription-job --transcription-job-name my-job \
    --language-code en-US --media-format mp3 \
    --media '{"MediaFileUri":"s3://my-bucket/audio.mp3"}'
aws transcribe get-transcription-job --transcription-job-name my-job
aws transcribe list-transcription-jobs
```

## Supported operations

| Area | Operations |
|------|------------|
| Transcription jobs | `StartTranscriptionJob`, `GetTranscriptionJob`, `ListTranscriptionJobs`, `DeleteTranscriptionJob` |
| Medical jobs | `StartMedicalTranscriptionJob`, `GetMedicalTranscriptionJob`, `ListMedicalTranscriptionJobs`, `DeleteMedicalTranscriptionJob`, `StartMedicalScribeJob`, `GetMedicalScribeJob`, `ListMedicalScribeJobs`, `DeleteMedicalScribeJob` |
| Call Analytics | `StartCallAnalyticsJob`, `GetCallAnalyticsJob`, `ListCallAnalyticsJobs`, `DeleteCallAnalyticsJob`, `Create/Get/Update/Delete/ListCallAnalyticsCategory` |
| Vocabularies | `Create/Get/Update/Delete/ListVocabulary`, `Create/Get/Update/Delete/ListMedicalVocabulary`, `Create/Get/Update/Delete/ListVocabularyFilter` |
| Language models | `CreateLanguageModel`, `DescribeLanguageModel`, `UpdateLanguageModel`, `DeleteLanguageModel`, `ListLanguageModels` |
| Tags | `TagResource`, `UntagResource`, `ListTagsForResource` |

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--transcribe-engine` | `TRANSCRIBE_ENGINE` | `stub` | `stub` or `docker` |
| `--transcribe-engine-image` | `TRANSCRIBE_ENGINE_IMAGE` | pinned default | Vosk server image for the Docker engine |
| `--transcribe-model-dir` | `TRANSCRIBE_MODEL_DIR` | user cache dir | Cache for downloaded Vosk models |

## Differences from AWS

> [!WARNING]
> In `stub` mode no audio is processed and every transcript is empty.

With the Docker engine:

- Jobs with `ShowSpeakerLabels`, `ContentRedaction` or `MedicalContentIdentificationType` fail: Vosk has no diarization or PII detection.
- Custom vocabularies and custom language models are not applied to recognition. Vocabulary filters (mask, remove, tag) are applied to the transcript.
- `IdentifyLanguage` and `IdentifyMultipleLanguages` report the first `LanguageOptions` entry with a fixed score.
- MP4/M4A (AAC), AMR, WebM and Ogg Opus cannot be decoded. Those jobs fail.
- Decoded audio is capped at 64 MiB of 16 kHz 16-bit PCM across all channels (about 33 minutes mono). Longer jobs fail with `Invalid file size: file size too large`, where AWS accepts up to 4 hours.
- Call Analytics sentiment, loudness, talk time, categories and summaries are not produced. `Skipped` is never set.
- Medical and Call Analytics transcripts follow the documented schema, but PHI annotations and the `Categories` and `IssuesDetected` sections are not emitted.

In all modes:

- `StartMedicalScribeJob` completes at once and reports `s3://<OutputBucketName>/<JobName>-transcript.json` and `-clinical-document.json` without writing either object. The clinical document comes from a generative model that cannot be reproduced.
- `OutputEncryptionKMSKeyId` on Call Analytics, Medical Scribe and Medical Transcription jobs is not applied.
