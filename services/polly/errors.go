package polly

import "errors"

var (
	// ErrLexiconNotFound is returned when a requested lexicon is absent.
	ErrLexiconNotFound = errors.New("LexiconNotFoundException")
	// ErrTaskNotFound is returned when a requested synthesis task is absent.
	ErrTaskNotFound = errors.New("SynthesisTaskNotFoundException")
	// ErrValidation is returned when request parameters do not meet Polly constraints
	// that AWS models as a generic/unlisted validation failure.
	ErrValidation = errors.New("InvalidParameterValueException")
	// ErrTextLengthExceeded is returned when Text exceeds the format-specific length limit.
	ErrTextLengthExceeded = errors.New("TextLengthExceededException")
	// ErrInvalidSampleRate is returned when SampleRate is not valid for the requested OutputFormat.
	ErrInvalidSampleRate = errors.New("InvalidSampleRateException")
	// ErrEngineNotSupported is returned when the requested voice does not support the requested engine.
	ErrEngineNotSupported = errors.New("EngineNotSupportedException")
	// ErrLanguageNotSupported is returned when the requested voice does not support the requested language.
	ErrLanguageNotSupported = errors.New("LanguageNotSupportedException")
	// ErrMarksNotSupportedForFormat is returned when SpeechMarkTypes is requested with a non-json OutputFormat.
	ErrMarksNotSupportedForFormat = errors.New("MarksNotSupportedForFormatException")
	// ErrSsmlMarksNotSupportedForTextType is returned when the "ssml" speech mark type is
	// requested with a plain-text TextType.
	ErrSsmlMarksNotSupportedForTextType = errors.New("SsmlMarksNotSupportedForTextTypeException")
	// ErrInvalidNextToken is returned when a pagination token cannot be decoded.
	ErrInvalidNextToken = errors.New("InvalidNextTokenException")
	// ErrInvalidLexicon is returned when lexicon Content is not well-formed PLS lexicon XML.
	ErrInvalidLexicon = errors.New("InvalidLexiconException")
	// ErrLexiconSizeExceeded is returned when lexicon Content exceeds the maximum lexicon size.
	ErrLexiconSizeExceeded = errors.New("LexiconSizeExceededException")
	// ErrMaxLexemeLengthExceeded is returned when a <phoneme>/<alias> replacement in
	// lexicon Content exceeds the maximum lexeme replacement length.
	ErrMaxLexemeLengthExceeded = errors.New("MaxLexemeLengthExceededException")
	// ErrMaxLexiconsNumberExceeded is returned when PutLexicon would create a new
	// lexicon beyond the per-account lexicon count quota.
	ErrMaxLexiconsNumberExceeded = errors.New("MaxLexiconsNumberExceededException")
	// ErrUnsupportedPlsAlphabet is returned when lexicon Content specifies an
	// alphabet other than "ipa" or "x-sampa".
	ErrUnsupportedPlsAlphabet = errors.New("UnsupportedPlsAlphabetException")
	// ErrUnsupportedPlsLanguage is returned when lexicon Content specifies an
	// xml:lang value outside Polly's supported LanguageCode set.
	ErrUnsupportedPlsLanguage = errors.New("UnsupportedPlsLanguageException")
	// ErrInvalidS3Bucket is returned when OutputS3BucketName is not a valid S3 bucket name.
	ErrInvalidS3Bucket = errors.New("InvalidS3BucketException")
	// ErrInvalidS3Key is returned when OutputS3KeyPrefix is not a valid S3 object key.
	ErrInvalidS3Key = errors.New("InvalidS3KeyException")
	// ErrInvalidSnsTopicArn is returned when SnsTopicArn is not a well-formed SNS topic ARN.
	ErrInvalidSnsTopicArn = errors.New("InvalidSnsTopicArnException")
	// ErrInvalidSsml is returned when TextType is "ssml" but Text is not
	// well-formed SSML wrapped in a <speak> root element.
	ErrInvalidSsml = errors.New("InvalidSsmlException")
	// ErrInvalidTaskID is returned when a TaskId path parameter is not a
	// syntactically valid task identifier (as opposed to a well-formed one that
	// simply does not exist, which is ErrTaskNotFound).
	ErrInvalidTaskID = errors.New("InvalidTaskIdException")
	// ErrStreamValidation is returned for StartSpeechSynthesisStream input
	// validation failures. AWS models this operation's client errors as the
	// generic smithy ValidationException, not the op-specific exceptions
	// (EngineNotSupportedException, InvalidSampleRateException, ...) used by
	// SynthesizeSpeech/StartSpeechSynthesisTask -- see
	// aws-sdk-go-v2/service/polly's StartSpeechSynthesisStream deserializer
	// error switch, which only lists ServiceFailureException,
	// ServiceQuotaExceededException, ThrottlingException, and ValidationException.
	ErrStreamValidation = errors.New("ValidationException")
)
