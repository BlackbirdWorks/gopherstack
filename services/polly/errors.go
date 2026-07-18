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
	// ErrResourceNotFound is returned when a tagged resource ARN is unknown.
	ErrResourceNotFound = errors.New("ResourceNotFoundException")
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
)
