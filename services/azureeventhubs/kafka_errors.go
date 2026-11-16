package azureeventhubs

// Kafka protocol error codes (Apache Kafka protocol error table).
const (
	kerrNone                   int16 = 0
	kerrOffsetOutOfRange       int16 = 1
	kerrCorruptMessage         int16 = 2
	kerrUnknownTopic           int16 = 3
	kerrMessageTooLarge        int16 = 10
	kerrCoordinatorUnavailable int16 = 15
	kerrIllegalGeneration      int16 = 22
	kerrInconsistentProtocol   int16 = 23
	kerrInvalidGroupID         int16 = 24
	kerrUnknownMember          int16 = 25
	kerrInvalidSessionTimeout  int16 = 26
	kerrRebalanceInProgress    int16 = 27
	kerrTopicAuthFailed        int16 = 29
	kerrGroupAuthFailed        int16 = 30
	kerrUnsupportedSASL        int16 = 33
	kerrIllegalSASLState       int16 = 34
	kerrUnsupportedVersion     int16 = 35
	kerrTopicAlreadyExists     int16 = 36
	kerrInvalidPartitions      int16 = 37
	kerrInvalidRequest         int16 = 42
	kerrOutOfOrderSequence     int16 = 45
	kerrInvalidProducerEpoch   int16 = 47
	kerrUnsupportedCompression int16 = 76
	kerrSASLAuthFailed         int16 = 58
	kerrUnknownProducerID      int16 = 59
	kerrMemberIDRequired       int16 = 79
)
