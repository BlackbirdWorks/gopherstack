package ocistreaming

// EncodeTestBatch builds a one-record batch for tests.
func EncodeTestBatch(value []byte) []byte {
	return encodeBatch([]Message{{Value: value}}, 0)
}

// KafkaCommittedForTest reads a committed offset.
func (b *InMemoryBackend) KafkaCommittedForTest(poolID, topic, group string, part int) (int64, int16) {
	return b.kafkaCommitted(poolID, topic, group, part)
}
