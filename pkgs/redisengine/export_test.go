package redisengine

// CRC64 exposes the RDB checksum for tests.
var CRC64 = crc64Jones //nolint:gochecknoglobals // test hook

// LZFDecompress exposes the LZF decoder for tests.
var LZFDecompress = lzfDecompress //nolint:gochecknoglobals // test hook
