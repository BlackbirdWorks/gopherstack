# Gopherstack

[![Release](https://github.com/agbishop/Gopherstack/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/agbishop/Gopherstack/actions/workflows/release.yml)
[![Coverage](https://codecov.io/gh/agbishop/Gopherstack/branch/main/graph/badge.svg)](https://codecov.io/gh/agbishop/Gopherstack)
[![Go Report Card](https://goreportcard.com/badge/github.com/agbishop/Gopherstack)](https://goreportcard.com/report/github.com/agbishop/Gopherstack)

Gopherstack is a lightweight, in-memory AWS stack implementation for Go. It provides high-performance, mock-compatible versions of core AWS services like DynamoDB and S3, designed for rapid development, testing, and CI/CD pipelines.

> [!IMPORTANT]
> **This project is vibe coded.** 🚀 It's built for speed, performance, and developer experience.

## Features

### DynamoDB
- **In-Memory Storage**: Blazing fast in-memory storage for tables and items.
- **Secondary Indexes**: Full support for Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI).
- **Rich Querying**: Complex queries with Sort Key conditions, pagination (`Limit`, `ExclusiveStartKey`), and ordering control.
- **Efficient Scanning**: Flexible table scans with filtering and projection supporting DynamoDB expressions.
- **Expression Support**: Robust handling of Expression Attribute Values and Names.
- **Optimized Memory Layout**: Struct field alignment optimized for minimal memory footprint.

### S3
- **Bucket Management**: Complete lifecycle management for versioned and unversioned buckets.
- **Object Operations**: Reliable Get, Put, Head, and List operations.
- **Versioning & Tagging**: First-class support for object versioning and metadata tagging.
- **Data Integrity**: Automatic checksum calculation supporting CRC32, CRC32C, SHA1, and SHA256.
- **Compression**: Integrated Gzip compression for efficient memory usage.

## Dashboard

Gopherstack includes a built-in web dashboard for managing DynamoDB tables and S3 buckets.

Access the dashboard at: `http://localhost:8000/dashboard`

Features:
- **DynamoDB**:
  - List tables
  - View table details (keys, indexes, item count)
  - Query and Scan tables
  - Create new tables
- **S3**:
  - List buckets
  - File browser with folder support
  - Upload and Download files
  - Manage versioning
  - View object metadata

## Usage

### Prerequisites
- Go 1.26+

### Development
```bash
# Run unit tests
make test

# Run integration tests
make integration-test

# Check linting
make lint
```

### Integration
You can use Gopherstack directly in your Go tests by initializing the in-memory backends:

```go
import "Gopherstack/dynamodb"

db := dynamodb.NewInMemoryDB()
// Use db for your application logic...
```

## License

Gopherstack is released under the [MIT License](LICENSE).
