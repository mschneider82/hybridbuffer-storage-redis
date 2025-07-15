# Redis Storage Backend

This package provides Redis storage backend for HybridBuffer that stores data in Redis as key-value pairs.

## Features

- **In-memory speed** with Redis performance
- **Configurable expiration** for automatic cleanup
- **Key prefixing** for namespace organization
- **Connection pooling** via go-redis client
- **Atomic operations** for data consistency

## Installation

```bash
go get schneider.vip/hybridbuffer/storage/redis
```

## Usage

### Basic Setup

```go
import (
    "github.com/redis/go-redis/v9"
    "schneider.vip/hybridbuffer/storage/redis"
)

// Create Redis client
client := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "", // no password
    DB:       9,  // default DB
})

// Create storage factory
storage := redis.New(client)

// Use with HybridBuffer
buf := hybridbuffer.New(
    hybridbuffer.WithStorage(storage),
)
```

### With Custom Options

```go
storage := redis.New(client,
    redis.WithKeyPrefix("myapp:buffers"),
    redis.WithExpiration(12 * time.Hour),
    redis.WithTimeout(10 * time.Second),
    redis.WithDatabase(5),        // Use Redis database 5
    redis.WithChunkSize(128*1024), // 128KB chunks for streaming
)
```

### Redis Cluster Example

```go
import "github.com/redis/go-redis/v9"

// Redis Cluster configuration
client := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{
        "localhost:7000",
        "localhost:7001", 
        "localhost:7002",
    },
})

storage := redis.New(client,
    redis.WithKeyPrefix("cluster:buffers"),
)
```

### Redis Sentinel Example

```go
client := redis.NewFailoverClient(&redis.FailoverOptions{
    MasterName:    "mymaster",
    SentinelAddrs: []string{"localhost:26379"},
})

storage := redis.New(client)
```

## Configuration Options

### WithKeyPrefix(prefix string)
Sets the Redis key prefix. Default is "hybridbuffer".

```go
storage := redis.New(client,
    redis.WithKeyPrefix("myapp:temp"),
)
```

Keys will be created like: `myapp:temp:1640995200:a1b2c3d4`

### WithExpiration(duration time.Duration)
Sets the expiration time for Redis keys. Default is 24 hours.

```go
storage := redis.New(client,
    redis.WithExpiration(6 * time.Hour),
)
```

Use `0` for no expiration (keys persist until manually deleted).

### WithTimeout(duration time.Duration)
Sets the timeout for Redis operations. Default is 30 seconds.

```go
storage := redis.New(client,
    redis.WithTimeout(5 * time.Second),
)
```

### WithDatabase(db int)
Sets the Redis database number to use. Default is database 9. Valid range is 0-15.

```go
storage := redis.New(client,
    redis.WithDatabase(5), // Use database 5
)
```

### WithChunkSize(size int)
Sets the chunk size for streaming data. Default is 64KB. Smaller chunks mean more Redis operations but less memory usage.

```go
storage := redis.New(client,
    redis.WithChunkSize(128*1024), // 128KB chunks
)
```

## Key Naming

Keys are created with unique names following the pattern:
```
{prefix}:{timestamp}:{random}
```

- **prefix**: Configurable prefix (default: "hybridbuffer")
- **timestamp**: Unix timestamp when key was created
- **random**: 8 random hex characters for uniqueness

Example: `hybridbuffer:1640995200:a1b2c3d4`

## Data Storage

- **Binary safe**: Stores data as binary strings in Redis
- **Streaming**: Data is stored in configurable chunks (default 64KB)
- **Memory efficient**: Only one chunk buffered in memory at a time
- **Metadata tracking**: Chunk count stored separately for reassembly
- **Atomic writes**: Each chunk written atomically to Redis
- **Automatic expiration**: All keys (chunks + metadata) expire based on configuration

## Error Handling

The Redis backend handles various error conditions:

- **Connection failures**: Network connectivity issues
- **Authentication errors**: Invalid credentials
- **Memory limits**: Redis out of memory
- **Key expiration**: Automatic cleanup of expired keys
- **Timeout errors**: Configurable operation timeouts

## Performance Considerations

- **In-memory speed**: Fast read/write operations
- **Network latency**: Performance depends on Redis connection
- **Memory usage**: Data stored in Redis memory
- **Persistence**: Depends on Redis configuration (RDB/AOF)

## Memory Management

- **Buffered writes**: Data is buffered in memory until `Close()`
- **Redis memory**: Consider Redis memory limits and policies
- **Expiration**: Automatic cleanup prevents memory leaks
- **Eviction policies**: Configure Redis eviction appropriately

## Security

- **Authentication**: Use Redis AUTH if configured
- **Network security**: Use TLS for production deployments
- **Access control**: Redis 6+ ACL support
- **Data encryption**: Use encryption middleware for sensitive data

## Redis Configuration Recommendations

For optimal performance with HybridBuffer:

```
# Memory settings
maxmemory 2gb
maxmemory-policy allkeys-lru

# Persistence (optional)
save 900 1
save 300 10
save 60 10000

# Network
timeout 300
tcp-keepalive 300

# Security
requirepass your-secure-password
```

## High Availability

For production deployments:

- **Redis Sentinel**: Automatic failover
- **Redis Cluster**: Horizontal scaling
- **Backup strategy**: Regular RDB/AOF backups
- **Monitoring**: Redis metrics and health checks

## Dependencies

- `github.com/redis/go-redis/v9` - Go Redis client
- `github.com/pkg/errors` - Enhanced error handling

## Compatibility

- **Redis 6.0+**: Recommended for full feature support
- **Redis 5.0+**: Basic functionality supported
- **Redis Cluster**: Supported via go-redis cluster client
- **Redis Sentinel**: Supported via go-redis failover client