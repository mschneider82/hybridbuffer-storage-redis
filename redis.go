// Package redis provides Redis storage backend for HybridBuffer
package redis

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"schneider.vip/hybridbuffer/storage"
)

// Default chunk size for streaming (64KB)
const defaultChunkSize = 64 * 1024

// RedisClient interface for testing and flexibility
type RedisClient interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	Pipeline() redis.Pipeliner
	FlushDB(ctx context.Context) *redis.StatusCmd
	Close() error
}

// Backend implements storage.Backend for Redis with chunking support
type Backend struct {
	client     RedisClient
	keyPrefix  string
	key        string
	chunkSize  int
	expiration time.Duration
	timeout    time.Duration
	database   int
}

// Option configures Redis storage backend
type Option func(*Backend)

// WithKeyPrefix sets the Redis key prefix
func WithKeyPrefix(prefix string) Option {
	return func(r *Backend) {
		r.keyPrefix = prefix
	}
}

// WithExpiration sets the expiration time for Redis keys
func WithExpiration(expiration time.Duration) Option {
	return func(r *Backend) {
		r.expiration = expiration
	}
}

// WithTimeout sets the timeout for Redis operations
func WithTimeout(timeout time.Duration) Option {
	return func(r *Backend) {
		r.timeout = timeout
	}
}

// WithChunkSize sets the chunk size for streaming
func WithChunkSize(size int) Option {
	return func(r *Backend) {
		if size > 0 {
			r.chunkSize = size
		}
	}
}

// WithDatabase sets the Redis database number to use (default: 9)
func WithDatabase(db int) Option {
	return func(r *Backend) {
		if db >= 0 && db <= 15 { // Redis supports databases 0-15
			r.database = db
		}
	}
}

// newBackend creates a new Redis-based storage backend
func newBackend(client RedisClient, opts ...Option) (*Backend, error) {
	if client == nil {
		return nil, errors.New("Redis client cannot be nil")
	}

	backend := &Backend{
		client:     client,
		keyPrefix:  "hybridbuffer",
		chunkSize:  defaultChunkSize,
		expiration: 24 * time.Hour, // Default: 24 hours
		timeout:    30 * time.Second,
		database:   9, // Default: database 9
	}

	// Apply options
	for _, opt := range opts {
		opt(backend)
	}

	return backend, nil
}

// Create implements storage.Backend
func (r *Backend) Create() (io.WriteCloser, error) {
	// Generate unique key
	key, err := r.generateKey()
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate Redis key")
	}
	r.key = key

	return &streamingWriteCloser{
		backend:    r,
		chunkIndex: 0,
		buffer:     make([]byte, 0, r.chunkSize),
	}, nil
}

// Open implements storage.Backend
func (r *Backend) Open() (io.ReadCloser, error) {
	if r.key == "" {
		return nil, errors.New("no key created yet")
	}

	// Get metadata to determine total chunks
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	metaKey := r.getMetadataKey()
	result := r.client.Get(ctx, metaKey)
	if err := result.Err(); err != nil {
		if err == redis.Nil {
			return nil, errors.New("metadata not found or expired")
		}
		return nil, errors.Wrap(err, "failed to get metadata")
	}

	metaData, err := result.Result()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read metadata")
	}

	totalChunks, err := strconv.Atoi(metaData)
	if err != nil {
		return nil, errors.Wrap(err, "invalid metadata format")
	}

	return &streamingReadCloser{
		backend:     r,
		totalChunks: totalChunks,
		chunkIndex:  0,
	}, nil
}

// Remove implements storage.Backend
func (r *Backend) Remove() error {
	if r.key == "" {
		return nil // Nothing to remove
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	// Get metadata to determine how many chunks to delete
	metaKey := r.getMetadataKey()
	result := r.client.Get(ctx, metaKey)
	if result.Err() != nil && result.Err() != redis.Nil {
		// If we can't get metadata, try to delete what we can
		r.client.Del(ctx, metaKey)
		return nil
	}

	keysToDelete := []string{metaKey}

	if result.Err() != redis.Nil {
		metaData, err := result.Result()
		if err == nil {
			totalChunks, err := strconv.Atoi(metaData)
			if err == nil {
				// Add all chunk keys to deletion list
				for i := 0; i < totalChunks; i++ {
					chunkKey := r.getChunkKey(i)
					keysToDelete = append(keysToDelete, chunkKey)
				}
			}
		}
	}

	// Delete all keys
	delResult := r.client.Del(ctx, keysToDelete...)
	if err := delResult.Err(); err != nil {
		return errors.Wrap(err, "failed to delete Redis keys")
	}

	return nil
}

// generateKey creates a unique Redis key
func (r *Backend) generateKey() (string, error) {
	// Generate random suffix
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}

	timestamp := time.Now().Unix()
	randomSuffix := fmt.Sprintf("%x", randomBytes)

	key := fmt.Sprintf("%s:%d:%s", r.keyPrefix, timestamp, randomSuffix)
	return key, nil
}

// getChunkKey returns the Redis key for a specific chunk
func (r *Backend) getChunkKey(chunkIndex int) string {
	return fmt.Sprintf("%s:chunk:%d", r.key, chunkIndex)
}

// getMetadataKey returns the Redis key for metadata
func (r *Backend) getMetadataKey() string {
	return fmt.Sprintf("%s:meta", r.key)
}

// streamingWriteCloser implements io.WriteCloser for chunked Redis storage
type streamingWriteCloser struct {
	backend    *Backend
	chunkIndex int
	buffer     []byte
}

// Write implements io.Writer
func (w *streamingWriteCloser) Write(p []byte) (n int, err error) {
	written := 0

	for len(p) > 0 {
		// How much space is left in current buffer?
		spaceLeft := w.backend.chunkSize - len(w.buffer)

		if spaceLeft == 0 {
			// Buffer is full, flush it
			if err := w.flushChunk(); err != nil {
				return written, err
			}
		}

		// How much can we write to current buffer?
		toWrite := len(p)
		if toWrite > spaceLeft {
			toWrite = spaceLeft
		}

		// Append to buffer
		w.buffer = append(w.buffer, p[:toWrite]...)
		written += toWrite
		p = p[toWrite:]
	}

	return written, nil
}

// flushChunk writes the current buffer as a chunk to Redis
func (w *streamingWriteCloser) flushChunk() error {
	if len(w.buffer) == 0 {
		return nil // Nothing to flush
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.backend.timeout)
	defer cancel()

	chunkKey := w.backend.getChunkKey(w.chunkIndex)
	result := w.backend.client.Set(ctx, chunkKey, w.buffer, w.backend.expiration)
	if err := result.Err(); err != nil {
		return errors.Wrapf(err, "failed to store chunk %d", w.chunkIndex)
	}

	// Reset buffer and increment chunk index
	w.buffer = w.buffer[:0]
	w.chunkIndex++

	return nil
}

// Close implements io.Closer and finalizes the chunked storage
func (w *streamingWriteCloser) Close() error {
	// Flush any remaining data
	if err := w.flushChunk(); err != nil {
		return err
	}

	// Store metadata about total chunks
	ctx, cancel := context.WithTimeout(context.Background(), w.backend.timeout)
	defer cancel()

	metaKey := w.backend.getMetadataKey()
	result := w.backend.client.Set(ctx, metaKey, strconv.Itoa(w.chunkIndex), w.backend.expiration)
	if err := result.Err(); err != nil {
		return errors.Wrap(err, "failed to store metadata")
	}

	return nil
}

// streamingReadCloser implements io.ReadCloser for chunked Redis storage
type streamingReadCloser struct {
	backend     *Backend
	totalChunks int
	chunkIndex  int
	chunkData   []byte
	chunkPos    int
}

// Read implements io.Reader
func (r *streamingReadCloser) Read(p []byte) (n int, err error) {
	if r.chunkIndex >= r.totalChunks {
		return 0, io.EOF
	}

	totalRead := 0

	for len(p) > 0 && r.chunkIndex < r.totalChunks {
		// Do we need to load the next chunk?
		if len(r.chunkData) == 0 || r.chunkPos >= len(r.chunkData) {
			if err := r.loadNextChunk(); err != nil {
				if totalRead > 0 {
					return totalRead, nil // Return what we read so far
				}
				return 0, err
			}
		}

		// Copy from current chunk
		available := len(r.chunkData) - r.chunkPos
		toCopy := len(p)
		if toCopy > available {
			toCopy = available
		}

		copy(p, r.chunkData[r.chunkPos:r.chunkPos+toCopy])
		r.chunkPos += toCopy
		totalRead += toCopy
		p = p[toCopy:]

		// Move to next chunk if current is exhausted
		if r.chunkPos >= len(r.chunkData) {
			r.chunkIndex++
			r.chunkData = nil
			r.chunkPos = 0
		}
	}

	if totalRead == 0 && r.chunkIndex >= r.totalChunks {
		return 0, io.EOF
	}

	return totalRead, nil
}

// loadNextChunk loads the next chunk from Redis
func (r *streamingReadCloser) loadNextChunk() error {
	if r.chunkIndex >= r.totalChunks {
		return io.EOF
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.backend.timeout)
	defer cancel()

	chunkKey := r.backend.getChunkKey(r.chunkIndex)
	result := r.backend.client.Get(ctx, chunkKey)
	if err := result.Err(); err != nil {
		if err == redis.Nil {
			return errors.Errorf("chunk %d not found", r.chunkIndex)
		}
		return errors.Wrapf(err, "failed to load chunk %d", r.chunkIndex)
	}

	data, err := result.Result()
	if err != nil {
		return errors.Wrapf(err, "failed to read chunk %d data", r.chunkIndex)
	}

	r.chunkData = []byte(data)
	r.chunkPos = 0

	return nil
}

// Close implements io.Closer
func (r *streamingReadCloser) Close() error {
	// Nothing special to do for reading
	return nil
}

// New creates a new Redis storage backend provider function
func New(client RedisClient, opts ...Option) func() storage.Backend {
	return func() storage.Backend {
		backend, _ := newBackend(client, opts...)
		return backend
	}
}
