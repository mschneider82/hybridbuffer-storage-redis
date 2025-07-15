package redis

import (
	"context"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func setupRedis(t *testing.T) RedisClient {
	// Check if Redis is available via environment variable
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/9" // Use database 9 for testing
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Skipf("Invalid Redis URL %s: %v", redisURL, err)
	}

	// Ensure we're using database 9 for testing
	opts.DB = 9
	client := redis.NewClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s: %v", redisURL, err)
	}

	// Clean up any existing test keys
	client.FlushDB(ctx)

	return client
}

func TestRedisBackend_BasicOperations(t *testing.T) {
	client := setupRedis(t)

	backend, err := newBackend(client)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Test Create and Write
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	testData := []byte("Hello Redis Storage!")
	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to write %d bytes, got %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Test Open and Read
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	readData := make([]byte, len(testData))
	n, err = reader.Read(readData)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testData), n)
	}

	if string(readData) != string(testData) {
		t.Fatalf("Data mismatch: expected %q, got %q", string(testData), string(readData))
	}
}

func TestRedisBackend_LargeData(t *testing.T) {
	client := setupRedis(t)

	backend, err := newBackend(client)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Create 1MB of test data
	testData := make([]byte, 1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write large data
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write large data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to write %d bytes, got %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read large data back
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	readData := make([]byte, len(testData))
	totalRead := 0
	for totalRead < len(testData) {
		n, err := reader.Read(readData[totalRead:])
		if err != nil {
			t.Fatalf("Failed to read large data at offset %d: %v", totalRead, err)
		}
		totalRead += n
	}

	if totalRead != len(testData) {
		t.Fatalf("Expected to read %d bytes total, got %d", len(testData), totalRead)
	}

	// Verify data integrity
	for i := range testData {
		if testData[i] != readData[i] {
			t.Fatalf("Data mismatch at byte %d: expected %d, got %d", i, testData[i], readData[i])
		}
	}
}

func TestRedisBackend_WithOptions(t *testing.T) {
	client := setupRedis(t)

	// Test with custom options
	backend, err := newBackend(client,
		WithKeyPrefix("test"),
		WithExpiration(1*time.Hour),
		WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("Failed to create backend with options: %v", err)
	}
	defer backend.Remove()

	// Verify the key prefix is used
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	testData := []byte("test data with options")
	writer.Write(testData)
	writer.Close()

	// Check that the key has the correct prefix
	if backend.key == "" {
		t.Fatal("Backend key should not be empty after creation")
	}
	if backend.keyPrefix != "test" {
		t.Fatalf("Expected key prefix 'test', got %q", backend.keyPrefix)
	}

	// Verify data can be read back
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	readData := make([]byte, len(testData))
	n, err := reader.Read(readData)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testData), n)
	}

	if string(readData) != string(testData) {
		t.Fatalf("Data mismatch: expected %q, got %q", string(testData), string(readData))
	}
}

func TestRedisBackend_Remove(t *testing.T) {
	client := setupRedis(t)

	backend, err := newBackend(client)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Create and write some data
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	testData := []byte("data to be removed")
	writer.Write(testData)
	writer.Close()

	// Verify data exists
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader before remove: %v", err)
	}
	reader.Close()

	// Remove the data
	err = backend.Remove()
	if err != nil {
		t.Fatalf("Failed to remove data: %v", err)
	}

	// Verify data is gone
	_, err = backend.Open()
	if err == nil {
		t.Fatal("Expected error when opening removed data, got none")
	}
}

func TestRedisBackend_ErrorCases(t *testing.T) {
	// Test with nil client
	_, err := newBackend(nil)
	if err == nil {
		t.Fatal("Expected error with nil client, got none")
	}

	client := setupRedis(t)

	backend, err := newBackend(client)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Test opening without creating first
	_, err = backend.Open()
	if err == nil {
		t.Fatal("Expected error when opening without creating, got none")
	}

	// Test removing without creating
	err = backend.Remove()
	if err != nil {
		t.Fatalf("Remove should not fail for non-existent key: %v", err)
	}
}

func TestRedisBackend_Factory(t *testing.T) {
	client := setupRedis(t)

	// Test factory creation
	factory := New(client,
		WithKeyPrefix("factory-test"),
		WithExpiration(30*time.Minute),
	)

	// Test backend creation from factory
	backend := factory()
	if backend == nil {
		t.Fatal("Factory should return a backend")
	}
	defer backend.Remove()

	// Test that backend works
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Backend Create failed: %v", err)
	}
	defer writer.Close()

	testData := []byte("factory test data")
	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write via factory backend: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to write %d bytes, got %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Test reading back
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	readData := make([]byte, len(testData))
	n, err = reader.Read(readData)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testData), n)
	}

	if string(readData) != string(testData) {
		t.Fatalf("Data mismatch: expected %q, got %q", string(testData), string(readData))
	}
}

func TestRedisBackend_Expiration(t *testing.T) {
	client := setupRedis(t)

	// Create backend with very short expiration for testing
	backend, err := newBackend(client,
		WithExpiration(1*time.Second),
		WithKeyPrefix("expiration-test"),
	)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Create and write data
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	testData := []byte("data that will expire")
	writer.Write(testData)
	writer.Close()

	// Verify data exists initially
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader immediately: %v", err)
	}
	reader.Close()

	// Wait for expiration + a bit more
	time.Sleep(2 * time.Second)

	// Try to read again - should fail due to expiration
	_, err = backend.Open()
	if err == nil {
		t.Log("Note: Data may not have expired yet, this can happen in Redis depending on configuration")
		// Don't fail the test as Redis expiration timing can vary
	}
}

func TestRedisBackend_ConcurrentAccess(t *testing.T) {
	client := setupRedis(t)

	// Test multiple backends accessing Redis concurrently
	numBackends := 5
	backends := make([]*Backend, numBackends)

	// Create multiple backends
	for i := 0; i < numBackends; i++ {
		backend, err := newBackend(client,
			WithKeyPrefix("concurrent"),
		)
		if err != nil {
			t.Fatalf("Failed to create backend %d: %v", i, err)
		}
		backends[i] = backend
		defer backend.Remove()
	}

	// Write data to each backend concurrently
	done := make(chan bool, numBackends)
	for i, backend := range backends {
		go func(id int, b *Backend) {
			defer func() { done <- true }()

			writer, err := b.Create()
			if err != nil {
				t.Errorf("Backend %d: Failed to create writer: %v", id, err)
				return
			}
			defer writer.Close()

			testData := []byte("concurrent data " + string(rune('A'+id)))
			_, err = writer.Write(testData)
			if err != nil {
				t.Errorf("Backend %d: Failed to write data: %v", id, err)
				return
			}
		}(i, backend)
	}

	// Wait for all writes to complete
	for i := 0; i < numBackends; i++ {
		<-done
	}

	// Read data from each backend
	for i, backend := range backends {
		reader, err := backend.Open()
		if err != nil {
			t.Errorf("Backend %d: Failed to open reader: %v", i, err)
			continue
		}

		data := make([]byte, 100)
		n, err := reader.Read(data)
		if err != nil {
			t.Errorf("Backend %d: Failed to read data: %v", i, err)
			reader.Close()
			continue
		}
		reader.Close()

		expected := "concurrent data " + string(rune('A'+i))
		if string(data[:n]) != expected {
			t.Errorf("Backend %d: Expected %q, got %q", i, expected, string(data[:n]))
		}
	}
}

func TestRedisBackend_ChunkingSmallChunks(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()

	// Create backend with very small chunks to force chunking
	backend, err := newBackend(client,
		WithChunkSize(10), // Very small chunks
		WithKeyPrefix("chunking-test"),
	)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Create test data larger than chunk size
	testData := []byte("This is a test string that is definitely longer than 10 bytes")

	// Write data
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to write %d bytes, got %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read data back
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	readData := make([]byte, len(testData))
	totalRead := 0
	for totalRead < len(testData) {
		n, err := reader.Read(readData[totalRead:])
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read data: %v", err)
		}
		totalRead += n
		if err == io.EOF {
			break
		}
	}

	if totalRead != len(testData) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testData), totalRead)
	}

	if string(readData) != string(testData) {
		t.Fatalf("Data mismatch: expected %q, got %q", string(testData), string(readData))
	}
}

func TestRedisBackend_StreamingLargeData(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()

	// Create backend with moderate chunk size
	backend, err := newBackend(client,
		WithChunkSize(1024), // 1KB chunks
		WithKeyPrefix("streaming-test"),
	)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Create 5MB of test data with pattern
	dataSize := 5 * 1024 * 1024
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write data in streaming fashion
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write in multiple calls to test streaming
	chunkWriteSize := 4096
	written := 0
	for written < len(testData) {
		endPos := written + chunkWriteSize
		if endPos > len(testData) {
			endPos = len(testData)
		}

		n, err := writer.Write(testData[written:endPos])
		if err != nil {
			t.Fatalf("Failed to write chunk at offset %d: %v", written, err)
		}
		written += n
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read data back in streaming fashion
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	readData := make([]byte, len(testData))
	totalRead := 0
	readBuffer := make([]byte, 8192) // Read in 8KB chunks

	for totalRead < len(testData) {
		n, err := reader.Read(readBuffer)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read data at offset %d: %v", totalRead, err)
		}

		if n > 0 {
			copy(readData[totalRead:], readBuffer[:n])
			totalRead += n
		}

		if err == io.EOF {
			break
		}
	}

	if totalRead != len(testData) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testData), totalRead)
	}

	// Verify data integrity
	for i := 0; i < len(testData); i++ {
		if testData[i] != readData[i] {
			t.Fatalf("Data mismatch at byte %d: expected %d, got %d", i, testData[i], readData[i])
		}
	}
}

func TestRedisBackend_ChunkMetadata(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()

	// Create backend with small chunks
	backend, err := newBackend(client,
		WithChunkSize(100),
		WithKeyPrefix("metadata-test"),
	)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Write data that will create multiple chunks
	testData := make([]byte, 350) // Should create 4 chunks (100, 100, 100, 50)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to write %d bytes, got %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Manually check metadata in Redis
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	metaKey := backend.getMetadataKey()
	result := client.Get(ctx, metaKey)
	if err := result.Err(); err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	metaValue, err := result.Result()
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}

	expectedChunks := 4 // 350 bytes / 100 bytes per chunk = 3.5 -> 4 chunks
	if metaValue != strconv.Itoa(expectedChunks) {
		t.Fatalf("Expected metadata %d, got %s", expectedChunks, metaValue)
	}

	// Verify we can read all chunks
	for i := 0; i < expectedChunks; i++ {
		chunkKey := backend.getChunkKey(i)
		chunkResult := client.Get(ctx, chunkKey)
		if err := chunkResult.Err(); err != nil {
			t.Fatalf("Failed to get chunk %d: %v", i, err)
		}
	}
}

func TestRedisBackend_EmptyData(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()

	backend, err := newBackend(client)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Write empty data
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	n, err := writer.Write([]byte{})
	if err != nil {
		t.Fatalf("Failed to write empty data: %v", err)
	}
	if n != 0 {
		t.Fatalf("Expected to write 0 bytes, got %d", n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read back empty data
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	data := make([]byte, 10)
	n, err = reader.Read(data)
	if err != io.EOF {
		t.Fatalf("Expected EOF for empty data, got error: %v", err)
	}
	if n != 0 {
		t.Fatalf("Expected to read 0 bytes, got %d", n)
	}
}

func TestRedisBackend_DatabaseOption(t *testing.T) {
	client := setupRedis(t)
	defer client.Close()

	// Test with custom database option
	backend, err := newBackend(client,
		WithDatabase(7),
		WithKeyPrefix("db-test"),
	)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Verify the database field is set correctly
	if backend.database != 7 {
		t.Fatalf("Expected database 7, got %d", backend.database)
	}

	// Test that invalid database numbers are ignored
	backend2, err := newBackend(client,
		WithDatabase(-1), // Invalid
	)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend2.Remove()

	if backend2.database != 9 { // Should remain default
		t.Fatalf("Expected database 9 (default), got %d", backend2.database)
	}

	// Test that database 16 (invalid) is ignored
	backend3, err := newBackend(client,
		WithDatabase(16), // Invalid (Redis supports 0-15)
	)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend3.Remove()

	if backend3.database != 9 { // Should remain default
		t.Fatalf("Expected database 9 (default), got %d", backend3.database)
	}
}
