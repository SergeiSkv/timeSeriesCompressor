# TimeSeriesCompressor

High-performance time series compression library for ClickHouse and other time series databases. Achieves up to **99% compression** on real-world metrics data.

## Features

- **Multiple compression algorithms** with configurable methods
- **Zero-allocation design** using sync.Pool
- **Configurable field mapping** for any JSON schema
- **Parallel processing** support
- **1000+ points/ms** processing speed

## Installation

```bash
go get github.com/SergeiSkv/timeSeriesCompressor
```

## Quick Start

```go
import "github.com/SergeiSkv/timeSeriesCompressor/pkg/compressor"

// Create compressor with default config
c := compressor.NewCompressor(nil)

// Compress JSON data
input := `[
    {"timestamp": 1000, "value": 100.0, "host": "server1"},
    {"timestamp": 1001, "value": 100.01, "host": "server1"},
    {"timestamp": 1002, "value": 100.02, "host": "server1"}
]`

compressed, err := c.CompressJSON([]byte(input))
// Result: significantly smaller JSON with similar points removed
```

## Compression Methods

### 1. Delta Compression (`MethodDelta`)
Removes data points where the change (delta) from the previous value is below a threshold.

**Use case**: Sensors that report frequently but values change rarely.

**Example**: Temperature sensor reporting every second but temperature changes slowly.
```go
config := compressor.DefaultConfig()
config.Method = compressor.MethodDelta
config.DeltaThreshold = 0.1  // Skip if change < 0.1
config.TimeWindow = time.Minute  // Within 1 minute window
```

### 2. Similarity Compression (`MethodSimilarity`) 
Removes consecutive values that are similar within a percentage threshold.

**Use case**: Metrics with small noise/jitter that isn't meaningful.

**Example**: CPU usage with minor fluctuations (75.1%, 75.2%, 75.15%).
```go
config.Method = compressor.MethodSimilarity
config.SimilarityThreshold = 0.01  // 1% threshold
```

### 3. Downsampling (`MethodDownsample`)
Reduces data resolution by aggregating points into time buckets.

**Use case**: Converting high-frequency data to lower frequency for long-term storage.

**Example**: Convert per-second metrics to per-minute averages.
```go
config.Method = compressor.MethodDownsample
config.DownsampleRate = time.Minute  // 1-minute buckets
config.AggregationFunc = "avg"  // or "min", "max", "sum", "last", "first"
```

### 4. Gorilla Compression (`MethodGorilla`)
Facebook's Gorilla time series compression using XOR and delta-of-delta encoding.

**Use case**: Time series with regular intervals and gradual value changes.

**Example**: Regular monitoring metrics collected every 10 seconds.
```go
config.Method = compressor.MethodGorilla
config.UseGorilla = true
```

### 5. Hybrid Compression (`MethodHybrid`) - Default
Combines multiple methods for maximum compression:
1. First applies similarity compression to remove noise
2. Then downsamples to reduce resolution
3. Optionally applies Gorilla compression

**Use case**: General purpose, achieves best compression ratios.

**Example**: Mixed workload with various metric types.
```go
config.Method = compressor.MethodHybrid  // Default
// Achieves 99%+ compression on typical metrics
```

## Field Configuration

Configure which JSON fields contain timestamps, values, and tags:

```go
c := compressor.NewCompressor(nil).
    WithTimestampFields("@timestamp", "time", "ts").
    WithValueFields("cpu_usage", "memory", "value").
    WithGroupByFields("host", "service", "region")

// Works with custom field names
input := `{
    "@timestamp": 1234567890,
    "cpu_usage": 45.5,
    "host": "web-01",
    "service": "api"
}`
```

## Performance

Benchmark results on Apple M3:

| Method | Input Points | Output Points | Compression | Speed |
|--------|--------------|---------------|-------------|-------|
| Hybrid | 10,000 | 60 | 99.4% | 1,250 points/ms |
| Downsample | 1,000 | 59 | 94.1% | 1,000 points/ms |
| Similarity | 1,000 | 980 | 2.0% | 1,000 points/ms |
| Delta | 1,000 | 994 | 0.6% | 1,000 points/ms |

## Architecture

The compressor is designed to be:
- **Stateless**: No internal state, safe for concurrent use
- **Single responsibility**: Only does compression, no buffering or batching
- **Zero-allocation**: Reuses memory via sync.Pool
- **Fast**: Processes thousands of points per millisecond

Typical pipeline:
```
Data Collector → Batch (10k points) → TimeSeriesCompressor → ClickHouse
                                            ↓
                                      100 points (99% compression)
```

## Configuration Options

```go
type Config struct {
    // Field mapping
    TimestampFields []string  // Fields containing timestamps
    ValueFields     []string  // Fields containing values to compress
    TagFields       []string  // Fields containing tags/labels
    GroupByFields   []string  // Fields to group series by
    
    // Compression method
    Method CompressionMethod
    
    // Method-specific settings
    DeltaThreshold      float64       // For MethodDelta
    SimilarityThreshold float64       // For MethodSimilarity (0.01 = 1%)
    DownsampleRate      time.Duration // For MethodDownsample
    AggregationFunc     string        // "avg", "min", "max", "sum", "last"
    UseGorilla          bool          // Enable Gorilla compression
    
    // Performance
    Workers    int  // Parallel workers (0 = auto)
    BatchSize  int  // Internal batch size
}
```

## Use Cases

### Monitoring & Observability
```go
// Compress Prometheus-style metrics
config := compressor.DefaultConfig()
config.TimestampFields = []string{"timestamp"}
config.ValueFields = []string{"value"}
config.TagFields = []string{"labels"}
config.DownsampleRate = time.Minute
```

### IoT Sensor Data
```go
// High-frequency sensor data
config := compressor.DefaultConfig()
config.Method = compressor.MethodDelta
config.DeltaThreshold = 0.01  // Ignore tiny changes
config.TimeWindow = 5 * time.Second
```

### Log Aggregation
```go
// Aggregate log metrics
config := compressor.DefaultConfig()
config.Method = compressor.MethodDownsample
config.DownsampleRate = 5 * time.Minute
config.AggregationFunc = "sum"  // Sum up counts
```

## License

MIT