package compressor

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// BenchmarkCompressor_SmallBatch tests compression of small batches (100 points)
func BenchmarkCompressor_SmallBatch(b *testing.B) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"value"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
	}
	c := NewCompressor(config)

	data := generateTestData(100, 10, 1)
	jsonData, _ := json.Marshal(data)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(jsonData)))

	for i := 0; i < b.N; i++ {
		_, _ = c.CompressJSON(jsonData)
	}
}

// BenchmarkCompressor_MediumBatch tests compression of medium batches (1000 points)
func BenchmarkCompressor_MediumBatch(b *testing.B) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"value"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
	}
	c := NewCompressor(config)

	data := generateTestData(1000, 50, 1)
	jsonData, _ := json.Marshal(data)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(jsonData)))

	for i := 0; i < b.N; i++ {
		_, _ = c.CompressJSON(jsonData)
	}
}

// BenchmarkCompressor_LargeBatch tests compression of large batches (10000 points)
func BenchmarkCompressor_LargeBatch(b *testing.B) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"value"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
	}
	c := NewCompressor(config)

	data := generateTestData(10000, 100, 1)
	jsonData, _ := json.Marshal(data)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(jsonData)))

	for i := 0; i < b.N; i++ {
		_, _ = c.CompressJSON(jsonData)
	}
}

// BenchmarkCompressor_WithGroupBy tests compression with grouping
func BenchmarkCompressor_WithGroupBy(b *testing.B) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"cpu", "memory"},
		GroupByFields:     []string{"host", "service"},
		AggregationMethod: "avg",
		TimeWindow:        60 * time.Second,
	}
	c := NewCompressor(config)

	data := generateComplexTestData(1000, 10, 5)
	jsonData, _ := json.Marshal(data)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(jsonData)))

	for i := 0; i < b.N; i++ {
		_, _ = c.CompressJSON(jsonData)
	}
}

// BenchmarkCompressor_WithUniqueFields tests compression with unique fields
func BenchmarkCompressor_WithUniqueFields(b *testing.B) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"bytes"},
		GroupByFields:     []string{"server"},
		UniqueFields:      []string{"customer_id"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
	}
	c := NewCompressor(config)

	data := generateBillingTestData(1000, 10, 50)
	jsonData, _ := json.Marshal(data)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(jsonData)))

	for i := 0; i < b.N; i++ {
		_, _ = c.CompressJSON(jsonData)
	}
}

// Benchmark different aggregation methods
func BenchmarkAggregationMethods(b *testing.B) {
	methods := []string{"sum", "avg", "min", "max", "count", "first", "last"}

	for _, method := range methods {
		b.Run(
			method, func(b *testing.B) {
				config := &Config{
					TimestampField:    "ts",
					ValueFields:       []string{"value"},
					AggregationMethod: method,
					TimeWindow:        60 * time.Second,
				}
				c := NewCompressor(config)

				data := generateTestData(1000, 50, 1)
				jsonData, _ := json.Marshal(data)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					_, _ = c.CompressJSON(jsonData)
				}
			},
		)
	}
}

// BenchmarkCompressBatch tests parallel batch processing
func BenchmarkCompressBatch(b *testing.B) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"value"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
		Workers:           4,
	}
	c := NewCompressor(config)

	// Create 10 batches
	var batches [][]byte
	for i := 0; i < 10; i++ {
		data := generateTestData(100, 10, i)
		jsonData, _ := json.Marshal(data)
		batches = append(batches, jsonData)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = c.CompressBatch(batches)
	}
}

// BenchmarkCompressionRatio tests the compression ratio calculation
func BenchmarkCompressionRatio(b *testing.B) {
	c := NewCompressor(nil)

	input := make([]byte, 10000)
	output := make([]byte, 1000)
	rand.Read(input)
	rand.Read(output)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = c.GetCompressionRatio(input, output)
	}
}

// Helper functions to generate test data

func generateTestData(points, hosts, timeOffset int) []map[string]interface{} {
	var data []map[string]interface{}
	baseTime := 1000 + timeOffset*3600

	for i := 0; i < points; i++ {
		data = append(
			data, map[string]interface{}{
				"ts":    baseTime + i,
				"value": rand.Float64() * 100,
				"host":  fmt.Sprintf("host-%d", i%hosts),
			},
		)
	}
	return data
}

func generateComplexTestData(points, hosts, services int) []map[string]interface{} {
	var data []map[string]interface{}
	baseTime := 1000

	for i := 0; i < points; i++ {
		data = append(
			data, map[string]interface{}{
				"ts":      baseTime + i,
				"cpu":     rand.Float64() * 100,
				"memory":  rand.Float64() * 100,
				"host":    fmt.Sprintf("host-%d", i%hosts),
				"service": fmt.Sprintf("service-%d", i%services),
			},
		)
	}
	return data
}

func generateBillingTestData(points, servers, customers int) []map[string]interface{} {
	var data []map[string]interface{}
	baseTime := 1000

	for i := 0; i < points; i++ {
		data = append(
			data, map[string]interface{}{
				"ts":          baseTime + i,
				"bytes":       rand.Int63n(1000000),
				"server":      fmt.Sprintf("server-%d", i%servers),
				"customer_id": fmt.Sprintf("customer-%d", i%customers),
			},
		)
	}
	return data
}
