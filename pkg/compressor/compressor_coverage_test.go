package compressor

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	require.NotNil(t, cfg)
	require.Equal(t, "timestamp", cfg.TimestampField)
	require.Equal(t, []string{"value"}, cfg.ValueFields)
	require.Equal(t, "sum", cfg.AggregationMethod)
	require.Equal(t, time.Minute, cfg.TimeWindow)
	require.Equal(t, 4, cfg.Workers)
}

func TestNewCompressor_NilConfig(t *testing.T) {
	c := NewCompressor(nil)
	require.NotNil(t, c)
	require.Equal(t, "timestamp", c.config.TimestampField)
}

func TestNewCompressor_EmptyFields(t *testing.T) {
	// Test with empty fields to trigger defaults
	config := &Config{
		TimestampField:    "",
		ValueFields:       []string{},
		AggregationMethod: "",
		TimeWindow:        0,
		Workers:           -1,
	}

	c := NewCompressor(config)
	require.Equal(t, "timestamp", c.config.TimestampField)
	require.Equal(t, []string{"value"}, c.config.ValueFields)
	require.Equal(t, "sum", c.config.AggregationMethod)
	require.Equal(t, time.Minute, c.config.TimeWindow)
	require.Equal(t, 4, c.config.Workers)
}

func TestCompressJSON_InvalidInput(t *testing.T) {
	c := NewCompressor(nil)

	// Not an array
	_, err := c.CompressJSON([]byte(`{"not": "array"}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected JSON array")
}

func TestCompressJSON_NonObjectInArray(t *testing.T) {
	c := NewCompressor(nil)

	// Array with non-object elements - should skip them
	input := `[1, "string", {"timestamp": 1000, "value": 10}, null]`
	result, err := c.CompressJSON([]byte(input))
	require.NoError(t, err)

	var output []map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &output))
	require.Len(t, output, 1)
}

func TestCompressJSON_MissingTimestamp(t *testing.T) {
	c := NewCompressor(nil)

	// Objects without timestamp field - should skip them
	input := `[
		{"value": 10},
		{"timestamp": 1000, "value": 20},
		{"no_ts": 123, "value": 30}
	]`
	result, err := c.CompressJSON([]byte(input))
	require.NoError(t, err)

	var output []map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &output))
	require.Len(t, output, 1)
	require.Equal(t, float64(20), output[0]["value"])
}

func TestCompressJSON_ZeroTimestamp(t *testing.T) {
	c := NewCompressor(nil)

	// Objects with zero timestamp - should skip them
	input := `[
		{"timestamp": 0, "value": 10},
		{"timestamp": 1000, "value": 20}
	]`
	result, err := c.CompressJSON([]byte(input))
	require.NoError(t, err)

	var output []map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &output))
	require.Len(t, output, 1)
	require.Equal(t, float64(20), output[0]["value"])
}

func TestCompressJSON_ZeroWindowSec(t *testing.T) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"v"},
		TimeWindow:        0, // This will trigger default of 60s
		AggregationMethod: "sum",
	}
	c := NewCompressor(config)

	// All timestamps in same 60s window (960-1020)
	input := `[
		{"ts": 1000, "v": 10},
		{"ts": 1010, "v": 20}
	]`
	result, err := c.CompressJSON([]byte(input))
	require.NoError(t, err)

	var output []map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &output))
	require.Len(t, output, 1)
	require.Equal(t, float64(30), output[0]["v"])
}

func TestCompressJSON_UpdateFirstLastTime(t *testing.T) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"v"},
		TimeWindow:        300 * time.Second, // 5 minutes to definitely capture all
		AggregationMethod: "sum",
	}
	c := NewCompressor(config)

	// Test that FirstTime and LastTime are properly updated
	// Window calculation: ts/300*300, so 1000/300=3, 3*300=900
	// All timestamps fall in window 900-1200
	input := `[
		{"ts": 1050, "v": 10},
		{"ts": 1000, "v": 20},
		{"ts": 1100, "v": 30}
	]`
	result, err := c.CompressJSON([]byte(input))
	require.NoError(t, err)

	var output []map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &output))
	require.Len(t, output, 1)
	
	// Timestamp should be average of first and last
	ts := output[0]["ts"].(float64)
	require.Equal(t, float64(1050), ts) // (1000 + 1100) / 2
	require.Equal(t, float64(60), output[0]["v"]) // sum of all values
}

func TestAggregation_Methods(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		values   []float64
		expected float64
		tsField  float64 // expected timestamp
	}{
		{"min", "min", []float64{5, 2, 8, 1}, 1, 0},
		{"max", "max", []float64{5, 2, 8, 1}, 8, 0},
		{"count", "count", []float64{5, 2, 8, 1}, 4, 0},
		{"first", "first", []float64{5, 2, 8, 1}, 5, 1000},  // FirstTime
		{"last", "last", []float64{5, 2, 8, 1}, 1, 1015},   // LastTime (updated)
		{"default", "unknown", []float64{5, 2, 8, 1}, 16, 0}, // defaults to sum
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				TimestampField:    "ts",
				ValueFields:       []string{"val"},
				AggregationMethod: tt.method,
				TimeWindow:        120 * time.Second, // Larger window to capture all
			}
			c := NewCompressor(config)

			// Create input with multiple values - all in same 120s window
			input := `[
				{"ts": 1000, "val": 5},
				{"ts": 1005, "val": 2},
				{"ts": 1010, "val": 8},
				{"ts": 1015, "val": 1}
			]`

			result, err := c.CompressJSON([]byte(input))
			require.NoError(t, err)

			var output []map[string]interface{}
			require.NoError(t, json.Unmarshal(result, &output))
			require.Len(t, output, 1)
			require.Equal(t, tt.expected, output[0]["val"])

			if tt.tsField > 0 {
				require.Equal(t, tt.tsField, output[0]["ts"])
			}
		})
	}
}

func TestAggregation_EmptyValues(t *testing.T) {
	c := NewCompressor(nil)
	result := c.aggregate([]float64{})
	require.Equal(t, float64(0), result)
}

func TestAggregation_SingleValue(t *testing.T) {
	tests := []struct {
		method   string
		expected float64
	}{
		{"min", 5},
		{"max", 5},
		{"first", 5},
		{"last", 5},
		{"avg", 5},
		{"sum", 5},
		{"count", 1},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			config := &Config{
				AggregationMethod: tt.method,
			}
			c := NewCompressor(config)
			result := c.aggregate([]float64{5})
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCompressBatch(t *testing.T) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"val"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
		Workers:           2,
	}
	c := NewCompressor(config)

	batch1 := []byte(`[{"ts": 1000, "val": 10}, {"ts": 1010, "val": 20}]`)
	batch2 := []byte(`[{"ts": 2000, "val": 30}, {"ts": 2010, "val": 40}]`)
	batch3 := []byte(`invalid json`)

	results := c.CompressBatch([][]byte{batch1, batch2, batch3})
	require.Len(t, results, 3)
	
	// First two should be compressed
	require.NotNil(t, results[0])
	require.NotNil(t, results[1])
	// Third should be nil due to error
	require.Nil(t, results[2])

	// Verify first batch result
	var output []map[string]interface{}
	require.NoError(t, json.Unmarshal(results[0], &output))
	require.Len(t, output, 1)
	require.Equal(t, float64(30), output[0]["val"])
}

func TestGetCompressionRatio(t *testing.T) {
	c := NewCompressor(nil)

	// Test normal case
	input := []byte("1234567890")  // 10 bytes
	output := []byte("12345")      // 5 bytes
	ratio := c.GetCompressionRatio(input, output)
	require.Equal(t, 0.5, ratio) // 1.0 - 5/10 = 0.5

	// Test empty input
	ratio = c.GetCompressionRatio([]byte{}, output)
	require.Equal(t, float64(0), ratio)
}

func TestCompressJSON_MultipleValueFields(t *testing.T) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"cpu", "mem"}, // Multiple value fields
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
	}
	c := NewCompressor(config)

	input := `[
		{"ts": 1000, "cpu": 50, "mem": 70},
		{"ts": 1010, "cpu": 60, "mem": 75}
	]`

	result, err := c.CompressJSON([]byte(input))
	require.NoError(t, err)

	var output []map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &output))
	require.Len(t, output, 1)
	
	// When multiple value fields, it uses "value" as the output field
	require.Equal(t, float64(255), output[0]["value"]) // sum of all values: 50+70+60+75
}