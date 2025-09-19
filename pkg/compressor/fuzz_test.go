package compressor

import (
	"encoding/json"
	"testing"
	"time"
)

// FuzzCompressJSON tests the CompressJSON function with random inputs
func FuzzCompressJSON(f *testing.F) {
	// Add seed corpus
	f.Add([]byte(`[{"timestamp": 1000, "value": 100}]`))
	f.Add([]byte(`[{"timestamp": 1000, "value": 100}, {"timestamp": 1010, "value": 200}]`))
	f.Add([]byte(`[{"ts": 1000, "val": 50.5, "host": "server1"}]`))
	f.Add([]byte(`[{"timestamp": 1000, "cpu": 50, "memory": 70, "host": "web1", "service": "api"}]`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`[null]`))
	f.Add([]byte(`[1, 2, 3]`))
	f.Add([]byte(`[{"no_timestamp": 123}]`))
	f.Add([]byte(`[{"timestamp": 0, "value": 100}]`))
	f.Add([]byte(`[{"timestamp": -1000, "value": 100}]`))
	f.Add([]byte(`[{"timestamp": 9999999999, "value": 100}]`))
	f.Add([]byte(`[{"timestamp": "not_a_number", "value": 100}]`))
	f.Add([]byte(`[{"timestamp": 1000, "value": "not_a_number"}]`))
	f.Add([]byte(`[{"timestamp": 1000, "value": null}]`))
	f.Add([]byte(`[{"timestamp": 1000, "value": 100, "nested": {"field": "value"}}]`))

	config := &Config{
		TimestampField:    "timestamp",
		ValueFields:       []string{"value"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
	}
	c := NewCompressor(config)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Should not panic
		result, err := c.CompressJSON(data)
		
		// If no error, result should be valid JSON
		if err == nil && len(result) > 0 {
			var output []map[string]interface{}
			if err := json.Unmarshal(result, &output); err != nil {
				t.Errorf("CompressJSON returned invalid JSON: %v", err)
			}
		}
	})
}

// FuzzAggregate tests the aggregate function with random values
func FuzzAggregate(f *testing.F) {
	// Add seed corpus
	f.Add([]byte{1, 2, 3, 4, 5})
	f.Add([]byte{})
	f.Add([]byte{255})
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{255, 255, 255})
	f.Add([]byte{1})
	f.Add([]byte{0, 255, 128, 64, 32, 16, 8, 4, 2, 1})

	methods := []string{"sum", "avg", "min", "max", "count", "first", "last", "invalid"}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Convert bytes to float64 slice
		values := make([]float64, len(data))
		for i, b := range data {
			values[i] = float64(b)
		}

		for _, method := range methods {
			config := &Config{
				AggregationMethod: method,
			}
			c := NewCompressor(config)
			
			// Should not panic
			result := c.aggregate(values)
			
			// Verify result is not NaN or Inf
			if result != result { // NaN check
				t.Errorf("aggregate returned NaN for method %s", method)
			}
			// Check for positive and negative infinity using math package
			if result > 1e308 || result < -1e308 { // Inf check
				t.Errorf("aggregate returned Inf for method %s", method)
			}
			
			// Verify specific methods
			if len(values) > 0 {
				switch method {
				case "count":
					if result != float64(len(values)) {
						t.Errorf("count should return %d, got %f", len(values), result)
					}
				case "first":
					if result != values[0] {
						t.Errorf("first should return %f, got %f", values[0], result)
					}
				case "last":
					if result != values[len(values)-1] {
						t.Errorf("last should return %f, got %f", values[len(values)-1], result)
					}
				}
			}
		}
	})
}

// FuzzConfigValidation tests configuration validation with random inputs
func FuzzConfigValidation(f *testing.F) {
	// Add seed corpus
	f.Add("timestamp", "value", "sum", int64(60), 4)
	f.Add("", "", "", int64(0), 0)
	f.Add("ts", "val", "avg", int64(3600), 16)
	f.Add("@timestamp", "cpu,memory", "max", int64(1), 1)
	f.Add("time", "value1,value2,value3", "min", int64(86400), 100)

	f.Fuzz(func(t *testing.T, timestampField, valueFields, method string, window int64, workers int) {
		config := &Config{
			TimestampField:    timestampField,
			ValueFields:       parseTestFields(valueFields),
			AggregationMethod: method,
			TimeWindow:        time.Duration(window) * time.Second,
			Workers:           workers,
		}
		
		// Should not panic
		c := NewCompressor(config)
		
		// Verify defaults are applied
		if c.config.TimestampField == "" {
			t.Error("TimestampField should have default value")
		}
		if len(c.config.ValueFields) == 0 {
			t.Error("ValueFields should have default value")
		}
		if c.config.AggregationMethod == "" {
			t.Error("AggregationMethod should have default value")
		}
		if c.config.TimeWindow == 0 {
			t.Error("TimeWindow should have default value")
		}
		if c.config.Workers <= 0 {
			t.Error("Workers should have positive value")
		}
	})
}

// FuzzGetCompressionRatio tests compression ratio calculation
func FuzzGetCompressionRatio(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("input"), []byte("output"))
	f.Add([]byte{}, []byte{})
	f.Add([]byte("large input data"), []byte("small"))
	f.Add([]byte("a"), []byte("much larger output than input"))
	f.Add(make([]byte, 1000), make([]byte, 100))
	
	c := NewCompressor(nil)

	f.Fuzz(func(t *testing.T, input, output []byte) {
		// Should not panic
		ratio := c.GetCompressionRatio(input, output)
		
		// Verify ratio is valid
		if len(input) == 0 {
			if ratio != 0 {
				t.Errorf("Ratio should be 0 for empty input, got %f", ratio)
			}
		} else {
			expectedRatio := 1.0 - float64(len(output))/float64(len(input))
			if ratio != expectedRatio {
				t.Errorf("Incorrect ratio: expected %f, got %f", expectedRatio, ratio)
			}
		}
		
		// Ratio should be between -inf and 1 (can be negative if output > input)
		if ratio > 1 {
			t.Errorf("Ratio should not exceed 1, got %f", ratio)
		}
	})
}

// FuzzCompressBatch tests parallel batch processing with random inputs
func FuzzCompressBatch(f *testing.F) {
	// Add seed corpus
	f.Add([]byte(`[{"ts": 1000, "val": 10}]`), []byte(`[{"ts": 2000, "val": 20}]`))
	f.Add([]byte(`[]`), []byte(`[{"ts": 1000, "val": 10}]`))
	f.Add([]byte(`invalid`), []byte(`[{"ts": 1000, "val": 10}]`))
	
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"val"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
		Workers:           2,
	}
	c := NewCompressor(config)

	f.Fuzz(func(t *testing.T, batch1, batch2 []byte) {
		batches := [][]byte{batch1, batch2}
		
		// Should not panic
		results := c.CompressBatch(batches)
		
		// Verify results length
		if len(results) != len(batches) {
			t.Errorf("Expected %d results, got %d", len(batches), len(results))
		}
		
		// If batch is valid JSON, result should be non-nil
		for i, batch := range batches {
			var testData []interface{}
			if json.Unmarshal(batch, &testData) == nil {
				// Valid JSON array
				if results[i] != nil {
					// Result should also be valid JSON
					var output []map[string]interface{}
					if err := json.Unmarshal(results[i], &output); err != nil {
						t.Errorf("Batch %d returned invalid JSON: %v", i, err)
					}
				}
			}
		}
	})
}

// Helper function to parse comma-separated fields
func parseTestFields(s string) []string {
	if s == "" {
		return []string{}
	}
	
	var fields []string
	var current string
	
	for _, c := range s {
		if c == ',' {
			if current != "" {
				fields = append(fields, current)
				current = ""
			}
		} else if c != ' ' {
			current += string(c)
		}
	}
	
	if current != "" {
		fields = append(fields, current)
	}
	
	return fields
}