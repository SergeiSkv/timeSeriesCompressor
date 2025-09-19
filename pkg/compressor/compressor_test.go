package compressor

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompressor_SimpleAggregation(t *testing.T) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"value"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second, // 1 minute window
	}

	c := NewCompressor(config)

	// Input: 3 events in same minute (all in window 960-1020)
	input := `[
		{"ts": 960, "value": 5},
		{"ts": 980, "value": 3},
		{"ts": 1000, "value": 2}
	]`

	result, err := c.CompressJSON([]byte(input))
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	var output []map[string]interface{}
	if err := json.Unmarshal(result, &output); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}

	// Should have 1 aggregated row
	if len(output) != 1 {
		t.Errorf("Expected 1 row, got %d", len(output))
	}

	// Sum should be 5+3+2 = 10
	require.Equal(t, float64(10), output[0]["value"])

	// Check timestamp is in the window
	ts := output[0]["ts"].(float64)
	if ts < 960 || ts > 1000 {
		t.Errorf("Timestamp %v not in expected window [960-1000]", ts)
	}

	t.Logf("Compressed 3 rows to 1: %s", result)
}

func TestCompressor_GroupBy(t *testing.T) {
	config := &Config{
		TimestampField:    "timestamp",
		ValueFields:       []string{"cpu"},
		GroupByFields:     []string{"host"},
		AggregationMethod: "avg",
		TimeWindow:        120 * time.Second, // Larger window to capture all events
	}

	c := NewCompressor(config)

	// Input: metrics from 2 hosts (all within same 120s window)
	input := `[
		{"timestamp": 1000, "cpu": 50, "host": "server1"},
		{"timestamp": 1020, "cpu": 60, "host": "server1"},
		{"timestamp": 1000, "cpu": 80, "host": "server2"},
		{"timestamp": 1020, "cpu": 90, "host": "server2"}
	]`

	result, err := c.CompressJSON([]byte(input))
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	var output []map[string]interface{}
	if err := json.Unmarshal(result, &output); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}

	// Should have 2 rows (one per host)
	if len(output) != 2 {
		t.Errorf("Expected 2 rows (one per host), got %d", len(output))
	}

	// Check averages
	for _, row := range output {
		host := row["host"].(string)
		cpu := row["cpu"].(float64)

		if host == "server1" && cpu != 55.0 { // avg(50,60) = 55
			t.Errorf("server1: expected avg=55, got %v", cpu)
		}
		if host == "server2" && cpu != 85.0 { // avg(80,90) = 85
			t.Errorf("server2: expected avg=85, got %v", cpu)
		}
	}

	t.Logf("Compressed 4 rows to 2 (grouped by host): %s", result)
}

func TestCompressor_UniqueFields(t *testing.T) {
	config := &Config{
		TimestampField:    "timestamp",
		ValueFields:       []string{"bytes"},
		GroupByFields:     []string{"server"},
		UniqueFields:      []string{"customer_id"}, // IMPORTANT!
		AggregationMethod: "sum",
		TimeWindow:        120 * time.Second, // Larger window to capture all events
	}

	c := NewCompressor(config)

	// Input: same server, different customers (all within same 120s window)
	input := `[
		{"timestamp": 1000, "bytes": 100, "server": "web1", "customer_id": "cust1"},
		{"timestamp": 1020, "bytes": 200, "server": "web1", "customer_id": "cust1"},
		{"timestamp": 1000, "bytes": 300, "server": "web1", "customer_id": "cust2"},
		{"timestamp": 1020, "bytes": 400, "server": "web1", "customer_id": "cust2"}
	]`

	result, err := c.CompressJSON([]byte(input))
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	var output []map[string]interface{}
	if err := json.Unmarshal(result, &output); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}

	// Should have 2 rows (one per customer, NOT aggregated together!)
	if len(output) != 2 {
		t.Errorf("Expected 2 rows (one per customer), got %d", len(output))
	}

	// Check sums
	for _, row := range output {
		customer := row["customer_id"].(string)
		bytes := row["bytes"].(float64)

		if customer == "cust1" && bytes != 300.0 { // sum(100,200) = 300
			t.Errorf("cust1: expected sum=300, got %v", bytes)
		}
		if customer == "cust2" && bytes != 700.0 { // sum(300,400) = 700
			t.Errorf("cust2: expected sum=700, got %v", bytes)
		}
	}

	t.Logf("Kept customers separate: %s", result)
}

func TestCompressor_TimeWindows(t *testing.T) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"value"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second, // 1 minute
	}

	c := NewCompressor(config)

	// Input: data in different 60s windows
	// Window 1: 960-1020 contains timestamps 1000, 1010
	// Window 2: 1020-1080 contains timestamps 1060, 1070
	// Window 3: 1080-1140 contains timestamp 1120
	// Window 4: 1140-1200 contains timestamp 1150
	input := `[
		{"ts": 1000, "value": 1},
		{"ts": 1010, "value": 2},
		{"ts": 1060, "value": 3},
		{"ts": 1070, "value": 4},
		{"ts": 1120, "value": 5},
		{"ts": 1150, "value": 6}
	]`

	result, err := c.CompressJSON([]byte(input))
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	var output []map[string]interface{}
	if err := json.Unmarshal(result, &output); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}

	// Should have 4 windows
	if len(output) != 4 {
		t.Errorf("Expected 4 windows, got %d", len(output))
	}

	// Check sums in each window
	expectedSums := map[float64]float64{
		960:  3.0, // sum(1,2) = 3
		1020: 7.0, // sum(3,4) = 7
		1080: 5.0, // sum(5) = 5
		1140: 6.0, // sum(6) = 6
	}

	for _, row := range output {
		ts := row["ts"].(float64)
		value := row["value"].(float64)
		
		// Find the window this timestamp belongs to
		windowStart := (int64(ts) / 60) * 60
		
		if expected, ok := expectedSums[float64(windowStart)]; ok {
			if value != expected {
				t.Errorf("Window %v: expected sum=%v, got %v", windowStart, expected, value)
			}
		}
	}

	t.Logf("Compressed by time windows: %s", result)
}

func BenchmarkCompressor_1000Points(b *testing.B) {
	config := &Config{
		TimestampField:    "ts",
		ValueFields:       []string{"val"},
		GroupByFields:     []string{"host"},
		AggregationMethod: "sum",
		TimeWindow:        60 * time.Second,
	}

	c := NewCompressor(config)

	// Generate test data
	input := `[`
	for i := 0; i < 1000; i++ {
		if i > 0 {
			input += ","
		}
		input += fmt.Sprintf(
			`{"ts":%d,"val":%d,"host":"h%d"}`,
			1000+i%60, i, i%10,
		)
	}
	input += `]`
	data := []byte(input)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = c.CompressJSON(data)
	}
}
