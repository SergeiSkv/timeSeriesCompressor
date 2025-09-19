package compressor

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

type Compressor struct {
	config Config
}

type Config struct {
	TimestampField string   // Field with timestamp (default: "timestamp")
	ValueFields    []string // Fields with values for aggregation (default: ["value"])
	GroupByFields  []string // Fields for grouping (for example: ["host", "service"])

	// Правила агрегации
	AggregationMethod string        // "sum", "avg", "min", "max", "count", "last", "first"
	TimeWindow        time.Duration // Time window for grouping (default: 1 minute)

	UniqueFields []string // Fields that must match for aggregation (for example: ["customer_id"])
	// If customer_id is different - do NOT aggregate, even if host is the same

	Workers int // Number of Forkers for parallel processing
}

func DefaultConfig() *Config {
	return &Config{
		TimestampField:    "timestamp",
		ValueFields:       []string{"value"},
		AggregationMethod: "sum",
		TimeWindow:        time.Minute,
		Workers:           4,
	}
}

func NewCompressor(config *Config) *Compressor {
	if config == nil {
		config = DefaultConfig()
	}

	if config.TimestampField == "" {
		config.TimestampField = "timestamp"
	}
	if len(config.ValueFields) == 0 {
		config.ValueFields = []string{"value"}
	}
	const defaultAggregation = "sum"
	if config.AggregationMethod == "" {
		config.AggregationMethod = defaultAggregation
	}
	if config.TimeWindow == 0 {
		config.TimeWindow = time.Minute
	}
	if config.Workers <= 0 {
		config.Workers = 4
	}

	return &Compressor{
		config: *config,
	}
}

func (c *Compressor) CompressJSON(data []byte) ([]byte, error) {
	result := gjson.ParseBytes(data)
	if !result.IsArray() {
		return nil, fmt.Errorf("expected JSON array")
	}

	groups := make(map[string]*Group)

	result.ForEach(
		func(key, value gjson.Result) bool {
			if !value.IsObject() {
				return true // Skip non-objects
			}

			timestamp := value.Get(c.config.TimestampField).Int()
			if timestamp == 0 {
				return true // Skip if no timestamp
			}

			// Time window in seconds
			windowSec := int64(c.config.TimeWindow.Seconds())
			if windowSec == 0 {
				windowSec = 60
			}
			window := (timestamp / windowSec) * windowSec

			groupKey := fmt.Sprintf("window:%d", window)

			for _, field := range c.config.GroupByFields {
				if val := value.Get(field); val.Exists() {
					groupKey += fmt.Sprintf(";%s:%s", field, val.String())
				}
			}

			// IMPORTANT: Check UniqueFields - if they are different, do NOT group them.
			for _, field := range c.config.UniqueFields {
				if val := value.Get(field); val.Exists() {
					groupKey += fmt.Sprintf(";unique_%s:%s", field, val.String())
				}
			}

			group, exists := groups[groupKey]
			if !exists {
				group = &Group{
					Window:    window,
					Tags:      make(map[string]string),
					Values:    make([]float64, 0),
					FirstTime: timestamp,
					LastTime:  timestamp,
				}

				for _, field := range c.config.GroupByFields {
					if val := value.Get(field); val.Exists() {
						group.Tags[field] = val.String()
					}
				}

				for _, field := range c.config.UniqueFields {
					if val := value.Get(field); val.Exists() {
						group.Tags[field] = val.String()
					}
				}

				groups[groupKey] = group
			}

			if timestamp < group.FirstTime {
				group.FirstTime = timestamp
			}
			if timestamp > group.LastTime {
				group.LastTime = timestamp
			}

			for _, field := range c.config.ValueFields {
				if val := value.Get(field); val.Exists() {
					group.Values = append(group.Values, val.Float())
				}
			}

			group.Count++

			return true
		},
	)

	output := make([]map[string]interface{}, 0, len(groups))

	for _, group := range groups {
		aggregatedValue := c.aggregate(group.Values)

		obj := make(map[string]interface{})

		switch c.config.AggregationMethod {
		case "first":
			obj[c.config.TimestampField] = group.FirstTime
		case "last":
			obj[c.config.TimestampField] = group.LastTime
		default:
			obj[c.config.TimestampField] = (group.FirstTime + group.LastTime) / 2
		}

		if len(c.config.ValueFields) == 1 {
			obj[c.config.ValueFields[0]] = aggregatedValue
		} else {
			obj["value"] = aggregatedValue
		}

		for k, v := range group.Tags {
			obj[k] = v
		}

		output = append(output, obj)
	}

	return json.Marshal(output)
}

func (c *Compressor) aggregate(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	switch c.config.AggregationMethod {
	case "sum":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum

	case "avg", "mean":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))

	case "min":
		minVal := values[0]
		for _, v := range values[1:] {
			if v < minVal {
				minVal = v
			}
		}
		return minVal

	case "max":
		maxVal := values[0]
		for _, v := range values[1:] {
			if v > maxVal {
				maxVal = v
			}
		}
		return maxVal

	case "count":
		return float64(len(values))

	case "first":
		return values[0]

	case "last":
		return values[len(values)-1]

	default:
		// Default to sum
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum
	}
}

type Group struct {
	Window    int64             // Time window
	Tags      map[string]string // Group Tags.
	Values    []float64         // Values for aggregation
	Count     int               // Number of records
	FirstTime int64             // First timestamp
	LastTime  int64             // Last timestamp
}

// CompressBatch processes several batches in parallel
func (c *Compressor) CompressBatch(batches [][]byte) [][]byte {
	results := make([][]byte, len(batches))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, c.config.Workers)

	for i, batch := range batches {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(idx int, data []byte) {
			defer wg.Done()
			defer func() { <-semaphore }()

			if compressed, err := c.CompressJSON(data); err == nil {
				results[idx] = compressed
			}
		}(i, batch)
	}

	wg.Wait()
	return results
}

func (c *Compressor) GetCompressionRatio(input, output []byte) float64 {
	if len(input) == 0 {
		return 0
	}
	return 1.0 - float64(len(output))/float64(len(input))
}
