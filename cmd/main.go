package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"

	"github.com/SergeiSkv/timeSeriesCompressor/config"
	"github.com/SergeiSkv/timeSeriesCompressor/pkg/compressor"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Convert to compressor config
	compressorConfig := &compressor.Config{
		TimestampField:    cfg.Timestamp,
		ValueFields:       cfg.Values,
		GroupByFields:     cfg.GroupBy,
		UniqueFields:      cfg.Unique,
		AggregationMethod: cfg.Method,
		TimeWindow:        cfg.Window,
		Workers:           cfg.Workers,
	}

	c := compressor.NewCompressor(compressorConfig)

	// Connect to NATS
	nc, err := nats.Connect(cfg.NATS.URL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	log.Printf("Connected to NATS at %s", cfg.NATS.URL)
	log.Printf("Subscribing to subject: %s", cfg.NATS.Subject)
	log.Printf("Publishing compressed data to: %s", cfg.NATS.OutputSubject)
	log.Printf("Config: %+v", cfg)

	// Subscribe to input subject
	sub, err := nc.QueueSubscribe(cfg.NATS.Subject, cfg.NATS.Queue, func(msg *nats.Msg) {
		// Compress the message
		compressed, err := c.CompressJSON(msg.Data)
		if err != nil {
			log.Printf("Failed to compress message: %v", err)
			return
		}

		// Calculate compression ratio
		ratio := c.GetCompressionRatio(msg.Data, compressed)
		log.Printf("Compressed %d bytes to %d bytes (%.2f%% reduction)", 
			len(msg.Data), len(compressed), ratio*100)

		// Publish compressed data
		if err := nc.Publish(cfg.NATS.OutputSubject, compressed); err != nil {
			log.Printf("Failed to publish compressed data: %v", err)
		}
	})
	if err != nil {
		nc.Close()
		log.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("TimeSeriesCompressor is running. Press Ctrl+C to exit.")
	<-sigChan

	log.Println("Shutting down...")
}