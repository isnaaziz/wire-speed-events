package main

import (
	"log"
	"time"

	"github.com/wire-speed-events/internal/models"
	"github.com/wire-speed-events/internal/repository"
	"github.com/wire-speed-events/internal/service"
	"github.com/wire-speed-events/internal/transport/kafka"
)

func main() {
	cfg := kafka.Config{
		Brokers:  []string{"localhost:9092"},
		Topic:    "events-topic",
		GroupID:  "test-group",
		Username: "admin",
		Password: "admin-secret",
	}

	repo := repository.NewMemoryRepository()
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	orchestrator := service.NewOrchestrator(repo, producer)

	consumer := kafka.NewConsumer(cfg)
	go func() {
		_ = consumer.Consume(cfg.Topic, func(e *models.Event) error {
			return nil
		})
	}()

	log.Println("Simulating high-speed event stream...")
	for i := 1; i <= 10000; i++ {
		event := &models.Event{
			ID:        time.Now().Format("20060102150405.000"),
			Type:      "simulation.event",
			Payload:   "data batch segment",
			Timestamp: time.Now(),
		}

		if err := orchestrator.HandleEvent(event); err != nil {
			log.Printf("Failed to process event %d: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Println("Simulation completed.")
	time.Sleep(2 * time.Second)
}
