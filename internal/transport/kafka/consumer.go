package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/wire-speed-events/internal/models"
)

type EventConsumer struct {
	reader *kafka.Reader
}

func NewConsumer(cfg Config) *EventConsumer {
	mechanism := plain.Mechanism{
		Username: cfg.Username,
		Password: cfg.Password,
	}

	return &EventConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.Brokers,
			GroupID:  cfg.GroupID,
			Topic:    cfg.Topic,
			MinBytes: 10e3,
			MaxBytes: 10e6,
			Dialer: &kafka.Dialer{
				SASLMechanism: mechanism,
			},
		}),
	}
}

func (c *EventConsumer) Consume(topic string, handler func(*models.Event) error) error {
	for {
		m, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			return err
		}

		var event models.Event
		if err := json.Unmarshal(m.Value, &event); err != nil {
			continue
		}

		if err := handler(&event); err != nil {
			log.Printf("handler error: %v", err)
		}
	}
}

func (c *EventConsumer) Close() error {
	return c.reader.Close()
}
