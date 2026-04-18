package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/wire-speed-events/internal/models"
)

type EventProducer struct {
	writer *kafka.Writer
}

func NewProducer(cfg Config) *EventProducer {
	mechanism := plain.Mechanism{
		Username: cfg.Username,
		Password: cfg.Password,
	}

	return &EventProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    cfg.Topic,
			Balancer: &kafka.LeastBytes{},
			Transport: &kafka.Transport{
				SASL: mechanism,
			},
		},
	}
}

func (p *EventProducer) Publish(event *models.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(event.ID),
			Value: payload,
		},
	)
}

func (p *EventProducer) Close() error {
	return p.writer.Close()
}
