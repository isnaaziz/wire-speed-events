package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	events "github.com/wire-speed-events/gen/pb"
	"github.com/wire-speed-events/internal/models"
	"google.golang.org/protobuf/proto"
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
			Brokers:     cfg.Brokers,
			Topic:       cfg.Topic,
			MinBytes:    1,
			MaxBytes:    10e6,
			StartOffset: kafka.FirstOffset,
			Dialer: &kafka.Dialer{
				SASLMechanism: mechanism,
			},
		}),
	}
}

func (c *EventConsumer) Consume(topic string, handler func(*models.Event) error) error {
	log.Println("consumer started, waiting for messages...")

	for {
		log.Println("waiting for message...")

		m, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("read error: %v", err)
			return err
		}

		log.Printf("raw message received: offset=%d, len=%d bytes, hex=%x", m.Offset, len(m.Value), m.Value)

		var protoEvent events.Event

		if err := proto.Unmarshal(m.Value, &protoEvent); err != nil {
			log.Printf("non-protobuf message (raw): %s", string(m.Value))
			continue
		}

		log.Printf("Event received: ID=%s, Type=%s, Timestamp=%d",
			protoEvent.Id, protoEvent.Type, protoEvent.Timestamp)

		if handler != nil {
			event := &models.Event{
				ID:      protoEvent.Id,
				Type:    protoEvent.Type,
				Payload: protoEvent.Payload,
			}
			if err := handler(event); err != nil {
				log.Printf("handler error: %v", err)
			}
		}
	}
}

func (c *EventConsumer) Close() error {
	return c.reader.Close()
}
