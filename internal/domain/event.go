package domain

import (
	"github.com/wire-speed-events/internal/models"
)

type EventRepository interface {
	Save(event *models.Event) error
}

type EventPublisher interface {
	Publish(event *models.Event) error
}

type EventConsumer interface {
	Consume(topic string, handler func(*models.Event) error) error
}

type EventStreamer interface {
	EventPublisher
	EventConsumer
}
