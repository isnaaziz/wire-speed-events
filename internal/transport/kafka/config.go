package kafka

type Config struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Username string
	Password string
}
