package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

type KafkaTopics struct {
	User    string
	Payment string
	Movie   string
}

type MovieEventPayload struct {
	MovieId int    `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserId  int    `json:"user_id"`
}

type UserEventPayload struct {
	UserId    int       `json:"user_id"`
	Username  string    `json:"username"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEventPayload struct {
	PaymentId int       `json:"payment_id"`
	UserId    int       `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"method_type"`
}

type Event struct {
	ID        string      `json:"id"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type KafkaService struct {
	brokers []string
	topics  KafkaTopics

	movieProducer   *kafka.Writer
	userProducer    *kafka.Writer
	paymentProducer *kafka.Writer

	movieConsumer   *kafka.Reader
	userConsumer    *kafka.Reader
	paymentConsumer *kafka.Reader

	stopChan chan struct{}
	wg       sync.WaitGroup
}

type EventService struct {
	kafkaService *KafkaService
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("[CINEMAABYSS-EVENTS-SERVICE] ")

	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")
	httpPort := getEnv("HTTP_PORT", "8082")

	topics := KafkaTopics{
		User:    getEnv("KAFKA_USER_TOPIC", "user-events"),
		Payment: getEnv("KAFKA_PAYMENT_TOPIC", "payment-events"),
		Movie:   getEnv("KAFKA_MOVIE_TOPIC", "movie-events"),
	}

	log.Println("Starting Events Service...")
	log.Printf("Kafka brokers: %s", kafkaBrokers)
	log.Printf("Topics: User=%s, Payment=%s, Movie=%s",
		topics.User, topics.Payment, topics.Movie)

	kafkaService, err := NewKafkaService(kafkaBrokers, topics)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer kafkaService.Close()

	kafkaService.StartConsumers()

	eventService := &EventService{
		kafkaService: kafkaService,
	}

	router := mux.NewRouter()

	router.HandleFunc("/api/events/movie", eventService.handleMovieEvent).Methods("POST")
	router.HandleFunc("/api/events/user", eventService.handleUserEvent).Methods("POST")
	router.HandleFunc("/api/events/payment", eventService.handlePaymentEvent).Methods("POST")

	router.HandleFunc("/api/events/health", handleHealth).Methods("GET")

	server := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Service listening on port %s", httpPort)

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Service failed to start: %v", err)
	}
}

func NewKafkaService(brokers string, topics KafkaTopics) (*KafkaService, error) {
	brokerList := []string{brokers}

	service := &KafkaService{
		brokers:  brokerList,
		topics:   topics,
		stopChan: make(chan struct{}),
	}

	service.movieProducer = &kafka.Writer{
		Addr:     kafka.TCP(brokerList...),
		Topic:    topics.Movie,
		Balancer: &kafka.LeastBytes{},
	}

	service.userProducer = &kafka.Writer{
		Addr:     kafka.TCP(brokerList...),
		Topic:    topics.User,
		Balancer: &kafka.LeastBytes{},
	}

	service.paymentProducer = &kafka.Writer{
		Addr:     kafka.TCP(brokerList...),
		Topic:    topics.Payment,
		Balancer: &kafka.LeastBytes{},
	}

	// Инициализация консьюмеров
	service.movieConsumer = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokerList,
		Topic:    topics.Movie,
		GroupID:  "events-service-movie-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	service.userConsumer = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokerList,
		Topic:    topics.User,
		GroupID:  "events-service-user-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	service.paymentConsumer = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokerList,
		Topic:    topics.Payment,
		GroupID:  "events-service-payment-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	return service, nil
}

func (ks *KafkaService) StartConsumers() {
	ks.wg.Add(3)

	go ks.consumeMovieEvents()
	go ks.consumeUserEvents()
	go ks.consumePaymentEvents()

	log.Println("All Kafka consumers started")
}

func (ks *KafkaService) consumeMovieEvents() {
	defer ks.wg.Done()
	log.Println("Starting movie events consumer...")

	for {
		select {
		case <-ks.stopChan:
			log.Println("Stopping movie events consumer")
			return
		default:
			msg, err := ks.movieConsumer.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error reading movie event: %v", err)
				continue
			}

			var event Event
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal movie event: %v", err)
				continue
			}

			log.Printf("[MOVIE] Topic: %s, Partition: %d, Offset: %d, Payload: %s",
				ks.topics.Movie,
				msg.Partition,
				msg.Offset,
				convertEventPayload(event))
		}
	}
}

func (ks *KafkaService) consumeUserEvents() {
	defer ks.wg.Done()
	log.Println("Starting user events consumer...")

	for {
		select {
		case <-ks.stopChan:
			log.Println("Stopping user events consumer")
			return
		default:
			msg, err := ks.userConsumer.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error reading user event: %v", err)
				continue
			}

			var event Event
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal user event: %v", err)
				continue
			}

			log.Printf("[USER] Topic: %s, Partition: %d, Offset: %d, Payload: %s",
				ks.topics.User,
				msg.Partition,
				msg.Offset,
				convertEventPayload(event))
		}
	}
}

func convertEventPayload(event Event) string {
	payloadJson, _ := json.Marshal(event.Payload)
	return string(payloadJson)
}

func (ks *KafkaService) consumePaymentEvents() {
	defer ks.wg.Done()
	log.Println("Starting payment events consumer...")

	for {
		select {
		case <-ks.stopChan:
			log.Println("Stopping payment events consumer")
			return
		default:
			msg, err := ks.paymentConsumer.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error reading payment event: %v", err)
				continue
			}

			var event Event
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal payment event: %v", err)
				continue
			}

			log.Printf("[PAYMENT] Topic: %s, Partition: %d, Offset: %d, Payload: %s",
				ks.topics.Payment,
				msg.Partition,
				msg.Offset,
				convertEventPayload(event))
		}
	}
}

func (ks *KafkaService) SendMovieEvent(event Event) error {
	return ks.sendEvent(ks.movieProducer, event, ks.topics.Movie, "movie")
}

func (ks *KafkaService) SendUserEvent(event Event) error {
	return ks.sendEvent(ks.userProducer, event, ks.topics.User, "user")
}

func (ks *KafkaService) SendPaymentEvent(event Event) error {
	return ks.sendEvent(ks.paymentProducer, event, ks.topics.Payment, "payment")
}

func (ks *KafkaService) sendEvent(writer *kafka.Writer, event Event, topic, eventType string) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal %s event: %v", eventType, err)
	}

	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(eventType),
			Value: eventJSON,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to send %s event to Kafka topic %s: %v",
			eventType, topic, err)
	}

	log.Printf("Produced %s event: %s to topic %s", eventType, event.ID, topic)
	return nil
}

func (ks *KafkaService) Close() {
	close(ks.stopChan)
	ks.wg.Wait()

	ks.movieProducer.Close()
	ks.userProducer.Close()
	ks.paymentProducer.Close()

	ks.movieConsumer.Close()
	ks.userConsumer.Close()
	ks.paymentConsumer.Close()

	log.Println("Kafka service closed")
}

func (es *EventService) handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	var payload MovieEventPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Payload:   payload,
	}

	if err := es.kafkaService.SendMovieEvent(event); err != nil {
		http.Error(w, "Failed to send movie event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"event_id": event.ID,
	})
}

func (es *EventService) handleUserEvent(w http.ResponseWriter, r *http.Request) {
	var payload UserEventPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Payload:   payload,
	}

	if err := es.kafkaService.SendUserEvent(event); err != nil {
		http.Error(w, "Failed to send user event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"event_id": event.ID,
	})
}

func (es *EventService) handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	var payload PaymentEventPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Payload:   payload,
	}

	if err := es.kafkaService.SendPaymentEvent(event); err != nil {
		http.Error(w, "Failed to send payment event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"event_id": event.ID,
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    true,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
