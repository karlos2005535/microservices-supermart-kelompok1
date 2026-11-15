package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"
)

type StockExhaustedEvent struct {
	EventID    string `json:"event_id"`
	EventName  string `json:"event_name"`
	OccurredAt string `json:"occurred_at"`
	Producer   string `json:"producer"`
	Payload    struct {
		AppointmentID string `json:"appointment_id"`
		DrugCode      string `json:"drug_code"`
	} `json:"payload"`
}

// [DEMO - KOMPENSASI] Fungsi ini dipanggil oleh Consumer Farmasi jika stok kosong.
// Fungsinya: Memberi tahu Appointment Service bahwa pesanan tidak bisa dilanjutkan.
func PublishStockExhausted(amqpURI, appointmentID, drugCode string) {
	// 1. Buka koneksi baru ke RabbitMQ (karena berjalan di goroutine terpisah)
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Printf("[Pharmacy Publisher] Gagal koneksi: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("[Pharmacy Publisher] Gagal buka channel: %v", err)
		return
	}
	defer ch.Close()

	// 2. Bentuk payload event kompensasi
	event := StockExhaustedEvent{
		EventID:    fmt.Sprintf("evt-%s", uuid.New().String()),
		EventName:  "StockExhaustedEvent",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Producer:   "pharmacy-service",
	}
	event.Payload.AppointmentID = appointmentID
	event.Payload.DrugCode = drugCode

	body, _ := json.Marshal(event)
	
	// 3. Kirim ke RabbitMQ
	err = ch.PublishWithContext(
		context.Background(),
		"medcore.clinical.events", // Exchange Utama
		"pharmacy.stock.exhausted", // [DEMO - ROUTING KEY] Kunci khusus kompensasi
		false, false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // Wajib Persistent
			ContentType:  "application/json",
			Body:         body,
		},
	)
	if err == nil {
		log.Printf("[Pharmacy Publisher] Mengirim event kompensasi stok habis untuk Appointment: %s", appointmentID)
	}
}