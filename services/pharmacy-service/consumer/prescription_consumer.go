package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	// Import publisher untuk mengirim pesan balik (kompensasi) jika stok habis
	"pharmacy-service/publisher"
)

type EventEnvelope struct {
	EventID string `json:"event_id"`
	Payload struct {
		AppointmentID string `json:"appointment_id"`
		Prescription  struct {
			PrescriptionID string `json:"prescription_id"`
			Items          []struct {
				DrugCode string `json:"drug_code"`
				Quantity int    `json:"quantity"`
			} `json:"items"`
		} `json:"prescription"`
	} `json:"payload"`
}

type ProcessedEventRecord struct {
	EventID     string    `bson:"event_id"`
	ProcessedAt time.Time `bson:"processed_at"`
	Status      string    `bson:"status"`
}

func StartPrescriptionConsumer(amqpURI string, mongoClient *mongo.Client) {
	// [DEMO - REDIS] Inisialisasi koneksi ke Redis (sebagai Fast-Path Cache)
	idem := NewRedisIdempotency("localhost:6379")

	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal koneksi broker: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal membuka channel: %v", err)
	}
	defer ch.Close()

	// Memastikan Exchange sudah ada sebelum kita binding antrean
	err = ch.ExchangeDeclare(
		"medcore.clinical.events", // Nama Exchange
		"topic",                   // Tipe
		true,                      // Durable
		false,                     // Auto-deleted
		false,                     // Internal
		false,                     // No-wait
		nil,                       // Arguments
	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal mendeklarasikan exchange: %v", err)
	}

	// [DEMO - DLQ] Mendeklarasikan antrean dengan argumen Dead Letter Exchange (DLX).
	// Jika ada pesan yang ditolak (NACK), RabbitMQ akan melemparnya ke DLX.
	args := amqp.Table{
		"x-dead-letter-exchange": "medcore.deadletter.exchange",
	}
	q, err := ch.QueueDeclare(
		"pharmacy.prescription.order.q",
		true,  // Durable: Antrean tidak hilang jika RabbitMQ restart
		false, // Auto-delete
		false, // Exclusive
		false, // No-wait
		args,  // Arguments DLX
	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi queue: %v", err)
	}

	// [DEMO - BINDING] Menghubungkan antrean ke Exchange.
	// Tanda bintang (*) artinya menerima event apapun yang berakhiran 'clinical.consultation.sesuatu'
	err = ch.QueueBind(
		q.Name,
		"clinical.consultation.*",
		"medcore.clinical.events",
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal binding queue: %v", err)
	}

	// [DEMO - QOS] Prefetch Count = 10.
	// Tujuan: Maksimal 10 pesan yang dipegang di RAM. Mencegah server OOM/Crash jika pesan menumpuk.
	err = ch.Qos(10, 0, false)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal set QoS: %v", err)
	}

	// [DEMO - MANUAL ACK] autoAck = FALSE.
	// Artinya: Pesan dianggap selesai HANYA JIKA kita memanggil d.Ack(false) secara manual.
	msgs, err := ch.Consume(
		q.Name,
		"pharmacy-worker",
		false, // autoAck = FALSE (Wajib Manual ACK untuk keandalan)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal subscribe: %v", err)
	}

	log.Printf("[Pharmacy Consumer] Menunggu pesan resep pada antrean: %s", q.Name)
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var evt EventEnvelope
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				log.Printf("[Pharmacy Consumer] Format pesan tidak valid, NACK tanpa requeue: %v", err)
				// [DEMO - POISON PILL] Jika JSON rusak, langsung buang ke DLQ (requeue = false)
				d.Nack(false, false)
				continue
			}

			log.Println("---------------------------------------------------------")
			log.Printf("[Pharmacy Consumer] Menerima Resep Baru!")
			log.Printf(" Event ID        : %s", evt.EventID)
			log.Printf(" Appointment ID  : %s", evt.Payload.AppointmentID)
			log.Printf(" Prescription ID : %s", evt.Payload.Prescription.PrescriptionID)

			// Validasi Poison Message (Kuantitas <= 0)
			isPoison := false
			for _, item := range evt.Payload.Prescription.Items {
				log.Printf(" -> Item: %s | Qty: %d", item.DrugCode, item.Quantity)
				if item.Quantity <= 0 {
					isPoison = true
				}
			}

			// [DEMO - POISON PILL] Jika kuantitas negatif/nol, tolak pesan.
			if isPoison {
				log.Printf("[Pharmacy Consumer] Terdeteksi Poison Message! Mengirim NACK ke DLQ.")
				// requeue = FALSE -> Pesan dialihkan ke DLQ (pharmacy.dlq)
				d.Nack(false, false)
				log.Println("---------------------------------------------------------")
				continue
			}

			// -------------------------------------------------------------
			// [DEMO - KOMPENSASI STOK] Simulasi jika stok obat di gudang kosong.
			// Ubah menjadi true saat presentasi untuk menunjukkan event balik ke Appointment.
			// -------------------------------------------------------------
			stokHabis := true
			if stokHabis {
				log.Printf("[Pharmacy Consumer] Stok kosong! Mengirim event kompensasi ke Appointment...")
				// Memanggil fungsi dari package publisher (mengirim event balik ke RabbitMQ)
				publisher.PublishStockExhausted(amqpURI, evt.Payload.AppointmentID, "MED-AMX-500")
				// Tetap ACK karena pesan berhasil ditangani (pesanan gagal, tapi sistem berjalan sukses)
				d.Ack(false)
				log.Println("---------------------------------------------------------")
				continue
			}
			// -------------------------------------------------------------

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			// [DEMO - REDIS FAST-PATH] Pengecekan duplikat TERCEPAT.
			// Mengecek apakah EventID ini sudah ada di Redis (memori).
			status, acquired, err := idem.TryAcquire(ctx, evt.EventID)
			if err != nil {
				log.Printf("[Pharmacy Consumer] Redis error: %v, requeue", err)
				d.Nack(false, true)
				cancel()
				continue
			}
			if !acquired {
				if status == "SUCCESS" {
					log.Printf("[Deduplication Guard - REDIS] Event %s sudah diproses, ACK.", evt.EventID)
					d.Ack(false)
					cancel()
					log.Println("---------------------------------------------------------")
					continue
				}
				// Jika statusnya masih 'PROCESSING' oleh worker lain, kembalikan ke antrean
				log.Printf("[Deduplication Guard - REDIS] Event %s sedang diproses, requeue.", evt.EventID)
				d.Nack(false, true)
				cancel()
				log.Println("---------------------------------------------------------")
				continue
			}

			// [DEMO - MONGO FALLBACK] Jika di Redis tidak ada (atau Redis mati), cek ke MongoDB.
			// Ini adalah pertahanan lapis kedua (Durable Storage).
			eventColl := mongoClient.Database("medcore_pharmacy_db").Collection("processed_events")
			var existing ProcessedEventRecord
			err = eventColl.FindOne(ctx, bson.M{"event_id": evt.EventID}).Decode(&existing)
			if err == nil {
				log.Printf("[Deduplication Guard - MONGO] Event %s duplikat! ACK.", evt.EventID)
				_ = idem.MarkSuccess(ctx, evt.EventID) // Sinkronkan kembali ke Redis
				d.Ack(false)
				cancel()
				log.Println("---------------------------------------------------------")
				continue
			}

			// Logika Bisnis (Pemotongan Stok)
			log.Printf("[Business Logic] Memproses stok resep untuk Event ID: %s", evt.EventID)

			// Simpan ID Event ke MongoDB sebagai riwayat permanen
			_, err = eventColl.InsertOne(ctx, ProcessedEventRecord{
				EventID:     evt.EventID,
				ProcessedAt: time.Now(),
				Status:      "SUCCESS",
			})

			if err != nil {
				log.Printf("[Error] Gagal mencatat deduplikasi: %v. Requeue pesan.", err)
				d.Nack(false, true)
				cancel()
				log.Println("---------------------------------------------------------")
				continue
			}

			// [DEMO - SUKSES] Tandai Redis sukses, lalu konfirmasi ACK ke RabbitMQ.
			_ = idem.MarkSuccess(ctx, evt.EventID)
			d.Ack(false)
			cancel()
			log.Printf("[Pharmacy Consumer] Transaksi selesai sempurna dan Event %s dicatat.", evt.EventID)
			log.Println("---------------------------------------------------------")
		}
	}()

	<-forever
}