package main

import (
	"context"
	"fmt"
	"log"
	"time"

<<<<<<< Updated upstream
	pb "appointment-service/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
=======
	"appointment-service/consumer"
	"appointment-service/publisher"
	"github.com/google/uuid"
>>>>>>> Stashed changes
)

const pharmacyAddr = "localhost:50051"

func checkDrug(client pb.PharmacyServiceClient, code string, qty int32, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Printf("[RPC] CheckDrugAvailability code=%s qty=%d timeout=%s", code, qty, timeout)

	resp, err := client.CheckDrugAvailability(ctx, &pb.CheckDrugRequest{
		DrugCode:       code,
		QuantityNeeded: qty,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			log.Printf("[ERROR] non-gRPC error: %v", err)
			return
		}

		switch st.Code() {
		case codes.NotFound:
			log.Printf("[CLINICAL WARNING] Kode obat tidak ditemukan: %s", st.Message())
		case codes.ResourceExhausted:
			log.Printf("[CLINICAL WARNING] Permintaan melebihi kuota farmasi: %s", st.Message())
		case codes.DeadlineExceeded:
			log.Printf("[CLINICAL WARNING] Farmasi timeout, verifikasi manual diperlukan: %s", st.Message())
		case codes.Unavailable:
			log.Printf("[CLINICAL WARNING] Pharmacy Service tidak tersedia: %s", st.Message())
		default:
			log.Printf("[RPC ERROR] code=%s message=%s", st.Code(), st.Message())
		}
		return
	}

	log.Printf("[OK] code=%s available=%t stock=%d price=%.2f msg=%s",
		resp.GetDrugCode(),
		resp.GetIsAvailable(),
		resp.GetCurrentStock(),
		resp.GetUnitPrice(),
		resp.GetMessage(),
	)
}

func main() {
	amqpURI := "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/%2Fmedcore_vhost"
	log.Println("[Appointment Service] Menginisialisasi koneksi messaging...")

<<<<<<< Updated upstream
	conn, err := grpc.Dial(pharmacyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
=======
	// 1. Jalankan Consumer Kompensasi Stok di background
	go consumer.StartStockExhaustedConsumer(amqpURI)

	// 2. Inisialisasi Publisher
	pub, err := publisher.NewEventPublisher(amqpURI)
>>>>>>> Stashed changes
	if err != nil {
		log.Fatalf("Inisialisasi publisher gagal: %v", err)
	}
	defer pub.Close()

<<<<<<< Updated upstream
	client := pb.NewPharmacyServiceClient(conn)

	// Skenario 1: normal
	checkDrug(client, "MED-AMX-500", 20, 2*time.Second)

	// Skenario 2: NotFound
	checkDrug(client, "MED-TIDAK-ADA", 10, 2*time.Second)

	// Skenario 3: ResourceExhausted
	checkDrug(client, "MED-AMX-500", 10000, 2*time.Second)
	// Skenario 4: DeadlineExceeded (MED-SLOW-500 memicu sleep 2s di server)
        checkDrug(client, "MED-SLOW-500", 10, 500*time.Millisecond)	
=======
	// 3. Simulasi event ketika konsultasi selesai
	simulatedEvent := publisher.ConsultationCompletedEvent{
		EventID:    fmt.Sprintf("evt-%s", uuid.New().String()),
		EventName:  "ConsultationCompletedEvent",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Producer:   "appointment-service",
		Payload: publisher.ConsultationPayload{
			AppointmentID:      "b1111111-1111-1111-1111-111111111111",
			PatientID:          "a0000000-0000-0000-0000-000000000001",
			DoctorID:           "b0000000-0000-0000-0000-000000000001",
			ConsultationStatus: "COMPLETED",
			Prescription: publisher.PrescriptionData{
				PrescriptionID: "RX-2026-9901",
				Items: []publisher.PrescriptionItem{
					{DrugCode: "MED-AMX-500", Quantity: 15, Dosage: "3x1 sehari sesudah makan"},
				},
			},
			BillingSummary: publisher.BillingData{
				ConsultationFee: -150000.0,
				TriageUrgency:   "GREEN",
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Println("[Appointment Service] Menerbitkan event konsultasi ke RabbitMQ...")
	if err := pub.PublishConsultationCompleted(ctx, simulatedEvent); err != nil {
		log.Fatalf("Gagal mempublikasikan event: %v", err)
	}
	log.Println("[Appointment Service] Proses publikasi selesai sempurna.")
>>>>>>> Stashed changes
}