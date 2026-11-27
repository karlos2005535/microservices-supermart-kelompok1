const amqp = require("amqplib");

// Format URI: amqp://<user>:<password>@<host>:<port>/<vhost>
//vhost-nya adalah "/medcore_vhost"
const AMQP_URI =
  "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/%2Fmedcore_vhost";

const EXCHANGE_NAME = "medcore.clinical.events"; // Exchange tempat event klinis dipublikasikan
const QUEUE_NAME = "notification.patient.alert.q"; // Antrean khusus untuk worker notifikasi pasien
const ROUTING_KEY_PATTERN = "clinical.#"; // "#" = mendengarkan semua event klinis

async function startNotificationWorker() {
  try {
    // Buka koneksi ke RabbitMQ, lalu buat channel untuk mengirim/menerima pesan
    const connection = await amqp.connect(AMQP_URI);
    const channel = await connection.createChannel();

    // 1. Pastikan exchange & queue sudah ada (dibuat otomatis jika belum)
    //    "topic"        -> pesan diarahkan berdasarkan pola routing key
    //    durable: true  -> tetap ada walau RabbitMQ restart
    await channel.assertExchange(EXCHANGE_NAME, "topic", { durable: true });
    const q = await channel.assertQueue(QUEUE_NAME, { durable: true });

    // 2. Hubungkan queue ke exchange: hanya pesan yang cocok dengan pola
    //    "clinical.#" yang akan masuk ke queue ini
    await channel.bindQueue(q.queue, EXCHANGE_NAME, ROUTING_KEY_PATTERN);

    // 3. Prefetch = 1: worker hanya menerima 1 pesan dalam satu waktu
    //    dan baru dapat pesan berikutnya setelah pesan sebelumnya berhasil di peroses
    await channel.prefetch(1);
    console.log(`[Notification Worker] Berjalan pada antrean: ${q.queue}`);

    // 4. Mulai menerima pesan
    //    noAck: false -> proses dilakukan manual, jadi pesan tidak hilang jika worker gagal
    channel.consume(
      q.queue,
      (msg) => {
        // msg bernilai null jika consumer dibatalkan oleh server
        if (msg !== null) {
          // Isi pesan berupa Buffer, ubah ke string lalu parse menjadi objek JSON
          const eventData = JSON.parse(msg.content.toString());

          // Simulasi pengiriman notifikasi ke pasien
          console.log(
            `\n[SMS/WhatsApp Gateway] Mengirim Notifikasi ke Pasien: ${eventData.payload.patient_id}`,
          );
          console.log(
            `Pesan: "Sesi konsultasi Anda (${eventData.payload.appointment_id}) telah selesai. Resep sedang disiapkan oleh Farmasi."`,
          );

          // 5. ACK = kabar ke RabbitMQ bahwa pesan sudah selesai diproses,
          //    sehingga pesan dihapus dari queue
          channel.ack(msg);
          console.log(
            `[Notification Worker] Event ${eventData.event_id} di-ACK.`,
          );
        }
      },
      { noAck: false },
    );
  } catch (error) {
    // Gagal konek, autentikasi salah, atau error saat setup
    console.error("[Notification Worker] Error:", error);
  }
}

startNotificationWorker();
