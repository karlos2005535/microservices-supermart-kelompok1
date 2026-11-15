package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	idemPrefix    = "medcore:idem:pharmacy:"
	processingTTL = 5 * time.Minute  // Waktu tunggu maksimal jika sedang diproses
	successTTL    = 24 * time.Hour   // Waktu simpan status sukses di memori (1 hari)
)

type RedisIdempotency struct {
	rdb *redis.Client
}

func NewRedisIdempotency(addr string) *RedisIdempotency {
	return &RedisIdempotency{
		rdb: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

func (r *RedisIdempotency) key(eventID string) string {
	return idemPrefix + eventID
}

// [DEMO - LOGIKA INTI] TryAcquire mencoba "mengunci" event_id di Redis.
// Menggunakan perintah atomik SETNX (Set if Not eXists).
func (r *RedisIdempotency) TryAcquire(ctx context.Context, eventID string) (string, bool, error) {
	key := r.key(eventID)
	
	// Set key menjadi "PROCESSING". Jika berhasil (true), berarti kita yang memegang kendali.
	ok, err := r.rdb.SetNX(ctx, key, "PROCESSING", processingTTL).Result()
	if err != nil {
		return "", false, err
	}
	if ok {
		return "PROCESSING", true, nil
	}

	// Jika gagal di-set, berarti key sudah ada. Cek statusnya.
	status, err := r.rdb.Get(ctx, key).Result()
	
	// Antisipasi jika key expired tepat saat mau diambil (race condition)
	if errors.Is(err, redis.Nil) {
		ok2, err2 := r.rdb.SetNX(ctx, key, "PROCESSING", processingTTL).Result()
		if err2 != nil {
			return "", false, err2
		}
		if ok2 {
			return "PROCESSING", true, nil
		}
		status, err = r.rdb.Get(ctx, key).Result()
	}
	if err != nil {
		return "", false, err
	}
	return status, false, nil
}

// [DEMO] Jika pemrosesan sukses, ubah status dari "PROCESSING" menjadi "SUCCESS"
func (r *RedisIdempotency) MarkSuccess(ctx context.Context, eventID string) error {
	return r.rdb.Set(ctx, r.key(eventID), "SUCCESS", successTTL).Err()
}

// [DEMO] Jika terjadi error, hapus key agar pesan bisa di-retry di masa depan
func (r *RedisIdempotency) Release(ctx context.Context, eventID string) {
	_ = r.rdb.Del(ctx, r.key(eventID)).Err()
}