package dlock_test

import (
	"context"
	. "github.com/mhmdhelmy28/dlock"
	"github.com/redis/go-redis/v9"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var redisOpts = &redis.Options{
	Network: "tcp",
	Addr:    "127.0.0.1:6379",
	DB:      9,
}

func TestObtain(t *testing.T) {
	ctx := context.Background()
	redisConn := redis.NewClient(redisOpts)
	c := NewClient(redisConn)
	defer closeConn(t, redisConn)
	l, err := c.GetLock(ctx, "a1215", time.Hour)
	defer l.Release(ctx)
	if err != nil {
		t.Fatalf("failed to get lock, %s", err)
	}
	_, got := c.GetLock(ctx, "a1215", time.Second)
	exp := ErrLockInUse
	if got != exp {
		t.Fatalf("exp %s, got %s", exp, got)
	}
}

func TestRelease(t *testing.T) {
	ctx := context.Background()
	redisConn := redis.NewClient(redisOpts)
	c := NewClient(redisConn)
	defer closeConn(t, redisConn)
	l, _ := c.GetLock(ctx, "a222", time.Hour)
	got := l.Release(ctx)
	if got != nil {
		t.Fatalf("exp nil, got %s", got)
	}
	got = l.Release(ctx)
	exp := ErrNoLockExists
	if got != exp {
		t.Fatalf("exp %s, got %s", exp, got)
	}
}

func TestConcurrentObtain(t *testing.T) {
	ctx := context.Background()
	redisConn := redis.NewClient(redisOpts)
	c := NewClient(redisConn)
	locksObtained := int32(0)
	key := "locks"

	defer closeConn(t, redisConn)
	wg := sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.GetLock(ctx, key, time.Minute)
			if err == nil {
				atomic.AddInt32(&locksObtained, 1)
			} else if err == ErrLockInUse {
				return
			} else {
				t.Fatalf("err: %s", err)
			}
		}()
	}
	wg.Wait()
	if exp, got := 1, int(locksObtained); exp != got {
		t.Fatalf("exp %d, got %d", exp, got)
	}
}

func closeConn(t *testing.T, rc *redis.Client) {
	if err := rc.Del(context.Background(), "locks").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
}
