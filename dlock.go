package dlock

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"time"
)

var (
	lockRelease     = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	ErrLockInUse    = errors.New("lock in use")
	ErrNoLockExists = errors.New("lock does not exist")
)

type Client struct {
	client *redis.Client
}
type Lock struct {
	client *Client
	key,
	value string
}

func NewClient(conn *redis.Client) *Client {
	return &Client{client: conn}
}

func (c *Client) GetLock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	value := key + time.Nanosecond.String()
	ok, err := c.set(ctx, key, value, ttl)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLockInUse
	}
	return &Lock{
		client: c,
		key:    key,
		value:  value,
	}, nil
}

func (c *Client) set(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, ttl).Result()
}

func (l *Lock) Release(ctx context.Context) error {
	res, err := lockRelease.Run(ctx, l.client.client, []string{l.key}, l.value).Result()
	if err == redis.Nil {
		return ErrNoLockExists
	} else if err != nil {
		return err
	}
	if i, ok := res.(int64); !ok || i != 1 {
		return ErrNoLockExists
	}
	return nil
}
