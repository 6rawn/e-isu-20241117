package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type Client struct {
	Client *redis.Client
}

type Cache[T any] struct {
	Client     Client
	expiration time.Duration
	sfg        *singleflight.Group
}

func NewCache[T any](client Client, expiration time.Duration) *Cache[T] {
	return &Cache[T]{
		Client:     client,
		expiration: expiration,
		sfg:        &singleflight.Group{},
	}
}

func NewClient(ctx context.Context) *Client {
	client := redis.NewClient(&redis.Options{
		// Password:     pass,
		Addr:         "192.168.0.127:6379",
		DB:           0,
		PoolSize:     100,
		MinIdleConns: 30,
	})

	// 疎通確認
	for {
		if err := client.Ping(ctx).Err(); err != nil {
			log.Println("Failed to connect to Redis: ", err.Error())
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	return &Client{
		Client: client,
	}
}

// キャッシュがあれば取得する、なければセットする
func (c *Cache[T]) GetOrSet(
	ctx context.Context,
	key string, // ユーザーのkey
	callback func(context.Context) (T, error), // キャッシュがなければDBにインサートする
) (T, error) {
	// singleflightでリクエストをまとめる
	res, err, _ := c.sfg.Do(key, func() (any, error) {
		// キャッシュから取得
		bytes, exist, err := c.Client.Get(ctx, key)
		if err != nil {
			log.Println(err.Error())
		}
		if exist {
			return bytes, nil
		}
		// キャッシュがなければcallbackを実行
		t, err := callback(ctx)
		if err != nil {
			return nil, err
		}
		bytes, err = json.Marshal(t)
		if err != nil {
			return nil, err
		}
		// キャッシュに保存
		err = c.Client.Set(ctx, key, bytes, c.expiration)
		if err != nil {
			log.Println(err.Error())
		}
		return bytes, nil
	})

	var value T
	if err != nil {
		return value, err
	}

	bytes, ok := res.([]byte)
	if !ok {
		// 実装上、起きることはないはず
		return value, fmt.Errorf("failed to get from cache: invalid type %T", res)
	}

	err = json.Unmarshal(bytes, &value)
	if err != nil {
		return value, err
	}
	return value, nil
}

// Redisクライアントの接続を閉じる
func (c *Client) Close() {
	defer c.Close()
}

// キャッシュを取得する
func (c *Client) Get(
	ctx context.Context,
	key string,
) ([]byte, bool, error) {
	bytes, err := c.Client.Get(ctx, key).Bytes()
	// キャッシュが存在しない場合
	if err == redis.Nil {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("failed to get from redis: %w", err)
	}

	// キャッシュが存在する場合
	return bytes, true, nil
}

// redisにvalueをsetする
func (c *Client) Set(
	ctx context.Context,
	key string,
	bytes []byte,
	expiration time.Duration,
) error {
	err := c.Client.Set(ctx, key, bytes, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set to redis: %w", err)
	}
	return nil
}

// キャッシュを取得する
func (c *Client) MGet(
	ctx context.Context,
	keys []string,
) ([]interface{}, bool, error) {
	result, err := c.Client.MGet(ctx, keys...).Result()

	if err == redis.Nil {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("failed to get from redis: %w", err)
	}

	return result, true, nil
}

// redisに複数のvalueをsetする
func (c *Client) MSet(
	ctx context.Context,
	values map[string]interface{},
) error {
	err := c.Client.MSet(ctx, values).Err()

	if err != nil {
		return fmt.Errorf("failed to set to redis: %w", err)
	}

	return nil
}

func (c *Client) FlushDB() error {
	return c.Client.FlushDB(context.Background()).Err()
}
