package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type RedisClient struct {
	client *redis.Client
}

type Cache[T any] struct {
	client     RedisClient
	expiration time.Duration
	sfg        *singleflight.Group
}

func NewCache[T any](client RedisClient, expiration time.Duration) *Cache[T] {
	return &Cache[T]{
		client:     client,
		expiration: expiration,
		sfg:        &singleflight.Group{},
	}
}

func NewRedisClient(ctx context.Context) *RedisClient {
	client := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		DB:           0,
		PoolSize:     20,
		MinIdleConns: 10,
	})

	// 疎通確認
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	return &RedisClient{
		client: client,
	}
}

// Redisクライアントの接続を閉じる
func (c *RedisClient) Close() {
	defer c.client.Close()
}

func (c *RedisClient) Clear() error {
	return c.client.FlushAll(context.Background()).Err()
}

type redisRepository[T any] struct {
	db    *sqlx.DB
	cache *Cache[T]
}

func NewRedisRepository[T any](
	db *sqlx.DB,
	cacheClient RedisClient,
) *redisRepository[T] {
	return &redisRepository[T]{
		db: db,
		cache: NewCache[T](
			cacheClient,
			time.Second*10,
		),
	}
}

// キャッシュがあれば取得する、なければセットする
func (c *Cache[T]) GetOrSet(
	ctx context.Context,
	cacheKey string, // ユーザーのkey
	callback func(context.Context) (T, error), // キャッシュがなければDBにインサートする
	extraSetFunc ...func(bytes []byte) error,
) (T, error) {
	// singleflightでリクエストをまとめる
	res, err, _ := c.sfg.Do(cacheKey, func() (any, error) {
		// キャッシュから取得
		bytes, exist, err := c.client.Get(ctx, cacheKey)
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
		err = c.client.Set(ctx, cacheKey, bytes, c.expiration)
		if err != nil {
			log.Println(err.Error())
		}

		if len(extraSetFunc) > 0 {
			err = extraSetFunc[0](bytes)
			if err != nil {
				log.Println(err.Error())
			}
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

// キャッシュを取得する
func (c *RedisClient) Get(
	ctx context.Context,
	key string,
) ([]byte, bool, error) {
	bytes, err := c.client.Get(ctx, key).Bytes()
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
func (c *RedisClient) Set(
	ctx context.Context,
	key string,
	bytes []byte,
	expiration time.Duration,
) error {
	err := c.client.Set(ctx, key, bytes, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set to redis: %w", err)
	}
	return nil
}

// redisからvalueを削除する
func (c *RedisClient) Del(
	ctx context.Context,
	key string,
) error {
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to delete from redis: %w", err)
	}
	return nil
}

// キャッシュを取得する
func (c *RedisClient) MGet(
	ctx context.Context,
	keys []string,
) ([]interface{}, bool, error) {
	result, err := c.client.MGet(ctx, keys...).Result()

	if err == redis.Nil {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("failed to get from redis: %w", err)
	}

	return result, true, nil
}

// redisに複数のvalueをsetする
func (c *RedisClient) MSet(
	ctx context.Context,
	values map[string]interface{},
) error {
	err := c.client.MSet(ctx, values).Err()

	if err != nil {
		return fmt.Errorf("failed to set to redis: %w", err)
	}

	return nil
}

func (r *redisRepository[T]) GetByColumn(
	ctx context.Context,
	columnName string, // 検索するカラム名 (e.g., "id", "name")
	columnValue string, // 検索値 (e.g., idの値, nameの値)
	tableName string,
	columns ...string, // 取得するカラム（オプション）
) (T, error) {
	cacheKey := fmt.Sprintf("%s:%s:%s", tableName, columnName, columnValue) // columnNameとcolumnValueでキャッシュキーを生成

	return r.cache.GetOrSet(
		ctx, cacheKey, func(ctx context.Context) (T, error) {
			var result T
			dest := any(&result)

			selectColumns := "*"
			if len(columns) > 0 {
				selectColumns = strings.Join(columns, ", ")
			}

			query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s = ?", selectColumns, tableName, columnName)

			if err := r.db.GetContext(ctx, dest, query, columnValue); err != nil {
				return result, err
			}

			return result, nil
		},
	)
}

func (r *redisRepository[T]) GetById(
	ctx context.Context,
	id string,
	tableName string,
	columns ...string,
) (T, error) {
	return r.GetByColumn(ctx, "id", id, tableName, columns...)
}

func (r *redisRepository[T]) GetByName(
	ctx context.Context,
	name string,
	tableName string,
	columns ...string,
) (T, error) {
	return r.GetByColumn(ctx, "name", name, tableName, columns...)
}

func (r *redisRepository[T]) GetByUserId(
	ctx context.Context,
	userId string,
	tableName string,
	columns ...string,
) (T, error) {
	return r.GetByColumn(ctx, "user_id", userId, tableName, columns...)
}
