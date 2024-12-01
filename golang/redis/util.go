package redis

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type db interface {
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type RedisRepository[T any] struct {
	db    db
	Cache *Cache[T]
}

func NewRedisRepository[T any](
	db db,
	cacheClient Client,
) *RedisRepository[T] {
	return &RedisRepository[T]{
		db: db,
		Cache: NewCache[T](
			cacheClient,
			time.Second*10,
		),
	}
}

func (r *RedisRepository[T]) GetByColumn(
	ctx context.Context,
	columnName string, // 検索するカラム名 (e.g., "id", "name")
	columnValue string, // 検索値 (e.g., idの値, nameの値)
	tableName string,
	columns ...string, // 取得するカラム（オプション）
) (T, error) {
	cacheKey := fmt.Sprintf("%s:%s:%s", tableName, columnName, columnValue) // columnNameとcolumnValueでキャッシュキーを生成

	return r.Cache.GetOrSet(
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

func (r *RedisRepository[T]) GetById(
	ctx context.Context,
	id string,
	tableName string,
	columns ...string,
) (T, error) {
	return r.GetByColumn(ctx, "id", id, tableName, columns...)
}

func (r *RedisRepository[T]) GetByName(
	ctx context.Context,
	name string,
	tableName string,
	columns ...string,
) (T, error) {
	return r.GetByColumn(ctx, "name", name, tableName, columns...)
}

type Count struct {
	Count int `db:"count"`
}

func (r *RedisRepository[T]) GetCountByColumn(
	ctx context.Context,
	columnName string,
	columnValue string,
	tableName string,
) (T, error) {
	cacheKey := fmt.Sprintf("%s:count:%s:%s", tableName, columnName, columnValue)

	return r.Cache.GetOrSet(
		ctx, cacheKey, func(ctx context.Context) (T, error) {
			var result T
			dest := any(&result)

			query := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s` WHERE %s = ?", tableName, columnName)

			if err := r.db.GetContext(ctx, dest, query, columnValue); err != nil {
				return result, err
			}

			return result, nil
		},
	)
}

func (r *RedisRepository[T]) GetByUserId(
	ctx context.Context,
	userId string,
	tableName string,
	columns ...string,
) (T, error) {
	return r.GetByColumn(ctx, "user_id", userId, tableName, columns...)
}

func (r *RedisRepository[T]) Select(
	ctx context.Context,
	tableName string,
	columns ...string,
) (T, error) {
	cacheKey := fmt.Sprintf("%s:all", tableName)

	return r.Cache.GetOrSet(
		ctx, cacheKey, func(ctx context.Context) (T, error) {
			var results T
			dest := any(&results)

			selectColumns := "*"
			if len(columns) > 0 {
				selectColumns = strings.Join(columns, ", ")
			}

			query := fmt.Sprintf("SELECT %s FROM `%s`", selectColumns, tableName)

			if err := r.db.SelectContext(ctx, dest, query); err != nil {
				return results, err
			}

			return results, nil
		},
	)
}

func (r *RedisRepository[T]) SelectByColumn(
	ctx context.Context,
	columnName string,
	columnValue string,
	tableName string,
	columns ...string,
) (T, error) {
	cacheKey := fmt.Sprintf("%s:%s:%s", tableName, columnName, columnValue)

	return r.Cache.GetOrSet(
		ctx, cacheKey, func(ctx context.Context) (T, error) {
			var results T
			dest := any(&results)

			selectColumns := "*"
			if len(columns) > 0 {
				selectColumns = strings.Join(columns, ", ")
			}

			query := fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` = ?", selectColumns, tableName, columnName)

			if err := r.db.SelectContext(ctx, dest, query, columnValue); err != nil {
				return results, err
			}

			return results, nil
		},
	)
}

func (r *RedisRepository[T]) SelectByColumnWithLimit(
	ctx context.Context,
	columnName string,
	columnValue string,
	tableName string,
	limit int,
	columns ...string,
) (T, error) {
	cacheKey := fmt.Sprintf("%s:limit:%v", tableName, limit)

	return r.Cache.GetOrSet(
		ctx, cacheKey, func(ctx context.Context) (T, error) {
			var results T
			dest := any(&results)

			selectColumns := "*"
			if len(columns) > 0 {
				selectColumns = strings.Join(columns, ", ")
			}

			query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s = ? LIMIT %v", selectColumns, tableName, columnName, limit)

			if err := r.db.SelectContext(ctx, dest, query, columnValue); err != nil {
				return results, err
			}

			return results, nil
		},
	)
}
