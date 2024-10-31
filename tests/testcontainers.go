package tests

import (
	"context"
	"database/sql"
	_ "github.com/lib/pq"
	goRedis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"time"
)

func startRedisContainer(ctx context.Context) (*redis.RedisContainer, *goRedis.Client) {
	redisContainer, err := redis.Run(ctx, "redis:latest")
	if err != nil {
		panic("could not start redis container: " + err.Error())
	}

	host, err := redisContainer.Host(ctx)
	if err != nil {
		panic("could not get redis container connection host: " + err.Error())
	}

	port, err := redisContainer.MappedPort(ctx, "6379")
	if err != nil {
		panic("could not get redis container connection port: " + err.Error())
	}

	options := &goRedis.Options{
		Addr: host + ":" + port.Port(),
	}
	redisClient := goRedis.NewClient(options)
	redisClient.FlushDB(ctx)
	return redisContainer, redisClient
}

func startPostgresContainer(ctx context.Context) (*postgres.PostgresContainer, *sql.DB) {
	dbName := "test"
	dbUser := "user"
	dbPassword := "password"

	postgresContainer, err := postgres.Run(ctx, "postgres",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)

	if err != nil {
		panic("could not start postgres container: " + err.Error())
	}

	connectionString, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic("could not get postgres container connection string: " + err.Error())
	}

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic("could not connect to postgres container: " + err.Error())
	}

	err = db.Ping()
	if err != nil {
		panic("could not ping to postgres container: " + err.Error())
	}

	return postgresContainer, db
}
