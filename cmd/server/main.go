package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"baylis-market-data/internal/adapter"
	"baylis-market-data/internal/adapter/mansa"
	"baylis-market-data/internal/cache"
	"baylis-market-data/internal/config"
	"baylis-market-data/internal/handler"
	"baylis-market-data/internal/worker"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("redis: invalid REDIS_URL: %v", err)
	}
	redisClient := redis.NewClient(redisOpts)
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer redisClient.Close()

	priceCache := cache.New(redisClient)
	var marketData adapter.MarketDataAdapter = mansa.New(cfg.MansaAPIKey, cfg.OpenExchangeAppID, priceCache)

	go worker.New(marketData, priceCache).Run(ctx)

	router := handler.New(marketData, priceCache).Routes()
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("listening on :%s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErrCh <- err
			return
		}
		close(serverErrCh)
	}()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received")
	case err := <-serverErrCh:
		if err != nil {
			log.Printf("server error: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}

	log.Printf("server stopped")
}
