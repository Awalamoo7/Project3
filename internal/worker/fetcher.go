package worker

import (
	"context"
	"log"
	"time"

	"baylis-market-data/internal/adapter"
	"baylis-market-data/internal/cache"
)

const (
	fetchInterval = 30 * time.Second
	ngxExchange   = "NGX"
)

// Fetcher periodically pulls the NGX price list from a MarketDataAdapter
// and refreshes the price cache, so /prices can usually serve from Redis
// instead of hitting Mansa on every request.
type Fetcher struct {
	marketData adapter.MarketDataAdapter
	cache      *cache.PriceCache
}

// New builds a Fetcher against an existing adapter and price cache.
func New(marketData adapter.MarketDataAdapter, priceCache *cache.PriceCache) *Fetcher {
	return &Fetcher{marketData: marketData, cache: priceCache}
}

// Run fetches immediately, then every 30s, until ctx is cancelled. It
// blocks the calling goroutine, so callers typically invoke it with `go`.
func (f *Fetcher) Run(ctx context.Context) {
	f.fetchAndStore()

	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("worker: NGX fetcher stopping: %v", ctx.Err())
			return
		case <-ticker.C:
			f.fetchAndStore()
		}
	}
}

func (f *Fetcher) fetchAndStore() {
	quotes, err := f.marketData.GetPriceList(ngxExchange)
	if err != nil {
		log.Printf("worker: NGX fetch failed: %v", err)
		return
	}

	f.cache.SetPriceList(ngxExchange, quotes)
	log.Printf("worker: NGX fetch succeeded: cached %d quotes", len(quotes))
}
