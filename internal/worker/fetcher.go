package worker

import (
	"context"
	"log"
	"time"

	"baylis-market-data/internal/adapter"
	"baylis-market-data/internal/cache"
)

const (
	fetchInterval = 10 * time.Minute
	ngxExchange   = "NGX"

	marketOpenHour  = 9  // 09:00 WAT
	marketCloseHour = 16 // 16:00 WAT (exclusive)
)

// westAfricaTime is a fixed UTC+1 offset rather than a named IANA zone:
// WAT has no DST, and a fixed offset needs no tzdata database, so this
// works in a minimal container image with no zoneinfo installed.
var westAfricaTime = time.FixedZone("WAT", 60*60)

// Fetcher periodically pulls the NGX price list from a MarketDataAdapter
// and refreshes the price cache, so /prices can usually serve from Redis
// instead of hitting Mansa on every request. Fetches only run during NGX
// market hours, since Mansa's free tier caps out at 100 calls/day and
// fetching around the clock would exhaust that in under an hour.
type Fetcher struct {
	marketData adapter.MarketDataAdapter
	cache      *cache.PriceCache
}

// New builds a Fetcher against an existing adapter and price cache.
func New(marketData adapter.MarketDataAdapter, priceCache *cache.PriceCache) *Fetcher {
	return &Fetcher{marketData: marketData, cache: priceCache}
}

// Run attempts a fetch immediately, then every 10 minutes, until ctx is
// cancelled. It blocks the calling goroutine, so callers typically invoke
// it with `go`.
func (f *Fetcher) Run(ctx context.Context) {
	f.tick()

	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("worker: NGX fetcher stopping: %v", ctx.Err())
			return
		case <-ticker.C:
			f.tick()
		}
	}
}

// tick only calls Mansa when the NGX market is open, so the fetch cadence
// (6/hour) times a market-hours window (~7 hours/day) rather than 24/7.
func (f *Fetcher) tick() {
	now := time.Now()
	if !isMarketOpen(now) {
		log.Printf("worker: market closed, skipping NGX fetch (WAT time %s)", now.In(westAfricaTime).Format("Mon 15:04"))
		return
	}
	f.fetchAndStore()
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

// isMarketOpen reports whether t falls within NGX trading hours: Monday
// to Friday, 09:00 to 16:00 West Africa Time.
func isMarketOpen(t time.Time) bool {
	local := t.In(westAfricaTime)
	if local.Weekday() == time.Saturday || local.Weekday() == time.Sunday {
		return false
	}
	hour := local.Hour()
	return hour >= marketOpenHour && hour < marketCloseHour
}
