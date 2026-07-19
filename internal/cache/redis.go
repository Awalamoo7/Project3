package cache

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"baylis-market-data/internal/model"
)

const (
	priceListTTL = 30 * time.Second
	keyPrefix    = "prices:"
)

// PriceCache caches per-exchange price lists in Redis with a short TTL,
// since live quotes go stale quickly.
type PriceCache struct {
	client *redis.Client
}

// New wraps an existing Redis client for price-list caching.
func New(client *redis.Client) *PriceCache {
	return &PriceCache{client: client}
}

func priceListKey(exchange string) string {
	return keyPrefix + exchange
}

// GetPriceList returns the cached quotes for exchange, or nil on a cache
// miss or any error (connection issues, corrupt data). Errors other than
// a plain miss are logged; callers should treat nil as "fetch live."
func (c *PriceCache) GetPriceList(exchange string) []model.NormalisedQuote {
	if exchange == "" {
		return nil
	}

	val, err := c.client.Get(context.Background(), priceListKey(exchange)).Bytes()
	if err != nil {
		if err != redis.Nil {
			log.Printf("cache: get price list for %s: %v", exchange, err)
		}
		return nil
	}

	var quotes []model.NormalisedQuote
	if err := json.Unmarshal(val, &quotes); err != nil {
		log.Printf("cache: unmarshal price list for %s: %v", exchange, err)
		return nil
	}
	return quotes
}

// SetPriceList caches quotes for exchange with a 30s TTL. Failures are
// logged, never panicked or returned, since a cache write failure should
// not break the request path.
func (c *PriceCache) SetPriceList(exchange string, quotes []model.NormalisedQuote) {
	if exchange == "" {
		return
	}

	data, err := json.Marshal(quotes)
	if err != nil {
		log.Printf("cache: marshal price list for %s: %v", exchange, err)
		return
	}

	if err := c.client.Set(context.Background(), priceListKey(exchange), data, priceListTTL).Err(); err != nil {
		log.Printf("cache: set price list for %s: %v", exchange, err)
	}
}
