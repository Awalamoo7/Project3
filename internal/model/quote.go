package model

import "time"

// NormalisedQuote is the canonical quote shape returned by every
// MarketDataAdapter, regardless of upstream source.
type NormalisedQuote struct {
	Symbol        string
	Exchange      string
	Name          string
	PriceLocal    float64
	Currency      string
	PriceGBP      float64
	Open          float64
	High          float64
	Low           float64
	Volume        float64
	ChangePercent float64
	UpdatedAt     time.Time
}

// SymbolResult is a single match returned by an adapter's symbol search.
type SymbolResult struct {
	Symbol   string
	Exchange string
	Name     string
}
