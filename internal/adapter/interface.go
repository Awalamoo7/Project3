package adapter

import "baylis-market-data/internal/model"

// MarketDataAdapter is implemented by each upstream data source
// (e.g. Mansa Markets) and normalises its responses to model.NormalisedQuote.
type MarketDataAdapter interface {
	GetPriceList(exchange string) ([]model.NormalisedQuote, error)
	GetQuote(exchange, symbol string) (*model.NormalisedQuote, error)
	SearchSymbol(exchange, query string) ([]model.SymbolResult, error)
}
