package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"baylis-market-data/internal/adapter"
	"baylis-market-data/internal/cache"
)

// PriceHandler serves price/search endpoints, reading from the price
// cache first and only falling back to the live adapter on a cache miss.
type PriceHandler struct {
	marketData adapter.MarketDataAdapter
	cache      *cache.PriceCache
}

// New builds a PriceHandler against an existing adapter and price cache.
func New(marketData adapter.MarketDataAdapter, priceCache *cache.PriceCache) *PriceHandler {
	return &PriceHandler{marketData: marketData, cache: priceCache}
}

// Routes returns a chi router with all endpoints mounted.
func (h *PriceHandler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Get("/health", h.health)
	r.Get("/v1/prices/{exchange}", h.getPriceList)
	r.Get("/v1/prices/{exchange}/{symbol}", h.getQuote)
	r.Get("/v1/search", h.searchSymbol)
	return r
}

func (h *PriceHandler) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// getPriceList returns the full price list (already in GBP) for an
// exchange, serving from cache when available.
func (h *PriceHandler) getPriceList(w http.ResponseWriter, r *http.Request) {
	exchange := chi.URLParam(r, "exchange")

	quotes := h.cache.GetPriceList(exchange)
	if quotes == nil {
		fetched, err := h.marketData.GetPriceList(exchange)
		if err != nil {
			log.Printf("handler: GetPriceList(%s): %v", exchange, err)
			writeError(w, http.StatusBadGateway, "failed to fetch price list")
			return
		}
		quotes = fetched
		h.cache.SetPriceList(exchange, quotes)
	}

	writeJSON(w, http.StatusOK, quotes)
}

// getQuote returns a single quote (already in GBP). It first looks for
// the symbol inside the cached price list for the exchange, since that's
// the only cache primitive available; a miss there falls back to a live
// single-quote call.
func (h *PriceHandler) getQuote(w http.ResponseWriter, r *http.Request) {
	exchange := chi.URLParam(r, "exchange")
	symbol := chi.URLParam(r, "symbol")

	if quotes := h.cache.GetPriceList(exchange); quotes != nil {
		for _, q := range quotes {
			if strings.EqualFold(q.Symbol, symbol) {
				writeJSON(w, http.StatusOK, q)
				return
			}
		}
	}

	quote, err := h.marketData.GetQuote(exchange, symbol)
	if err != nil {
		log.Printf("handler: GetQuote(%s, %s): %v", exchange, symbol, err)
		writeError(w, http.StatusBadGateway, "failed to fetch quote")
		return
	}

	writeJSON(w, http.StatusOK, quote)
}

// searchSymbol looks up symbols matching q, optionally scoped to exchange.
func (h *PriceHandler) searchSymbol(w http.ResponseWriter, r *http.Request) {
	exchange := r.URL.Query().Get("exchange")
	query := r.URL.Query().Get("q")
	if query == "" {
		writeError(w, http.StatusBadRequest, "q is required")
		return
	}

	results, err := h.marketData.SearchSymbol(exchange, query)
	if err != nil {
		log.Printf("handler: SearchSymbol(%s, %q): %v", exchange, query, err)
		writeError(w, http.StatusBadGateway, "search failed")
		return
	}

	writeJSON(w, http.StatusOK, results)
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("handler: encode response: %v", err)
	}
}
