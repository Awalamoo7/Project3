// Package mansa implements adapter.MarketDataAdapter against the Mansa
// Markets API at https://mansaapi.com (bare domain, no "www" — that
// subdomain serves an unrelated site and 404s on these paths).
//
// Endpoint paths and response shapes here were confirmed against live
// requests with a real key, cross-checked against https://mansaapi.com/docs/markets.
package mansa

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"baylis-market-data/internal/adapter"
	"baylis-market-data/internal/model"
)

const (
	mansaBaseURL        = "https://mansaapi.com"
	openExchangeBaseURL = "https://openexchangerates.org/api"
	requestTimeout      = 10 * time.Second
	fxCacheTTL          = 5 * time.Minute
	priceListPageSize   = 100
	maxPriceListPages   = 20
)

var _ adapter.MarketDataAdapter = (*Adapter)(nil)

// Adapter is a MarketDataAdapter backed by the Mansa Markets API, with
// local-currency prices converted to GBP via Open Exchange Rates.
type Adapter struct {
	apiKey            string
	openExchangeAppID string
	httpClient        *http.Client

	fxMu        sync.Mutex
	fxRates     map[string]float64
	fxFetchedAt time.Time
}

// New builds a Mansa adapter. apiKey authenticates against Mansa Markets;
// openExchangeAppID authenticates against Open Exchange Rates.
func New(apiKey, openExchangeAppID string) *Adapter {
	return &Adapter{
		apiKey:            apiKey,
		openExchangeAppID: openExchangeAppID,
		httpClient:        &http.Client{Timeout: requestTimeout},
	}
}

// mansaStock covers both the per-exchange list endpoint (which stamps
// ScrapedAt) and the single-ticker endpoint (which stamps LastUpdated
// and adds a few extra fields); each response only populates the field
// names it actually uses.
type mansaStock struct {
	Ticker      string  `json:"ticker"`
	Name        string  `json:"name"`
	Price       float64 `json:"price"`
	Change      float64 `json:"change"`
	ChangePct   float64 `json:"change_pct"`
	Volume      float64 `json:"volume"`
	ScrapedAt   string  `json:"scraped_at"`
	LastUpdated string  `json:"last_updated"`
}

// stocksMeta is returned alongside both the list and single-ticker
// endpoints; exchange/currency apply to every item in the response
// rather than being repeated per item.
type stocksMeta struct {
	Exchange string `json:"exchange"`
	Currency string `json:"currency"`
}

type pagination struct {
	Total   int  `json:"total"`
	Limit   int  `json:"limit"`
	Offset  int  `json:"offset"`
	HasMore bool `json:"has_more"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type stockListResponse struct {
	Data       []mansaStock `json:"data"`
	Meta       stocksMeta   `json:"meta"`
	Pagination pagination   `json:"pagination"`
	Error      *apiError    `json:"error"`
}

type stockResponse struct {
	Data  mansaStock `json:"data"`
	Meta  stocksMeta `json:"meta"`
	Error *apiError  `json:"error"`
}

// searchResult is self-contained (unlike mansaStock, exchange/currency
// come per item since a search can span exchanges).
type searchResult struct {
	Ticker   string `json:"ticker"`
	Name     string `json:"name"`
	Exchange string `json:"exchange"`
}

type searchResponse struct {
	Data  []searchResult `json:"data"`
	Error *apiError      `json:"error"`
}

type openExchangeRatesResponse struct {
	Rates map[string]float64 `json:"rates"`
}

// GetPriceList returns every quote Mansa reports for exchange, paging
// through the API's limit/offset pagination until has_more is false.
func (a *Adapter) GetPriceList(exchange string) ([]model.NormalisedQuote, error) {
	if exchange == "" {
		err := fmt.Errorf("mansa: exchange is required")
		log.Printf("mansa: GetPriceList: %v", err)
		return nil, err
	}

	var stocks []mansaStock
	var currency string
	offset := 0

	for page := 0; page < maxPriceListPages; page++ {
		reqURL := fmt.Sprintf("%s/api/v1/markets/exchanges/%s/stocks?limit=%d&offset=%d",
			mansaBaseURL, url.PathEscape(exchange), priceListPageSize, offset)

		var listResp stockListResponse
		if err := a.getMansaJSON(reqURL, &listResp); err != nil {
			log.Printf("mansa: GetPriceList(%s): %v", exchange, err)
			return nil, fmt.Errorf("mansa: get price list for %s: %w", exchange, err)
		}

		currency = listResp.Meta.Currency
		stocks = append(stocks, listResp.Data...)

		if !listResp.Pagination.HasMore || len(listResp.Data) == 0 {
			break
		}
		offset = listResp.Pagination.Offset + listResp.Pagination.Limit
	}

	quotes := make([]model.NormalisedQuote, 0, len(stocks))
	for _, s := range stocks {
		quotes = append(quotes, a.toNormalisedQuote(s, exchange, currency))
	}
	return quotes, nil
}

// GetQuote returns a single normalised quote for exchange/symbol.
func (a *Adapter) GetQuote(exchange, symbol string) (*model.NormalisedQuote, error) {
	if exchange == "" || symbol == "" {
		err := fmt.Errorf("mansa: exchange and symbol are required")
		log.Printf("mansa: GetQuote: %v", err)
		return nil, err
	}

	reqURL := fmt.Sprintf("%s/api/v1/markets/exchanges/%s/stocks/%s",
		mansaBaseURL, url.PathEscape(exchange), url.PathEscape(symbol))

	var stockResp stockResponse
	if err := a.getMansaJSON(reqURL, &stockResp); err != nil {
		log.Printf("mansa: GetQuote(%s, %s): %v", exchange, symbol, err)
		return nil, fmt.Errorf("mansa: get quote for %s/%s: %w", exchange, symbol, err)
	}

	quote := a.toNormalisedQuote(stockResp.Data, stockResp.Meta.Exchange, stockResp.Meta.Currency)
	return &quote, nil
}

// SearchSymbol looks up symbols matching query, optionally scoped to exchange.
func (a *Adapter) SearchSymbol(exchange, query string) ([]model.SymbolResult, error) {
	if query == "" {
		err := fmt.Errorf("mansa: query is required")
		log.Printf("mansa: SearchSymbol: %v", err)
		return nil, err
	}

	params := url.Values{}
	params.Set("q", query)
	if exchange != "" {
		params.Set("exchange", exchange)
	}
	reqURL := fmt.Sprintf("%s/api/v1/markets/search?%s", mansaBaseURL, params.Encode())

	var searchResp searchResponse
	if err := a.getMansaJSON(reqURL, &searchResp); err != nil {
		log.Printf("mansa: SearchSymbol(%s, %q): %v", exchange, query, err)
		return nil, fmt.Errorf("mansa: search %q: %w", query, err)
	}

	results := make([]model.SymbolResult, 0, len(searchResp.Data))
	for _, r := range searchResp.Data {
		results = append(results, model.SymbolResult{
			Symbol:   r.Ticker,
			Exchange: r.Exchange,
			Name:     r.Name,
		})
	}
	return results, nil
}

// toNormalisedQuote maps a Mansa stock into model.NormalisedQuote and
// fills PriceGBP. Mansa's stock endpoints don't return open/high/low, so
// those stay zero. A failed or unavailable FX conversion is logged and
// leaves PriceGBP at zero rather than dropping the quote entirely.
func (a *Adapter) toNormalisedQuote(s mansaStock, exchange, currency string) model.NormalisedQuote {
	timestamp := s.LastUpdated
	if timestamp == "" {
		timestamp = s.ScrapedAt
	}

	updatedAt := time.Now().UTC()
	if timestamp != "" {
		if t, err := time.Parse(time.RFC3339Nano, timestamp); err == nil {
			updatedAt = t
		} else {
			log.Printf("mansa: %s/%s: unparseable timestamp %q, using current time", exchange, s.Ticker, timestamp)
		}
	}

	quote := model.NormalisedQuote{
		Symbol:        s.Ticker,
		Exchange:      exchange,
		Name:          s.Name,
		PriceLocal:    s.Price,
		Currency:      currency,
		Volume:        s.Volume,
		ChangePercent: s.ChangePct,
		UpdatedAt:     updatedAt,
	}

	if currency == "" {
		log.Printf("mansa: %s/%s: missing currency, skipping GBP conversion", exchange, s.Ticker)
		return quote
	}

	rate, err := a.fxRateToGBP(currency)
	if err != nil {
		log.Printf("mansa: %s/%s: FX conversion failed: %v", exchange, s.Ticker, err)
		return quote
	}
	quote.PriceGBP = quote.PriceLocal * rate
	return quote
}

// fxRateToGBP returns the multiplier that converts an amount in currency
// into GBP, using cached Open Exchange Rates data (free-tier, USD-based).
func (a *Adapter) fxRateToGBP(currency string) (float64, error) {
	if currency == "GBP" {
		return 1, nil
	}

	rates, err := a.latestRates()
	if err != nil {
		return 0, err
	}

	usdToCurrency, ok := rates[currency]
	if !ok || usdToCurrency == 0 {
		return 0, fmt.Errorf("no FX rate available for %s", currency)
	}
	usdToGBP, ok := rates["GBP"]
	if !ok || usdToGBP == 0 {
		return 0, fmt.Errorf("no FX rate available for GBP")
	}

	// rates are USD->currency, so USD->GBP / USD->currency = currency->GBP.
	return usdToGBP / usdToCurrency, nil
}

func (a *Adapter) latestRates() (map[string]float64, error) {
	a.fxMu.Lock()
	defer a.fxMu.Unlock()

	if a.fxRates != nil && time.Since(a.fxFetchedAt) < fxCacheTTL {
		return a.fxRates, nil
	}

	reqURL := fmt.Sprintf("%s/latest.json?app_id=%s", openExchangeBaseURL, url.QueryEscape(a.openExchangeAppID))

	var oxr openExchangeRatesResponse
	if err := a.getJSON(reqURL, nil, &oxr); err != nil {
		return nil, fmt.Errorf("open exchange rates: %w", err)
	}
	if len(oxr.Rates) == 0 {
		return nil, fmt.Errorf("open exchange rates: empty rates response")
	}

	a.fxRates = oxr.Rates
	a.fxFetchedAt = time.Now()
	return a.fxRates, nil
}

// getMansaJSON performs an authenticated GET against Mansa. It always
// attempts to decode the body into out, even on a non-200 status, since
// Mansa consistently returns a JSON {"error": {"code","message"}} body
// for failures (confirmed against real 402/404 responses) rather than
// plain text — that parsed error is preferred over a raw status dump.
func (a *Adapter) getMansaJSON(reqURL string, out interface{ mansaError() *apiError }) error {
	headers := map[string]string{"Authorization": "Bearer " + a.apiKey}

	status, body, decodeErr := a.getJSONWithStatus(reqURL, headers, out)
	if decodeErr != nil {
		if status != 0 {
			return fmt.Errorf("unexpected status %d: %s", status, string(body))
		}
		return decodeErr
	}

	if apiErr := out.mansaError(); apiErr != nil {
		return fmt.Errorf("mansa api error %s: %s (status %d)", apiErr.Code, apiErr.Message, status)
	}
	if status != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", status, string(body))
	}
	return nil
}

func (r *stockListResponse) mansaError() *apiError { return r.Error }
func (r *stockResponse) mansaError() *apiError     { return r.Error }
func (r *searchResponse) mansaError() *apiError    { return r.Error }

// getJSON performs a GET and decodes a 200 response's JSON body into out.
// Used for APIs (Open Exchange Rates) with no structured error envelope
// worth parsing, so any non-200 status is a plain error.
func (a *Adapter) getJSON(reqURL string, headers map[string]string, out any) error {
	status, body, err := a.doGet(reqURL, headers)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", status, string(body))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// getJSONWithStatus performs a GET and always attempts to decode the
// body into out, regardless of status, returning the status code and
// raw body alongside any decode error so the caller can fall back to
// them when the body isn't the expected JSON shape.
func (a *Adapter) getJSONWithStatus(reqURL string, headers map[string]string, out any) (int, []byte, error) {
	status, body, err := a.doGet(reqURL, headers)
	if err != nil {
		return 0, nil, err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return status, body, fmt.Errorf("decode response: %w", err)
	}
	return status, body, nil
}

func (a *Adapter) doGet(reqURL string, headers map[string]string) (int, []byte, error) {
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, nil, fmt.Errorf("read response: %w", err)
	}

	return resp.StatusCode, body, nil
}
