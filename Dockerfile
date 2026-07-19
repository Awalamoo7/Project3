# ---- Build stage ----
FROM golang:1.23-alpine AS build

WORKDIR /src

COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/server ./cmd/server

# ---- Final stage ----
FROM alpine:latest

# ca-certificates is required for the service's outbound HTTPS calls to
# Mansa Markets and Open Exchange Rates; alpine doesn't include it by default.
RUN apk add --no-cache ca-certificates \
    && addgroup -S baylis \
    && adduser -S -G baylis baylis

COPY --from=build --chown=baylis:baylis /out/server /usr/local/bin/server

USER baylis

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/server"]
