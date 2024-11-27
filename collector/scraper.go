package collector

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

type Scraper interface {
	Scrape(ctx context.Context, db *sqlx.DB, ch chan<- prometheus.Metric)
}
