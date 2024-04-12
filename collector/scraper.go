package collector

import (
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

type Scraper interface {
	Scrape(db *sqlx.DB, ch chan<- prometheus.Metric)
}
