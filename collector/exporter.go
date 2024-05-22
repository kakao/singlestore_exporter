package collector

import (
	"fmt"
	"sync"
	"time"

	"singlestore_exporter/log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "singlestore"
)

var (
	dbConnectionSuccessfulDesc = prometheus.NewDesc(
		"db_connection_successful",
		"is db connection successful? (for aggregator)",
		[]string{},
		nil,
	)
)

type Exporter struct {
	dsn      string
	scrapers []Scraper
}

func New(
	dsn string,
	flagSlowQuery bool,
	flagSlowQueryThreshold int,
	flagReplicationStatus bool,
	flagDataDiskUsage bool,
) *Exporter {
	scrapers := []Scraper{
		&ScrapeNodes{},
	}
	if dsn != "" {
		scrapers = append(scrapers,
			&ScrapeCachedBlobs{},
		)
		if flagSlowQuery {
			scrapers = append(scrapers, &ScrapeProcessList{Threshold: flagSlowQueryThreshold})
		}
		if flagReplicationStatus {
			scrapers = append(scrapers, &ScrapeReplicationStatus{})
		}
	}
	if flagDataDiskUsage {
		scrapers = append(scrapers, &ScrapeDataDiskUsage{})
	}

	return &Exporter{
		dsn,
		scrapers,
	}
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- dbConnectionSuccessfulDesc
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.scrape(e.dsn, ch)
}

func (e *Exporter) scrape(dsn string, ch chan<- prometheus.Metric) {
	var db *sqlx.DB
	var err error

	if dsn != "" {
		db, err = e.conn(dsn + "information_schema?parseTime=true")
		if db != nil {
			defer func(db *sqlx.DB) {
				err := db.Close()
				if err != nil {
					log.ErrorLogger.Errorf("failed to close db: err=%v", err)
				}
			}(db)
			ch <- prometheus.MustNewConstMetric(dbConnectionSuccessfulDesc, prometheus.GaugeValue, 1)
		} else if err != nil {
			log.ErrorLogger.Errorf("db conn failed: err=%v", err)
			ch <- prometheus.MustNewConstMetric(dbConnectionSuccessfulDesc, prometheus.GaugeValue, 0)
		}
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, scraper := range e.scrapers {
		wg.Add(1)
		go func(scraper Scraper) {
			defer wg.Done()
			scraper.Scrape(db, ch)
		}(scraper)
	}
}

func (e *Exporter) conn(dsn string) (*sqlx.DB, error) {
	if dsn == "" {
		return nil, nil
	}

	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("dsn is not valid: dsn=%s, err=%v", dsn, err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(1 * time.Minute)

	if err := db.Ping(); err != nil {
		err := db.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close db: dsn=%s, err=%v", dsn, err)
		}
		return nil, fmt.Errorf("connection failed: dsn=%s, err=%v", dsn, err)
	}

	return db, nil
}
