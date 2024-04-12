package collector

import (
	"singlestore_exporter/log"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

type CachedBlobs struct {
	DatabaseName string `db:"DATABASE_NAME"`
	Status       string `db:"STATUS"`
	Evictable    string `db:"EVICTABLE"`
	Type         string `db:"TYPE"`
	FileCount    int    `db:"FILE_COUNT"`
	FileSizeSum  int    `db:"FILE_SIZE_SUM"`
}

const (
	cachedBlobs = "cached_blobs"

	infoSchemaCachedBlobQuery = `
SELECT DATABASE_NAME, STATUS, EVICTABLE, TYPE, COUNT(*) AS FILE_COUNT, SUM(SIZE) AS FILE_SIZE_SUM
FROM information_schema.MV_CACHED_BLOBS
GROUP BY DATABASE_NAME, STATUS, EVICTABLE, TYPE`
)

var (
	cachedBlobFileCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, cachedBlobs, "file_count"),
		"The count of blob cache file per database",
		[]string{"database", "status", "evictable", "type"},
		nil,
	)

	cachedBlobFileSizeSumDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, cachedBlobs, "file_size_sum"),
		"The sum of blob cache file per database",
		[]string{"database", "status", "evictable", "type"},
		nil,
	)
)

type ScrapeCachedBlobs struct{}

func (s *ScrapeCachedBlobs) Help() string {
	return "Collect metrics from information_schema.MV_CACHED_BLOBS"
}

func (s *ScrapeCachedBlobs) Scrape(db *sqlx.DB, ch chan<- prometheus.Metric) {
	if db == nil {
		return
	}

	rows := make([]CachedBlobs, 0)
	if err := db.Select(&rows, infoSchemaCachedBlobQuery); err != nil {
		log.ErrorLogger.Errorf("scraping query failed: query=%s error=%v", infoSchemaCachedBlobQuery, err)
		return
	}

	for _, row := range rows {
		ch <- prometheus.MustNewConstMetric(
			cachedBlobFileCountDesc, prometheus.GaugeValue, float64(row.FileCount),
			row.DatabaseName,
			row.Status,
			row.Evictable,
			row.Type,
		)
		ch <- prometheus.MustNewConstMetric(
			cachedBlobFileSizeSumDesc, prometheus.GaugeValue, float64(row.FileSizeSum),
			row.DatabaseName,
			row.Status,
			row.Evictable,
			row.Type,
		)
	}
}
