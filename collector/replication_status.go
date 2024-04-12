package collector

import (
	"database/sql"
	"strconv"

	"singlestore_exporter/log"
	"singlestore_exporter/util"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

type ReplicationStatus struct {
	IsDr                      int             `db:"IS_DR"`
	IsSync                    int             `db:"IS_SYNC"`
	DatabaseName              string          `db:"DATABASE_NAME"`
	PrimaryURI                string          `db:"PRIMARY_URI"`
	SecondaryURI              sql.NullString  `db:"SECONDARY_URI"`
	PrimaryState              string          `db:"PRIMARY_STATE"`
	SecondaryState            string          `db:"SECONDARY_STATE"`
	LSNLag                    sql.NullInt64   `db:"LSN_LAG"`
	VolumeLagMB               sql.NullFloat64 `db:"VOLUME_LAG_MB"`
	ReplicationThroughputMBPS sql.NullFloat64 `db:"REPLICATION_THROUGHPUT_MBPS"`
	EstimatedCatchupTimeS     sql.NullFloat64 `db:"ESTIMATED_CATCHUP_TIME_S"`
}

const (
	replicationStatus = "replication_status"

	infoSchemaReplicationStatusQuery = `
SELECT
    IS_DR, IS_SYNC, DATABASE_NAME, PRIMARY_URI, SECONDARY_URI, PRIMARY_STATE, SECONDARY_STATE,
    LSN_LAG, VOLUME_LAG_MB, REPLICATION_THROUGHPUT_MBPS, ESTIMATED_CATCHUP_TIME_S
FROM information_schema.MV_REPLICATION_STATUS`
)

var (
	replicationStatusLSNLagDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, replicationStatus, "lsn_lag"),
		"The LSN lag of secondary partition which is in replication",
		[]string{"database", "is_dr", "is_sync", "primary_uri", "secondary_uri", "primary_state", "secondary_state"},
		nil,
	)
)

type ScrapeReplicationStatus struct{}

func (s *ScrapeReplicationStatus) Help() string {
	return "Collect metrics from information_schema.MV_REPLICATION_STATUS"
}

func (s *ScrapeReplicationStatus) Scrape(db *sqlx.DB, ch chan<- prometheus.Metric) {
	if db == nil {
		return
	}

	rows := make([]ReplicationStatus, 0)
	if err := db.Select(&rows, infoSchemaReplicationStatusQuery); err != nil {
		log.ErrorLogger.Errorf("scraping query failed: query=%s error=%v", infoSchemaReplicationStatusQuery, err)
		return
	}

	for _, row := range rows {
		// SecondaryURI가 NULL인 경우는 DR 측면에서 마스터 파티션
		// 별도로 모니터링을 하지 않는다
		if !row.SecondaryURI.Valid {
			continue
		}

		if row.LSNLag.Valid {
			ch <- prometheus.MustNewConstMetric(
				replicationStatusLSNLagDesc, prometheus.GaugeValue, float64(row.LSNLag.Int64),
				row.DatabaseName,
				strconv.Itoa(row.IsDr),
				strconv.Itoa(row.IsSync),
				row.PrimaryURI,
				util.NullStringToString(row.SecondaryURI, ""),
				row.PrimaryState,
				row.DatabaseName,
			)
		}
	}
}
