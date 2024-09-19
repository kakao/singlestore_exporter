package collector

import (
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"singlestore_exporter/log"
	"strings"
)

type ActiveDistributedTransactionsView struct {
	TableSchema string `db:"TABLE_SCHEMA"`
	TableName   string `db:"TABLE_NAME"`
}

type ActiveDistributedTransactions struct {
	PartitionName string `db:"PARTITION_NAME"`
	Count         int    `db:"COUNT"`
	TimeMax       int    `db:"TIME_MAX"`
	RowLocksTotal int    `db:"ROW_LOCKS_TOTAL"`
	Database      string
}

const (
	activeTransaction = "active_transaction"

	infoSchemaActiveDistributedTransactionsViewExistsQuery = `SELECT TABLE_SCHEMA, TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'information_schema' AND TABLE_NAME LIKE 'MV_ACTIVE_DISTRIBUTED_TRANSACTIONS'`

	infoSchemaActiveDistributedTransactionsQuery = `SELECT
    NVL(DATABASE_NAME, '') AS PARTITION_NAME,
    COUNT(*) AS COUNT,
    MAX(TIMESTAMPDIFF(SECOND, NVL(REAL_CLOCK_BEGIN_TIME_STAMP, NOW()), NOW()) ) AS TIME_MAX,
    SUM(ROW_LOCKS) AS ROW_LOCKS_TOTAL
FROM information_schema.MV_ACTIVE_DISTRIBUTED_TRANSACTIONS
WHERE PARTITION_NAME != ''
GROUP BY PARTITION_NAME;`
)

var (
	activeTransactionCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, activeTransaction, "count"),
		"The number of active transactions per partition",
		[]string{"database", "partition_name"},
		nil,
	)

	activeTransactionTimeMaxDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, activeTransaction, "time_max"),
		"The max time in seconds since the transaction started",
		[]string{"database", "partition_name"},
		nil,
	)

	activeTransactionRowLocksTotalDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, activeTransaction, "row_locks_total"),
		"The count of row locks per database",
		[]string{"database", "partition_name"},
		nil,
	)
)

type ScrapeActiveTransactions struct{}

func (s *ScrapeActiveTransactions) Help() string {
	return "Collect active transactions"
}

func (s *ScrapeActiveTransactions) Scrape(db *sqlx.DB, ch chan<- prometheus.Metric) {
	if db == nil {
		return
	}

	views := make([]ActiveDistributedTransactionsView, 0)
	if err := db.Select(&views, infoSchemaActiveDistributedTransactionsViewExistsQuery); err != nil {
		log.ErrorLogger.Errorf("checking existence view query failed: query=%s error=%v", infoSchemaActiveDistributedTransactionsViewExistsQuery, err)
	} else if len(views) == 0 {
		return
	}

	activeTransactionList := make([]ActiveDistributedTransactions, 0)
	if err := db.Select(&activeTransactionList, infoSchemaActiveDistributedTransactionsQuery); err != nil {
		log.ErrorLogger.Errorf("scraping query failed: query=%s error=%v", infoSchemaActiveDistributedTransactionsQuery, err)
		return
	}

	for i := range activeTransactionList {
		if postfix := strings.LastIndex(activeTransactionList[i].PartitionName, "_"); postfix != -1 {
			activeTransactionList[i].Database = activeTransactionList[i].PartitionName[:strings.LastIndex(activeTransactionList[i].PartitionName, "_")]
		} else {
			activeTransactionList[i].Database = activeTransactionList[i].PartitionName
		}
	}

	for _, activeTransaction := range activeTransactionList {
		ch <- prometheus.MustNewConstMetric(
			activeTransactionCountDesc, prometheus.GaugeValue, float64(activeTransaction.Count),
			activeTransaction.Database,
			activeTransaction.PartitionName,
		)

		ch <- prometheus.MustNewConstMetric(
			activeTransactionTimeMaxDesc, prometheus.GaugeValue, float64(activeTransaction.TimeMax),
			activeTransaction.Database,
			activeTransaction.PartitionName,
		)

		ch <- prometheus.MustNewConstMetric(
			activeTransactionRowLocksTotalDesc, prometheus.GaugeValue, float64(activeTransaction.RowLocksTotal),
			activeTransaction.Database,
			activeTransaction.PartitionName,
		)
	}
}
