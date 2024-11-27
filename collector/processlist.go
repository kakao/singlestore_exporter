package collector

import (
	"context"
	"database/sql"
	"time"

	"singlestore_exporter/log"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

var systemUsers = map[string]bool{
	"distributed": true,
}

type Process struct {
	ID                 int64          `db:"ID" json:"-"`
	User               string         `db:"USER" size:"320" json:"user"`
	Host               string         `db:"HOST" size:"64" json:"host"`
	DB                 sql.NullString `db:"DB" size:"64" json:"db"`
	Command            string         `db:"COMMAND" size:"16" json:"command"`
	Time               int            `db:"TIME" json:"time"`
	State              sql.NullString `db:"STATE" size:"128" json:"state"`
	Info               sql.NullString `db:"INFO" json:"info"`
	RPCInfo            sql.NullString `db:"RPC_INFO" json:"-"`
	PlanID             sql.NullInt64  `db:"PLAN_ID" json:"-"`
	TransactionState   sql.NullString `db:"TRANSACTION_STATE" size:"64" json:"transaction_state"`
	RowLocksHeld       sql.NullInt64  `db:"ROW_LOCKS_HELD" json:"row_locks_held"`
	PartitionLocksHeld sql.NullInt64  `db:"PARTITION_LOCKS_HELD" json:"partition_locks_held"`
	Epoch              sql.NullInt64  `db:"EPOCH" json:"-"`
	LWPID              sql.NullInt64  `db:"LWPID" json:"-"`
	ResourcePool       sql.NullString `db:"RESOURCE_POOL" size:"64" json:"resource_pool"`
	StmtVersion        int64          `db:"STMT_VERSION" json:"-"`
	ReasonForQueueing  sql.NullString `db:"REASON_FOR_QUEUEING" size:"128" json:"-"`
	SubmittedTime      time.Time      `db:"SUBMITTED_TIME" json:"submitted_time"`
}

const (
	process = "process"

	// length of INFO is limited to 1000 characters to avoid memory overflow
	infoSchemaProcessListQuery = `SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 1000) AS INFO, RPC_INFO, PLAN_ID, TRANSACTION_STATE, ROW_LOCKS_HELD, PARTITION_LOCKS_HELD, EPOCH, LWPID, RESOURCE_POOL, STMT_VERSION, REASON_FOR_QUEUEING, DATE_SUB(now(), INTERVAL time SECOND) AS SUBMITTED_TIME
FROM information_schema.PROCESSLIST`
)

var (
	processListTimeMaxDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, process, "time_max"),
		"The max time of processlist of user",
		[]string{"user"},
		nil,
	)

	processListSlowQueriesCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, process, "time_slow_queries_count"),
		"The count of slow queries of user",
		[]string{"user"},
		nil,
	)
)

type ScrapeProcessList struct {
	Threshold int
	Query     string
}

func NewScrapeProcessList(threshold int, exceptionHosts []string, exceptionInfoPatterns []string) *ScrapeProcessList {
	query := infoSchemaProcessListQuery
	if len(exceptionHosts) != 0 || len(exceptionInfoPatterns) != 0 {
		query += "\nWHERE "
	}
	for _, host := range exceptionHosts {
		query += "HOST NOT LIKE '" + host + ":%' AND "
	}
	for _, pattern := range exceptionInfoPatterns {
		query += "NVL(INFO, '') NOT LIKE '%" + pattern + "%' AND "
	}
	if len(exceptionHosts) != 0 || len(exceptionInfoPatterns) != 0 {
		query = query[:len(query)-5]
	}
	return &ScrapeProcessList{
		Threshold: threshold,
		Query:     query,
	}
}

func (s *ScrapeProcessList) Help() string {
	return "Collect metrics from information_schema.PROCESSLIST"
}

func (s *ScrapeProcessList) Scrape(ctx context.Context, db *sqlx.DB, ch chan<- prometheus.Metric) {
	if db == nil {
		return
	}

	processList := make([]Process, 0)
	if err := db.SelectContext(ctx, &processList, s.Query); err != nil {
		log.ErrorLogger.Errorf("scraping query failed: query=%s error=%v", s.Query, err)
		return
	}

	maxTime := make(map[string]int)
	counter := make(map[string]int)
	for _, process := range processList {
		if _, exists := systemUsers[process.User]; exists {
			continue
		} else if process.Command == "Sleep" {
			continue
		} else if process.Time < s.Threshold {
			continue
		}

		if m, exists := maxTime[process.User]; !exists {
			maxTime[process.User] = process.Time
		} else if process.Time > m {
			maxTime[process.User] = process.Time
		}

		counter[process.User]++

		log.SlowQueryLogger.WithFields(map[string]interface{}{
			"id":                process.ID,
			"user":              process.User,
			"host":              process.Host,
			"db":                StringOrEmpty(process.DB),
			"command":           process.Command,
			"time":              process.Time,
			"state":             StringOrEmpty(process.State),
			"info":              StringOrEmpty(process.Info),
			"transaction_state": StringOrEmpty(process.TransactionState),
			"submitted_time":    process.SubmittedTime,
		}).Info("slow query detected")
	}

	for user, maxTime := range maxTime {
		ch <- prometheus.MustNewConstMetric(
			processListTimeMaxDesc, prometheus.GaugeValue, float64(maxTime),
			user,
		)
	}

	for user, count := range counter {
		ch <- prometheus.MustNewConstMetric(
			processListSlowQueriesCountDesc, prometheus.GaugeValue, float64(count),
			user,
		)
	}
}

func StringOrEmpty(str sql.NullString) string {
	if str.Valid {
		return str.String
	} else {
		return ""
	}
}
