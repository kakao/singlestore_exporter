package collector

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"singlestore_exporter/log"
)

type PipelineState struct {
	DatabaseName string `db:"DATABASE_NAME"`
	PipelineName string `db:"PIPELINE_NAME"`
	State        string `db:"STATE"`
	ErrorCount   int    `db:"ERROR_COUNT"`
}

const (
	pipline = "pipeline"

	infoSchemaPipelineStateQuery = `SELECT p.DATABASE_NAME, p.PIPELINE_NAME, p.STATE
FROM information_schema.PIPELINES p`
)

var (
	pipelineStateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, pipline, "state"),
		"The state of the pipeline. Running = 0, Stopped = 1, Error = 2",
		[]string{"database", "pipeline"},
		nil,
	)

	pipelineStateMap = map[string]float64{
		"Running": 0,
		"Stopped": 1,
		"Error":   2,
	}
)

type ScrapePipeline struct{}

func (s *ScrapePipeline) Help() string {
	return "Collect metrics from information_schema.PIPELINES"
}

func (s *ScrapePipeline) Scrape(ctx context.Context, db *sqlx.DB, ch chan<- prometheus.Metric) {
	if db == nil {
		return
	}

	rows := make([]PipelineState, 0)
	if err := db.SelectContext(ctx, &rows, infoSchemaPipelineStateQuery); err != nil {
		log.ErrorLogger.Errorf("scraping query failed: query=%s error=%v", infoSchemaPipelineStateQuery, err)
		return
	}

	for _, row := range rows {
		if state, exists := pipelineStateMap[row.State]; exists {
			ch <- prometheus.MustNewConstMetric(
				pipelineStateDesc, prometheus.GaugeValue, state,
				row.DatabaseName,
				row.PipelineName,
			)
		} else {
			log.ErrorLogger.Errorf("unknown pipeline state: %s", row.State)
			continue
		}
	}
}
