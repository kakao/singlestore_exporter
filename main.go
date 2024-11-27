package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"singlestore_exporter/collector"
	"singlestore_exporter/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Version string
var versionFlag = flag.Bool("version", false, "print the version")

func main() {
	flagListenAddress := flag.String("net.listen_address", "0.0.0.0:9105", "network address on which the exporter listens")
	flagPprof := flag.Bool("debug.pprof", false, "enable pprof")

	flagReplicationStatusPtr := flag.Bool("collect.replication_status", false, "collect replication status")

	flagSlowQueryPtr := flag.Bool("collect.slow_query", false, "collect slow query")
	flagSlowQueryThresholdPtr := flag.Int("collect.slow_query.threshold", 10, "slow query threshold in seconds")
	flagSlowQueryLogPathPtr := flag.String("collect.slow_query.log_path", "", "slow query log path")
	flagSlowQueryExceptionHostsPtr := flag.String("collect.slow_query.exception.hosts", "", "slow query exception patterns host")
	flagSlowQueryExceptionInfoPatternsPtr := flag.String("collect.slow_query.exception.info.patterns", "", "slow query exception patterns info")

	flagDataDiskUsagePtr := flag.Bool("collect.data_disk_usage", false, "collect data disk usage")
	flagDataDiskUsageScrapeIntervalPtr := flag.Int("collect.data_disk_usage.scrape_interval", 30, "data disk usage scrape interval in seconds")

	flagActiveTransactionPtr := flag.Bool("collect.active_transaction", false, "collect active transaction")

	flagLogPathPtr := flag.String("log.log_path", "", "singlestore_exporter log path")
	flagLogLevel := flag.String("log.level", "info", "log level (default: info)")

	flag.Parse()
	if *versionFlag {
		fmt.Printf("singlestore_exporter version %s\n", Version)
		return
	}

	slowQueryExceptionHosts := make([]string, 0)
	if *flagSlowQueryExceptionHostsPtr != "" {
		slowQueryExceptionHosts = strings.Split(*flagSlowQueryExceptionHostsPtr, ",")
	}
	slowQueryExceptionInfoPatterns := make([]string, 0)
	if *flagSlowQueryExceptionInfoPatternsPtr != "" {
		slowQueryExceptionInfoPatterns = strings.Split(*flagSlowQueryExceptionInfoPatternsPtr, ",")
	}

	if err := log.InitLoggers(*flagLogPathPtr, *flagLogLevel, *flagSlowQueryLogPathPtr); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// only aggregator node need DSN
	dsn := os.Getenv("DATA_SOURCE_NAME")

	// scrape data_disk_usage in separate goroutine, because query execution time is too long (> 1s)
	if *flagDataDiskUsagePtr {
		ticker := time.Tick(time.Duration(*flagDataDiskUsageScrapeIntervalPtr) * time.Second)
		go func() {
			for range ticker {
				collector.ScrapeDataDiskUsages()
			}
		}()
	}

	flags := &collector.ExporterFlags{
		FlagSlowQuery:                      *flagSlowQueryPtr,
		FlagSlowQueryThreshold:             *flagSlowQueryThresholdPtr,
		FlagReplicationStatus:              *flagReplicationStatusPtr,
		FlagDataDiskUsage:                  *flagDataDiskUsagePtr,
		FlagActiveTransactionPtr:           *flagActiveTransactionPtr,
		FlagSlowQueryExceptionHosts:        slowQueryExceptionHosts,
		FlagSlowQueryExceptionInfoPatterns: slowQueryExceptionInfoPatterns,
	}

	mux := http.NewServeMux()
	mux.Handle(
		"/metrics",
		promhttp.InstrumentMetricHandler(
			prometheus.DefaultRegisterer,
			newHandler(
				Version,
				dsn,
				flags,
			),
		),
	)

	// pprof
	if *flagPprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	log.ErrorLogger.Infof("listening on %s", *flagListenAddress)
	if err := http.ListenAndServe(*flagListenAddress, mux); err != nil {
		panic(err)
	}
}

func newHandler(
	version string,
	dsn string,
	flags *collector.ExporterFlags,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		registry := prometheus.NewRegistry()

		ctx := r.Context()
		timeoutSeconds, err := getScrapeTimeoutSeconds(r)
		if err != nil {
			log.ErrorLogger.Infof("Error getting timeout from Prometheus header: err=%v", err)
		} else if timeoutSeconds > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(timeoutSeconds*float64(time.Second)))
			defer cancel()
			r = r.WithContext(ctx)
		}

		registry.MustRegister(
			collector.New(
				ctx,
				version,
				dsn,
				flags,
			),
		)

		gatherers := prometheus.Gatherers{
			prometheus.DefaultGatherer,
			registry,
		}

		h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}

// {"level":"info","msg":"Headers: map[Accept:[text/plain;version=0.0.4;q=1,*/*;q=0.1] Accept-Encoding:[gzip] User-Agent:[vm_promscrape] X-Prometheus-Scrape-Timeout-Seconds:[5.000]]","time":"2024-10-31T10:42:53+09:00"}
func getScrapeTimeoutSeconds(r *http.Request) (float64, error) {
	var timeoutSeconds float64
	if v := r.Header.Get("X-Prometheus-Scrape-Timeout-Seconds"); v != "" {
		var err error
		timeoutSeconds, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse timeout from Prometheus header: key=X-Prometheus-Scrape-Timeout-Seconds v=%s err=%v", v, err)
		}
	}
	if timeoutSeconds < 0 {
		return 0, fmt.Errorf("timeout value from Prometheus header is invalid: %f", timeoutSeconds)
	}
	return timeoutSeconds, nil
}
