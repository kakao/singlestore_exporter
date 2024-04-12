package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"

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

	flagLogPathPtr := flag.String("log.log_path", "", "singlestore_exporter log path")
	flagLogLevel := flag.String("log.level", "info", "log level (default: info)")

	flag.Parse()
	if *versionFlag {
		fmt.Printf("singlestore_exporter version %s\n", Version)
		return
	}

	if err := log.InitLoggers(*flagLogPathPtr, *flagLogLevel, *flagSlowQueryLogPathPtr); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// only aggregator node need DSN
	dsn := os.Getenv("DATA_SOURCE_NAME")

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, newHandler(dsn, *flagSlowQueryPtr, *flagSlowQueryThresholdPtr, *flagReplicationStatusPtr)))

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
	dsn string,
	flagSlowQuery bool,
	flagSlowQueryThreshold int,
	flagReplicationStatus bool,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		registry := prometheus.NewRegistry()

		registry.MustRegister(collector.New(dsn, flagSlowQuery, flagSlowQueryThreshold, flagReplicationStatus))

		gatherers := prometheus.Gatherers{
			prometheus.DefaultGatherer,
			registry,
		}

		h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}
