package beater

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	_ "github.com/denisenkom/go-mssqldb"

	"github.com/mathenning/mssqlbeat/config"
)

// Mssqlbeat configuration.
type Mssqlbeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client
}

type DmOsPerfResult struct {
	ObjectName   string
	CounterName  string
	InstanceName string
	CounterValue int64
	CounterType  int
}

type BeatResult struct {
	EventKey   string
	EventValue float64
}

// New creates an instance of mssqlbeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Mssqlbeat{
		done:   make(chan struct{}),
		config: c,
	}
	return bt, nil
}

// Run starts mssqlbeat.
func (bt *Mssqlbeat) Run(b *beat.Beat) error {
	logp.Info("mssqlbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		conn, err := Connect(bt.config)
		if err != nil {
			return err
		}

		var beatResults []BeatResult
		beatResults, err = QueryDmOsPerformanceCounters(conn)
		if err != nil {
			return err
		}

		event, err := GenerateEvent(&beatResults)
		if err != nil {
			return err
		}

		bt.client.Publish(event)

		err = conn.Close()
		if err != nil {
			return err
		}

		//bt.client.Publish(event)
		logp.Info("Loop done")
	}
}

// Stop stops mssqlbeat.
func (bt *Mssqlbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func Connect(c config.Config) (*sql.DB, error) {
	flag.Parse()

	server := c.Host
	if c.Instance != "" {
		server += "\\" + c.Instance
	}
	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s", server, c.Username, c.Password)

	drivers := sql.Drivers()
	conn, err := sql.Open("mssql", dsn)
	if err != nil {
		return nil, err
	}

	print(drivers)

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func QueryDmOsPerformanceCounters(conn *sql.DB) ([]BeatResult, error) {
	query := `
		SELECT * FROM sys.dm_os_performance_counters
		WHERE counter_name IN (
			'SQL Compilations/sec',
			'SQL Re-Compilations/sec',
			'User Connections',
			'Batch Requests/sec',
			'Logouts/sec',
			'Logins/sec',
			'Processes blocked',
			'Latch Waits/sec',
			'Full Scans/sec',
			'Index Searches/sec',
			'Page Splits/sec',
			'Page Lookups/sec',
			'Page Reads/sec',
			'Page Writes/sec',
			'Readahead Pages/sec',
			'Lazy Writes/sec',
			'Checkpoint Pages/sec',
			'Page life expectancy',
			'Log File(s) Size (KB)',
			'Log File(s) Used Size (KB)',
			'Data File(s) Size (KB)',
			'Transactions/sec',
			'Write Transactions/sec',
			'Active Temp Tables',
			'Temp Tables Creation Rate',
			'Temp Tables For Destruction',
			'Free Space in tempdb (KB)',
			'Version Store Size (KB)',
			'Memory Grants Pending',
			'Memory Grants Outstanding',
			'Free list stalls/sec',
			'Buffer cache hit ratio',
			'Buffer cache hit ratio base',
			'Backup/Restore Throughput/sec',
			'Total Server Memory (KB)',
			'Target Server Memory (KB)',
			'Log Flushes/sec',
			'Log Flush Wait Time',
			'Memory broker clerk size',
			'Log Bytes Flushed/sec',
			'Bytes Sent to Replica/sec',
			'Log Send Queue',
			'Bytes Sent to Transport/sec',
			'Sends to Replica/sec',
			'Bytes Sent to Transport/sec',
			'Sends to Transport/sec',
			'Bytes Received from Replica/sec',
			'Receives from Replica/sec',
			'Flow Control Time (ms/sec)',
			'Flow Control/sec',
			'Resent Messages/sec',
			'Redone Bytes/sec',
			'XTP Memory Used (KB)',
			'Transaction Delay',
			'Log Bytes Received/sec',
			'Log Apply Pending Queue',
			'Redone Bytes/sec',
			'Recovery Queue',
			'Log Apply Ready Queue',
			'CPU usage %',
			'CPU usage % base',
			'Queued requests',
			'Requests completed/sec',
			'Blocked tasks',
			'Active memory grant amount (KB)',
			'Disk Read Bytes/sec',
			'Disk Read IO Throttled/sec',
			'Disk Read IO/sec',
			'Disk Write Bytes/sec',
			'Disk Write IO Throttled/sec',
			'Disk Write IO/sec',
			'Used memory (KB)',
			'Forwarded Records/sec',
			'Background Writer pages/sec',
			'Percent Log Used',
			'Log Send Queue KB',
			'Redo Queue KB'
		)
	`
	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}

	counters_by_type := make(map[int][]DmOsPerfResult)

	for rows.Next() {
		result := DmOsPerfResult{}
		err = rows.Scan(&result.ObjectName,
			&result.CounterName,
			&result.InstanceName,
			&result.CounterValue,
			&result.CounterType)
		if err != nil {
			return nil, err
		}

		result.ObjectName = strings.TrimSpace(result.ObjectName)
		result.CounterName = strings.TrimSpace(result.CounterName)
		result.InstanceName = strings.TrimSpace(result.InstanceName)
		counters_by_type[result.CounterType] = append(counters_by_type[result.CounterType], result)
	}

	beatResults := make([]BeatResult, 0)
	for ctype, results := range counters_by_type {
		for _, result := range results {
			var beatResult BeatResult
			var err error
			if ctype == 1073939712 {
				continue //PERF_LARGE_RAW_BASE is used in PERF_LARGE_RAW_FRACTION
			}
			switch ctype {
			case 537003264:
				baseResults := counters_by_type[1073939712]
				beatResult, err = CalculatePerfLargeRawFraction(&result, &baseResults)
			case 272696576:
				beatResult, err = CalculatePerfCounterBulkCount(&result)
			case 1073874176:
				beatResult, err = CalculatePerfAverageBulk(&result, &results)
			case 65792:
				beatResult, err = CalculatePerfCounterLargeRawcount(&result)
			default:
				return nil, errors.New(fmt.Sprintf("Unknown counter type: %d", ctype))
			}

			if err != nil {
				return nil, err
			}
			beatResults = append(beatResults, beatResult)
		}
	}

	return beatResults, nil
}

func CalculatePerfCounterLargeRawcount(result *DmOsPerfResult) (BeatResult, error) {
	e := BeatResult{
		EventKey:   fmt.Sprintf("dm_os_performance_counters.%s", result.CounterName),
		EventValue: float64(result.CounterValue),
	}

	return e, nil
}

func CalculatePerfCounterBulkCount(result *DmOsPerfResult) (BeatResult, error) {
	e := BeatResult{
		EventKey:   fmt.Sprintf("dm_os_performance_counters.%s", result.CounterName),
		EventValue: float64(result.CounterValue),
	}

	return e, nil
}

func CalculatePerfLargeRawFraction(result *DmOsPerfResult, baseResults *[]DmOsPerfResult) (BeatResult, error) {
	var base DmOsPerfResult
	for _, baseResult := range *baseResults {
		if baseResult.CounterName == fmt.Sprintf("%s base", result.CounterName) {
			base = baseResult
		}
	}

	if base == (DmOsPerfResult{}) {
		return BeatResult{}, errors.New(fmt.Sprintf("Base Counter not found for %s: %s", result.CounterName, fmt.Sprintf("%s base", result.CounterName)))
	}

	var key string
	if base.InstanceName != "" {
		key = fmt.Sprintf("dm_os_performance_counters.%s.%s", result.CounterName, result.InstanceName)
	} else {
		key = fmt.Sprintf("dm_os_performance_counters.%s", result.CounterName)
	}

	perfValue := float64(100.0 * result.CounterValue / base.CounterValue)
	e := BeatResult{
		EventKey:   key,
		EventValue: perfValue,
	}

	return e, nil
}

func CalculatePerfAverageBulk(result *DmOsPerfResult, results *[]DmOsPerfResult) (BeatResult, error) {
	e := BeatResult{}
	return e, nil
}

func GenerateEvent(beatResults *[]BeatResult) (beat.Event, error) {
	fields := common.MapStr{}
	for _, beatResult := range *beatResults {
		_, err := fields.Put(beatResult.EventKey, beatResult.EventValue)

		if err != nil {
			return beat.Event{}, err
		}
	}

	event := beat.Event{
		Timestamp: time.Now(),
		Fields:    fields,
	}

	return event, nil
}
