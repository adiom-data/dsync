package metrics

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
)

var st statsd.ClientInterface
var prefix string

func init() {
	var stats statsd.ClientInterface
	var opts []statsd.Option
	if tagsStr, ok := os.LookupEnv("STATSD_TAGS"); ok && len(tagsStr) > 0 {
		tags := strings.Split(tagsStr, ",")
		opts = append(opts, statsd.WithTags(tags))
	}

	stats, err := statsd.New("", opts...)
	if err != nil {
		stats = &statsd.NoOpClient{}
	}
	st = stats

	p, ok := os.LookupEnv("STATSD_PREFIX")
	if !ok {
		prefix = "dsync."
	} else {
		prefix = p
	}
}

func WriteData(ns string, d time.Duration, numItems int) {
	tags := []string{"namespace:" + ns}
	_ = st.Distribution(prefix+"write_data_latency", float64(d.Milliseconds()), tags, 1)
	_ = st.Distribution(prefix+"write_data_batch_size", float64(numItems), tags, 1)
}

func WriteUpdates(ns string, d time.Duration, numItems int) {
	tags := []string{"namespace:" + ns}
	_ = st.Distribution(prefix+"write_updates_latency", float64(d.Milliseconds()), tags, 1)
	_ = st.Distribution(prefix+"write_updates_batch_size", float64(numItems), tags, 1)
}

func UpdateAttempts(ns string, tryNum int, numItems int) {
	tags := []string{"namespace:" + ns, fmt.Sprintf("try:%v", tryNum)}
	_ = st.Distribution(prefix+"update_attempts", float64(numItems), tags, 1)
}

func PrefixAndClient() (string, statsd.ClientInterface) {
	return prefix, st
}

func Done() {
	_ = st.Close()
}
