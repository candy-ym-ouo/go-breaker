package breaker

type Window interface {
	Record(ResultType)
	Snapshot() WindowSnapshot
	Reset()
}

type WindowSnapshot struct {
	Total     int64            `json:"total"`
	Succeeded int64            `json:"succeeded"`
	Failed    int64            `json:"failed"`
	Timeouts  int64            `json:"timeouts"`
	RejectedC int64            `json:"rejected_c"`
	RejectedB int64            `json:"rejected_b"`
	ErrorRate float64          `json:"error_rate"`
	Buckets   []BucketSnapshot `json:"buckets"`
}

func (s WindowSnapshot) Rejected() int64 {
	return s.RejectedC + s.RejectedB
}
