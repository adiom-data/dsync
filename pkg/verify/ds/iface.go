package ds

type Result struct {
	ID    string
	Left  uint64
	Right uint64
}

type Verifier interface {
	Put(id string, left bool, v uint64, direct bool)
	Find(int) []Result
	MismatchCountAndTotal() (int64, int64)
}
