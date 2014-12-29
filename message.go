package paxos

type msgType int

const (
	Prepare msgType = iota + 1
	Propose
	Promise
	Accept
)

type message struct {
	from, to int
	typ      msgType
	n        int
	prevn    int
	value    string
}

func (m message) number() int {
	return m.n
}

type promise interface {
	number() int
}
