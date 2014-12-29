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
