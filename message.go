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

func (m message) proposalValue() string {
	switch m.typ {
	case Promise, Accept:
		return m.value
	default:
		panic("unexpected proposalV")
	}
}

func (m message) proposalNumber() int {
	switch m.typ {
	case Promise:
		return m.prevn
	case Accept:
		return m.n
	default:
		panic("unexpected proposalN")
	}
}

type promise interface {
	number() int
}

type accept interface {
	proposalValue() string
	proposalNumber() int
}
