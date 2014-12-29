package paxos

import (
	"log"
	"time"
)

type proposer struct {
	id int
	// stable
	lastN int

	value  string
	valueN int

	acceptors map[int]message
	nt        network
}

func newProposer(id int, value string, nt network, acceptors ...int) *proposer {
	p := &proposer{id: id, nt: nt, lastN: id << 16, value: value, acceptors: make(map[int]message)}
	for _, a := range acceptors {
		p.acceptors[a] = message{}
	}
	return p
}

func (p *proposer) run() {
	var ok bool
	var m message

	// do prepare until reach the majority
	for !p.majorityReached() {
		if !ok {
			ms := p.prepare()
			for i := range ms {
				p.nt.send(ms[i])
			}
		}
		m, ok = p.nt.recv(time.Second)
		if !ok {
			// the previous prepare is failed
			// continue to do another prepare
			continue
		}

		switch m.typ {
		case "promise":
			p.receivePromise(m)
		default:
			log.Panicf("proposer: %d unexpected message type: ", p.id, m.typ)
		}
	}

	log.Printf("proposer: %d promise %d reached majority %d", p.id, p.lastN, p.majority())

	log.Printf("proposer: %d starts to propose [%d: %s]", p.id, p.lastN, p.value)
	ms := p.propose(p.value)
	for i := range ms {
		p.nt.send(ms[i])
	}
}

// If the proposer receives the requested responses from a majority of
// the acceptors, then it can issue a proposal with number n and value
// v, where v is the value of the highest-numbered proposal among the
// responses, or is any value selected by the proposer if the responders
// reported no proposals.
func (p *proposer) propose(value string) []message {
	ms := make([]message, p.majority())

	i := 0
	for to := range p.acceptors {
		ms[i] = message{from: p.id, to: to, typ: "propose", n: p.lastN, value: value}
		i++
		if i == p.majority() {
			break
		}
	}
	return ms
}

// A proposer chooses a new proposal number n and sends a request to
// each member of some set of acceptors, asking it to respond with:
// (a) A promise never again to accept a proposal numbered less than n, and
// (b) The proposal with the highest number less than n that it has accepted, if any.
func (p *proposer) prepare() []message {
	p.lastN++

	ms := make([]message, p.majority())
	i := 0
	for to := range p.acceptors {
		ms[i] = message{from: p.id, to: to, typ: "prepare", n: p.lastN}
		i++
		if i == p.majority() {
			break
		}
	}
	return ms
}

func (p *proposer) receivePromise(promise message) {
	a := p.acceptors[promise.from]
	if a.n < promise.n {
		log.Printf("proposer: %d received a new promise %+v", p.id, promise)
		p.acceptors[promise.from] = promise
		if promise.prevn != 0 && promise.prevn > p.valueN {
			log.Printf("proposer: %d updated the value [%s] to %s", p.id, p.value, promise.value)
			p.valueN = promise.prevn
			p.value = promise.value
		}
	}
}

func (p *proposer) majority() int { return len(p.acceptors)/2 + 1 }

func (p *proposer) majorityReached() bool {
	m := 0
	for _, promise := range p.acceptors {
		if promise.n == p.lastN {
			m++
		}
	}
	if m >= p.majority() {
		return true
	}
	return false
}
