package paxos

import (
	"log"
	"time"
)

// P1. An acceptor must accept the first proposal that it receives.
// If a proposal with value v is chosen, then every higher-numbered proposal
// accepted by any acceptor has value v.
type acceptor struct {
	id       int
	learners []int

	accept   message
	promised promise

	nt network
}

func newAcceptor(id int, nt network, learners ...int) *acceptor {
	return &acceptor{id: id, nt: nt, promised: message{}, learners: learners}
}

func (a *acceptor) run() {
	for {
		m, ok := a.nt.recv(time.Hour)
		if !ok {
			continue
		}
		switch m.typ {
		case Propose:
			accepted := a.receivePropose(m)
			if accepted {
				for _, l := range a.learners {
					m := a.accept
					m.from = a.id
					m.to = l
					a.nt.send(m)
				}
			}
		case Prepare:
			promise, ok := a.receivePrepare(m)
			if ok {
				a.nt.send(promise)
			}
		default:
			log.Panicf("acceptor: %d unexpected message type: %v", a.id, m.typ)
		}
	}
}

// If an acceptor receives a prepare request with number n greater
// than that of any prepare request to which it has already responded,
// then it responds to the request with a promise not to accept any more
// proposals numbered less than n and with the highest-numbered proposal
// (if any) that it has accepted.
func (a *acceptor) receivePrepare(prepare message) (message, bool) {
	if a.promised.number() >= prepare.number() {
		log.Printf("acceptor: %d [promised: %+v] ignored prepare %+v", a.id, a.promised, prepare)
		return message{}, false
	}
	log.Printf("acceptor: %d [promised: %+v] promised %+v", a.id, a.promised, prepare)
	a.promised = prepare
	m := message{
		typ:  Promise,
		from: a.id, to: prepare.from,
		n: a.promised.number(),
		// previously accepted proposal
		prevn: a.accept.n, value: a.accept.value,
	}
	return m, true
}

// If an acceptor receives an accept request for a proposal numbered
// n, it accepts the proposal unless it has already responded to a prepare
// request having a number greater than n.
func (a *acceptor) receivePropose(propose message) bool {
	if a.promised.number() > propose.number() {
		log.Printf("acceptor: %d [promised: %+v] ignored proposal %+v", a.id, a.promised, propose)
		return false
	}
	if a.promised.number() < propose.number() {
		log.Panicf("acceptor: %d received unexpected proposal %+v", a.id, propose)
	}
	log.Printf("acceptor: %d [promised: %+v, accept: %+v] accepted proposal %+v", a.id, a.promised, a.accept, propose)
	a.accept = propose
	a.accept.typ = Accept
	return true
}

func (a *acceptor) restart() {}
func (a *acceptor) delay()   {}
