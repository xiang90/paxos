package paxos

import (
	"log"
	"time"
)

type learner struct {
	id        int
	acceptors map[int]accept

	nt network
}

func newLearner(id int, nt network, acceptors ...int) *learner {
	l := &learner{id: id, nt: nt, acceptors: make(map[int]accept)}
	for _, a := range acceptors {
		l.acceptors[a] = message{typ: Accept}
	}
	return l
}

// A value is learned when a single proposal with that value has been accepted by
// a majority of the acceptors.
func (l *learner) learn() string {
	for {
		m, ok := l.nt.recv(time.Hour)
		if !ok {
			continue
		}
		if m.typ != Accept {
			log.Panicf("learner: %d received unexpected msg %+v", l.id, m)
		}
		l.receiveAccepted(m)
		accept, ok := l.chosen()
		if !ok {
			continue
		}
		log.Printf("learner: %d learned the chosen propose %+v", l.id, accept)
		return accept.proposalValue()
	}
}

func (l *learner) receiveAccepted(accepted message) {
	a := l.acceptors[accepted.from]
	if a.proposalNumber() < accepted.n {
		log.Printf("learner: %d received a new accepted proposal %+v", l.id, accepted)
		l.acceptors[accepted.from] = accepted
	}
}

func (l *learner) majority() int { return len(l.acceptors)/2 + 1 }

// A proposal is chosen when it has been accepted by a majority of the
// acceptors.
// The leader might choose multiple proposals when it learns multiple times,
// but we guarantee that all chosen proposals have the same value.
func (l *learner) chosen() (accept, bool) {
	counts := make(map[int]int)
	accepteds := make(map[int]accept)

	for _, accepted := range l.acceptors {
		if accepted.proposalNumber() != 0 {
			counts[accepted.proposalNumber()]++
			accepteds[accepted.proposalNumber()] = accepted
		}
	}

	for n, count := range counts {
		if count >= l.majority() {
			return accepteds[n], true
		}
	}
	return message{}, false
}
