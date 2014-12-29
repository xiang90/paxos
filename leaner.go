package paxos

import (
	"log"
	"time"
)

type learner struct {
	id        int
	acceptors map[int]message

	nt network
}

func newLearner(id int, nt network, acceptors ...int) *learner {
	l := &learner{id: id, nt: nt, acceptors: make(map[int]message)}
	for _, a := range acceptors {
		l.acceptors[a] = message{}
	}
	return l
}

func (l *learner) learn() string {
	for {
		m, ok := l.nt.recv(time.Hour)
		if !ok {
			continue
		}
		if m.typ != "accepted" {
			panic("bad msg type")
		}
		l.receiveAccepted(m)
		m, ok = l.chosen()
		if !ok {
			continue
		}
		log.Printf("learner: %d learned the chosen propose %+v", l.id, m)
		return m.value
	}
}

func (l *learner) receiveAccepted(accepted message) {
	a := l.acceptors[accepted.from]
	if a.n < accepted.n {
		log.Printf("learner: %d received a new accepted %+v", l.id, accepted)
		l.acceptors[accepted.from] = accepted
	}
}

func (l *learner) majority() int { return len(l.acceptors)/2 + 1 }

func (l *learner) chosen() (message, bool) {
	counts := make(map[int]int)
	accepteds := make(map[int]message)

	for _, accepted := range l.acceptors {
		if accepted.n != 0 {
			counts[accepted.n]++
			accepteds[accepted.n] = accepted
		}
	}

	for n, count := range counts {
		if count >= l.majority() {
			return accepteds[n], true
		}
	}
	return message{}, false
}
