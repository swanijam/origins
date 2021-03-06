package testutil

import (
	"math/rand"
	"time"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
)

func RandFacts(n int, d string) fact.Facts {
	rand.Seed(time.Now().UnixNano())

	facts := make(fact.Facts, n)
	c := identity.Cache{}

	var (
		e, a, v *identity.Ident
		f       *fact.Fact
	)

	for i := 0; i < n; i++ {
		e = c.Add(d, string(rand.Int63()))
		a = c.Add(d, string(rand.Int63()))
		v = c.Add(d, string(rand.Int63()))

		f = fact.Assert(e, a, v)

		facts[i] = f
	}

	return facts
}

func RandFactsWithTx(n int, d string) fact.Facts {
	rand.Seed(time.Now().UnixNano())

	facts := make(fact.Facts, n)
	c := identity.Cache{}

	var (
		e, a, v, t *identity.Ident
		f          *fact.Fact
	)

	for i := 0; i < n; i++ {
		e = c.Add(d, string(rand.Int63()))
		a = c.Add(d, string(rand.Int63()))
		v = c.Add(d, string(rand.Int63()))
		t = c.Add(d, string(rand.Int63()))

		f = fact.Assert(e, a, v)
		f.Transaction = t

		facts[i] = f
	}

	return facts
}
