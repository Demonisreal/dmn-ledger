package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := ledger.Dial(ctx, ledger.Config{
		Cluster: 1,
		Nodes:   []string{"127.0.0.1:7101", "127.0.0.1:7102", "127.0.0.1:7103"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	_, err = c.CreateAccounts(ctx, []ledger.Account{
		{ID: ledger.From64(1), Ledger: 1, Code: 10},
		{ID: ledger.From64(2), Ledger: 1, Code: 10},
	})
	if err != nil {
		log.Fatal(err)
	}

	pay := []ledger.Transfer{{
		ID:              ledger.NewID(),
		DebitAccountID:  ledger.From64(1),
		CreditAccountID: ledger.From64(2),
		Amount:          ledger.From64(500),
		Ledger:          1,
		Code:            1,
	}}
	res, err := c.CreateTransfers(ctx, pay)
	switch {
	case errors.Is(err, ledger.ErrUncertain):
		log.Fatal("sent, but no answer in time: send the same transfers again, they keep their IDs")
	case err != nil:
		log.Fatal(err)
	}
	for _, f := range res.Failures {
		fmt.Println("transfer", f.Index, "failed:", f.Reason)
	}
}
