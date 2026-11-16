package domain

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func validItems() []Item {
	return []Item{{ProductID: "sku-1", Quantity: 2, UnitPriceMinor: 1500}, {ProductID: "sku-2", Quantity: 1, UnitPriceMinor: 250}}
}

func TestNewOrderComputesTotal(t *testing.T) {
	o, err := NewOrder("id-1", "cust-1", "USD", "key", validItems(), now)
	if err != nil {
		t.Fatal(err)
	}
	if o.TotalMinor != 3250 {
		t.Fatalf("total = %d, want 3250", o.TotalMinor)
	}
	if o.Status != StatusPending || o.Version != 1 {
		t.Fatalf("unexpected initial state: %+v", o)
	}
}

func TestNewOrderValidation(t *testing.T) {
	tests := []struct {
		name      string
		customer  string
		currency  string
		items     []Item
		wantField string
	}{
		{"empty customer", "", "USD", validItems(), "customer_id"},
		{"long customer", strings.Repeat("x", 200), "USD", validItems(), "customer_id"},
		{"lowercase currency", "c", "usd", validItems(), "currency"},
		{"short currency", "c", "US", validItems(), "currency"},
		{"no items", "c", "USD", nil, "items"},
		{"too many items", "c", "USD", make([]Item, MaxItems+1), "items"},
		{"zero quantity", "c", "USD", []Item{{ProductID: "p", Quantity: 0, UnitPriceMinor: 1}}, "items[0].quantity"},
		{"negative price", "c", "USD", []Item{{ProductID: "p", Quantity: 1, UnitPriceMinor: -1}}, "items[0].unit_price_minor"},
		{"empty product", "c", "USD", []Item{{Quantity: 1, UnitPriceMinor: 1}}, "items[0].product_id"},
		{"line overflow", "c", "USD", []Item{{ProductID: "p", Quantity: 1000, UnitPriceMinor: math.MaxInt64 / 2}}, "items[0]"},
		{"total overflow", "c", "USD", []Item{
			{ProductID: "p", Quantity: 1, UnitPriceMinor: math.MaxInt64 - 1},
			{ProductID: "q", Quantity: 1, UnitPriceMinor: 5},
		}, "items"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewOrder("id", tt.customer, tt.currency, "", tt.items, now)
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("want ValidationError, got %v", err)
			}
			if ve.Field != tt.wantField {
				t.Fatalf("field = %q, want %q", ve.Field, tt.wantField)
			}
		})
	}
}

// Exhaustively test the state machine: every (from, to) pair.
func TestStateMachine(t *testing.T) {
	all := []Status{StatusPending, StatusPaid, StatusPaymentFailed, StatusCancelled}
	allowed := map[[2]Status]bool{
		{StatusPending, StatusPaid}:          true,
		{StatusPending, StatusPaymentFailed}: true,
		{StatusPending, StatusCancelled}:     true,
		{StatusPaid, StatusCancelled}:        true,
	}
	apply := func(o *Order, to Status) error {
		switch to {
		case StatusPaid:
			return o.MarkPaid(now)
		case StatusPaymentFailed:
			return o.MarkPaymentFailed("declined", now)
		case StatusCancelled:
			return o.Cancel("changed my mind", now)
		}
		return nil
	}
	for _, from := range all {
		for _, to := range all[1:] { // nothing transitions *to* PENDING
			o, _ := NewOrder("id", "c", "USD", "", validItems(), now)
			o.Status = from
			err := apply(o, to)
			if allowed[[2]Status{from, to}] {
				if err != nil {
					t.Errorf("%s -> %s should be allowed: %v", from, to, err)
				}
				if o.Status != to || o.Version != 2 {
					t.Errorf("%s -> %s: state not updated: %+v", from, to, o)
				}
			} else {
				if !errors.Is(err, ErrInvalidTransition) {
					t.Errorf("%s -> %s should be rejected, got %v", from, to, err)
				}
				if o.Status != from || o.Version != 1 {
					t.Errorf("%s -> %s: rejected transition must not mutate", from, to)
				}
			}
		}
	}
}

func TestIsFinal(t *testing.T) {
	if StatusPending.IsFinal() || StatusPaid.IsFinal() {
		t.Fatal("PENDING and PAID are not final")
	}
	if !StatusCancelled.IsFinal() || !StatusPaymentFailed.IsFinal() {
		t.Fatal("CANCELLED and PAYMENT_FAILED are final")
	}
}

func TestCloneIsDeep(t *testing.T) {
	o, _ := NewOrder("id", "c", "USD", "", validItems(), now)
	c := o.Clone()
	c.Items[0].Quantity = 99
	if o.Items[0].Quantity == 99 {
		t.Fatal("clone shares the items slice")
	}
}
