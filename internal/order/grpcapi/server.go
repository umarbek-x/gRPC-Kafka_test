// Package grpcapi is the gRPC ADAPTER: it translates protobuf messages to
// service calls and domain errors to gRPC status codes. No business logic.
package grpcapi

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/order/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/service"
)

type Server struct {
	// Embedding the Unimplemented server keeps us forward compatible: when a
	// new RPC is added to the .proto, this still compiles and the new RPC
	// returns codes.Unimplemented until we write it.
	orderv1.UnimplementedOrderServiceServer
	svc *service.Service
	log *slog.Logger
}

func NewServer(svc *service.Service, log *slog.Logger) *Server {
	return &Server{svc: svc, log: log}
}

func (s *Server) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	items := make([]domain.Item, len(req.GetItems()))
	for i, it := range req.GetItems() {
		items[i] = domain.Item{ProductID: it.GetProductId(), Quantity: it.GetQuantity(), UnitPriceMinor: it.GetUnitPriceMinor()}
	}
	o, created, err := s.svc.CreateOrder(ctx, service.CreateInput{
		CustomerID: req.GetCustomerId(), Currency: req.GetCurrency(),
		IdempotencyKey: req.GetIdempotencyKey(), Items: items,
	})
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &orderv1.CreateOrderResponse{Order: toProto(o), Created: created}, nil
}

func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	o, err := s.svc.GetOrder(ctx, req.GetOrderId())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &orderv1.GetOrderResponse{Order: toProto(o)}, nil
}

func (s *Server) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	orders, next, err := s.svc.ListOrders(ctx, req.GetCustomerId(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	resp := &orderv1.ListOrdersResponse{NextPageToken: next, Orders: make([]*orderv1.Order, len(orders))}
	for i, o := range orders {
		resp.Orders[i] = toProto(o)
	}
	return resp, nil
}

func (s *Server) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	o, err := s.svc.CancelOrder(ctx, req.GetOrderId(), req.GetReason())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &orderv1.CancelOrderResponse{Order: toProto(o)}, nil
}

// WatchOrder streams the current state, then each status change, and ends
// when the order is settled (no longer PENDING) or the client disconnects.
func (s *Server) WatchOrder(req *orderv1.WatchOrderRequest, stream orderv1.OrderService_WatchOrderServer) error {
	ctx := stream.Context()

	// Subscribe BEFORE reading the current state. The other order would have
	// a race: a change between "read" and "subscribe" would be missed.
	updates, unsubscribe := s.svc.Watch(req.GetOrderId())
	defer unsubscribe()

	current, err := s.svc.GetOrder(ctx, req.GetOrderId())
	if err != nil {
		return s.toStatus(ctx, err)
	}
	if err := stream.Send(&orderv1.WatchOrderResponse{Order: toProto(current)}); err != nil {
		return err
	}
	if current.Status != domain.StatusPending {
		return nil
	}
	lastVersion := current.Version

	for {
		select {
		case <-ctx.Done():
			// Client went away or deadline hit. Returning frees the goroutine
			// gRPC runs this handler on: no leak.
			return status.FromContextError(ctx.Err()).Err()
		case o := <-updates:
			if o.Version <= lastVersion {
				continue // already sent (update raced with our initial read)
			}
			lastVersion = o.Version
			if err := stream.Send(&orderv1.WatchOrderResponse{Order: toProto(o)}); err != nil {
				return err
			}
			if o.Status != domain.StatusPending {
				return nil
			}
		}
	}
}

func toProto(o *domain.Order) *orderv1.Order {
	p := &orderv1.Order{
		Id: o.ID, CustomerId: o.CustomerID, TotalMinor: o.TotalMinor, Currency: o.Currency,
		Status: statusToProto(o.Status), FailureReason: o.FailureReason, Version: o.Version,
		CreatedAt: timestamppb.New(o.CreatedAt), UpdatedAt: timestamppb.New(o.UpdatedAt),
		Items: make([]*orderv1.OrderItem, len(o.Items)),
	}
	for i, it := range o.Items {
		p.Items[i] = &orderv1.OrderItem{ProductId: it.ProductID, Quantity: it.Quantity, UnitPriceMinor: it.UnitPriceMinor}
	}
	return p
}

func statusToProto(s domain.Status) orderv1.OrderStatus {
	switch s {
	case domain.StatusPending:
		return orderv1.OrderStatus_ORDER_STATUS_PENDING
	case domain.StatusPaid:
		return orderv1.OrderStatus_ORDER_STATUS_PAID
	case domain.StatusPaymentFailed:
		return orderv1.OrderStatus_ORDER_STATUS_PAYMENT_FAILED
	case domain.StatusCancelled:
		return orderv1.OrderStatus_ORDER_STATUS_CANCELLED
	}
	return orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED
}

// toStatus maps domain errors to gRPC codes. Clients decide what to do based
// on the code (retry on Unavailable, fix the request on InvalidArgument), so
// choosing the right one is part of the API contract.
//
// Unknown errors become codes.Internal with a generic message: never leak SQL
// errors or stack traces to clients (the real error is logged server-side).
func (s *Server) toStatus(ctx context.Context, err error) error {
	var ve *domain.ValidationError
	switch {
	case errors.As(err, &ve):
		st := status.New(codes.InvalidArgument, ve.Error())
		// Rich error details: machine-readable list of bad fields.
		if detailed, dErr := st.WithDetails(&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: ve.Field, Description: ve.Message}},
		}); dErr == nil {
			st = detailed
		}
		return st.Err()
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "order not found")
	case errors.Is(err, domain.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	default:
		s.log.ErrorContext(ctx, "unexpected error in grpc handler", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}
