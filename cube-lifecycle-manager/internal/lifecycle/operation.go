package lifecycle

import "context"

type operationKey struct{}

// WithOperationID carries the existing Master request ID through bookkeeping.
func WithOperationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, operationKey{}, id)
}
func OperationID(ctx context.Context) string {
	id, _ := ctx.Value(operationKey{}).(string)
	return id
}
