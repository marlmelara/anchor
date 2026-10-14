package api

import "context"

func withTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

// tenantFrom reads the authenticated tenant. It falls back to "default" rather
// than panicking, so a handler reached without the auth middleware degrades to
// the development tenant instead of crashing the server.
func tenantFrom(ctx context.Context) string {
	if v, ok := ctx.Value(tenantKey{}).(string); ok && v != "" {
		return v
	}
	return "default"
}
