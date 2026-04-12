package middleware

import (
	"context"
	"fmt"
	"strings"
)

// AuthConfig holds configuration for authentication middleware
type AuthConfig struct {
	// Validator is the token validation function
	Validator TokenValidator
	// SkipMethods is a list of methods that don't require authentication
	SkipMethods []string
	// HeaderName is the name of the header containing the token
	HeaderName string
	// TokenPrefix is the expected prefix for the token (e.g., "Bearer ")
	TokenPrefix string
}

// TokenValidator validates authentication tokens
type TokenValidator func(ctx context.Context, token string) (context.Context, error)

// DefaultAuthConfig returns the default authentication configuration
func DefaultAuthConfig(validator TokenValidator) *AuthConfig {
	return &AuthConfig{
		Validator:   validator,
		SkipMethods: []string{},
		HeaderName:  "authorization",
		TokenPrefix: "Bearer ",
	}
}

// AuthMiddlewareWithConfig creates an authentication middleware with custom configuration
func AuthMiddlewareWithConfig(config *AuthConfig) Middleware {
	if config == nil {
		panic("auth config cannot be nil")
	}

	skipMap := make(map[string]bool)
	for _, method := range config.SkipMethods {
		skipMap[method] = true
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			// Check if method should skip auth
			if method, ok := ctx.Value("method").(string); ok && skipMap[method] {
				return next(ctx, req)
			}

			// Extract token from metadata
			md := getMetadata(ctx)
			if md == nil {
				return nil, fmt.Errorf("missing metadata")
			}

			token := md[config.HeaderName]
			if token == "" {
				return nil, fmt.Errorf("missing authorization token")
			}

			// Remove token prefix if present
			if config.TokenPrefix != "" && strings.HasPrefix(token, config.TokenPrefix) {
				token = strings.TrimPrefix(token, config.TokenPrefix)
			}

			// Validate token
			newCtx, err := config.Validator(ctx, token)
			if err != nil {
				return nil, fmt.Errorf("authentication failed: %w", err)
			}

			return next(newCtx, req)
		}
	}
}

// SimpleTokenValidator creates a simple token validator that checks against a set of valid tokens
func SimpleTokenValidator(validTokens map[string]string) TokenValidator {
	return func(ctx context.Context, token string) (context.Context, error) {
		if _, ok := validTokens[token]; !ok {
			return nil, fmt.Errorf("invalid token")
		}
		return ctx, nil
	}
}

// ContextKey is the type for context keys
type ContextKey string

const (
	// UserIDKey is the context key for user ID
	UserIDKey ContextKey = "user-id"
	// UserRoleKey is the context key for user role
	UserRoleKey ContextKey = "user-role"
)

// WithUserID adds user ID to context
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, UserIDKey, userID)
}

// UserIDFromContext extracts user ID from context
func UserIDFromContext(ctx context.Context) string {
	if userID, ok := ctx.Value(UserIDKey).(string); ok {
		return userID
	}
	return ""
}

// WithUserRole adds user role to context
func WithUserRole(ctx context.Context, role string) context.Context {
	return context.WithValue(ctx, UserRoleKey, role)
}

// UserRoleFromContext extracts user role from context
func UserRoleFromContext(ctx context.Context) string {
	if role, ok := ctx.Value(UserRoleKey).(string); ok {
		return role
	}
	return ""
}

// getMetadata extracts metadata from context
func getMetadata(ctx context.Context) map[string]string {
	if md, ok := ctx.Value("metadata").(map[string]string); ok {
		return md
	}
	return nil
}
