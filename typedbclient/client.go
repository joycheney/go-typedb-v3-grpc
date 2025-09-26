package typedbclient

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	pb "github.com/joycheney/go-typedb-v3-grpc/pb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// connWrapper wraps connection and gRPC client for atomic access
type connWrapper struct {
	conn       *grpc.ClientConn
	grpcClient pb.TypeDBClient
}

// Client TypeDB gRPC client (completely lock-free design, consistent with HTTP client architecture)
type Client struct {
	address  string
	username string
	password string

	// Lock-free connection management using atomic pointer
	connRef atomic.Value // stores *connWrapper

	// Lock-free authentication state management
	tokenValue     atomic.Value // Store JWT token (string)
	authInProgress atomic.Bool  // Mark if authentication is in progress (lock-free CAS operation)

	// Lock-free connection state
	reconnecting atomic.Bool
	lastConnTime atomic.Value // time.Time
}


// NewClient creates a new TypeDB gRPC client
// Follows Go standard library conventions, uses Options pattern instead of Builder pattern
func NewClient(opts *Options) (*Client, error) {
	if opts == nil {
		opts = DefaultOptions()
	} else {
		// Fill missing default values
		opts.fillDefaults()
	}

	client := &Client{
		address:  opts.Address,
		username: opts.Username,
		password: opts.Password,
	}

	// Establish gRPC connection
	if err := client.connect(opts); err != nil {
		return nil, fmt.Errorf("failed to connect to TypeDB: %w", err)
	}

	// Perform initial authentication
	if err := client.authenticate(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to authenticate: %w", err)
	}

	return client, nil
}

// connect establishes gRPC connection
func (c *Client) connect(opts *Options) error {
	// gRPC connection options with TypeDB official standards
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                2 * time.Hour, // TypeDB official GRPC_CONNECTION_KEEPALIVE
			Timeout:             0,             // Use gRPC default (no custom timeout)
			PermitWithoutStream: true,
		}),
		// Use gRPC default message size limits (no custom limits)
	}

	// Establish connection
	conn, err := grpc.Dial(c.address, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	// Create new connection wrapper and store atomically
	wrapper := &connWrapper{
		conn:       conn,
		grpcClient: pb.NewTypeDBClient(conn),
	}
	c.connRef.Store(wrapper)

	c.lastConnTime.Store(time.Now())

	return nil
}

// authenticate performs initial authentication (used for first connection)
func (c *Client) authenticate() error {
	// Use CAS operation to prevent concurrent authentication
	if !c.authInProgress.CompareAndSwap(false, true) {
		// Another goroutine is authenticating, wait for completion
		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)

			// First check if still authenticating
			if c.authInProgress.Load() {
				continue
			}

			// Authentication completed, recheck token (add brief delay to ensure token is set)
			time.Sleep(10 * time.Millisecond)
			if token := c.getToken(); token != "" {
				return nil
			}

			// If no token, authentication by other goroutine failed
			return fmt.Errorf("authentication failed by other goroutine")
		}
		return fmt.Errorf("authentication timeout")
	}
	defer c.authInProgress.Store(false)

	// Use ConnectionOpen to establish connection and get token
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build connection request
	req := &pb.Connection_Open_Req{
		Version:       pb.Version_VERSION, // Use version 7
		DriverLang:    "go",
		DriverVersion: "3.0.0",
		Authentication: &pb.Authentication_Token_Create_Req{
			Credentials: &pb.Authentication_Token_Create_Req_Password_{
				Password: &pb.Authentication_Token_Create_Req_Password{
					Username: c.username,
					Password: c.password,
				},
			},
		},
	}

	// Get current connection atomically
	connRef := c.connRef.Load()
	if connRef == nil {
		return fmt.Errorf("gRPC client not initialized")
	}

	wrapper := connRef.(*connWrapper)
	client := wrapper.grpcClient

	// Call ConnectionOpen for initial connection
	resp, err := client.ConnectionOpen(ctx, req)
	if err != nil {
		return fmt.Errorf("connection open failed: %w", err)
	}

	if resp.Authentication == nil || resp.Authentication.Token == "" {
		return fmt.Errorf("received empty authentication token")
	}

	// Store token (lock-free)
	c.tokenValue.Store(resp.Authentication.Token)

	return nil
}

// refreshToken refreshes authentication token (used for token expiration)
func (c *Client) refreshToken() error {
	// When token expires completely, we cannot use it to refresh
	// Instead, we must re-authenticate from scratch
	// This avoids the paradox where we need a valid token to get a new token
	return c.authenticate()
}

// getToken safely gets token (lock-free access)
func (c *Client) getToken() string {
	if val := c.tokenValue.Load(); val != nil {
		if token, ok := val.(string); ok {
			return token
		}
	}
	return ""
}

// withAuth adds authentication information to context
func (c *Client) withAuth(ctx context.Context) context.Context {
	token := c.getToken()
	if token == "" {
		return ctx
	}

	// Add Bearer token to metadata
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewOutgoingContext(ctx, md)
}

// tryReconnect tries to reconnect (lock-free design)
func (c *Client) tryReconnect() error {
	// Use CAS to prevent concurrent reconnection
	if !c.reconnecting.CompareAndSwap(false, true) {
		// Another goroutine is reconnecting, wait
		for i := 0; i < 30; i++ {
			time.Sleep(100 * time.Millisecond)

			// Check if still reconnecting
			if c.reconnecting.Load() {
				continue
			}

			// Reconnection completed, check if successful (by checking token)
			time.Sleep(10 * time.Millisecond)
			if token := c.getToken(); token != "" {
				return nil
			}

			// Reconnection failed
			return fmt.Errorf("reconnection failed by other goroutine")
		}
		return fmt.Errorf("reconnection timeout")
	}
	defer c.reconnecting.Store(false)

	// Clear old token to ensure re-authentication
	c.tokenValue.Store("")

	// Close old connection atomically
	if oldConnRef := c.connRef.Load(); oldConnRef != nil {
		if oldWrapper := oldConnRef.(*connWrapper); oldWrapper != nil && oldWrapper.conn != nil {
			oldWrapper.conn.Close()
		}
	}

	// Reconnect
	opts := &Options{
		Address:  c.address,
		Username: c.username,
		Password: c.password,
	}
	opts.fillDefaults() // Still need to fill basic defaults

	if err := c.connect(opts); err != nil {
		return err
	}

	// Reauthenticate (authenticate already has CAS protection against concurrency)
	return c.authenticate()
}

// Close closes client connection
func (c *Client) Close() error {
	if connRef := c.connRef.Load(); connRef != nil {
		if wrapper := connRef.(*connWrapper); wrapper != nil && wrapper.conn != nil {
			return wrapper.conn.Close()
		}
	}
	return nil
}

// GetConn gets gRPC connection (internal use)
func (c *Client) GetConn() *grpc.ClientConn {
	if connRef := c.connRef.Load(); connRef != nil {
		if wrapper := connRef.(*connWrapper); wrapper != nil {
			return wrapper.conn
		}
	}
	return nil
}

// executeWithRetry executor with retry (unified request execution pattern, similar to HTTP client's executeRequest)
func (c *Client) executeWithRetry(ctx context.Context, operation func(context.Context) error) error {
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Add authentication information
		authCtx := c.withAuth(ctx)

		// Execute operation
		err := operation(authCtx)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check protocol error (server returns HTTP instead of gRPC when token expires)
		// Protocol errors usually indicate token is completely invalid, need reconnection
		if isProtocolError(err) {
			// Directly reconnect and re-authenticate to avoid token refresh paradox
			// tryReconnect has internal CAS protection ensuring only one goroutine reconnects
			if reconnErr := c.tryReconnect(); reconnErr != nil {
				return fmt.Errorf("reconnection after protocol error failed: %w", reconnErr)
			}
			continue
		}

		// Check authentication error
		if isAuthError(err) {
			// Authentication errors also need reconnection to get new token
			// refreshToken now directly calls authenticate, has CAS protection
			if refreshErr := c.refreshToken(); refreshErr != nil {
				// If refresh fails, try full reconnection
				if reconnErr := c.tryReconnect(); reconnErr != nil {
					return fmt.Errorf("reconnection after auth error failed: %w", reconnErr)
				}
			}
			continue
		}

		// Check if reconnection is needed for network issues
		if isConnectionError(err) {
			// Try to reconnect
			if reconnErr := c.tryReconnect(); reconnErr != nil {
				return fmt.Errorf("reconnection failed: %w", reconnErr)
			}
			continue
		}

		// Other errors, use exponential backoff retry
		if attempt < maxRetries-1 {
			time.Sleep(time.Duration(100<<attempt) * time.Millisecond)
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", maxRetries, lastErr)
}

// isAuthError checks if it's an authentication error
func isAuthError(err error) bool {
	if err == nil {
		return false
	}

	// Check gRPC status code - according to documentation line 230: UNAUTHENTICATED status code
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.Unauthenticated {
			return true
		}
	}

	// Check error message content
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "invalid token") ||
		   strings.Contains(errMsg, "token expired") ||
		   strings.Contains(errMsg, "authentication failed") ||
		   strings.Contains(errMsg, "unauthorized")
}

// isProtocolError checks if it's a protocol mismatch error (e.g., HTTP response to gRPC request)
// When token expires, server may return HTTP response instead of gRPC, causing protocol mismatch error
func isProtocolError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "malformed header") ||
		   strings.Contains(errMsg, "unexpected http") ||
		   strings.Contains(errMsg, "missing http content-type") ||
		   strings.Contains(errMsg, "unimplemented") ||
		   strings.Contains(errMsg, "404 (not found)")
}

// isConnectionError checks if it's a connection error
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check gRPC status code - connection-related error states
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Aborted:
			return true
		}
	}

	// Check error message content - connection-related error messages
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "connection") ||
		   strings.Contains(errMsg, "network") ||
		   strings.Contains(errMsg, "dial") ||
		   strings.Contains(errMsg, "timeout") ||
		   strings.Contains(errMsg, "unavailable") ||
		   strings.Contains(errMsg, "refused") ||
		   strings.Contains(errMsg, "broken") ||
		   strings.Contains(errMsg, "reset")
}

// CreateAuthToken creates authentication token (independent authentication API)
func (c *Client) CreateAuthToken(ctx context.Context, username, password string) (string, error) {
	var token string

	err := c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.Authentication_Token_Create_Req{
			Credentials: &pb.Authentication_Token_Create_Req_Password_{
				Password: &pb.Authentication_Token_Create_Req_Password{
					Username: username,
					Password: password,
				},
			},
		}

		resp, err := client.AuthenticationTokenCreate(ctx, req)
		if err != nil {
			return err
		}

		token = resp.Token
		return nil
	})

	return token, err
}

// Server server information
type Server struct {
	Address string
}

// ListServers lists all servers
func (c *Client) ListServers(ctx context.Context) ([]*Server, error) {
	var servers []*Server

	err := c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.ServerManager_All_Req{}
		resp, err := client.ServersAll(ctx, req)
		if err != nil {
			return err
		}

		servers = make([]*Server, 0, len(resp.Servers))
		for _, pbServer := range resp.Servers {
			server := &Server{
				Address: pbServer.Address,
			}
			servers = append(servers, server)
		}
		return nil
	})

	return servers, err
}


// Remove type aliases: explicitly use *Client and *Transaction pointer types
// This avoids user confusion caused by type aliases hiding pointer semantics

// Connect convenience function to directly connect to TypeDB server
// Follows standard library conventions, uses Options pattern
func Connect(address, username, password string) (*Client, error) {
	return NewClient(&Options{
		Address:  address,
		Username: username,
		Password: password,
	})
}

// ConnectDefault connects to specified address using default credentials
func ConnectDefault(address string) (*Client, error) {
	return Connect(address, "admin", "password")
}

// ConnectLocal connects to local TypeDB server (127.0.0.1:1729)
func ConnectLocal() (*Client, error) {
	return Connect("127.0.0.1:1729", "admin", "password")
}