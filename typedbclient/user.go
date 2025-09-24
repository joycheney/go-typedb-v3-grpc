package typedbclient

import (
	"context"
	"fmt"

	pb "github.com/joycheney/go-typedb-v3-grpc/pb/proto"
)

// User user information
type User struct {
	Name     string
	Password string
}

// ListUsers list all users
func (c *Client) ListUsers(ctx context.Context) ([]*User, error) {
	var users []*User

	err := c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.UserManager_All_Req{}
		resp, err := client.UsersAll(ctx, req)
		if err != nil {
			return err
		}

		users = make([]*User, 0, len(resp.Users))
		for _, pbUser := range resp.Users {
			user := &User{
				Name: pbUser.Name,
			}
			if pbUser.Password != nil {
				user.Password = *pbUser.Password
			}
			users = append(users, user)
		}
		return nil
	})

	return users, err
}

// GetUser get specified user
func (c *Client) GetUser(ctx context.Context, name string) (*User, error) {
	var user *User

	err := c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.UserManager_Get_Req{
			Name: name,
		}
		resp, err := client.UsersGet(ctx, req)
		if err != nil {
			return err
		}

		user = &User{
			Name: resp.User.Name,
		}
		if resp.User.Password != nil {
			user.Password = *resp.User.Password
		}
		return nil
	})

	return user, err
}

// UserExists check if user exists
func (c *Client) UserExists(ctx context.Context, name string) (bool, error) {
	var exists bool

	err := c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.UserManager_Contains_Req{
			Name: name,
		}
		resp, err := client.UsersContains(ctx, req)
		if err != nil {
			return err
		}

		exists = resp.Contains
		return nil
	})

	return exists, err
}

// CreateUser create user
func (c *Client) CreateUser(ctx context.Context, name, password string) error {
	return c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.UserManager_Create_Req{
			User: &pb.User{
				Name:     name,
				Password: &password,
			},
		}
		_, err := client.UsersCreate(ctx, req)
		return err
	})
}

// UpdateUser update user
func (c *Client) UpdateUser(ctx context.Context, name string, newPassword string) error {
	return c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.User_Update_Req{
			Name: name,
			User: &pb.User{
				Name:     name,
				Password: &newPassword,
			},
		}
		_, err := client.UsersUpdate(ctx, req)
		return err
	})
}

// DeleteUser delete user
func (c *Client) DeleteUser(ctx context.Context, name string) error {
	return c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.User_Delete_Req{
			Name: name,
		}
		_, err := client.UsersDelete(ctx, req)
		return err
	})
}