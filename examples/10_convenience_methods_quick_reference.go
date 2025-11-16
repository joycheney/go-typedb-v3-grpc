package main

import (
	"context"
	"fmt"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 10: Convenience Methods - Simplify value extraction with Get*Or()
func main() {
	fmt.Println("=== Convenience Methods Demo ===\n")

	client, _ := typedbclient.NewClient(nil)
	defer client.Close()

	setupDB(context.Background(), client)
	db := client.GetDatabase("conv_demo")
	defer client.DeleteDatabase(context.Background(), "conv_demo")

	// Old way (verbose)
	fmt.Println("❌ Old way:")
	query := "match $p isa person, has name $n, has age $a; limit 1;"
	result, err := db.ExecuteRead(context.Background(), query)
	if err != nil || len(result.TypedRows) == 0 {
		fmt.Println("   No data found")
		return
	}
	row := result.TypedRows[0]

	var name string
	var age int64
	if n, err := row.GetString("n"); err == nil {
		name = n
	} else {
		name = "Unknown"
	}
	if a, err := row.GetInt64("a"); err == nil {
		age = a
	} else {
		age = 0
	}
	fmt.Printf("   Name: %s, Age: %d\n", name, age)

	// New way (concise)
	fmt.Println("\n✅ New way:")
	name = row.GetStringOr("n", "Unknown")
	age = row.GetInt64Or("a", 0)
	premium := row.GetBoolOr("premium", false) // Missing field → default
	fmt.Printf("   Name: %s, Age: %d, Premium: %v\n", name, age, premium)

	// Works with Documents too
	fmt.Println("\n📄 TypedDocument example:")
	fetchResult, err2 := db.ExecuteRead(context.Background(), "match $p isa person; fetch $p: name;")
	if err2 != nil || len(fetchResult.TypedDocuments) == 0 {
		fmt.Println("   No documents found")
		return
	}
	for i, doc := range fetchResult.TypedDocuments {
		if i >= 2 {
			break
		}
		// One-liner with defaults
		userName := doc.GetStringOr("name", "Anonymous")
		userAge := doc.GetInt64Or("age", 18) // Field missing → default
		fmt.Printf("   User: %s (age: %d)\n", userName, userAge)
	}

	// Available methods
	fmt.Println("\n📚 Available Get*Or() methods:")
	fmt.Println("   GetStringOr(field, default)   → string")
	fmt.Println("   GetInt64Or(field, default)    → int64")
	fmt.Println("   GetBoolOr(field, default)     → bool")
	fmt.Println("   GetFloat64Or(field, default)  → float64")

	fmt.Println("\n✨ No error handling needed, missing fields use defaults!")
}

func setupDB(ctx context.Context, client *typedbclient.Client) {
	client.DeleteDatabase(ctx, "conv_demo")
	if err := client.CreateDatabase(ctx, "conv_demo"); err != nil {
		panic(fmt.Sprintf("Failed to create database: %v", err))
	}
	db := client.GetDatabase("conv_demo")

	if _, err := db.ExecuteSchema(ctx, `
		define
		entity person, owns name, owns age;
		attribute name value string;
		attribute age value integer;
	`); err != nil {
		panic(fmt.Sprintf("Failed to define schema: %v", err))
	}

	if _, err := db.ExecuteWrite(ctx, `
		insert
		$p1 isa person, has name "Alice", has age 30;
		$p2 isa person, has name "Bob", has age 25;
		$p3 isa person, has name "Charlie";
	`); err != nil {
		panic(fmt.Sprintf("Failed to insert data: %v", err))
	}
}
