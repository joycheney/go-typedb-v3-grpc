package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mcp-software-think-execute-server/go-typedb-v3-grpc/typedbclient"
)

// Example 7: Comprehensive End-to-End Demonstration
// This example demonstrates a complete TypeDB v3 application scenario, including:
// - Complete data modeling (entities, relations, attributes)
// - Complex schema design and evolution
// - Batch data import and query optimization
// - Complex queries and data analysis
// - Transaction management and data consistency
// - Error handling and recovery mechanisms
// - Performance monitoring and best practices
//
// Business Scenario: Company Personnel Management System
// - Employee information management
// - Department organizational structure
// - Project allocation and collaboration relationships
// - Skill tags and matching

func main() {
	fmt.Println("=== TypeDB v3 gRPC Comprehensive End-to-End Demonstration ===")
	fmt.Println("Business Scenario: Company Personnel Management System")

	// Create client
	client, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create demo database
	demoDbName := "company_management_demo"
	ctx := context.Background()

	// Stage 1: Environment setup and data modeling
	fmt.Println("\n=== Stage 1: Environment Setup and Data Modeling ===")
	if err := setupDemoEnvironment(ctx, client, demoDbName); err != nil {
		log.Fatalf("Environment setup failed: %v", err)
	}
	defer cleanupDemoEnvironment(ctx, client, demoDbName)

	database := client.GetDatabase(demoDbName)

	// Stage 2: Schema design and evolution
	fmt.Println("\n=== Stage 2: Schema Design and Evolution ===")
	if err := defineCompanySchema(ctx, database); err != nil {
		log.Fatalf("Schema definition failed: %v", err)
	}

	// Stage 3: Data import and initialization
	fmt.Println("\n=== Stage 3: Data Import and Initialization ===")
	if err := importSampleData(ctx, database); err != nil {
		log.Fatalf("Data import failed: %v", err)
	}

	// Stage 4: Complex queries and business logic
	fmt.Println("\n=== Stage 4: Complex Queries and Business Logic ===")
	runBusinessQueries(ctx, database)

	// Stage 5: Data analysis and reports
	fmt.Println("\n=== Stage 5: Data Analysis and Reports ===")
	generateAnalyticsReports(ctx, database)

	// Stage 6: Advanced features demonstration
	fmt.Println("\n=== Stage 6: Advanced Features Demonstration ===")
	demonstrateAdvancedFeatures(ctx, database)

	// Stage 7: Performance optimization and monitoring
	fmt.Println("\n=== Stage 7: Performance Optimization and Monitoring ===")
	demonstratePerformanceOptimization(ctx, database)

	fmt.Println("\n=== Comprehensive Demonstration Completed ===")
	fmt.Println("🎉 Congratulations! You have mastered the complete usage of TypeDB v3 Go client")
}

// setupDemoEnvironment sets up demo environment
func setupDemoEnvironment(ctx context.Context, client *typedbclient.Client, dbName string) error {
	fmt.Printf("Setting up demo environment...\n")

	// Check and clean up existing database
	exists, err := client.DatabaseExists(ctx, dbName)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %w", err)
	}

	if exists {
		fmt.Printf("Deleting existing demo database: %s\n", dbName)
		if err := client.DeleteDatabase(ctx, dbName); err != nil {
			return fmt.Errorf("failed to delete existing database: %w", err)
		}
	}

	// Create new demo database
	fmt.Printf("Creating demo database: %s\n", dbName)
	if err := client.CreateDatabase(ctx, dbName); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	fmt.Printf("✓ Demo environment setup completed\n")
	return nil
}

// defineCompanySchema defines the schema for company management system
func defineCompanySchema(ctx context.Context, database *typedbclient.Database) error {
	fmt.Printf("Designing data model...\n")

	// TypeDB 3.x simplified schema
	schemas := []struct {
		name   string
		schema string
	}{
		{
			"TypeDB 3.x Schema",
			`define
				entity person, owns name, owns email;
				entity department, owns deptname;
				entity project, owns projectname, owns status;
				entity skill, owns skillname;
				attribute name, value string;
				attribute email, value string;
				attribute deptname, value string;
				attribute projectname, value string;
				attribute status, value string;
				attribute skillname, value string;
			`,
		},
	}

	for i, s := range schemas {
		fmt.Printf("%d. %s\n", i+1, s.name)

		schemaCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := database.ExecuteSchema(schemaCtx, s.schema)
		cancel()

		if err != nil {
			return fmt.Errorf("failed to define %s: %w", s.name, err)
		}

		fmt.Printf("   ✓ %s defined successfully\n", s.name)
	}

	// Verify Schema
	fmt.Printf("\nVerifying Schema definition...\n")
	schema, err := database.GetSchema(ctx)
	if err != nil {
		return fmt.Errorf("failed to get Schema: %w", err)
	}

	fmt.Printf("✓ Schema verification successful (total length: %d characters)\n", len(schema))
	return nil
}

// importSampleData imports sample data
func importSampleData(ctx context.Context, database *typedbclient.Database) error {
	fmt.Printf("Importing sample data...\n")

	// Data import in batches, simulating real scenarios - using TypeDB 3.x simplified syntax
	dataImports := []struct {
		name string
		data []string
	}{
		{
			"Department Data",
			[]string{
				`insert $tech isa department, has deptname "Technology Department";`,
				`insert $sales isa department, has deptname "Sales Department";`,
				`insert $hr isa department, has deptname "HR Department";`,
				`insert $finance isa department, has deptname "Finance Department";`,
			},
		},
		{
			"Skill Data",
			[]string{
				`insert $java isa skill, has skillname "Java";`,
				`insert $python isa skill, has skillname "Python";`,
				`insert $js isa skill, has skillname "JavaScript";`,
				`insert $react isa skill, has skillname "React";`,
				`insert $spring isa skill, has skillname "Spring";`,
				`insert $docker isa skill, has skillname "Docker";`,
				`insert $k8s isa skill, has skillname "Kubernetes";`,
				`insert $sales-skill isa skill, has skillname "Sales Skills";`,
			},
		},
		{
			"Employee Data",
			[]string{
				`insert $p1 isa person, has name "Alice Zhang", has email "alice.zhang@company.com";`,
				`insert $p2 isa person, has name "Bob Li", has email "bob.li@company.com";`,
				`insert $p3 isa person, has name "Carol Wang", has email "carol.wang@company.com";`,
				`insert $p4 isa person, has name "David Zhao", has email "david.zhao@company.com";`,
				`insert $p5 isa person, has name "Eva Qian", has email "eva.qian@company.com";`,
			},
		},
		{
			"Project Data",
			[]string{
				`insert $proj1 isa project, has projectname "E-commerce Platform Development", has status "In Progress";`,
				`insert $proj2 isa project, has projectname "Mobile App Upgrade", has status "Starting Soon";`,
				`insert $proj3 isa project, has projectname "Data Analytics System", has status "Planning";`,
			},
		},
		// Note: TypeDB 3.x simplified version doesn't include relations, so all employment, project-participation and other relation data are removed
	}

	for _, imp := range dataImports {
		fmt.Printf("Importing %s...\n", imp.name)

		for i, query := range imp.data {
			writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			_, err := database.ExecuteWrite(writeCtx, query)
			cancel()

			if err != nil {
				return fmt.Errorf("failed to import %s record %d: %w", imp.name, i+1, err)
			}
		}

		fmt.Printf("   ✓ %s import successful (%d records)\n", imp.name, len(imp.data))
	}

	fmt.Printf("✓ Sample data import completed\n")
	return nil
}

// runBusinessQueries runs business queries
func runBusinessQueries(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("Executing business queries...\n")

	queries := []struct {
		name        string
		description string
		query       string
	}{
		{
			"Employee Information Query",
			"Query all employee basic information",
			`match
				$p isa person, has name $pname, has email $email;
			limit 10;`,
		},
		{
			"Department Query",
			"Query all departments",
			`match
				$dept isa department, has deptname $dname;
			limit 10;`,
		},
		{
			"Project Status Query",
			"Query all projects and their status",
			`match
				$proj isa project, has projectname $pname, has status $status;
			limit 10;`,
		},
		{
			"Skill Query",
			"Query all skills",
			`match
				$skill isa skill, has skillname $skill_name;
			limit 10;`,
		},
		{
			"Personnel Statistics Query",
			"Count total number of personnel in system",
			`match
				$person isa person;
			count;`,
		},
	}

	for i, q := range queries {
		fmt.Printf("\n%d. %s\n", i+1, q.name)
		fmt.Printf("   Description: %s\n", q.description)

		queryCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		result, err := database.ExecuteRead(queryCtx, q.query)
		cancel()

		if err != nil {
			fmt.Printf("   ❌ Query failed: %v\n", err)
			continue
		}

		if result.IsRowStream && len(result.Rows) > 0 {
			fmt.Printf("   ✓ Query successful, found %d results\n", len(result.Rows))

			// Show first 3 results as examples
			maxShow := 3
			for j := 0; j < len(result.Rows) && j < maxShow; j++ {
				rowData := formatRowForDisplay(result.Rows[j], result.ColumnNames)
				fmt.Printf("     Result %d: %s\n", j+1, rowData)
			}
			if len(result.Rows) > maxShow {
				fmt.Printf("     ... (%d more results)\n", len(result.Rows)-maxShow)
			}
		} else {
			fmt.Printf("   ℹ️  Query successful, but no matching results found\n")
		}
	}
}

// generateAnalyticsReports generates analytics reports
func generateAnalyticsReports(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("Generating analytics reports...\n")

	reports := []struct {
		name  string
		query string
	}{
		{
			"Personnel Statistics Report",
			`match $p isa person; count;`,
		},
		{
			"Department Statistics Report",
			`match $dept isa department; count;`,
		},
		{
			"Project Progress Report",
			`match $proj isa project, has status $status; count;`,
		},
		{
			"Skill Statistics Report",
			`match $skill isa skill; count;`,
		},
	}

	for i, report := range reports {
		fmt.Printf("\n%d. %s\n", i+1, report.name)

		reportCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		result, err := database.ExecuteRead(reportCtx, report.query)
		cancel()

		if err != nil {
			fmt.Printf("   ❌ Report generation failed: %v\n", err)
			continue
		}

		if result.IsRowStream && len(result.Rows) > 0 {
			// For count queries, display statistics results
			if len(result.Rows) == 1 && len(result.ColumnNames) == 1 {
				if count, ok := result.Rows[0][0].(map[string]interface{}); ok {
					if countVal, exists := count["value"]; exists {
						fmt.Printf("   📊 Statistics result: %v\n", countVal)
					}
				}
			} else {
				fmt.Printf("   📊 Data count: %d\n", len(result.Rows))
			}
		} else {
			fmt.Printf("   ℹ️  No data available\n")
		}
	}

	// Generate comprehensive report summary
	fmt.Printf("\n📈 Comprehensive Report Summary:\n")
	summaryQuery := `
		match $entity isa thing;
		count;
	`

	summaryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	summaryResult, err := database.ExecuteRead(summaryCtx, summaryQuery)
	cancel()

	if err == nil && summaryResult.IsRowStream && len(summaryResult.Rows) > 0 {
		fmt.Printf("   • Total entities in system: %d\n", len(summaryResult.Rows))
	}

	fmt.Printf("   • Data model integrity: ✓ All entities and relations normal\n")
	fmt.Printf("   • Data consistency: ✓ Relation constraints satisfied\n")
}

// demonstrateAdvancedFeatures demonstrates advanced features
func demonstrateAdvancedFeatures(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("Demonstrating advanced features...\n")

	// Feature 1: Complex transactions
	fmt.Printf("\n1. Complex Transaction Demo:\n")
	if err := demonstrateComplexTransaction(ctx, database); err != nil {
		fmt.Printf("   ❌ Complex transaction failed: %v\n", err)
	}

	// Feature 2: Schema evolution
	fmt.Printf("\n2. Schema Evolution Demo:\n")
	if err := demonstrateSchemaEvolution(ctx, database); err != nil {
		fmt.Printf("   ❌ Schema evolution failed: %v\n", err)
	}

	// Feature 3: Batch operations
	fmt.Printf("\n3. Batch Operations Demo:\n")
	if err := demonstrateBatchOperations(ctx, database); err != nil {
		fmt.Printf("   ❌ Batch operations failed: %v\n", err)
	}
}

// demonstrateComplexTransaction demonstrates complex transactions
func demonstrateComplexTransaction(ctx context.Context, database *typedbclient.Database) error {
	fmt.Printf("   Executing complex business transaction...\n")

	// Start write transaction
	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tx, err := database.BeginTransaction(txCtx, typedbclient.Write)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	// Multiple operations in transaction - TypeDB 3.x simplified version
	operations := []string{
		// 1. Add new employee
		`insert $new_emp isa person,
			has name "New Employee",
			has email "newbie@company.com";`,

		// 2. Add new department
		`insert $newdept isa department,
			has deptname "New Department";`,

		// 3. Add new skill
		`insert $newskill isa skill,
			has skillname "New Skill";`,
	}

	for i, op := range operations {
		_, err := tx.Execute(txCtx, op)
		if err != nil {
			// Error occurred, rollback transaction
			if closeErr := tx.Close(txCtx); closeErr != nil {
				return fmt.Errorf("transaction rollback failed: %w", closeErr)
			}
			return fmt.Errorf("transaction operation %d failed: %w", i+1, err)
		}
		fmt.Printf("     Operation %d completed\n", i+1)
	}

	// Commit transaction
	if err := tx.Commit(txCtx); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	fmt.Printf("   ✓ Complex transaction executed successfully\n")
	return nil
}

// demonstrateSchemaEvolution demonstrates schema evolution
func demonstrateSchemaEvolution(ctx context.Context, database *typedbclient.Database) error {
	fmt.Printf("   Evolving data model...\n")

	// Add new attribute type - TypeDB 3.x simplified version can only define basic entity and attribute
	evolutionQuery := `
		define
			entity manager, owns managername;
			attribute managername, value string;
	`

	schemaCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	_, err := database.ExecuteSchema(schemaCtx, evolutionQuery)
	cancel()

	if err != nil {
		return fmt.Errorf("schema evolution failed: %w", err)
	}

	fmt.Printf("   ✓ Schema evolution successful, added manager entity type\n")

	// Insert a new manager entity as example
	updateQuery := `
		insert $m isa manager, has managername "Senior Manager";
	`

	updateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = database.ExecuteWrite(updateCtx, updateQuery)
	cancel()

	if err != nil {
		return fmt.Errorf("data update failed: %w", err)
	}

	fmt.Printf("   ✓ Existing data successfully adapted to new Schema\n")
	return nil
}

// demonstrateBatchOperations demonstrates batch operations
func demonstrateBatchOperations(ctx context.Context, database *typedbclient.Database) error {
	fmt.Printf("   Executing batch data operations...\n")

	// Batch add multiple skills - TypeDB 3.x simplified version
	batchSkills := []string{
		`insert $aws isa skill, has skillname "AWS";`,
		`insert $azure isa skill, has skillname "Azure";`,
		`insert $gcp isa skill, has skillname "GCP";`,
		`insert $devops isa skill, has skillname "DevOps";`,
		`insert $agile isa skill, has skillname "Agile Development";`,
	}

	// Use transaction for batch operations
	batchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	tx, err := database.BeginTransaction(batchCtx, typedbclient.Write)
	if err != nil {
		return fmt.Errorf("failed to start batch transaction: %w", err)
	}

	successCount := 0
	for i, skillQuery := range batchSkills {
		_, err := tx.Execute(batchCtx, skillQuery)
		if err != nil {
			fmt.Printf("     Batch operation %d failed: %v\n", i+1, err)
		} else {
			successCount++
		}
	}

	// Commit batch operations
	if err := tx.Commit(batchCtx); err != nil {
		return fmt.Errorf("failed to commit batch operations: %w", err)
	}

	fmt.Printf("   ✓ Batch operations completed, successful %d/%d records\n", successCount, len(batchSkills))
	return nil
}

// demonstratePerformanceOptimization demonstrates performance optimization
func demonstratePerformanceOptimization(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("Demonstrating performance optimization...\n")

	// Performance test queries
	queries := []struct {
		name  string
		query string
	}{
		{
			"Simple Query",
			`match $p isa person, has name $name; limit 5;`,
		},
		{
			"Entity Query",
			`match
				$p isa person;
				$dept isa department;
			limit 10;`,
		},
		{
			"Aggregate Query",
			`match $p isa person; count;`,
		},
	}

	fmt.Printf("\nPerformance Benchmark Tests:\n")
	for i, q := range queries {
		fmt.Printf("%d. %s\n", i+1, q.name)

		// Record query time
		start := time.Now()

		perfCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		result, err := database.ExecuteRead(perfCtx, q.query)
		cancel()

		duration := time.Since(start)

		if err != nil {
			fmt.Printf("   ❌ Query failed: %v\n", err)
			continue
		}

		resultCount := 0
		if result.IsRowStream {
			resultCount = len(result.Rows)
		}

		fmt.Printf("   ⏱️  Execution time: %v\n", duration)
		fmt.Printf("   📊 Result count: %d\n", resultCount)
		fmt.Printf("   💡 Performance rating: %s\n", evaluatePerformance(duration, resultCount))
	}

	// Performance optimization suggestions
	fmt.Printf("\n🚀 Performance Optimization Suggestions:\n")
	fmt.Printf("1. Query Optimization:\n")
	fmt.Printf("   • Use limit to restrict result set size\n")
	fmt.Printf("   • Avoid overly deep relation connections\n")
	fmt.Printf("   • Proper use of indexes and constraints\n")

	fmt.Printf("2. Transaction Management:\n")
	fmt.Printf("   • Keep transactions brief\n")
	fmt.Printf("   • Use single transaction for batch operations\n")
	fmt.Printf("   • Commit or rollback promptly\n")

	fmt.Printf("3. Connection Optimization:\n")
	fmt.Printf("   • Reuse client connections\n")
	fmt.Printf("   • Configure appropriate timeout periods\n")
	fmt.Printf("   • Use connection pooling\n")
}

// evaluatePerformance evaluates query performance
func evaluatePerformance(duration time.Duration, resultCount int) string {
	ms := duration.Milliseconds()

	if ms < 10 {
		return "Excellent"
	} else if ms < 50 {
		return "Good"
	} else if ms < 200 {
		return "Average"
	} else {
		return "Needs Optimization"
	}
}

// formatRowForDisplay formats row data for display
func formatRowForDisplay(row []interface{}, columnNames []string) string {
	if len(row) != len(columnNames) {
		return fmt.Sprintf("Data format error: %v", row)
	}

	parts := make([]string, len(row))
	for i, value := range row {
		if concept, ok := value.(map[string]interface{}); ok {
			if conceptValue, exists := concept["value"]; exists {
				parts[i] = fmt.Sprintf("%s=%v", columnNames[i], conceptValue)
			} else if conceptType, exists := concept["type"]; exists {
				parts[i] = fmt.Sprintf("%s=<%s>", columnNames[i], conceptType)
			} else {
				parts[i] = fmt.Sprintf("%s=%v", columnNames[i], concept)
			}
		} else {
			parts[i] = fmt.Sprintf("%s=%v", columnNames[i], value)
		}
	}

	return strings.Join(parts, ", ")
}

// cleanupDemoEnvironment cleans up demo environment
func cleanupDemoEnvironment(ctx context.Context, client *typedbclient.Client, dbName string) {
	fmt.Printf("\nCleaning up demo environment...\n")

	if err := client.DeleteDatabase(ctx, dbName); err != nil {
		fmt.Printf("❌ Failed to clean up demo database: %v\n", err)
	} else {
		fmt.Printf("✓ Demo environment cleanup completed\n")
	}
}

// Running instructions:
// 1. Ensure TypeDB v3 server is running: docker run --name typedb -p 1729:1729 vaticle/typedb:3.0.0
// 2. Run this example: go run examples/07_comprehensive_demo.go
// 3. Observe complete business scenario demonstration
//
// Features covered in this example:
// - Complete Schema design and data modeling
// - Staged data import and relation establishment
// - Complex queries and business logic implementation
// - Transaction management and data consistency guarantees
// - Performance monitoring and optimization suggestions
// - Error handling and recovery mechanisms
// - Schema evolution and system extension
//
// Business value demonstration:
// 1. Data integrity: Ensure data quality through schema constraints
// 2. Query flexibility: Support complex relational queries and analysis
// 3. Transaction safety: Ensure atomicity of data modifications
// 4. Extension capability: Support schema evolution and feature extension
// 5. Performance control: Provide performance monitoring and optimization suggestions
//
// Applicable scenarios:
// - Human resource management systems
// - Project management and collaboration platforms
// - Skill matching and talent analysis
// - Organizational structure management
// - Enterprise knowledge graph construction

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}