package fit_test

import (
	"fmt"
	"strconv"
	"strings"

	fit "github.com/nyobe/pipe-fittings"
)

// Example_basic_transformation demonstrates basic data transformation.
func Example_basic_transformation() {
	input := []string{"10", "13", "6", "27"}

	pipeline := fit.Pipe3(
		fit.MapTry(strconv.Atoi),
		fit.FilterPure(func(i int) bool { return i%2 == 0 }),
		fit.Reduce(func(acc, val int) int { return acc + val }),
	)

	result, err := fit.Do1(input, pipeline)
	fmt.Printf("Sum of even numbers: %d, error: %v\n", result, err)
	// Output: Sum of even numbers: 16, error: <nil>
}

// Example_log_processing demonstrates processing log entries.
func Example_log_processing() {
	logs := []string{
		"2024-01-01 ERROR Failed to connect",
		"2024-01-01 INFO Connected successfully",
		"2024-01-01 ERROR Database timeout",
		"2024-01-01 DEBUG Query executed",
	}

	pipeline := fit.Pipe4(
		fit.FilterPure(func(line string) bool { return strings.Contains(line, "ERROR") }),
		fit.MapPure(func(line string) string { return strings.Split(line, " ")[2:][0] }),
		fit.Distinct[string](),
		fit.Reduce(func(acc []string, val string) []string { return append(acc, val) }),
	)

	errors, err := fit.Do1(logs, pipeline)
	fmt.Printf("Unique error types: %v, error: %v\n", errors, err)
	// Output: Unique error types: [Failed Database], error: <nil>
}

// Example_data_cleaning demonstrates cleaning and transforming data.
func Example_data_cleaning() {
	rawData := []string{"", "123", "invalid", "456", "", "789"}

	pipeline := fit.Pipe4(
		fit.Compact[string](),                                 // Remove empty strings
		fit.MapTry(strconv.Atoi),                              // Convert to integers
		fit.Filter(fit.IgnoreErrorIs[int](strconv.ErrSyntax)), // Ignore conversion errors
		fit.Chunk[int](2),                                     // Group into pairs
	)

	chunks, err := fit.Do(rawData, pipeline)
	fmt.Printf("Cleaned chunks: %v, error: %v\n", chunks, err)
	// Output: Cleaned chunks: [[123 456] [789]], error: <nil>
}

// Example_stream_processing demonstrates processing larger datasets efficiently.
func Example_stream_processing() {
	// Simulate processing a large dataset
	numbers := make([]int, 1000)
	for i := range numbers {
		numbers[i] = i + 1
	}

	pipeline := fit.Pipe5(
		fit.FilterPure(func(n int) bool { return n%2 == 0 }), // Even numbers only
		fit.MapPure(func(n int) int { return n * n }),        // Square them
		fit.Take[int](5), // Take first 5
		fit.Tap(func(n int, err error) { fmt.Printf("Processing: %d\n", n) }),
		fit.Reduce(func(acc, val int) int { return acc + val }), // Sum them
	)

	result, err := fit.Do1(numbers, pipeline)
	fmt.Printf("Sum of first 5 squared even numbers: %d, error: %v\n", result, err)
	// Output:
}
