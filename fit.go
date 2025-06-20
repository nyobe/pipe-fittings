// Package fit provides composable stream processing pipelines for Go.
// It allows you to build data transformation pipelines using a functional approach
// while maintaining Go's explicit error handling patterns.
package fit

import (
	"errors"
	"iter"
	"slices"
)

// StepFn represents a stream processing step that can transform values and handle errors.
type StepFn[In any, Out any] func(in In, err error) (Out, error)

// Errstream represents a stream that carries values along with potential errors.
type Errstream[E any] = iter.Seq2[E, error]

// Pipeline decouples transformation logic from iteration mechanics, enabling clean composition without nesting.
type Pipeline[In any, Out any] func(in Errstream[In]) Errstream[Out]

// == Composition ==

// Pipe2 chains 2 pipelines together into a single pipeline.
func Pipe2[A, B, C any](t1 Pipeline[A, B], t2 Pipeline[B, C]) Pipeline[A, C] {
	return func(in iter.Seq2[A, error]) iter.Seq2[C, error] {
		return t2(t1(in))
	}
}

// Pipe3 chains 3 pipelines together into a single pipeline.
func Pipe3[A, B, C, D any](t1 Pipeline[A, B], t2 Pipeline[B, C], t3 Pipeline[C, D]) Pipeline[A, D] {
	return func(in iter.Seq2[A, error]) iter.Seq2[D, error] {
		return t3(t2(t1(in)))
	}
}

// Pipe4 chains 4 pipelines together into a single pipeline.
func Pipe4[A, B, C, D, E any](t1 Pipeline[A, B], t2 Pipeline[B, C], t3 Pipeline[C, D], t4 Pipeline[D, E]) Pipeline[A, E] {
	return func(in iter.Seq2[A, error]) iter.Seq2[E, error] {
		return t4(t3(t2(t1(in))))
	}
}

// Pipe5 chains 5 pipelines together into a single pipeline.
func Pipe5[A, B, C, D, E, F any](t1 Pipeline[A, B], t2 Pipeline[B, C], t3 Pipeline[C, D], t4 Pipeline[D, E], t5 Pipeline[E, F]) Pipeline[A, F] {
	return func(in iter.Seq2[A, error]) iter.Seq2[F, error] {
		return t5(t4(t3(t2(t1(in)))))
	}
}

// Pipe6 chains 6 pipelines together into a single pipeline.
func Pipe6[A, B, C, D, E, F, G any](t1 Pipeline[A, B], t2 Pipeline[B, C], t3 Pipeline[C, D], t4 Pipeline[D, E], t5 Pipeline[E, F], t6 Pipeline[F, G]) Pipeline[A, G] {
	return func(in iter.Seq2[A, error]) iter.Seq2[G, error] {
		return t6(t5(t4(t3(t2(t1(in))))))
	}
}

// == Adapters ==

// OK transforms a regular iterator into an errstream iterator.
func OK[T any](it iter.Seq[T]) Errstream[T] {
	return func(yield func(T, error) bool) {
		for x := range it {
			if !yield(x, nil) {
				return
			}
		}
	}
}

// Pair represents a key-value pair.
type Pair[K, V any] struct {
	K K
	V V
}

// Pairs transforms a key-value iterator into an errstream of pairs.
func Pairs[K, V any](it iter.Seq2[K, V]) Errstream[Pair[K, V]] {
	return func(yield func(Pair[K, V], error) bool) {
		for k, v := range it {
			if !yield(Pair[K, V]{k, v}, nil) {
				return
			}
		}
	}
}

// Collect consumes an errstream into a slice, failing fast on any error.
func Collect[T any](in Errstream[T]) ([]T, error) {
	return Into(nil, in)
}

// Into appends all values from an errstream to an existing slice.
// It fails fast if any element has an error.
func Into[T any](slice []T, in Errstream[T]) ([]T, error) {
	for val, err := range in {
		if err != nil {
			return nil, err
		}
		slice = append(slice, val)
	}
	return slice, nil
}

// Do is a convenience function that applies a pipeline to a slice and returns a slice.
func Do[In any, Out any](in []In, pipeline Pipeline[In, Out]) ([]Out, error) {
	return Collect(pipeline(OK(slices.Values(in))))
}

// First returns the first value from an errstream.
func First[T any](in Errstream[T]) (T, error) {
	var zero T
	for val, err := range in {
		if err != nil {
			return zero, err
		}
		return val, nil
	}
	return zero, nil
}

// Last returns the last value from an errstream.
func Last[T any](in Errstream[T]) (T, error) {
	var last T
	for val, err := range in {
		if err != nil {
			var zero T
			return zero, err
		}
		last = val
	}
	return last, nil
}

// Do1 applies a pipeline to a slice and returns the first result.
// Useful for pipelines that reduce to a single value.
func Do1[In any, Out any](in []In, pipeline Pipeline[In, Out]) (Out, error) {
	return First(pipeline(OK(slices.Values(in))))
}

// === Core transforms ===

// liftTry wraps a function that returns (T, error) into a StepFn.
func liftTry[In any, Out any](fn func(In) (Out, error)) StepFn[In, Out] {
	return func(in In, err error) (Out, error) {
		if err != nil {
			var zero Out
			return zero, err
		}
		return fn(in)
	}
}

// liftPure wraps a pure function into a StepFn.
func liftPure[In any, Out any](fn func(In) Out) StepFn[In, Out] {
	return func(in In, err error) (Out, error) {
		if err != nil {
			var zero Out
			return zero, err
		}
		return fn(in), nil
	}
}

// Map transforms each element using a StepFn.
func Map[In any, Out any](transform StepFn[In, Out]) Pipeline[In, Out] {
	return func(in Errstream[In]) Errstream[Out] {
		return func(yield func(Out, error) bool) {
			for val, err := range in {
				if !yield(transform(val, err)) {
					return
				}
			}
		}
	}
}

// MapTry transforms each element using a function that can fail.
func MapTry[In any, Out any](transform func(In) (Out, error)) Pipeline[In, Out] {
	return Map(liftTry(transform))
}

// MapPure transforms each element using a pure function.
func MapPure[In any, Out any](transform func(In) Out) Pipeline[In, Out] {
	return Map(liftPure(transform))
}

// Filter keeps elements for which the predicate returns true.
func Filter[In any](pred StepFn[In, bool]) Pipeline[In, In] {
	return func(in iter.Seq2[In, error]) iter.Seq2[In, error] {
		return func(yield func(In, error) bool) {
			for val, err := range in {
				if ok, err := pred(val, err); ok || err != nil {
					if !yield(val, err) {
						return
					}
				}
			}
		}
	}
}

// FilterTry filters using a predicate that can fail.
func FilterTry[In any](pred func(In) (bool, error)) Pipeline[In, In] {
	return Filter(liftTry(pred))
}

// FilterPure filters using a pure predicate.
func FilterPure[In any](pred func(In) bool) Pipeline[In, In] {
	return Filter(liftPure(pred))
}

// === Additional transforms ===

// values returns an iterator over just the values, propagating errors automatically.
func values[T any, X any](in Errstream[T], yield func(X, error) bool) iter.Seq[T] {
	return func(yield_ func(T) bool) {
		for val, err := range in {
			if err != nil {
				var zero X
				if !yield(zero, err) {
					return
				}
			} else {
				yield_(val)
			}
		}
	}
}

// Take emits at most n elements from the stream.
func Take[T any](n int) Pipeline[T, T] {
	return func(in Errstream[T]) Errstream[T] {
		return func(yield func(T, error) bool) {
			count := 0
			for val := range values(in, yield) {
				count++
				if count > n {
					return
				}
				if !yield(val, nil) {
					return
				}
			}
		}
	}
}

// TakeWhile emits elements while the predicate returns true.
func TakeWhile[T any](pred StepFn[T, bool]) Pipeline[T, T] {
	return func(in Errstream[T]) Errstream[T] {
		return func(yield func(T, error) bool) {
			for val, err := range in {
				if keep, err := pred(val, err); err != nil {
					var zero T
					if !yield(zero, err) {
						return
					}
				} else if !keep {
					return // stop taking
				}
				if !yield(val, err) {
					return
				}
			}
		}
	}
}

// Drop skips the first n elements of the stream.
func Drop[T any](n int) Pipeline[T, T] {
	return func(in Errstream[T]) Errstream[T] {
		return func(yield func(T, error) bool) {
			dropped := 0
			for val, err := range in {
				if dropped < n {
					dropped++
					continue
				}
				if !yield(val, err) {
					return
				}
			}
		}
	}
}

// DropWhile skips elements while the predicate returns true.
func DropWhile[T any](pred StepFn[T, bool]) Pipeline[T, T] {
	return func(in Errstream[T]) Errstream[T] {
		return func(yield func(T, error) bool) {
			take := false
			for val, err := range in {
				if take {
					if !yield(val, err) {
						return
					}
					continue
				}
				if drop, err := pred(val, err); err != nil {
					var zero T
					if !yield(zero, err) {
						return
					}
				} else if !drop {
					take = true
					if !yield(val, err) {
						return
					}
				}
			}
		}
	}
}

// Chunk groups elements into slices of size n.
func Chunk[T any](n int) Pipeline[T, []T] {
	return func(in Errstream[T]) Errstream[[]T] {
		return func(yield func([]T, error) bool) {
			chunk := make([]T, 0, n)
			for val := range values(in, yield) {
				chunk = append(chunk, val)
				if len(chunk) < n {
					continue
				}
				if !yield(chunk, nil) {
					return
				}
				chunk = make([]T, 0, n)
			}
			// Yield final incomplete chunk if it exists
			if len(chunk) > 0 {
				yield(chunk, nil)
			}
		}
	}
}

// Distinct removes duplicate elements from the stream.
func Distinct[T comparable]() Pipeline[T, T] {
	return func(in Errstream[T]) Errstream[T] {
		return func(yield func(T, error) bool) {
			seen := map[T]struct{}{}
			for val := range values(in, yield) {
				if _, present := seen[val]; !present {
					seen[val] = struct{}{}
					if !yield(val, nil) {
						return
					}
				}
			}
		}
	}
}

// Compact removes zero values from the stream.
func Compact[T comparable]() Pipeline[T, T] {
	return func(in Errstream[T]) Errstream[T] {
		return func(yield func(T, error) bool) {
			var zero T
			for val := range values(in, yield) {
				if val != zero {
					if !yield(val, nil) {
						return
					}
				}
			}
		}
	}
}

// Flatten concatenates slices in the stream into individual elements.
func Flatten[T any]() Pipeline[[]T, T] {
	return func(in Errstream[[]T]) Errstream[T] {
		return func(yield func(T, error) bool) {
			for slice := range values(in, yield) {
				for _, val := range slice {
					if !yield(val, nil) {
						return
					}
				}
			}
		}
	}
}

// Enumerate adds indices to each element in the stream.
func Enumerate[T any]() Pipeline[T, Pair[int, T]] {
	return func(in Errstream[T]) Errstream[Pair[int, T]] {
		return func(yield func(Pair[int, T], error) bool) {
			count := 0
			for val, err := range in {
				if !yield(Pair[int, T]{count, val}, err) {
					return
				}
				count++
			}
		}
	}
}

// Tap performs a side effect on each element without modifying the stream.
func Tap[T any](tap func(T, error)) Pipeline[T, T] {
	return func(in Errstream[T]) Errstream[T] {
		return func(yield func(T, error) bool) {
			for val, err := range in {
				tap(val, err)
				if !yield(val, err) {
					return
				}
			}
		}
	}
}

// Reduce accumulates elements into a single value using a reduction function.
func Reduce[T, A any](reduce func(acc A, val T) A) Pipeline[T, A] {
	return func(in Errstream[T]) Errstream[A] {
		return func(yield func(A, error) bool) {
			var acc A
			for val := range values(in, yield) {
				acc = reduce(acc, val)
			}
			yield(acc, nil)
		}
	}
}

// GroupBy groups elements by a key function into a map.
func GroupBy[T any, K comparable](keyBy func(T) K) Pipeline[T, map[K][]T] {
	return func(in Errstream[T]) Errstream[map[K][]T] {
		return func(yield func(map[K][]T, error) bool) {
			grouped := map[K][]T{}
			for val := range values(in, yield) {
				key := keyBy(val)
				grouped[key] = append(grouped[key], val)
			}
			yield(grouped, nil)
		}
	}
}

// === Error handling helpers ===

// IgnoreErrorIs returns a predicate that ignores errors matching the sentinel.
func IgnoreErrorIs[T any](sentinel error) StepFn[T, bool] {
	return func(_ T, err error) (bool, error) {
		matches := errors.Is(err, sentinel)
		return !matches, nil
	}
}

// IgnoreErrorAs returns a predicate that ignores errors of a specific type.
func IgnoreErrorAs[T any, Err error]() StepFn[T, bool] {
	return func(_ T, err error) (bool, error) {
		var exemplar Err
		matches := errors.As(err, &exemplar)
		return !matches, nil
	}
}
