package budget

// ChargeSlice bills len/cap(p) against b. A nil Budget is a no-op success.
func ChargeSlice(b Budget, p []byte) (release func(), err error) {
	if b == nil {
		return func() {}, nil
	}
	if sb, ok := b.(SliceBudget); ok {
		return sb.TryAcquireSlice(p)
	}
	return b.TryAcquire(int64(cap(p)))
}
