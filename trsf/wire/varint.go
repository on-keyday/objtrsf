package wire

func EncodeVarint(value uint64) (r Varint, ok bool) {
	prefix := VarintPrefix(value)
	if prefix == 0xff {
		return Varint{}, false
	}
	r.SetPrefix(prefix)
	r.SetValue(value)
	return r, true
}
