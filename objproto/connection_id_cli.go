package objproto

import "strings"

// ConnectionID CLI helpers. Consumers identify a ConnectionID through the same
// flow used for command-line args / output; MarshalText/UnmarshalText do the
// heavy lifting. Kept as methods (not free functions) so ConnectionID satisfies
// the consumer-side arg/output interfaces structurally.

func (v *ConnectionID) AppendString(sb *strings.Builder) {
	sb.WriteString(v.String())
}

func (v *ConnectionID) ToCLIOutput() string {
	return v.String()
}

func (v *ConnectionID) ToArg() string {
	return v.String()
}

func (v *ConnectionID) FromArg(arg string) error {
	return v.UnmarshalText([]byte(arg))
}
