package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ByteSize is a size in bytes that reads and prints with a unit suffix, for
// configuration fields such as a rotation threshold.
//
// Accepted spellings are a bare integer (bytes), an integer followed by B,
// and an integer followed by one of KiB, MiB, GiB, TiB (powers of 1024), KB,
// MB, GB, TB (powers of 1000), or the bare letters K, M, G, T (powers of
// 1024, as most command line tools read them). Units are case-insensitive
// and may be separated from the number by spaces. Negative values are
// rejected.
type ByteSize int64

// Binary units of ByteSize.
const (
	KiB ByteSize = 1 << (10 * (iota + 1))
	MiB
	GiB
	TiB
)

var byteUnits = map[string]ByteSize{
	"":    1,
	"b":   1,
	"k":   KiB,
	"kib": KiB,
	"kb":  1000,
	"m":   MiB,
	"mib": MiB,
	"mb":  1000 * 1000,
	"g":   GiB,
	"gib": GiB,
	"gb":  1000 * 1000 * 1000,
	"t":   TiB,
	"tib": TiB,
	"tb":  1000 * 1000 * 1000 * 1000,
}

// ParseByteSize parses the spellings described on ByteSize.
func ParseByteSize(s string) (ByteSize, error) {
	text := strings.TrimSpace(s)
	if text == "" {
		return 0, errors.New("empty size")
	}
	i := 0
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
	}
	digits, unit := text[:i], strings.ToLower(strings.TrimSpace(text[i:]))
	if digits == "" {
		return 0, fmt.Errorf("invalid size %q: must start with a number", s)
	}
	mult, ok := byteUnits[unit]
	if !ok {
		return 0, fmt.Errorf("invalid size %q: unknown unit %q (use B, KiB, MiB, GiB, TiB, KB, MB, GB, TB)", s, text[i:])
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}
	if n > int64(^uint64(0)>>1)/int64(mult) {
		return 0, fmt.Errorf("invalid size %q: overflows", s)
	}
	return ByteSize(n) * mult, nil
}

// String renders the size with the largest binary unit that divides it
// exactly, or in bytes.
func (b ByteSize) String() string {
	if b < 0 {
		return strconv.FormatInt(int64(b), 10) + "B"
	}
	for _, u := range []struct {
		unit ByteSize
		name string
	}{{TiB, "TiB"}, {GiB, "GiB"}, {MiB, "MiB"}, {KiB, "KiB"}} {
		if b >= u.unit && b%u.unit == 0 {
			return strconv.FormatInt(int64(b/u.unit), 10) + u.name
		}
	}
	return strconv.FormatInt(int64(b), 10) + "B"
}

// MarshalText renders String, so the size prints with its unit in YAML and JSON.
func (b ByteSize) MarshalText() ([]byte, error) { return []byte(b.String()), nil }

// UnmarshalText parses the spellings described on ByteSize, so a YAML or
// JSON scalar decodes into the field.
func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := ParseByteSize(string(text))
	if err != nil {
		return err
	}
	if v < 0 {
		return fmt.Errorf("invalid size %q: negative", string(text))
	}
	*b = v
	return nil
}
