package report

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type cell func(float64) string

func grouped(x float64) string {
	digits := strconv.FormatInt(int64(math.Round(x)), 10)
	sign := ""
	if digits[0] == '-' {
		sign, digits = "-", digits[1:]
	}
	var b strings.Builder
	for i, c := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return sign + b.String()
}

func millis(horizon time.Duration) cell {
	return func(ns float64) string {
		if math.IsInf(ns, 1) {
			if horizon > 0 {
				return "over " + plainMillis(float64(horizon))
			}
			return "unfinished"
		}
		return plainMillis(ns)
	}
}

func plainMillis(ns float64) string {
	v := ns / 1e6
	switch {
	case v < 1:
		return fmt.Sprintf("%.3f", v)
	case v < 10:
		return fmt.Sprintf("%.2f", v)
	case v < 100:
		return fmt.Sprintf("%.1f", v)
	}
	return grouped(v)
}

func fixed(digits int) cell {
	return func(x float64) string {
		if math.IsInf(x, 1) {
			return "unfinished"
		}
		return strconv.FormatFloat(x, 'f', digits, 64)
	}
}

func percent(x float64) string {
	return strconv.FormatFloat(100*x, 'f', 1, 64)
}

func (s spread) show(c cell) string {
	switch s.n {
	case 0:
		return "n/a"
	case 1:
		return c(s.med)
	}
	return c(s.med) + " (" + c(s.lo) + "–" + c(s.hi) + ")"
}

func ratio(b, a float64) string {
	if a == 0 || math.IsInf(a, 0) || math.IsInf(b, 0) {
		return "n/a"
	}
	return strconv.FormatFloat(b/a, 'f', 2, 64)
}

func escape(s string) string {
	return strings.ReplaceAll(s, "|", `\|`)
}

type table struct {
	head []string
	rows [][]string
}

func (t *table) add(cells ...string) {
	t.rows = append(t.rows, cells)
}

func (t *table) write(b *strings.Builder) {
	if len(t.rows) == 0 {
		return
	}
	b.WriteString("| " + strings.Join(t.head, " | ") + " |\n|")
	for range t.head {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	for _, r := range t.rows {
		esc := make([]string, len(r))
		for i, c := range r {
			esc[i] = escape(c)
		}
		b.WriteString("| " + strings.Join(esc, " | ") + " |\n")
	}
	b.WriteString("\n")
}
