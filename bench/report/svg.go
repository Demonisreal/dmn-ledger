package report

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

type point struct {
	x, y float64
	fast bool
}

type line struct {
	label  string
	points []point
}

const (
	width    = 720
	height   = 440
	left     = 72
	right    = 190
	top      = 52
	bottom   = 58
	plotW    = width - left - right
	plotH    = height - top - bottom
	ink      = "#222222"
	gridInk  = "#dddddd"
	labelGap = 14
)

var dashes = []string{"", "7 4", "2 3", "10 3 2 3"}

var xmlText = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

type scale struct {
	log    bool
	lo, hi float64
}

func (s scale) at(v float64) float64 {
	if s.log {
		return (math.Log10(v) - math.Log10(s.lo)) / (math.Log10(s.hi) - math.Log10(s.lo))
	}
	return (v - s.lo) / (s.hi - s.lo)
}

func px(v float64) string {
	return strconv.FormatFloat(v, 'f', 1, 64)
}

func rateLabel(v float64) string {
	if v >= 1000 && math.Mod(v, 1000) == 0 {
		return strconv.FormatFloat(v/1000, 'f', -1, 64) + "k"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func decades(lo, hi float64) (scale, []float64) {
	a, b := math.Floor(math.Log10(lo)), math.Ceil(math.Log10(hi))
	if a == b {
		b++
	}
	var ticks []float64
	for k := a; k <= b; k++ {
		ticks = append(ticks, math.Pow(10, k))
	}
	return scale{log: true, lo: ticks[0], hi: ticks[len(ticks)-1]}, ticks
}

func linear(hi float64) (scale, []float64, int) {
	if hi <= 0 {
		hi = 1
	}
	rough := hi / 5
	mag := math.Pow(10, math.Floor(math.Log10(rough)))
	step := 10 * mag
	for _, m := range []float64{1, 2, 5} {
		if m*mag >= rough {
			step = m * mag
			break
		}
	}
	n := math.Ceil(hi / step)
	var ticks []float64
	for i := 0.0; i <= n; i++ {
		ticks = append(ticks, i*step)
	}
	return scale{lo: 0, hi: n * step}, ticks, max(0, int(-math.Floor(math.Log10(step))))
}

func plot(title, xLabel, yLabel string, logY bool, lines []line) []byte {
	var xs, ys []float64
	for i := range lines {
		lines[i].points = slices.DeleteFunc(slices.Clone(lines[i].points), func(p point) bool {
			return math.IsInf(p.y, 0) || math.IsNaN(p.y) || logY && p.y <= 0
		})
		for _, p := range lines[i].points {
			xs, ys = append(xs, p.x), append(ys, p.y)
		}
	}
	lines = slices.DeleteFunc(lines, func(l line) bool { return len(l.points) == 0 })
	if len(lines) == 0 {
		return nil
	}
	xlo, xhi := slices.Min(xs), slices.Max(xs)
	if xlo == xhi {
		xlo, xhi = xlo/2, xhi*2
	}
	xs2 := scale{log: true, lo: xlo, hi: xhi}
	var ys2 scale
	var yTicks []float64
	yDigits := -1
	if logY {
		ys2, yTicks = decades(slices.Min(ys), slices.Max(ys))
	} else {
		ys2, yTicks, yDigits = linear(slices.Max(ys) * 1.1)
	}
	xAt := func(v float64) float64 { return left + xs2.at(v)*plotW }
	yAt := func(v float64) float64 { return top + plotH - ys2.at(v)*plotH }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" font-family="system-ui, sans-serif" font-size="12">`+"\n", width, height, width, height)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="#ffffff"/>`+"\n", width, height)
	fmt.Fprintf(&b, `<text x="%d" y="26" font-size="14" fill="%s">%s</text>`+"\n", left, ink, xmlText.Replace(title))
	for _, v := range yTicks {
		y := px(yAt(v))
		fmt.Fprintf(&b, `<line x1="%d" y1="%s" x2="%d" y2="%s" stroke="%s"/>`+"\n", left, y, left+plotW, y, gridInk)
		fmt.Fprintf(&b, `<text x="%d" y="%s" text-anchor="end" fill="%s">%s</text>`+"\n", left-6, px(yAt(v)+4), ink, strconv.FormatFloat(v, 'f', yDigits, 64))
	}
	for v := xlo; v <= xhi*1.0001; v *= 2 {
		x := px(xAt(v))
		fmt.Fprintf(&b, `<line x1="%s" y1="%d" x2="%s" y2="%d" stroke="%s"/>`+"\n", x, top+plotH, x, top+plotH+5, ink)
		fmt.Fprintf(&b, `<text x="%s" y="%d" text-anchor="middle" fill="%s">%s</text>`+"\n", x, top+plotH+19, ink, rateLabel(v))
	}
	fmt.Fprintf(&b, `<path d="M%d %d V%d H%d" fill="none" stroke="%s"/>`+"\n", left, top, top+plotH, left+plotW, ink)
	fmt.Fprintf(&b, `<text x="%d" y="%d" text-anchor="middle" fill="%s">%s</text>`+"\n", left+plotW/2, height-14, ink, xmlText.Replace(xLabel))
	fmt.Fprintf(&b, `<text transform="translate(18 %d) rotate(-90)" text-anchor="middle" fill="%s">%s</text>`+"\n", top+plotH/2, ink, xmlText.Replace(yLabel))

	type end struct {
		y     float64
		x     float64
		label string
	}
	var ends []end
	anyFast := false
	for i, l := range lines {
		var pts []string
		for _, p := range l.points {
			pts = append(pts, px(xAt(p.x))+","+px(yAt(p.y)))
		}
		dash := ""
		if d := dashes[i%len(dashes)]; d != "" {
			dash = fmt.Sprintf(` stroke-dasharray="%s"`, d)
		}
		fmt.Fprintf(&b, `<polyline points="%s" fill="none" stroke="%s" stroke-width="1.5"%s/>`+"\n", strings.Join(pts, " "), ink, dash)
		for _, p := range l.points {
			fill := ink
			if p.fast {
				fill, anyFast = "#ffffff", true
			}
			fmt.Fprintf(&b, `<circle cx="%s" cy="%s" r="3" fill="%s" stroke="%s"/>`+"\n", px(xAt(p.x)), px(yAt(p.y)), fill, ink)
		}
		last := l.points[len(l.points)-1]
		ends = append(ends, end{y: yAt(last.y), x: xAt(last.x), label: l.label})
	}
	slices.SortStableFunc(ends, func(a, b end) int { return cmp.Compare(a.y, b.y) })
	for i := range ends {
		if i > 0 && ends[i].y < ends[i-1].y+labelGap {
			ends[i].y = ends[i-1].y + labelGap
		}
		fmt.Fprintf(&b, `<text x="%s" y="%s" fill="%s">%s</text>`+"\n", px(ends[i].x+8), px(ends[i].y+4), ink, xmlText.Replace(ends[i].label))
	}
	if anyFast {
		fmt.Fprintf(&b, `<text x="%d" y="40" fill="%s">open circles: runs at the fast disk speed</text>`+"\n", left, ink)
	}
	b.WriteString("</svg>\n")
	return []byte(b.String())
}
