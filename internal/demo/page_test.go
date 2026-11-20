package demo

import (
	"html"
	"regexp"
	"strings"
	"testing"
	"time"
)

var (
	tags   = regexp.MustCompile(`<[^>]*>`)
	spaces = regexp.MustCompile(`\s+`)
)

type rendered struct {
	raw   string
	page  string
	texts string
}

func render(t *testing.T, edit func(st *State)) rendered {
	t.Helper()
	show := newFake()
	show.change(func(st *State) {
		st.Now = time.Now().UnixMilli()
		if edit != nil {
			edit(st)
		}
	})
	raw := hit(NewWeb(show, testOrigin, nil), "GET", "/").Body.String()
	page, texts, ok := strings.Cut(raw, `<div id="texts" hidden>`)
	if !ok {
		t.Fatal("the page has no hidden list of texts")
	}
	return rendered{raw: raw, page: page, texts: texts}
}

func plain(s string) string {
	s = strings.ReplaceAll(html.UnescapeString(tags.ReplaceAllString(s, "")), "\u202f", "'")
	return strings.TrimSpace(spaces.ReplaceAllString(s, " "))
}

func (r rendered) shows(t *testing.T, want ...string) {
	t.Helper()
	text := plain(r.page)
	for _, w := range want {
		if !strings.Contains(text, w) {
			t.Errorf("the page does not show %q", w)
		}
	}
}

func (r rendered) hides(t *testing.T, unwanted ...string) {
	t.Helper()
	text := plain(r.page)
	for _, w := range unwanted {
		if strings.Contains(text, w) {
			t.Errorf("the page shows %q", w)
		}
	}
}

func (r rendered) knows(t *testing.T, want ...string) {
	t.Helper()
	text := plain(r.texts)
	for _, w := range want {
		if !strings.Contains(text, w) {
			t.Errorf("the hidden list has no %q", w)
		}
	}
}

func (r rendered) locked(t *testing.T, want bool) {
	t.Helper()
	buttons := regexp.MustCompile(`<button[^>]*data-kind="[a-z]+"[^>]*>`).FindAllString(r.page, -1)
	if len(buttons) != 4 {
		t.Fatalf("%d fault buttons, want 4", len(buttons))
	}
	for _, b := range buttons {
		if got := strings.Contains(b, " disabled"); got != want {
			t.Errorf("disabled is %v, want %v: %s", got, want, b)
		}
	}
}

func hm(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("15:04:05")
}

func TestPageStates(t *testing.T) {
	t.Run("first request", func(t *testing.T) {
		r := render(t, func(st *State) { st.Action = nil })
		r.shows(t,
			"Drei Server führen gemeinsam ein Hauptbuch. Einer darf ausfallen.",
			"60'000,00",
			"Summe der sechs Konten. Soll: 60'000,00. Gezählt um "+hm(1795036491000)+".",
			"18'734 Buchungen bestätigt. Bei der Zählung nach der letzten Störung um "+hm(1795036480000)+" alle 18'650 wiedergefunden.",
			"1 Befund seit dem Start des Steuerprogramms, zuletzt um "+hm(1795032000000)+". Unten steht er im Wortlaut.",
			"Seit dem Start am 18.11.2026 um "+hm(1795030000000)+": 40 Störungen ausgelöst, 40-mal erholt, 1 Befund.",
			"n1 Leader Term 7", "angewendet bis Eintrag 4'521", "Zustand a594ed26ac5edbed", "PID 41231, läuft seit "+hm(1795034000000),
			hm(1795036402300)+" Bit gekippt, 77 ms Nicht-Wähler",
			"Alle drei haben Eintrag 4'521 angewendet und denselben Zustand: a594ed26ac5edbed.",
			"Bereit. Jede Störung endet von selbst. Es läuft immer nur eine, und alle Besucher sehen dieselbe.",
			"Gerade sehen 12 zu.",
			"Befund F1: node 3 exited: signal: killed",
			"Buchung 18734: 123,45 von Konto 1 an Konto 6. Abgelehnt, das Konto hätte nicht gereicht (exceeds_credits).",
			"Stand bei der Zählung um "+hm(1795036491000),
			"Keinen Stromausfall.", "Kein Urteil über die Zeit.",
			"go build -o /tmp/demo/ ./cmd/ledgerd ./cmd/demo ./cmd/ledgerctl",
			"merkt sich der Server die IP-Adresse im Arbeitsspeicher: 30 Sekunden nach einer ausgelösten Störung, und solange diese Seite geöffnet ist.",
			"© DMN Software · Leon", "Impressum", "Datenschutz",
		)
		r.hides(t, "git clone", "Quelltext")
		r.locked(t, false)
		if strings.Contains(r.page, "<template") {
			t.Error("a template for the script stands outside the hidden list")
		}
	})

	t.Run("connected", func(t *testing.T) {
		r := render(t, nil)
		if !regexp.MustCompile(`<p class="note" id="conn" hidden></p>`).MatchString(r.page) {
			t.Error("the hint about the connection is not empty and hidden")
		}
		r.hides(t, "Die Verbindung ist unterbrochen", "Gerade sehen zu viele zu", "Von dieser Adresse sehen schon")
	})

	t.Run("no event for three seconds", func(t *testing.T) {
		render(t, nil).knows(t, "Stand {at}. Die Verbindung ist unterbrochen, ein neuer Versuch läuft.")
	})

	t.Run("no room in the stream", func(t *testing.T) {
		render(t, nil).knows(t, "Gerade sehen zu viele zu. Das ist der Stand von {at}; die Seite versucht es weiter.",
			"Von dieser Adresse sehen schon vier zu. Das ist der Stand von {at}; die Seite versucht es weiter, bis einer der vier Plätze frei wird.")
	})

	t.Run("without javascript", func(t *testing.T) {
		r := render(t, nil)
		now := regexp.MustCompile(`data-now="(\d+)"`).FindStringSubmatch(r.page)
		if now == nil {
			t.Fatal("the page carries no data-now")
		}
		note := regexp.MustCompile(`<noscript><p class="note">(.*?)</p></noscript>`).FindStringSubmatch(r.page)
		if note == nil || !strings.HasPrefix(plain(note[1]), "Das ist der Stand von ") || !strings.Contains(plain(note[1]), "Mit JavaScript läuft die Seite mit, und die Knöpfe erscheinen.") {
			t.Fatalf("no sentence for a visitor without JavaScript: %q", note)
		}
		for _, el := range regexp.MustCompile(`<[a-z]+[^>]*\sdata-js[\s>][^>]*>`).FindAllString(r.page, -1) {
			if !strings.Contains(el, " hidden") {
				t.Errorf("a control is there without JavaScript: %s", el)
			}
		}
		if n := strings.Count(r.page, "<noscript><b>"); n != 4 {
			t.Errorf("%d fault names without JavaScript, want 4", n)
		}
	})

	t.Run("cluster set up", func(t *testing.T) {
		r := render(t, func(st *State) {
			st.Phase, st.Load, st.Books, st.Recent, st.Finding = "setup", "held", Books{}, nil, nil
			st.Cluster.Why = "limit"
			for i := range st.Nodes {
				st.Nodes[i].Answers = false
			}
		})
		r.shows(t, "Der Cluster wird gerade neu aufgesetzt (20'000 Buchungen erreicht). In wenigen Sekunden geht es weiter.",
			"n1 startet", "n2 startet", "n3 startet", "Der Cluster ist gerade nicht vollständig. Gleich wieder.",
			"In diesem Cluster ist noch nichts gebucht.")
		r.hides(t, "Summe der sechs Konten")
		r.locked(t, true)
		render(t, func(st *State) {
			st.Phase, st.Books, st.Cluster.Why = "setup", Books{Confirmed: 20000}, "finding"
		}).shows(t, "Der Cluster wird gerade neu aufgesetzt (20'000 Buchungen erreicht).")
		render(t, func(st *State) {
			st.Phase, st.Books, st.Cluster.Why = "setup", Books{Confirmed: 20000}, "limit"
			st.Finding = &Finding{At: st.Now - 31000, Check: "F1", Reason: "node 3 exited", ResetAt: st.Now - 1000}
		}).shows(t, "Der Cluster wird gerade neu aufgesetzt (nach einem Befund).")
		for why, want := range map[string]string{"start": "nach dem Start", "finding": "nach einem Befund"} {
			render(t, func(st *State) {
				st.Phase, st.Books, st.Cluster.Why = "setup", Books{}, why
			}).shows(t, "Der Cluster wird gerade neu aufgesetzt ("+want+").")
		}
	})

	t.Run("finding", func(t *testing.T) {
		reason := strings.Repeat("ein langer Wortlaut ", 10) + "\nzweite Zeile " + strings.Repeat("x", 120)
		var at, reset int64
		r := render(t, func(st *State) {
			at, reset = st.Now-2000, st.Now+28000
			st.Phase, st.Load = "finding", "held"
			st.Finding = &Finding{At: at, Check: "replay", Reason: reason, ResetAt: reset, Evidence: "befund-1795036490-2"}
		})
		r.shows(t, "Befund um "+hm(at)+": replay, ", "Der Cluster steht und wird um "+hm(reset)+" neu aufgesetzt. Die Daten der Knoten sind gesichert.",
			"Gesperrt, solange der Befund steht.")
		if !strings.Contains(r.page, html.EscapeString(reason)) {
			t.Error("the wording of the finding is not on the page as it came, line break included")
		}
		r.hides(t, "Summe der sechs Konten")
		r.locked(t, true)
		if !strings.Contains(r.page, `<ol id="lines">`) {
			t.Error("the log is gone while a finding stands")
		}
	})

	t.Run("sum differs", func(t *testing.T) {
		render(t, func(st *State) { st.Books.Sum.Value = "5999900" }).shows(t, "Die Summe stimmt nicht: 59'999,00 statt 60'000,00. Das ist ein Befund.")
	})

	t.Run("fault running", func(t *testing.T) {
		r := render(t, func(st *State) {
			st.Phase = "action"
			st.Action = &Action{Kind: Kill, Node: 2, Phase: "struck", Began: st.Now - 300, LiftAt: st.Now + 1700}
			st.Nodes[1].Proc, st.Nodes[1].Answers, st.Nodes[1].Pid = "down", false, 0
		})
		r.shows(t, "Läuft: n2 ist beendet und startet in 2 s.", "n2 antwortet nicht", "beendet mit SIGKILL, startet in 2 s",
			"n1 und n3 stimmen bei Eintrag 4'521 überein. n2 antwortet nicht.")
		r.locked(t, true)

		cases := []struct {
			kind Kind
			edit func(st *State)
			want []string
		}{
			{Freeze, func(st *State) { st.Nodes[1].Proc, st.Nodes[1].Answers = "frozen", false },
				[]string{"Läuft: n2 ist eingefroren, noch 5 s.", "eingefroren mit SIGSTOP, läuft in 5 s weiter", "Ein eingefrorener Leader hält sich"}},
			{Cut, func(st *State) {
				for i := range st.Links {
					st.Links[i].Cut = st.Links[i].From == 2 || st.Links[i].To == 2
				}
				st.Nodes[0].Term, st.Nodes[1].Role = 8, "leader"
			}, []string{"Läuft: n2 ist abgetrennt, noch 5 s.", "von den beiden anderen getrennt, noch 5 s", "zu n1 getrennt, zu n3 getrennt", "zu n2 getrennt, zu n3 offen",
				"Zwei Knoten melden sich als Leader, in verschiedenen Terms. Festschreiben kann nur der mit der Mehrheit.", "Der abgetrennte Knoten kann nichts festschreiben"}},
			{Flip, func(st *State) { st.Nodes[1].Proc, st.Nodes[1].Answers = "down", false },
				[]string{"Läuft: in n2 ist ein Bit gekippt, n2 startet.", "beendet mit SIGKILL", "Eine Prüfsumme findet das kaputte Stück."}},
		}
		for _, c := range cases {
			render(t, func(st *State) {
				st.Phase = "action"
				st.Action = &Action{Kind: c.kind, Node: 2, Phase: "struck", Began: st.Now, LiftAt: st.Now + 4500}
				c.edit(st)
			}).shows(t, c.want...)
		}

		r = render(t, func(st *State) {
			st.Phase = "action"
			st.Action = &Action{Kind: Kill, Node: 2, Phase: "settling", Began: st.Now - 3000, LiftAt: st.Now - 1000}
		})
		r.shows(t, "Die Störung ist vorbei. Jetzt wird nachgezählt.")
		r.locked(t, true)
	})

	t.Run("after a fault", func(t *testing.T) {
		r := render(t, func(st *State) {
			st.Action = &Action{Kind: Kill, Node: 2, Phase: "done", Began: st.Now - 6000, Ended: st.Now - 3200}
			st.Next = st.Now + 7200
		})
		r.shows(t, "n2 abgeschossen. Erholt nach 2,8 s. Nachgezählt um "+hm(1795036480000)+": Summe und Buchungen stimmen.",
			"Die nächste Störung geht in 8 s.")
		r.locked(t, true)
		r.knows(t, "Von dieser Adresse geht die nächste Störung in {s} s.",
			"Dieser Cluster ist zu jung für einen Bitfehler in bestätigten Einträgen. In einigen Sekunden geht es.",
			"Das hat der Server nicht angenommen ({status}).")
	})

	t.Run("load rested", func(t *testing.T) {
		var since int64
		r := render(t, func(st *State) {
			since = st.Now - 90_000
			st.Load, st.Rest = "resting", Rest{Since: since}
		})
		r.shows(t, "Es hat niemand zugesehen, deshalb ruhte die Last seit "+hm(since)+". Sie läuft wieder an, sobald jemand zusieht.")
		render(t, nil).hides(t, "deshalb ruhte die Last")
	})

	t.Run("receipt", func(t *testing.T) {
		r := render(t, nil)
		r.knows(t, "Beleg wird geholt.",
			"Gerade gibt es keinen Leader, der einen Beleg ausstellen kann. Nach der Wahl geht es wieder.",
			"Dieser Browser kann Ed25519 nicht prüfen. Der Pfad stimmt, und der Kopf ist der, den der Controller errechnet hat; über den Stempel sagt diese Seite hier nichts.",
			"Der geänderte Beleg fällt durch: Der Pfad führt nicht mehr zur Wurzel im Kettenglied.",
			"Der Beleg ist in sich schlüssig und trägt den Stempel von Knoten {n}.", "stimmt nicht",
			"Der Beleg kam nicht an, der Server war nicht zu erreichen.")
		r.shows(t, "Jüngste Buchung beweisen", "Ein Byte im Beleg ändern", "Ändert nur die Kopie in diesem Browser.",
			"Was dieser Beleg zeigt:", "Was er nicht zeigt:", "ledgerctl verify proof")
	})

	t.Run("long texts", func(t *testing.T) {
		long := strings.Repeat("abcdefghij", 39) + "\nnext"
		show := newFake()
		show.write(long)
		raw := hit(NewWeb(show, testOrigin, nil), "GET", "/").Body.String()
		if !strings.Contains(raw, `<span class="said">`+long+`</span>`) {
			t.Error("a log line of 400 characters with a line break is not on the page as it came")
		}
		css, err := webFS.ReadFile("web/page.css")
		if err != nil {
			t.Fatal(err)
		}
		said := regexp.MustCompile(`(?s)\n\.said \{[^}]*\}`).FindString(string(css))
		if !strings.Contains(said, "white-space: pre-wrap") || !strings.Contains(said, "overflow-wrap: anywhere") {
			t.Errorf("long texts do not wrap: %q", said)
		}
	})

	t.Run("repeated lines", func(t *testing.T) {
		show := newFake()
		show.write("node 2 of 3 in cluster 13911521510627622913 stopped")
		raw := hit(NewWeb(show, testOrigin, nil), "GET", "/").Body.String()
		lines := regexp.MustCompile(`<li data-n="\d+" data-src="n2"( data-again)?`).FindAllStringSubmatch(raw, -1)
		if len(lines) != logTail {
			t.Fatalf("%d lines in the log, want %d", len(lines), logTail)
		}
		for i, l := range lines {
			if again := l[1] != ""; again != (i >= 2) {
				t.Errorf("line %d from the top: marked as a repetition %v", i+1, again)
			}
		}
		if strings.Index(raw, `id="log-about"`) < strings.Index(raw, `<ol id="lines">`) {
			t.Error("the explanation of the log stands above its lines")
		}
	})

	t.Run("nothing to find yet", func(t *testing.T) {
		r := render(t, func(st *State) { st.Books.Recount.Found = 0 })
		r.shows(t, "18'734 Buchungen bestätigt.")
		r.hides(t, "alle 0 wiedergefunden")
	})

	t.Run("large numbers", func(t *testing.T) {
		r := render(t, func(st *State) {
			st.Totals.Actions = Actions{Kill: 1_000_000, Freeze: 234_567}
			st.Books.Confirmed = 19_999
		})
		r.shows(t, ": 1'234'567 Störungen ausgelöst", "19'999 Buchungen bestätigt.")
		css, _ := webFS.ReadFile("web/page.css")
		if strings.Count(string(css), "font-variant-numeric: tabular-nums") < 5 {
			t.Error("the numbers do not use digits of equal width")
		}
	})

	t.Run("narrow screen", func(t *testing.T) {
		css, _ := webFS.ReadFile("web/page.css")
		if !strings.Contains(string(css), "@media (min-width: 40rem) {\n  .trio {\n    grid-template-columns: repeat(3, minmax(0, 1fr));") {
			t.Error("page.css has no rule that sets the three nodes side by side from 40rem on")
		}
	})

	t.Run("one viewer", func(t *testing.T) {
		render(t, func(st *State) { st.Viewers = 1 }).shows(t, "Gerade sieht 1 zu.")
	})

	t.Run("repo public", func(t *testing.T) {
		render(t, nil).hides(t, "git clone", "Quelltext", "ADR-009", "The simulator")
		repo = "https://example.org/dmn-ledger"
		defer func() { repo = "" }()
		render(t, nil).shows(t, "git clone https://example.org/dmn-ledger && cd dmn-ledger", "Quelltext", "ADR-009", "ADR-014", "The simulator")
	})
}

func TestDownNodeSaysOnlyWhatIsKnown(t *testing.T) {
	down := func(st *State, n int) {
		st.Nodes[n-1].Proc, st.Nodes[n-1].Answers, st.Nodes[n-1].Pid = "down", false, 0
	}
	node := func(r rendered, n int) rendered {
		lis := regexp.MustCompile(`(?s)<li data-node="\d".*?</li>`).FindAllString(r.page, -1)
		if len(lis) != 3 {
			t.Fatalf("%d nodes on the page", len(lis))
		}
		return rendered{page: lis[n-1]}
	}
	r := render(t, func(st *State) {
		st.Phase, st.Load = "finding", "held"
		st.Finding = &Finding{At: st.Now - 500, Check: "S8", Reason: "lost"}
		st.Action = &Action{Kind: Kill, Node: 2, Phase: "struck", Began: st.Now - 900, LiftAt: st.Now + 1100}
		down(st, 2)
	})
	node(r, 2).shows(t, "n2 antwortet nicht beendet")
	node(r, 2).hides(t, "startet in", "SIGKILL")
	r = render(t, func(st *State) {
		st.Phase, st.Load = "finding", "held"
		st.Finding = &Finding{At: st.Now - 500, Check: "F1", Reason: "n2 exited"}
		down(st, 2)
	})
	node(r, 2).shows(t, "beendet")
	node(r, 2).hides(t, "SIGKILL")
	r = render(t, func(st *State) {
		st.Phase = "action"
		st.Action = &Action{Kind: Flip, Node: 2, Phase: "struck", Began: st.Now - 20, LiftAt: st.Now}
		down(st, 2)
	})
	node(r, 2).shows(t, "beendet mit SIGKILL")
}

func TestTwoLeadersNameTheirTerms(t *testing.T) {
	apart := "Zwei Knoten melden sich als Leader, in verschiedenen Terms. Festschreiben kann nur der mit der Mehrheit."
	one := "Zwei Knoten melden sich im selben Term als Leader. Das darf nicht sein."
	r := render(t, func(st *State) { st.Nodes[0].Term, st.Nodes[1].Role = 8, "leader" })
	r.shows(t, apart)
	r.hides(t, one)
	r = render(t, func(st *State) { st.Nodes[1].Role = "leader" })
	r.shows(t, one)
	r.hides(t, "in verschiedenen Terms")
	render(t, nil).hides(t, "melden sich")
}

func TestFindingSaysOnlyWhatHappened(t *testing.T) {
	const reset, kept = "neu aufgesetzt", "Die Daten der Knoten sind gesichert."
	cases := []struct {
		name     string
		resetAt  int64
		evidence string
		shows    []string
		hides    []string
	}{
		{"before the evidence is copied", 0, "", nil, []string{reset, kept, "00:00:00"}},
		{"a fourth finding, nothing copied", 1795036520000, "", []string{"Der Cluster steht und wird um " + hm(1795036520000) + " neu aufgesetzt."}, []string{kept}},
		{"copied", 1795036520000, "befund-1795036490-2", []string{"neu aufgesetzt. " + kept}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := render(t, func(st *State) {
				st.Phase, st.Load = "finding", "held"
				st.Finding = &Finding{At: st.Now - 2000, Check: "F1", Reason: "node 3 exited", ResetAt: c.resetAt, Evidence: c.evidence}
			})
			verdict := rendered{page: regexp.MustCompile(`(?s)<p class="alarm" id="standing">.*?</p>`).FindString(r.page)}
			verdict.shows(t, append(c.shows, "Befund um ")...)
			verdict.hides(t, c.hides...)
		})
	}
}

func TestPageIsAccessible(t *testing.T) {
	r := render(t, nil)
	if !strings.HasPrefix(r.raw, "<!doctype html>\n<html lang=\"de\">") {
		t.Errorf("the page does not start with the doctype and lang=\"de\": %q", r.raw[:min(40, len(r.raw))])
	}
	if n := strings.Count(r.page, "<h1"); n != 1 {
		t.Errorf("%d h1, want 1", n)
	}
	sections := regexp.MustCompile(`<section [^>]*aria-labelledby="([a-z-]+)"`).FindAllStringSubmatch(r.page, -1)
	if len(sections) != 8 {
		t.Errorf("%d sections with a heading, want 8", len(sections))
	}
	for _, s := range sections[1:] {
		if !strings.Contains(r.page, `<h2 id="`+s[1]+`">`) {
			t.Errorf("section %s has no h2", s[1])
		}
	}
	for _, b := range regexp.MustCompile(`(?s)<button[^>]*>(.*?)</button>`).FindAllStringSubmatch(r.page, -1) {
		if plain(b[1]) == "" {
			t.Errorf("a button without text: %s", b[0])
		}
	}
	fieldset := regexp.MustCompile(`(?s)<fieldset[^>]*>\s*<legend>([^<]+)</legend>`).FindStringSubmatch(r.page)
	if fieldset == nil || fieldset[1] != "Ziel" {
		t.Error("the choice of the target has no fieldset with a legend")
	}
	if n := strings.Count(r.page, `type="radio" name="target"`); n != 4 {
		t.Errorf("%d targets, want 4", n)
	}
	live := regexp.MustCompile(`(?s)<(div|p)[^>]*aria-live="polite"[^>]*>(.*?)</(div|p)>`).FindAllStringSubmatch(r.page, -1)
	ids := map[string]bool{}
	for _, l := range live {
		id := regexp.MustCompile(`id="([a-z-]+)"`).FindStringSubmatch(l[0])
		if id != nil {
			ids[id[1]] = true
		}
		for _, bad := range []string{`id="sum"`, `data-node=`, `id="lines"`, `id="booked"`, `data-n=`} {
			if strings.Contains(l[0], bad) {
				t.Errorf("the live region %s holds %s", id, bad)
			}
		}
	}
	for _, want := range []string{"fault-state", "said"} {
		if !ids[want] {
			t.Errorf("no live region %s", want)
		}
	}
	for _, line := range regexp.MustCompile(`<li data-n="\d+" data-src="([a-z0-9]+)"([^>]*)>`).FindAllStringSubmatch(r.page, -1) {
		node := line[1] != "demo" && line[1] != "check"
		if node != strings.Contains(line[2], `lang="en"`) {
			t.Errorf("a line of %s has lang %q", line[1], line[2])
		}
	}
	for _, tm := range regexp.MustCompile(`<time[^>]*>`).FindAllString(r.page, -1) {
		if !regexp.MustCompile(`datetime="\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z"`).MatchString(tm) {
			t.Errorf("a time without datetime: %s", tm)
		}
	}
	css, err := webFS.ReadFile("web/page.css")
	if err != nil {
		t.Fatal(err)
	}
	if regexp.MustCompile(`\b(transition|animation)\s*:`).Match(css) && !strings.Contains(string(css), "@media (prefers-reduced-motion: reduce)") {
		t.Error("the stylesheet moves things and has no rule for prefers-reduced-motion")
	}
	for _, focus := range []string{"a:focus-visible", ".btn:focus-visible", ".aim input:focus-visible", ".lines:focus-visible", ".state:focus-visible"} {
		rule := regexp.MustCompile(`(?s)` + regexp.QuoteMeta(focus) + ` \{[^}]*\}`).FindString(string(css))
		if !strings.Contains(rule, "outline: 2px solid var(--red)") || !strings.Contains(rule, "outline-offset: 3px") {
			t.Errorf("%s has no visible focus like the site: %q", focus, rule)
		}
	}
	if !strings.Contains(string(css), "[hidden] { display: none !important; }") {
		t.Error("a hidden control could still show, because a class sets display")
	}
}
