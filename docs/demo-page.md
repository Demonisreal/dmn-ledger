# Design

The demo page belongs to dmn-software.com. It takes the site's tokens, type, spacing, link style, focus ring
and footer, and the component language of the site's own live tool page (status.dmn-software.com). Nothing
here is a second palette. What the page says, and in which order, is fixed in the template.

Files: `internal/demo/web/page.html` (Go `html/template`), `internal/demo/web/page.css`,
`internal/demo/web/app.js` and `internal/demo/web/proof.js` (behaviour, see below),
`internal/demo/web/icon.svg` and `internal/demo/web/grain-1.svg` (both copied unchanged from the site).

## Tokens

Copied from `site.css` of dmn-software.com, `:root`, as they stand there.

| Token | Value | Role |
|---|---|---|
| `--dark` | `#0b0b0d` | page ground |
| `--card` | `#14151a` | raised surface: notes, buttons, the finding |
| `--red` | `#e11d2e` | the one accent: focus ring, link underline, leader mark, warn border |
| `--white` | `#ffffff` | headings, figures, values, current state |
| `--text` | `#b9bcc4` | body text |
| `--muted` | `#8f939c` | captions, sources, secondary lines, button border |
| `--line` | `#3f4249` | stronger rule (sum row of the table, step rail), disabled border |
| `--ink` | `#101114` | sunken panel: log, code, receipt steps |
| `--edge` | `#25272d` | hairlines between rows and columns |
| `--red-hi` | `#f0414f` | text in red: something is wrong or a node is hit |
| `--green` | `#3fb27f` | semantic "holds" (receipt step), nothing else |
| `--sans` | `ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif` | all text |
| `--mono` | `ui-monospace, "SF Mono", "JetBrains Mono", Consolas, monospace` | data only: ids, hashes, PIDs, times in the log, code |
| `--ease` | `cubic-bezier(.23, 1, .32, 1)` | the press on a button |

Only a dark scheme, because the site has only one (`prefers-color-scheme` does not occur in `site.css`).
System fonts, so nothing is loaded and there is no licence question.

Spacing, radii and type sizes are the site's values, not a new scale: `.wrap` 72rem with
`clamp(1.25rem, 4vw, 2.5rem)`, radius 8px for buttons and panels (status page `.btn`, site
`.flow`, `.cite`), 6px for the skip link, h2 1.375rem/1.625rem like the status page's `.blk h2`, body 1rem/1.65.
The h1 starts like the status page's `.now h1` (1.75rem) but stops at 2rem from 48rem, where the status page goes to
2.5rem: there the h1 is a service name, here it is a sentence of two clauses, and at 2.5rem it takes three lines in
the left column and pushes the nodes below the fold at 1366 x 768. The steps at 1366 are 16, 26, 32 and 48px (body,
h2, h1, the sum), at 360 they are 16, 22, 28 and 44px.

## Contrast

Computed with the WCAG 2 formula from the token values (script: relative luminance, `(L1 + .05) / (L2 + .05)`).
The grain overlay (`opacity: .05`, white noise) sits behind content (`z-index: -1`) and only
lightens the bare page ground; the column "with grain" is the worst case of a fully white grain pixel at 5 %.

| Text | Ground | Ratio | with grain | Where | AA |
|---|---|---|---|---|---|
| `--white` | `--dark` | 19.66 | 17.90 | h1, the sum, node names, status line | pass |
| `--text` | `--dark` | 10.35 | 9.42 | body, node lines, descriptions | pass |
| `--muted` | `--dark` | 6.39 | 5.82 | caption of the sum, "Seit dem Start", log intro, explanation | pass |
| `--red-hi` | `--dark` | 5.22 | 4.75 | "antwortet nicht", hit node, wrong sum | pass |
| `--white` | `--card` | 18.23 | | note, finding | pass |
| `--text` | `--card` | 9.60 | | button label | pass |
| `--red-hi` | `--card` | 4.84 | | label of a fault button, check name in a finding | pass |
| `--muted` | `--card` | 5.92 | | label of a disabled button (exempt, still above 4.5) | pass |
| `--white` | `--ink` | 18.88 | | log lines of demo and check, code | pass |
| `--text` | `--ink` | 9.94 | | node log lines, hashes | pass |
| `--muted` | `--ink` | 6.13 | | log times and sources, a line that repeats the one above | pass |
| `--green` | `--ink` | 7.09 | | "stimmt" | pass |
| `--red-hi` | `--ink` | 5.01 | | "stimmt nicht" | pass |
| `--dark` | `--white` | 19.66 | | skip link | pass |

Non-text (3:1 needed):

| Element | Colour | Ground | Ratio |
|---|---|---|---|
| focus ring | `--red` | `--dark` | 4.14 |
| focus ring | `--red` | `--ink` | 3.97 |
| focus ring | `--red` | `--card` | 3.83 |
| fault button border | `--red` | `--dark` | 4.14 |
| button border | `--muted` | `--dark` | 6.39 |
| radio (accent) | `--red` | `--dark` | 4.14 |
| disabled button border | `--line` | `--dark` | 1.95 (disabled, exempt; the reason stands as text) |

Not used for text: `--red` (3.83 to 4.14, below 4.5), `--line`, `--edge`.

## Layout

The nine sections come in this order: verdict, nodes, faults, log, books, receipt, limits,
run it yourself, footer. Above them the site's header bar, without the section nav: brand linking back to
dmn-software.com, and the crumb "Projekte / dmn-ledger".

From 64rem the first four sections stand in two columns, 6fr and 5fr, each a `div.col` of its own:

```
verdict  | faults
nodes    | log
```

The columns flow independently: the left reads verdict then nodes, the right reads faults then the log, and a
long finding in the verdict does not push the log down. At 1366 x 768 the verdict, the three nodes, the faults
with status line and explanation, and the first lines of the log stand without scrolling (measured in
headless Chrome while idle and during each of the four faults; the first log line stands at 669px in every
one of them). While a node is cut off its column grows and the agreement line drops
below the fold; that is accepted.

The fault column has a minimum height of 32.5rem, the measured height of its longest state (after a bit flip:
what was, "Bereit", and the explanation of the flip, 520px with the system fonts of the build machine). The
space under the status line is empty while nothing has happened; it is kept so the log does not move when the
status text or the explanation changes length. The explanation of the log stands under its lines and is tied
to the scrollable region with `aria-describedby`, so the lines start right under the heading.

Below 64rem everything stacks in the order of the sections; the three nodes stand side by side from 40rem
and below each other under it.

Sections 5 to 9 use the site's rhythm (`.blk` 2.75rem, 3.5rem from 64rem). Books and "run it yourself" split
5fr/7fr and 7fr/5fr from 64rem.

No section repeats another's pattern: statement with one large figure; three equal columns (the only
repetition, because the nodes are three equal things); a button list with a status line; a log panel; a table
next to a list; steps on a rail; a list with square marks; prose with code.

## Components and states

| Component | Taken from | States |
|---|---|---|
| link | site | normal (white, red 1px underline), hover (underline white), focus 2px `--red` offset 3px |
| header, brand, crumb | site | static, not sticky |
| large figure | new, sized like `.page-head h1`, tabular figures | normal; wrong sum in `--red-hi` with the sentence that says so; replaced by the setup sentence or the finding |
| note | status page `.note`, border 1px instead of a 2px side bar | no JavaScript, stale connection, crowded stream or full address, load rested |
| finding | note with a 1px `--red` border, wording in mono with line breaks kept | |
| fact rows | site `.uptime` | empty row hidden |
| node column | new, lines like status page `.svc-s` | answers, catching up, silent, down, frozen, cut, repair, halted by a finding, starting; the leader carries the site's 5px red square, only while it answers |
| target choice | status page `.opts input`, `fieldset` with `legend` | checked label in white; focus ring offset 3px |
| fault button | status page `.btn.warn` | normal, hover (white), focus ring, pressed (`scale(.97)`), disabled (`--line` border, `--muted` text, reason in the status line right under it), hidden without JavaScript with the name shown as text instead |
| status line | new | ready, running with seconds left, counting, done, failed, pause, finding, setup; the script adds address limit, young log, refused; takes the focus (`tabindex="-1"`, ring as everywhere) when the focused fault button is locked |
| log | site `.pipe`/`.stage` panel in `--ink`, mono .75rem | newest line on top; demo and check lines in white, node lines `lang="en"`; a line whose source and text equal the line above (the newer one) in `--muted`, so a quiet minute of identical counts does not read as twelve news; long lines and line breaks wrap anywhere; pinned finding above in a red-bordered row; explanation under the panel |
| viewer count | status page `.sec-head p` | with scripting it stays invisible (`visibility: hidden`, `@media (scripting: enabled)`) until the first `tick`, because the count in the served page and in the first `state` does not yet include this visitor; without scripting it shows the count of the served page |
| accounts table | site `.sum` | sum row with a `--line` rule and weight 600; a caption for screen readers only (`.vh`), because heading, intro and column head already say it on screen; empty state sentence when nothing is booked |
| receipt steps | site `.flow` | dot `--muted` until the comparison that covers the step has held, `--green` with "stimmt", `--red-hi` with "stimmt nicht" |
| code block | site `.stage pre` in an `--ink` panel | wraps, `user-select: all` |
| limits list | site `.ticks` | |
| footer | site | privacy sentence, "© DMN Software · Leon", imprint, privacy, legal notes on the site |

State is never carried by colour alone: every red or green element carries its word ("antwortet nicht",
"getrennt", "stimmt", "stimmt nicht", "Befund").

## Focus order

1. "Zum Inhalt" (skip link)
2. brand, "Projekte"
3. the four targets (one radio group, arrow keys inside)
4. Abschießen, Einfrieren, Abtrennen, Bit kippen; when the focused one is locked (pressed, or another visitor
   started a fault), the focus goes to the status line, which says why, and Tab goes on to the log
5. the log panel (scrollable region)
6. "Jüngste Buchung beweisen", then "Ein Byte im Beleg ändern" once a receipt is shown
7. the repository links (only once the repository is public)
8. footer: DMN Software, Impressum, Datenschutz, Rechtliche Hinweise

Sections 1 and 2 hold nothing focusable, so the order is the same in the two-column layout and when stacked.
Focus ring everywhere as on the site: 2px `--red`, offset 3px.

## Breakpoints

| Width | Layout |
|---|---|
| 360 | one column; nodes below each other; log lines put the text under time and source; fault buttons 7rem wide, 44px tall |
| 768 | one column; nodes side by side; h1 2rem, figure 3rem |
| 1366 | two columns from 64rem as above; buttons and radios 36px tall |

Readable at 200 % zoom: at 1366 px that is a 683 px viewport, which gets the one-column layout.

## Behaviour

`internal/demo/web/app.js` reads the stream at `/events` and puts every `state` and `tick` into the page
that the server rendered; `internal/demo/web/proof.js` fetches `/proof` and shows what `check` from
`receipt.js` returns. No markup is invented by the scripts: every sentence comes from a `template[data-t]`
in the page, values are set as text, and an element is rewritten only where its text or attributes change,
so a selection or a screen reader is not disturbed by values that stay the same. Times are written in the
browser's zone, counted against `now` of the server, and only change when an event arrives.

| State | What the page shows |
|---|---|
| connected | values follow the stream; the stream is not mentioned |
| no event for three seconds | note in the verdict and in the status line, "Stand `<zeit>`" with the server time of the last event; values in the verdict, the nodes and the books turn `--muted`; fault buttons locked |
| no room in the stream (503) | the same with the sentence "Gerade sehen zu viele zu"; the page asks again after the server's `retry` |
| no room for this address (429) | the same with the sentence "Von dieser Adresse sehen schon vier zu"; the page asks again after the server's `retry` |
| tab was hidden | after one minute in the background the page closes its stream and shows the old-values state; on return it connects again and the first `state` makes everything current |
| click sent | all fault buttons locked, the pressed one keeps its red border and `aria-busy`; after 202 they stay locked until an event shows the action |
| refused | the sentence for `limited`, `cooling`, `recovering`, `young`, or "Das hat der Server nicht angenommen (`<status>`)" in place of "Bereit"; with `until` (or `Retry-After`) the buttons stay locked until then |
| receipt | steps up to the first that fails say "stimmt", that one "stimmt nicht", later ones stay empty and grey; with a changed byte step 3 fails; a receipt that is not a transfer never says it holds |
| receipt, no answer | "Der Beleg kam nicht an, der Server war nicht zu erreichen." next to the button; a receipt shown before stays |
| first count of a cluster | while the last full count found no booking (right after the setup), the line says only "`<n>` Buchungen bestätigt." instead of "alle 0 wiedergefunden" |

Step 7 ("denselben Kopf") compares the head that `check` computed with the head the controller sent; it is
the only comparison outside `receipt.js` and computes nothing. The explanatory text and the command block
under the steps use the receipt as the server sent it.

## Motion

No motion beyond the site's own: colour transitions of 150ms on links and buttons and the press of a button
(`scale(.97)`, 160ms `--ease`). Under `prefers-reduced-motion: reduce` both are off. The scripts add no
motion: values change up to four times a second and the log grows every few seconds, so anything that moves
would be seen constantly, and a repair of 77 ms or a count that did not change has nothing to show that a
plain new value does not. No animation shows a process; values change because the state changes.

## What is new against the site

- the two-column board of the first four sections
- the node column, the target choice in one row, the status line
- the log panel with newest line on top
- the receipt steps re-use the site's `.flow` rail with `--green` and `--red-hi` dots
- the header has no section nav and is not sticky
