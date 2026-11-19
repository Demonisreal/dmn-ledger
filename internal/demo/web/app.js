import { prove, tamper } from './proof.js'

const STALE = 3000
const AWAY = 60000
const KEEP = 200
const CATCHING_UP = 100
const REFUSALS = { cooling: 'pause', limited: 'limited', recovering: 'recovering', young: 'young' }

const page = document.querySelector('main')
const texts = new Map(Array.from(document.querySelectorAll('#texts template'), (t) => [t.dataset.t, t]))
const kinds = Array.from(document.querySelectorAll('button[data-kind]'))
const list = document.getElementById('lines')
const conn = document.getElementById('conn')
const initial = kinds.some((btn) => btn.disabled)

let st = null
let seen = Number(page.dataset.now)
let lost = ''
let hold = null
let pending = null
let sent = null
let top = Math.max(0, ...Array.from(list.children, (li) => Number(li.dataset.n)))
let said = ''
let born = null
let wait = 3000
let ctl = new AbortController()
let dog = 0
let retry = 0
let away = 0
let asleep = false
let reading = false

const pad = (n, width = 2) => String(n).padStart(width, '0')
const clock = (ms) => {
  const d = new Date(ms)
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}
const clockms = (ms) => `${clock(ms)}.${pad(new Date(ms).getMilliseconds(), 3)}`
const day = (ms) => {
  const d = new Date(ms)
  return `${pad(d.getDate())}.${pad(d.getMonth() + 1)}.${d.getFullYear()}`
}
const grouped = (n) => String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ' ')
const money = (hundredths) => {
  const sign = hundredths.startsWith('-') ? '-' : ''
  const digits = hundredths.slice(sign.length).padStart(3, '0')
  return `${sign}${grouped(digits.slice(0, -2))},${digits.slice(-2)}`
}
const left = (now, until) => Math.max(0, Math.trunc((until - now + 999) / 1000))
const tenths = (ms) => ((ms % 1000 === 250 ? ms - 1 : ms) / 1000).toFixed(1).replace('.', ',')

const el = (tag, attrs, ...kids) => {
  const node = document.createElement(tag)
  for (const [name, text] of Object.entries(attrs)) node.setAttribute(name, text)
  node.append(...kids)
  return node
}
const box = (...kids) => {
  const frag = document.createDocumentFragment()
  frag.append(...kids)
  return frag
}
const time = (ms, show = clock) => el('time', { datetime: new Date(ms).toISOString() }, show(ms))
const secs = (n) => el('span', { 'aria-live': 'off' }, String(n))

function fill(name, vals = {}) {
  const frag = texts.get(name).content.cloneNode(true)
  for (const t of frag.querySelectorAll('time')) {
    const ms = vals[t.dateTime.slice(1, -1)]
    t.dateTime = new Date(ms).toISOString()
    t.textContent = t.dataset.k === 'day' ? day(ms) : clock(ms)
  }
  const marked = []
  const walk = document.createTreeWalker(frag, NodeFilter.SHOW_TEXT)
  while (walk.nextNode()) if (walk.currentNode.data.includes('{')) marked.push(walk.currentNode)
  for (const text of marked) text.replaceWith(...text.data.split(/\{(\w+)\}/).map((part, i) => (i % 2 ? vals[part] : part)))
  frag.normalize()
  return frag
}
const say = (name, vals) => fill(name, vals).textContent

const solid = (node) => node.nodeType === Node.ELEMENT_NODE || node.data.trim() !== ''

function patch(into, fresh) {
  fresh.normalize()
  const had = Array.from(into.childNodes).filter(solid)
  const got = Array.from(fresh.childNodes).filter(solid)
  if (had.length !== got.length || had.some((node, i) => node.nodeName !== got[i].nodeName)) {
    into.replaceChildren(...fresh.childNodes)
    return
  }
  had.forEach((node, i) => {
    const want = got[i]
    if (node.nodeType !== Node.ELEMENT_NODE) {
      if (node.data !== want.data) node.data = want.data
      return
    }
    for (const attr of Array.from(node.attributes)) if (!want.hasAttribute(attr.name)) node.removeAttribute(attr.name)
    for (const attr of want.attributes) if (node.getAttribute(attr.name) !== attr.value) node.setAttribute(attr.name, attr.value)
    patch(node, want)
  })
}

function optional(id, wanted, make, after) {
  const there = document.getElementById(id)
  if (!wanted) {
    there?.remove()
    return null
  }
  if (there) return there
  const made = make()
  after.after(made)
  return made
}

const findings = () => st.totals.findings ?? []
const plural = (n, one, many) => say(n === 1 ? one : many)

function reason() {
  const { cluster } = st
  const full = cluster.limit && st.books.confirmed >= cluster.limit
  if (st.finding || (!full && cluster.why === 'finding')) return say('why-finding')
  if (full || cluster.why === 'limit') return say('why-limit', { limit: grouped(cluster.limit) })
  return say('why-start')
}

function booked() {
  const { books } = st
  const n = grouped(books.confirmed)
  if (!books.recount.at || !books.recount.found) return fill('booked-none', { n })
  return fill(books.recount.after ? 'booked-after' : 'booked', { n, at: books.recount.at, found: grouped(books.recount.found) })
}

function judged() {
  const { found } = st.totals
  if (found) {
    const last = findings().at(-1)
    return last ? fill('judged-found', { n: grouped(found), word: plural(found, 'w-befund', 'w-befunde'), at: last.at }) : ''
  }
  return st.books.judged.upto ? fill('judged', { upto: st.books.judged.upto }) : ''
}

function since() {
  const { actions, healed, found } = st.totals
  const acts = actions.kill + actions.freeze + actions.cut + actions.flip
  return fill('since', {
    at: st.started,
    actions: grouped(acts),
    actword: plural(acts, 'w-stoerung', 'w-stoerungen'),
    healed: grouped(healed),
    found: grouped(found),
    foundword: plural(found, 'w-befund', 'w-befunde'),
  })
}

function tally() {
  const { sum } = st.books
  const parts = []
  if (st.phase === 'finding' && st.finding) {
    const f = st.finding
    parts.push(el('p', { class: 'alarm', id: 'standing' }, fill('finding', { at: f.at, check: f.check, reason: f.reason }),
      f.resetat ? fill('finding-reset', { reset: f.resetat }) : '', f.evidence ? fill('finding-kept') : ''))
  } else if (st.phase === 'setup') {
    parts.push(el('p', { class: 'alarm quiet', id: 'setup' }, fill('setup', { why: reason() })))
  } else {
    const off = sum.value !== sum.want
    parts.push(el('p', { class: off ? 'big off' : 'big' }, el('data', { id: 'sum', value: sum.value }, money(sum.value))))
    parts.push(el('p', { class: 'cap', id: 'sum-cap' },
      off ? fill('sum-wrong', { sum: money(sum.value), want: money(sum.want) }) : fill('caption', { want: money(sum.want), at: sum.at })))
  }
  parts.push(el('ul', { class: 'facts' }, el('li', { id: 'booked' }, booked()), el('li', { id: 'judged' }, judged())))
  parts.push(el('p', { class: 'since', id: 'since' }, since()))
  patch(document.getElementById('tally'), box(...parts))

  const { rest } = st
  const resting = rest.since && (st.load === 'resting' || rest.until >= sum.at)
  const note = optional('rested', resting, () => el('p', { class: 'note', id: 'rested' }), conn)
  if (note) patch(note, fill('resting', { since: rest.since }))
}

function leaderCommit() {
  let term = 0
  let commit = 0
  for (const n of st.nodes) if (n.answers && n.role === 'leader' && n.term >= term) [term, commit] = [n.term, n.commit]
  return commit
}

const severed = (a, b) => st.links.some((link) => link.cut && ((link.from === a && link.to === b) || (link.from === b && link.to === a)))
const route = (from, to) => fill(severed(from, to) ? 'link-cut' : 'link-open', { n: to })

function node(n, commit) {
  const li = page.querySelector(`[data-node="${n.id}"]`)
  li.dataset.proc = n.proc
  li.dataset.role = n.role
  li.toggleAttribute('data-silent', !n.answers)
  let role = ''
  if (!n.answers) role = fill(st.phase === 'setup' ? 'starting' : 'silent')
  else if (texts.has(`role-${n.role}`)) role = fill(`role-${n.role}`)
  const head = el('h3', {}, el('span', { class: 'nid' }, `n${n.id}`), ' ', el('span', { 'data-k': 'role' }, role))
  if (n.answers) head.append(' ', el('span', { class: 'term', 'data-k': 'term' }, fill('term', { term: grouped(n.term) })))
  const rows = [head]
  const hurt = (name, vals) => rows.push(el('p', { class: 'hurt', 'data-k': 'hurt' }, fill(name, vals)))
  const act = st.action
  const hit = act && st.phase !== 'finding' && act.node === n.id && (act.phase === '' || act.phase === 'struck')
  const s = hit ? left(st.now, act.liftat) : 0
  if (hit && n.proc === 'down' && act.kind === 'kill') hurt('killed', { s })
  else if (hit && n.proc === 'down' && act.kind === 'flip') hurt('flipped')
  else if (hit && n.proc === 'frozen') hurt('frozen', { s })
  else if (hit && act.kind === 'cut') hurt('cut', { s })
  else if (n.proc === 'down') hurt('down')
  if (n.proc === 'frozen' && st.phase === 'finding') hurt('halted')
  if (n.repair) rows.push(el('p', { class: 'hurt', 'data-k': 'repair' }, fill('repair')))
  if (n.answers) {
    const applied = grouped(n.applied)
    rows.push(el('p', { 'data-k': 'applied' }, n.applied + CATCHING_UP < commit ? fill('behind', { i: applied, c: grouped(commit) }) : fill('applied', { i: applied })))
    rows.push(el('p', { 'data-k': 'apply' }, fill('apply', { h: n.apply })))
  }
  if (n.pid) rows.push(el('p', { 'data-k': 'proc' }, fill('proc', { pid: n.pid, at: n.since })))
  const [one, two] = [1, 2, 3].filter((id) => id !== n.id)
  if (severed(n.id, one) || severed(n.id, two)) rows.push(el('p', { class: 'hurt', 'data-k': 'net' }, route(n.id, one), ', ', route(n.id, two)))
  if (n.note.at) rows.push(el('p', { class: 'last', 'data-k': 'note' }, fill('note', { at: n.note.at, text: n.note.text })))
  patch(li, box(...rows))
}

function accord() {
  const at = new Map()
  const led = []
  for (const n of st.nodes) {
    if (!n.answers) continue
    at.set(n.applied, [...(at.get(n.applied) ?? []), n.id])
    if (n.role === 'leader') led.push(n.term)
  }
  let index = 0
  let pair = null
  for (const [applied, ids] of at) if (ids.length >= 2 && (!pair || applied > index)) [index, pair] = [applied, ids]
  let sentence = fill('apart')
  if (pair) {
    const hash = st.nodes[pair[0] - 1].apply
    const odd = st.nodes.find((n) => !pair.includes(n.id))
    const i = grouped(index)
    if (pair.slice(1).some((id) => st.nodes[id - 1].apply !== hash)) sentence = fill('differ', { i })
    else if (!odd) sentence = fill('agree3', { i, h: hash })
    else sentence = fill('agree2', { a: pair[0], b: pair[1], i, c: odd.id, what: odd.answers ? say('odd-at', { i: grouped(odd.applied) }) : say('silent') })
  }
  const shared = document.getElementById('accord')
  patch(shared, sentence)
  const both = optional('leaders', led.length >= 2, () => el('p', { class: 'accord hurt', id: 'leaders' }), shared)
  if (both) patch(both, fill(new Set(led).size < led.length ? 'leaders-one-term' : 'leaders'))
}

const running = (act) => act && (act.phase === '' || act.phase === 'struck' || (act.phase === 'lifted' && act.kind === 'flip'))

function faults() {
  patch(document.getElementById('viewers'), st.viewers === 1 ? fill('viewers-1', { n: 1 }) : fill('viewers', { n: grouped(st.viewers) }))
  const act = st.action
  const rows = []
  const now = (name, vals) => rows.push(el('p', { class: 'now' }, fill(name, vals)))
  if (st.phase === 'finding') {
    now('standing')
  } else if (st.phase === 'setup') {
    now('recovering')
  } else if (running(act)) {
    const s = secs(left(st.now, act.liftat))
    now('running', { was: act.kind === 'flip' ? fill('was-flip', { n: act.node }) : fill(`was-${act.kind}`, { n: act.node, s }) })
  } else if (act && (act.phase === 'lifted' || act.phase === 'settling')) {
    now('settling')
  } else {
    if (act) {
      const war = say(`war-${act.kind}`, { n: act.node })
      rows.push(el('p', { class: 'was' }, act.phase === 'failed' ? fill('failed', { war }) : fill('done', { war, s: tenths(act.ended - act.began), at: st.books.recount.at })))
    }
    if (hold) now(hold.name, { s: secs(left(st.now, hold.until)), status: hold.status })
    else if (st.next > st.now) now('pause', { s: secs(left(st.now, st.next)) })
    else now('ready')
  }
  if (lost) rows[rows.length - 1] = el('p', { class: 'now' }, fill(lost, { at: seen }))
  const status = document.getElementById('fault-state')
  patch(status, box(...rows))
  const why = optional('fault-why', act && act.kind !== 'kill', () => el('p', { class: 'why', id: 'fault-why' }), status)
  if (why) patch(why, fill(`why-${act.kind}`))
}

function pinned() {
  const last = findings().at(-1)
  const pin = optional('pinned', last, () => el('p', { class: 'pinned', id: 'pinned' }), document.getElementById('log-h'))
  if (pin) patch(pin, box(fill('pinned', { check: last.check, reason: last.reason }), last.evidence ? fill('evidence', { dir: last.evidence }) : ''))
}

function booking(b) {
  const vals = { id: b.id, amount: money(b.amount), debit: b.debit, credit: b.credit, why: b.refused }
  return el('li', b.refused ? { class: 'refused' } : {}, time(b.at), ' ', el('span', {}, fill(b.refused ? 'booking-refused' : 'booking', vals)))
}

function books() {
  const { sum } = st.books
  const table = document.getElementById('accounts')
  table.hidden = !sum.at
  patch(table.tHead.rows[0].cells[1], fill('accounts-head', { at: sum.at }))
  sum.accounts.forEach((acc, i) => {
    const row = table.tBodies[0].rows[i]
    patch(row.cells[0], box(String(acc.id)))
    patch(row.cells[1], box(money(acc.balance)))
  })
  patch(table.tFoot.rows[0].cells[1], box(money(sum.value)))

  const recent = st.recent ?? []
  const heading = page.querySelector('.recent h3')
  const ol = optional('recent', recent.length > 0, () => el('ol', { id: 'recent' }), heading)
  optional('recent-none', recent.length === 0, () => el('p', { class: 'none', id: 'recent-none' }, fill('empty')), heading)
  if (ol) patch(ol, box(...recent.map(booking)))
}

function row(line) {
  const attrs = { 'data-n': line.n, 'data-src': line.src }
  if (line.src !== 'demo' && line.src !== 'check') attrs.lang = 'en'
  return el('li', attrs, time(line.at, clockms), el('b', { class: 'src' }, line.src), el('span', { class: 'said' }, line.text))
}

function add(line) {
  const prev = list.firstElementChild
  if (prev?.dataset.src === line.src && prev.lastElementChild.textContent === line.text) prev.dataset.again = ''
  top = line.n
  list.prepend(row(line))
  while (list.children.length > KEEP) list.lastElementChild.remove()
}

function relog(lines) {
  const newest = lines.at(-1)?.n ?? 0
  if ((born !== null && born !== st.started) || newest < top || (lines.length > 0 && lines[0].n > top + 1)) {
    list.replaceChildren()
    top = 0
  }
  born = st.started
  for (const line of lines) if (line.n > top) add(line)
}

function heardLine(line) {
  if (line.n <= top) return
  add(line)
  if ((line.src === 'demo' || line.src === 'check') && line.text !== said) {
    said = line.text
    document.getElementById('said').textContent = line.text
  }
}

const mark = () => (st ? [st.phase, st.next, st.action?.began, st.action?.phase].join() : '')

function lock() {
  const off = st ? st.phase !== 'steady' || st.next > st.now : initial
  const held = hold !== null && hold.until > seen
  const focused = kinds.find((btn) => btn === document.activeElement)
  for (const btn of kinds) {
    btn.disabled = off || held || lost !== '' || pending !== null || sent !== null
    if (btn === pending) btn.setAttribute('aria-busy', 'true')
    else btn.removeAttribute('aria-busy')
  }
  if (focused?.disabled) document.getElementById('fault-state').focus({ preventScroll: true })
}

function show(next, full) {
  st = next
  seen = st.now
  if (hold && (hold.until ? st.now >= hold.until : mark() !== hold.mark)) hold = null
  if (sent !== null && ((st.action?.began ?? 0) !== sent || st.phase !== 'steady')) sent = null
  if (lost) {
    lost = ''
    conn.hidden = true
    conn.replaceChildren()
    delete page.dataset.stale
  }
  page.dataset.phase = st.phase
  page.dataset.load = st.load
  tally()
  const commit = leaderCommit()
  for (const n of st.nodes) node(n, commit)
  accord()
  faults()
  pinned()
  books()
  if (full) relog(st.lines ?? [])
  else document.getElementById('viewers').dataset.counted = ''
  lock()
  page.dataset.now = st.now
}

function lose(how) {
  if (lost === 'crowded' && how === 'stale') return
  lost = how
  patch(conn, fill(how, { at: seen }))
  conn.hidden = false
  page.dataset.stale = ''
  if (st) faults()
  lock()
}

function watch() {
  clearTimeout(dog)
  dog = setTimeout(() => {
    lose('stale')
    if (!reading) return
    reading = false
    ctl.abort()
    later(wait)
  }, STALE)
}

function heard(block) {
  let name = ''
  let body = ''
  for (const field of block.split('\n')) {
    const colon = field.indexOf(':')
    const key = field.slice(0, colon)
    const rest = field.slice(colon + 1).trimStart()
    if (key === 'event') name = rest
    else if (key === 'data') body = rest
    else if (key === 'retry') wait = Number(rest)
  }
  watch()
  if (name === 'line') heardLine(JSON.parse(body))
  else if (name === 'state' || name === 'tick') show(JSON.parse(body), name === 'state')
  return name !== ''
}

function later(ms) {
  if (!asleep) retry = setTimeout(listen, ms)
}

async function listen() {
  ctl = new AbortController()
  const { signal } = ctl
  const res = await fetch('/events', { signal, cache: 'no-store' }).catch(() => null)
  if (signal.aborted) return
  if (!res?.ok) {
    if (res?.status === 503) lose('crowded')
    later(wait)
    return
  }
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
  let buf = ''
  let heardAny = false
  reading = true
  watch()
  for (;;) {
    const got = await reader.read().catch(() => null)
    if (signal.aborted) return
    if (!got || got.done) {
      reading = false
      later(got && heardAny ? 0 : wait)
      return
    }
    buf += got.value
    for (let cut = buf.indexOf('\n\n'); cut >= 0; cut = buf.indexOf('\n\n')) {
      if (heard(buf.slice(0, cut))) heardAny = true
      buf = buf.slice(cut + 2)
    }
  }
}

function sleep() {
  asleep = true
  reading = false
  clearTimeout(dog)
  clearTimeout(retry)
  ctl.abort()
  lose('stale')
}

document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    away = setTimeout(sleep, AWAY)
    return
  }
  clearTimeout(away)
  if (asleep) {
    asleep = false
    listen()
  }
})

async function strike(btn) {
  pending = btn
  hold = null
  lock()
  const target = page.querySelector('input[name="target"]:checked').value
  const before = st?.action?.began ?? 0
  const res = await fetch(`/act/${btn.dataset.kind}/${target}`, { method: 'POST' }).catch(() => null)
  pending = null
  if (res?.ok) {
    const own = await res.json().catch(() => ({}))
    if ((st?.action?.began ?? 0) === before) sent = before
    if (own.until) hold = { name: 'limited', until: own.until, status: res.status, mark: mark() }
  } else if (res) {
    const why = await res.json().catch(() => ({}))
    const until = why.error === 'recovering' ? seen + Number(res.headers.get('Retry-After')) * 1000 : why.until ?? 0
    hold = { name: REFUSALS[why.error] ?? 'refused', until, status: res.status, mark: mark() }
  }
  if (st) faults()
  lock()
}

for (const part of document.querySelectorAll('[data-js]')) part.hidden = false
for (const t of page.querySelectorAll('time')) {
  const ms = Date.parse(t.dateTime)
  if (list.contains(t)) t.textContent = clockms(ms)
  else t.textContent = t.dataset.k === 'day' ? day(ms) : clock(ms)
}
for (const btn of kinds) btn.addEventListener('click', () => strike(btn))
document.getElementById('prove').addEventListener('click', () => prove())
document.getElementById('tamper').addEventListener('click', () => tamper())
watch()
listen()
if (document.hidden) away = setTimeout(sleep, AWAY)
