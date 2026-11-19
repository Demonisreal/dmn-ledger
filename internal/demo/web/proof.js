import { agrees, check, fromHex, toHex } from './receipt.js'

const TRANSFER = 2
const AMOUNT = 2 + 48 + 15
const STEPS = ['transfer', 'leaf', 'path', 'head', 'stamp', 'owner', 'go']
const BREAKS = { format: 'transfer', path: 'path', unsigned: 'stamp', signature: 'stamp', unsupported: 'stamp', cluster: 'stamp', key: 'owner' }
const REFUSED = { noleader: 'proof-noleader', finding: 'proof-finding', empty: 'empty' }

const out = document.getElementById('proof-out')
const steps = document.getElementById('steps')
const note = document.getElementById('proof-state')
const verdict = document.getElementById('proof-result')

let receipt = null
let copy = null
let tampered = false
let busy = false

const say = (name, vals = {}) => document.querySelector(`#texts template[data-t="${name}"]`).content.textContent.replace(/\{(\w+)\}/g, (_, key) => vals[key])
const pad = (n, width = 2) => String(n).padStart(width, '0')
const grouped = (n) => String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ' ')
const money = (hundredths) => {
  const digits = hundredths.padStart(3, '0')
  return `${grouped(digits.slice(0, -2))},${digits.slice(-2)}`
}
const big = (bytes) => BigInt(`0x${toHex(bytes)}`)
const stamp = (ms) => {
  const d = new Date(ms)
  const t = document.createElement('time')
  t.dateTime = d.toISOString()
  t.textContent = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`
  return t
}

const field = (root, key) => root.querySelector(`[data-k="${key}"]`)
const put = (root, key, text) => {
  for (const spot of root.querySelectorAll(`[data-k="${key}"]`)) spot.textContent = text
}

async function draw() {
  const seen = await check(copy, fromHex(receipt.cluster), receipt.keys.map(fromHex))
  const booking = seen.kind === TRANSFER
  const same = seen.ok && agrees(seen.head, receipt.head.seq, receipt.head.hash)
  let stop = STEPS.length
  if (!booking) stop = 0
  else if (seen.failed) stop = STEPS.indexOf(BREAKS[seen.failed])
  else if (!same) stop = STEPS.indexOf('go')

  STEPS.forEach((name, i) => {
    const step = steps.querySelector(`[data-step="${name}"]`)
    const word = field(step, 'ok')
    if (i < stop) {
      step.dataset.ok = 'true'
      word.textContent = say('holds')
    } else if (i === stop && seen.failed !== 'unsupported') {
      step.dataset.ok = 'false'
      word.textContent = say('fails')
    } else {
      delete step.dataset.ok
      word.textContent = ''
    }
  })

  for (const spot of steps.querySelectorAll('[data-k]:not(.verdict-word)')) spot.replaceChildren()
  if (booking && seen.record) {
    const rec = seen.record
    put(steps, 'id', big(rec.subarray(0, 16)).toString())
    put(steps, 'debit', big(rec.subarray(16, 32)).toString())
    put(steps, 'credit', big(rec.subarray(32, 48)).toString())
    put(steps, 'amount', money(big(rec.subarray(48, 64)).toString()))
    field(steps, 'at').append(stamp(Number(big(rec.subarray(92, 100)) / 1000000n)))
  }
  if (stop >= 1 && seen.leaf) put(steps, 'leaf', toHex(seen.leaf))
  if (stop >= 2 && seen.path) {
    const hops = seen.path.length
    put(steps, 'path', say('path', { leaf: grouped(seen.index + 1), leaves: grouped(seen.leaves), n: hops, word: say(hops === 1 ? 'w-hash' : 'w-hashes') }))
    field(steps, 'hashes').append(...seen.path.map((hash) => {
      const li = document.createElement('li')
      li.textContent = toHex(hash)
      return li
    }))
    put(steps, 'root', seen.root ? toHex(seen.root) : '')
  }
  if (stop >= 3 && seen.head) {
    put(steps, 'seq', grouped(seen.head.seq))
    put(steps, 'head', toHex(seen.head.hash))
  }
  if (stop >= 4 && seen.stamp) put(steps, 'key', toHex(seen.stamp.key))
  if (stop >= 5 && seen.signer !== undefined) put(steps, 'signer', seen.signer + 1)

  let says = ''
  if (booking && same) says = say('proof-ok', { n: seen.signer + 1 })
  else if (booking && seen.failed === 'unsupported') says = say('proof-unsupported')
  else if (booking && tampered && seen.failed === 'path') says = say('proof-changed')
  verdict.textContent = says
  return seen
}

function lay(seen) {
  const told = out.querySelectorAll('.proof-text, #proof-cmd')
  const id = seen.kind === TRANSFER && seen.record ? big(seen.record.subarray(0, 16)).toString() : ''
  for (const part of told) {
    put(part, 'signer', receipt.head.signer)
    put(part, 'seq', grouped(receipt.head.seq))
    put(part, 'base64', receipt.base64)
    put(part, 'id', id)
    put(part, 'cluster', receipt.cluster)
    receipt.keys.forEach((key, i) => put(part, `key${i + 1}`, key))
  }
}

async function guarded(btn, work) {
  if (busy) return
  busy = true
  btn.setAttribute('aria-busy', 'true')
  await work()
  btn.removeAttribute('aria-busy')
  busy = false
}

export function prove() {
  return guarded(document.getElementById('prove'), async () => {
    note.textContent = say('proof-loading')
    const res = await fetch('/proof', { cache: 'no-store' }).catch(() => null)
    if (!res) {
      note.textContent = ''
      return
    }
    if (!res.ok) {
      const why = await res.json().catch(() => ({}))
      note.textContent = REFUSED[why.error] ? say(REFUSED[why.error]) : say('refused', { status: res.status })
      return
    }
    receipt = await res.json()
    copy = fromHex(receipt.receipt)
    tampered = false
    lay(await draw())
    out.hidden = false
    note.textContent = ''
  })
}

export function tamper() {
  return guarded(document.getElementById('tamper'), async () => {
    copy[AMOUNT] += 1
    tampered = true
    await draw()
  })
}
