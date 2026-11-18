const subtle = globalThis.crypto.subtle
const LINK = 113
const STAMP = 104
const RECORD = { 0: 0, 1: 96, 2: 100, 3: 100 }
const DOMAIN = new TextEncoder().encode('dmn-ledger head v1\n')

export const fromHex = (s) => Uint8Array.from(s.match(/../g) ?? [], (b) => parseInt(b, 16))
export const toHex = (b) => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')
const same = (a, b) => a.length === b.length && a.every((x, i) => x === b[i])
const u64 = (n) => {
  const b = new Uint8Array(8)
  new DataView(b.buffer).setBigUint64(0, n)
  return b
}
const join = (...parts) => {
  const buf = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  parts.reduce((at, p) => (buf.set(p, at), at + p.length), 0)
  return buf
}
const sha = async (...parts) => new Uint8Array(await subtle.digest('SHA-256', join(...parts)))

async function rootFromPath(leaf, index, size, path) {
  if (index >= size) return null
  let fn = index
  let sn = size - 1
  let r = leaf
  for (const p of path) {
    if (sn === 0) return null
    if ((fn & 1) === 1 || fn === sn) {
      r = await sha(Uint8Array.of(1), p, r)
      while ((fn & 1) === 0 && fn !== 0) { fn >>>= 1; sn >>>= 1 }
    } else {
      r = await sha(Uint8Array.of(1), r, p)
    }
    fn >>>= 1
    sn >>>= 1
  }
  return sn === 0 ? r : null
}

function readLink(b) {
  const view = new DataView(b.buffer, b.byteOffset, LINK)
  return {
    time: view.getBigUint64(0),
    op: b[8],
    events: view.getUint32(9),
    leaves: view.getUint32(13),
    body: b.subarray(17, 49),
    reply: b.subarray(49, 81),
    root: b.subarray(81),
  }
}

const signed = (rest) => rest.length === 0 || rest.length === STAMP

async function decode(proof, seen) {
  if (proof.length < 2 || proof[0] !== 1) return 'format'
  const view = new DataView(proof.buffer, proof.byteOffset, proof.length)
  const kind = proof[1]
  seen.kind = kind
  if (kind === 4) {
    if (proof.length < 42 || !signed(proof.subarray(42))) return 'format'
    seen.head = { seq: view.getBigUint64(2), hash: proof.subarray(10, 42) }
    if (seen.head.seq === 0n && seen.head.hash.some((x) => x !== 0)) return 'format'
    seen.rest = proof.subarray(42)
    return null
  }
  const size = RECORD[kind]
  const start = 2 + size + 5
  if (size === undefined || proof.length < start) return 'format'
  const hops = proof[start - 1]
  const at = start + 32 * hops
  const end = at + 8 + 32 + LINK
  if (hops > 32 || proof.length < end || !signed(proof.subarray(end))) return 'format'
  seen.index = view.getUint32(2 + size)
  if (kind === 0 && (seen.index !== 0 || hops !== 0)) return 'format'
  const seq = view.getBigUint64(at)
  seen.prev = proof.subarray(at + 8, at + 40)
  if (seq === 0n) return 'format'
  if (seq === 1n && seen.prev.some((x) => x !== 0)) return 'format'
  const raw = proof.subarray(at + 40, end)
  seen.link = readLink(raw)
  seen.leaves = seen.link.leaves
  if (kind !== 0) {
    seen.record = proof.subarray(2, 2 + size)
    seen.path = Array.from({ length: hops }, (_, i) => proof.subarray(start + 32 * i, start + 32 * (i + 1)))
    seen.leaf = await sha(Uint8Array.of(0, kind), seen.record)
    seen.root = await rootFromPath(seen.leaf, seen.index, seen.leaves, seen.path)
    if (seen.root === null || !same(seen.root, seen.link.root)) return 'path'
  }
  seen.head = { seq, hash: await sha(Uint8Array.of(2), seen.prev, u64(seq), raw) }
  seen.rest = proof.subarray(end)
  return null
}

async function stamped(stamp, head) {
  let pub
  try {
    pub = await subtle.importKey('raw', stamp.key, { name: 'Ed25519' }, false, ['verify'])
  } catch (err) {
    return err.name === 'NotSupportedError' ? 'unsupported' : 'signature'
  }
  const msg = join(DOMAIN, stamp.cluster, u64(head.seq), head.hash)
  return (await subtle.verify({ name: 'Ed25519' }, pub, stamp.signature, msg)) ? null : 'signature'
}

export async function check(proof, cluster, keys) {
  const seen = { ok: false, failed: null }
  const fail = (step) => Object.assign(seen, { failed: step })
  const broken = await decode(proof, seen)
  const rest = seen.rest
  delete seen.rest
  if (broken !== null) return fail(broken)
  if (rest.length === 0) return fail('unsigned')
  seen.stamp = { cluster: rest.subarray(0, 8), key: rest.subarray(8, 40), signature: rest.subarray(40) }
  const forged = await stamped(seen.stamp, seen.head)
  if (forged !== null) return fail(forged)
  if (!same(seen.stamp.cluster, cluster)) return fail('cluster')
  const signer = keys.findIndex((k) => same(k, seen.stamp.key))
  if (signer < 0) return fail('key')
  seen.signer = signer
  seen.ok = true
  return seen
}

export async function verify(proof, cluster, key) {
  const seen = await check(proof, cluster, [key])
  return seen.ok ? seen.head : null
}
