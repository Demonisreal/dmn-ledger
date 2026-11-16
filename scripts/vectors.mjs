import { readFileSync } from 'node:fs'

const subtle = globalThis.crypto.subtle
const LINK = 113
const STAMP = 104
const RECORD = { 0: 0, 1: 96, 2: 100, 3: 100 }

const fromHex = (s) => Uint8Array.from(s.match(/../g) ?? [], (b) => parseInt(b, 16))
const toHex = (b) => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')
const same = (a, b) => a.length === b.length && a.every((x, i) => x === b[i])
const u64 = (n) => {
  const b = new Uint8Array(8)
  new DataView(b.buffer).setBigUint64(0, n)
  return b
}
const sha = async (...parts) => {
  const buf = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  parts.reduce((at, p) => (buf.set(p, at), at + p.length), 0)
  return new Uint8Array(await subtle.digest('SHA-256', buf))
}

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

async function decode(proof) {
  if (proof.length < 2 || proof[0] !== 1) return null
  const view = new DataView(proof.buffer, proof.byteOffset, proof.length)
  const kind = proof[1]
  if (kind === 4) {
    if (proof.length < 42) return null
    const head = { seq: view.getBigUint64(2), hash: proof.subarray(10, 42) }
    if (head.seq === 0n && head.hash.some((x) => x !== 0)) return null
    return { head, stamp: proof.subarray(42) }
  }
  const size = RECORD[kind]
  const start = 2 + size + 5
  if (size === undefined || proof.length < start) return null
  const index = view.getUint32(2 + size)
  const hops = proof[start - 1]
  const at = start + 32 * hops
  const end = at + 8 + 32 + LINK
  if (hops > 32 || proof.length < end) return null
  const seq = view.getBigUint64(at)
  const link = proof.subarray(at + 40, end)
  if (seq === 0n) return null
  if (kind === 0 && (index !== 0 || hops !== 0)) return null
  if (kind !== 0) {
    const path = Array.from({ length: hops }, (_, i) => proof.subarray(start + 32 * i, start + 32 * (i + 1)))
    const leaves = new DataView(link.buffer, link.byteOffset, LINK).getUint32(13)
    const leaf = await sha(Uint8Array.of(0, kind), proof.subarray(2, 2 + size))
    const root = await rootFromPath(leaf, index, leaves, path)
    if (root === null || !same(root, link.subarray(LINK - 32))) return null
  }
  const hash = await sha(Uint8Array.of(2), proof.subarray(at + 8, at + 40), u64(seq), link)
  return { head: { seq, hash }, stamp: proof.subarray(end) }
}

async function verify(proof, cluster, key) {
  const receipt = await decode(proof)
  if (receipt === null || receipt.stamp.length !== STAMP) return null
  const { head, stamp } = receipt
  if (!same(stamp.subarray(0, 8), cluster) || !same(stamp.subarray(8, 40), key)) return null
  const domain = new TextEncoder().encode('dmn-ledger head v1\n')
  const msg = new Uint8Array([...domain, ...cluster, ...u64(head.seq), ...head.hash])
  const pub = await subtle.importKey('raw', key, { name: 'Ed25519' }, false, ['verify'])
  return (await subtle.verify({ name: 'Ed25519' }, pub, stamp.subarray(40), msg)) ? head : null
}

const file = process.argv[2] ?? new URL('../internal/notary/testdata/vectors.json', import.meta.url)
const vectors = JSON.parse(readFileSync(file, 'utf8'))
let differ = 0
for (const v of vectors) {
  const head = await verify(fromHex(v.proof), fromHex(v.cluster), fromHex(v.key))
  const agrees = v.valid
    ? head !== null && head.seq === BigInt(v.seq) && toHex(head.hash) === v.head
    : head === null
  if (!agrees) differ++
  console.log(agrees ? 'ok  ' : 'FAIL', v.valid ? 'valid  ' : 'invalid', v.name)
}
console.log(differ === 0 ? `all ${vectors.length} vectors agree with Go` : `${differ} of ${vectors.length} vectors differ from Go`)
process.exit(differ === 0 ? 0 : 1)
