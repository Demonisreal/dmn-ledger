import { readFileSync } from 'node:fs'
import { fromHex, toHex, verify } from '../internal/demo/web/receipt.js'

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
