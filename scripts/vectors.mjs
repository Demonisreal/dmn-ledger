import { readFileSync } from 'node:fs'
import { agrees, fromHex, verify } from '../internal/demo/web/receipt.js'

const file = process.argv[2] ?? new URL('../internal/notary/testdata/vectors.json', import.meta.url)
const vectors = JSON.parse(readFileSync(file, 'utf8'))
let differ = 0
for (const v of vectors) {
  const head = await verify(fromHex(v.proof), fromHex(v.cluster), fromHex(v.key))
  const matches = v.valid ? head !== null && agrees(head, v.seq, v.head) : head === null
  if (!matches) differ++
  console.log(matches ? 'ok  ' : 'FAIL', v.valid ? 'valid  ' : 'invalid', v.name)
}
console.log(differ === 0 ? `all ${vectors.length} vectors agree with Go` : `${differ} of ${vectors.length} vectors differ from Go`)
process.exit(differ === 0 ? 0 : 1)
