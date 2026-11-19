import { fromHex, toHex, verify } from '/static/receipt.js'

const out = document.getElementById('out')
try {
  const vectors = await (await fetch('/vectors.json')).json()
  let differ = 0
  for (const v of vectors) {
    const head = await verify(fromHex(v.proof), fromHex(v.cluster), fromHex(v.key))
    const agrees = v.valid
      ? head !== null && head.seq === BigInt(v.seq) && toHex(head.hash) === v.head
      : head === null
    if (!agrees) differ++
  }
  out.textContent = `vectors ${vectors.length} differ ${differ}`
} catch (err) {
  out.textContent = `error ${err}`
}
