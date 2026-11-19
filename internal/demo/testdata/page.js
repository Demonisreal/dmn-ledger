document.addEventListener('securitypolicyviolation', (e) => console.error(`Refused to ${e.effectiveDirective}: ${e.blockedURI}`))

const main = document.querySelector('main')
const conn = document.getElementById('conn')
const shots = document.createElement('div')
shots.id = 'shots'
shots.hidden = true
document.body.append(shots)

new MutationObserver(() => {
  const copy = main.cloneNode(true)
  for (const named of copy.querySelectorAll('[id], [name]')) {
    named.removeAttribute('id')
    named.removeAttribute('name')
  }
  shots.append(copy)
}).observe(main, { attributeFilter: ['data-now'] })
new MutationObserver(() => {
  const said = document.createElement('p')
  said.dataset.conn = ''
  said.textContent = conn.textContent
  shots.append(said)
}).observe(conn, { childList: true, characterData: true, subtree: true })
