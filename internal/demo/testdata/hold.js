const button = document.getElementById('log-hold')
let ticks = 0

new MutationObserver(() => {
  ticks += 1
  if (ticks <= 2) button.click()
}).observe(document.querySelector('main'), { attributeFilter: ['data-now'] })
