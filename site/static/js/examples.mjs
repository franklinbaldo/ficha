/** Curated, publicly sourced CNPJ links. Never invent registration numbers at runtime. */
export function pickExamplePills(items, random = Math.random, count = 3) {
  const choices = [...items];
  for (let i = choices.length - 1; i > 0; i--) {
    const draw = random();
    const index = Math.max(0, Math.min(i, Math.floor(draw * (i + 1))));
    [choices[i], choices[index]] = [choices[index], choices[i]];
  }
  return choices.slice(0, Math.max(0, count));
}

/** Randomize visible examples only. Links work without JavaScript as well. */
export function mountExamplePills(documentRef, random = Math.random) {
  const container = documentRef.getElementById('example-pills');
  if (!container) return;
  const links = container.querySelectorAll('[data-cnpj-example]');
  container.replaceChildren(...pickExamplePills(links, random));
}
