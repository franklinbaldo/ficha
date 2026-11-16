import assert from 'node:assert/strict';
import test from 'node:test';
import {normalizeCnpj,validateSearch,createLookupController} from '../static/js/consult.mjs';
test('preserva letras e zeros do CNPJ alfanumérico',()=>{
  assert.equal(normalizeCnpj('00.000.000/E08G-12'),'00000000E08G12');
  assert.deepEqual(validateSearch('cnpj','00.000.000/E08G-12'),{ok:true,term:'00000000E08G12'});
});
test('inválidos não chamam adaptador',async()=>{
  let n=0;const c=createLookupController(async()=>{n++;return {state:'found',label:'x'}});
  assert.equal((await c.submit('cnpj','')).state,'invalid');
  assert.equal((await c.submit('cnpj','00.000.000/E08G-1')).state,'invalid');
  assert.equal(n,0);
});
test('ausência não é indisponibilidade nem erro',async()=>{
  const mk=value=>createLookupController(async()=>value);
  assert.equal((await mk({state:'not-found'}).submit('cnpj','00000000E08G12')).state,'not-found');
  assert.equal((await mk({state:'unavailable'}).submit('cnpj','00000000E08G12')).state,'unavailable');
  const bad=createLookupController(async()=>{throw Error('rede')});
  assert.equal((await bad.submit('cnpj','00000000E08G12')).state,'error');
});
test('resposta obsoleta ao trocar modalidade não aparece',async()=>{
  let resolve;const c=createLookupController(()=>new Promise(r=>{resolve=r}));
  const first=c.submit('pessoa','MARIA');c.invalidate();
  resolve({state:'found',label:'anterior'});
  assert.equal((await first).state,'stale');
});
test('mock controlado entrega valor, não fabrica dados',async()=>{
  const c=createLookupController(async({term})=>({state:'found',label:term}));
  assert.deepEqual(await c.submit('cnpj','00.000.000/E08G-12'),{state:'found',message:'00000000E08G12'});
});
