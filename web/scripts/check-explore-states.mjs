// Browser replay of Explorar against the built SSG, with an intercepted manifest.
import assert from 'node:assert/strict';

const fixture=()=>({current:'2026-06',snapshots:[
  {date:'2026-06',files:{cnpjs:{size:100},socios:{size:20}},row_counts:{cnpjs:0}},
  {date:'2026-07',files:{cnpjs:{size:200}},row_counts:{socios:8}},
]});

export async function checkExploreStates(browser,base,viewport) {
  const origin=new URL(base).origin;
  const requested=new URL('explorar/?competencia=2026-13&keep=1',base).href;
  const manifestUrl=new URL('manifest.json',base).href;
  const valid=fixture(), invalid=fixture(), noCounts=fixture();
  invalid.snapshots[1].files.cnpjs=null;
  for(const snapshot of noCounts.snapshots) delete snapshot.row_counts;
  const cases=[
    ['503',503,JSON.stringify(valid)],
    ['206',206,JSON.stringify(valid)],
    ['200-range',200,JSON.stringify(valid),{'content-range':'bytes 0-1/2'}],
    ['bad-json',200,'{'],
    ['invalid-other-month',200,JSON.stringify(invalid)],
    ['valid',200,JSON.stringify(valid)],
    ['no-series',200,JSON.stringify(noCounts)],
  ];
  const results=[];
  for(const [name,status,body,headers] of cases) {
    const positive=name==='valid'||name==='no-series';
    const page=await browser.newPage({viewport:{width:viewport.width,height:viewport.height},serviceWorkers:'block'});
    const errors=[],hits=[];
    let release;
    const gate=new Promise(resolve=>{release=resolve;});
    page.on('pageerror',error=>errors.push(error.message));
    try {
      await page.route('**/*',async route=>{
        const url=route.request().url();
        if(new URL(url).origin!==origin) return route.abort('blockedbyclient');
        if(url!==manifestUrl) return route.continue();
        hits.push(route.request().method());
        await gate;
        await route.fulfill({status,body,headers:{'content-type':'application/json',...headers}});
      });
      const arrival=page.waitForRequest(manifestUrl,{timeout:10000});
      await Promise.all([arrival,page.goto(requested,{waitUntil:'domcontentloaded'})]);
      const state=page.locator('#explorar-estado'),date=page.locator('#explorar-competencia');
      const filter=page.locator('#explorar-filtro'),kind=page.locator('#explorar-serie');
      const outputs=['contador','grafico','serie-grafico'].map(id=>page.locator('#explorar-'+id));
      const disabled=()=>Promise.all([date,filter,kind].map(control=>control.isDisabled()));
      const empty=async()=>assert.deepEqual(await Promise.all(outputs.map(x=>x.textContent())),['','','']);
      assert.equal(await state.getAttribute('role'),'status',name);
      assert.equal(await state.textContent(),'Lendo o manifesto…',name);
      assert.deepEqual(await disabled(),[true,true,true],name+' loading');
      await empty();
      release();
      await page.waitForFunction(expected=>document.querySelector('#explorar-estado')?.dataset.kind===expected,positive?'info':'error');
      if(!positive) {
        assert.match(await state.textContent(),/Exploração indisponível/);
        assert.deepEqual(await disabled(),[true,true,true],name);
        assert.equal(page.url(),requested,name);
        await empty();
        await filter.dispatchEvent('input');
        await date.dispatchEvent('change');
        await kind.dispatchEvent('change');
        await empty();
      } else {
        assert.deepEqual(await disabled(),[false,false,name==='no-series'],name);
        assert.equal(await date.inputValue(),'2026-06');
        assert.match(await state.textContent(),/Competência solicitada indisponível/);
        const query=()=>new URL(page.url()).searchParams;
        assert.equal(query().get('competencia'),'2026-06');
        assert.equal(query().get('keep'),'1');
        if(name==='no-series') assert.match(await outputs[2].textContent(),/Nenhuma série de linhas declarada/);
        else {
          assert.match(await outputs[2].textContent(),/0 linhas/);
          assert.match(await outputs[2].textContent(),/Não declarado/);
        }
        const bar=page.locator('.asset-row').filter({hasText:'socios'}).locator('.asset-bar');
        const before=await bar.evaluate(el=>el.style.width);
        await filter.fill('socios');
        assert.doesNotMatch(await outputs[1].textContent(),/cnpjs/);
        assert.equal(await bar.evaluate(el=>el.style.width),before);
        assert.match(await state.textContent(),/Competência solicitada indisponível/);
        await date.selectOption('2026-07');
        assert.doesNotMatch(await state.textContent(),/Competência solicitada indisponível/);
        assert.equal(query().get('competencia'),'2026-07');
        assert.equal(query().get('keep'),'1');
      }
      assert.deepEqual(hits,['GET'],name);
      assert.deepEqual(errors,[],name);
      results.push({case:name,status:'PASS',manifestGets:hits.length});
    } finally {
      release();
      await page.close();
    }
  }
  return results;
}
