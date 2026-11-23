// Read-only browser smoke of all four built FICHA product pages.
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import { chromium } from 'playwright';

const base=process.argv[2] || 'http://127.0.0.1:4321/ficha/';
const out=process.argv[3] || '../accessibility-evidence';
await mkdir(out,{recursive:true});
const browser=await chromium.launch({headless:true});
const reports=[];
try {
  for(const viewport of [{width:375,height:812,name:'mobile'},
                         {width:1440,height:900,name:'desktop'}]) {
    const page=await browser.newPage({viewport:{width:viewport.width,height:viewport.height},
      serviceWorkers:'block'});
    const errors=[];
    page.on('pageerror',err=>errors.push(err.message));
    // This smoke tests shipped frontend and manifest only: never fetch an archival Parquet.
    await page.route('https://archive.org/**',route=>route.abort('blockedbyclient'));
    const view={viewport:viewport.name,checks:[]};
    try {
      await page.goto(base+'acervo/',{waitUntil:'domcontentloaded'});
      await page.waitForFunction(()=>document.querySelectorAll('#acervo-resultado .archive-file').length>0);
      const files=await page.locator('#acervo-resultado .archive-file').count();
      assert.ok(files>=9,'catálogo precisa incluir arquivos da competência real');
      const select=page.locator('#acervo-competencia');
      assert.ok(await select.locator('option').count()>=2);
      assert.ok(await page.locator('#acervo-resultado').textContent());
      // Allow the initial SSG resources to settle before auditing local-only interactions.
      await page.waitForLoadState('networkidle');
      const filterRequests=[];
      const trackFilterRequest=request=>filterRequests.push(request.method()+' '+request.url());
      page.on('request',trackFilterRequest);
      const filter=page.getByRole('searchbox',{name:'Filtrar arquivos desta competência'});
      const counter=page.locator('#acervo-contagem');
      assert.equal((await counter.textContent()).trim(),files+' de '+files+' arquivos exibidos');
      const archiveUrl=page.url();
      await filter.focus();
      await page.keyboard.type('companies/0');
      const matches=await page.locator('#acervo-resultado .archive-file').count();
      assert.ok(matches>0 && matches<files,'filtro deve reduzir a lista localmente');
      assert.equal((await counter.textContent()).trim(),matches+' de '+files+' arquivos exibidos');
      assert.equal(page.url(),archiveUrl,'filtro não altera URL de competência');
      const firstFiltered=page.locator('#acervo-resultado .archive-file').first();
      const firstKey=(await firstFiltered.locator('summary strong').textContent()).trim();
      await firstFiltered.locator('summary').press('Enter');
      const catalogLink=firstFiltered.getByRole('link',{
        name:'Abrir arquivo indicado no catálogo: '+firstKey,
      });
      assert.equal(await catalogLink.count(),1,
        'arquivo filtrado deve conservar nome acessível único e proveniência cautelosa');
      assert.ok((await catalogLink.getAttribute('href')).startsWith('https://'));
      await catalogLink.focus();
      assert.equal(await catalogLink.evaluate(el=>document.activeElement===el),true,
        'link do arquivo deve receber foco dentro do disclosure');
      await filter.fill('nenhum-arquivo-corresponde');
      assert.equal(await page.locator('#acervo-resultado .archive-file').count(),0);
      assert.match(await page.locator('#acervo-resultado').textContent(),/Nenhum arquivo corresponde ao filtro/);
      await page.getByRole('button',{name:'Limpar filtro'}).focus();
      await page.keyboard.press('Enter');
      assert.equal(await page.locator('#acervo-resultado .archive-file').count(),files);
      assert.equal(await filter.inputValue(),'');
      assert.equal(await filter.evaluate(el=>document.activeElement===el),true,'limpar devolve foco');
      const originalDate=await select.inputValue();
      await filter.fill('companies/00');
      await select.selectOption({index:1});
      assert.equal(await filter.inputValue(),'','trocar competência limpa filtro');
      const newCount=await page.locator('#acervo-resultado .archive-file').count();
      assert.equal((await counter.textContent()).trim(),newCount+' de '+newCount+' arquivos exibidos');
      await select.selectOption(originalDate);
      assert.equal(await page.locator('#acervo-resultado .archive-file').count(),files);
      page.off('request',trackFilterRequest);
      assert.deepEqual(filterRequests,[],
        'filtro, limpeza e troca de competência não podem criar requisições de rede');
      view.checks.push({page:'acervo',files,filterKeyboard:true,filtered:matches,
        requestsDuringFilter:filterRequests.length});
      await page.screenshot({path:out+'/greenfield-acervo-'+viewport.name+'.png',fullPage:true});

      // Failure-mode regression: the SSG must not expose an inert, enabled filter.
      const outage=await browser.newPage({viewport:{width:viewport.width,height:viewport.height},
        serviceWorkers:'block'});
      try {
        await outage.route('**/manifest.json',route=>route.fulfill({
          status:503,contentType:'application/json',body:'{}'}));
        await outage.goto(base+'acervo/',{waitUntil:'domcontentloaded'});
        await outage.waitForFunction(()=>document.querySelector('#acervo-estado')?.textContent
          .includes('Não foi possível abrir o acervo'));
        assert.equal(await outage.locator('#acervo-filtro').isDisabled(),true);
        assert.equal(await outage.locator('#acervo-limpar').isDisabled(),true);
        assert.match(await outage.locator('#acervo-contagem').textContent(),
          /Lista de arquivos indisponível: filtro desativado/);
        assert.equal(await outage.locator('#acervo-resultado .archive-file').count(),0);
        view.checks.push({page:'acervo-outage',status:'filter-disabled-on-503'});
      } finally { await outage.close(); }

      await page.goto(base+'explorar/',{waitUntil:'domcontentloaded'});
      await page.waitForFunction(()=>document.querySelectorAll('.asset-row').length>0 &&
        document.querySelectorAll('.series-row').length>0);
      const count=await page.locator('.series-row').count();
      assert.ok(count>=2,'série deve refletir snapshots do manifesto');
      view.checks.push({page:'explorar',competences:count});
      await page.screenshot({path:out+'/greenfield-explorar-'+viewport.name+'.png',fullPage:true});

      await page.goto(base+'historia/',{waitUntil:'domcontentloaded'});
      await page.waitForFunction(()=>document.querySelectorAll('.timeline-item').length>0);
      assert.ok(await page.locator('.timeline-item').count()>=2);
      view.checks.push({page:'historia',snapshots:await page.locator('.timeline-item').count()});
      await page.screenshot({path:out+'/greenfield-historia-'+viewport.name+'.png',fullPage:true});

      await page.goto(base+'empresa/',{waitUntil:'domcontentloaded'});
      await page.waitForFunction(()=>document.querySelectorAll('#empresa-competencia option').length>0);
      await page.locator('#empresa-cnpj').fill('123');
      await page.locator('#empresa-enviar').click();
      assert.match(await page.locator('#empresa-estado').textContent(),/CNPJ completo/);
      assert.equal(await page.locator('#empresa-resultado').textContent(),'');
      view.checks.push({page:'empresa',invalidInputRejected:true});
      await page.screenshot({path:out+'/greenfield-empresa-'+viewport.name+'.png',fullPage:true});

      assert.deepEqual(errors,[],'nenhum erro de JS nas páginas novas');
      reports.push({...view,status:'PASS'});
    }catch(error){
      reports.push({...view,status:'FAIL',error:String(error),pageErrors:errors});
      throw error;
    }finally{await page.close();}
  }
}finally{
  await browser.close();
  await writeFile(out+'/greenfield-browser-report.json',JSON.stringify(reports,null,2)+'\n');
}
console.log('FICHA_GREENFIELD_BROWSER_PASS '+JSON.stringify(reports));
