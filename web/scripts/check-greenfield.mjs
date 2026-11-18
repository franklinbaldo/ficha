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
      view.checks.push({page:'acervo',files});
      await page.screenshot({path:out+'/greenfield-acervo-'+viewport.name+'.png',fullPage:true});

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
