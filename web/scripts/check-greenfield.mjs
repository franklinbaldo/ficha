// Read-only browser smoke of all four built FICHA product pages.
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import { chromium } from 'playwright';

const base=process.argv[2] || 'http://127.0.0.1:4321/ficha/';
const out=process.argv[3] || '../accessibility-evidence';
await mkdir(out,{recursive:true});
const browser=await chromium.launch({headless:true});
const reports=[];
// WCAG 2.x relative luminance/contrast for browser-computed opaque RGB colors.
function contrastRatio(colorA,colorB) {
  const luminance=color=>{
    const match=/^rgb\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)$/.exec(color);
    assert.ok(match,'opaque RGB color expected from getComputedStyle: '+color);
    const rgb=match.slice(1).map(channel=>Number(channel)/255)
      .map(value=>value<=0.04045?value/12.92:((value+0.055)/1.055)**2.4);
    return 0.2126*rgb[0]+0.7152*rgb[1]+0.0722*rgb[2];
  };
  const a=luminance(colorA),b=luminance(colorB);
  return (Math.max(a,b)+0.05)/(Math.min(a,b)+0.05);
}

try {
  for(const viewport of [{width:320,height:720,name:'reflow-320'},
                         {width:375,height:812,name:'mobile'},
                         {width:1440,height:900,name:'desktop'},
                         {width:320,height:720,name:'text-200-320',textScale:2}]) {
    const page=await browser.newPage({viewport:{width:viewport.width,height:viewport.height},
      serviceWorkers:'block'});
    const errors=[];
    page.on('pageerror',err=>errors.push(err.message));
    // This smoke tests shipped frontend and manifest only: never fetch an archival Parquet.
    await page.route('https://archive.org/**',route=>route.abort('blockedbyclient'));
    const view={viewport:viewport.name,checks:[]};
    try {
      await page.goto(base+'acervo/',{waitUntil:'domcontentloaded'});
      if(viewport.textScale) {
        // CSS text enlargement probes reflow, not native browser zoom.
        const regular=await page.evaluate(()=>parseFloat(
          getComputedStyle(document.documentElement).fontSize));
        const regularButton=await page.locator('#acervo-limpar').evaluate(el=>
          parseFloat(getComputedStyle(el).fontSize));
        await page.addStyleTag({content:'html { font-size: 200% !important; }'});
        const enlarged=await page.evaluate(()=>parseFloat(
          getComputedStyle(document.documentElement).fontSize));
        const enlargedButton=await page.locator('#acervo-limpar').evaluate(el=>
          parseFloat(getComputedStyle(el).fontSize));
        assert.ok(enlarged>=regular*1.99,
          '200% text probe must actually enlarge the root font: '+regular+' to '+enlarged);
        assert.ok(enlargedButton>=regularButton*1.99,
          'Limpar filtro deve ampliar com a raiz (não usar fonte fixa do navegador): '+
            JSON.stringify({regularButton,enlargedButton}));
        view.checks.push({page:'acervo-text-scale',regular,enlarged,
          regularButton,enlargedButton});
      }
      await page.waitForFunction(()=>document.querySelectorAll('#acervo-resultado .archive-file').length>0);
      const files=await page.locator('#acervo-resultado .archive-file').count();
      assert.ok(files>=9,'catálogo precisa incluir arquivos da competência real');
      const archiveRegion=page.getByRole('region',{name:'Arquivos declarados no manifesto'});
      assert.equal(await archiveRegion.count(),1,'lista deve expor uma região nomeada');
      assert.equal(await archiveRegion.locator('details.archive-file').count(),files);
      // Capture evidence from the unfiltered DOM. Filtering must not rewrite the
      // selected member's declared size, digest or destination URL.
      const beforeFilter=await page.evaluate(()=>{
        const item=[...document.querySelectorAll('#acervo-resultado .archive-file')]
          .find(el=>el.querySelector('summary strong')?.textContent.trim().startsWith('companies/0'));
        if(!item) return null;
        return {
          key:item.querySelector('summary strong')?.textContent.trim(),
          size:item.querySelector('summary span')?.textContent.trim(),
          digest:item.querySelector('.archive-detail code')?.textContent.trim()??null,
          url:item.querySelector('.archive-detail a')?.getAttribute('href')??null,
        };
      });
      assert.ok(beforeFilter?.key,'controle não filtrado precisa conter companies/0');
      const declaredSummary=await page.locator('#acervo-resultado .stat-strip').textContent();
      if(viewport.width===320) {
        const dimensions=await page.evaluate(()=>({
          scrollWidth:document.documentElement.scrollWidth,
          viewportWidth:window.innerWidth,
        }));
        assert.ok(dimensions.scrollWidth<=dimensions.viewportWidth+1,
          'Acervo a 320 CSS px não deve exigir rolagem horizontal: '+JSON.stringify(dimensions));
      }
      const select=page.locator('#acervo-competencia');
      assert.ok(await select.locator('option').count()>=2);
      assert.equal(await page.locator('#acervo-competencias small')
        .filter({hasText:'Referência'}).count(),1,
        'referência continua identificada fora do seletor, sem truncamento do valor');
      assert.equal(await select.isDisabled(),false,'competência ativa após manifesto');
      // A native select may truncate its selected option without page overflow.
      // Measure rendered label width and leave space for the platform arrow.
      if(viewport.width<=375) {
        const selectedLabel=await select.evaluate(element=>{
          const style=getComputedStyle(element);
          const canvas=document.createElement('canvas');
          const context=canvas.getContext('2d');
          context.font=style.font;
          const label=element.selectedOptions[0]?.textContent.trim()??'';
          const width=context.measureText(label).width;
          const space=element.getBoundingClientRect().width-
            parseFloat(style.paddingLeft)-parseFloat(style.paddingRight)-32;
          return {label,width,space};
        });
        assert.match(selectedLabel.label,/^\d{4}-\d{2}$/,
          'opções nativas devem exibir a data compacta, inclusive com texto a 200%');
        assert.ok(selectedLabel.width<=selectedLabel.space,
          'texto selecionado precisa caber com reserva de 32px para seta nativa: '+
            JSON.stringify(selectedLabel));
        view.checks.push({page:'acervo-select',selectedLabel});
      }
      const competenceStatus=(await page.locator('#acervo-estado').textContent()).trim();
      assert.match(competenceStatus,/^\d+ competências? declaradas? no manifesto publicado\.$/);
      assert.ok(await page.locator('#acervo-resultado').textContent());
      // Allow the initial SSG resources to settle before auditing local-only interactions.
      await page.waitForLoadState('networkidle');
      const filterRequests=[];
      const trackFilterRequest=request=>filterRequests.push(request.method()+' '+request.url());
      page.on('request',trackFilterRequest);
      const filter=page.getByRole('searchbox',{name:'Filtrar arquivos desta competência'});
      // Visual affordance: input and native select borders must be perceivable
      // both against their fill and the page. This is a component regression,
      // not a claim of site-wide WCAG conformance.
      const contrastSamples=await page.evaluate(()=>{
        const paper=getComputedStyle(document.body).backgroundColor;
        const fields=['#acervo-filtro','#acervo-competencia'].map(selector=>{
          const style=getComputedStyle(document.querySelector(selector));
          return {selector,border:style.borderTopColor,fill:style.backgroundColor,
            width:style.borderTopWidth,kind:style.borderTopStyle};
        });
        return {paper,fields};
      });
      for(const field of contrastSamples.fields) {
        assert.equal(field.width,'1px',field.selector+' must retain a visible border');
        assert.equal(field.kind,'solid',field.selector+' border style');
        for(const [surface,color] of Object.entries({paper:contrastSamples.paper,fill:field.fill})) {
          const ratio=contrastRatio(field.border,color);
          assert.ok(ratio>=3,field.selector+' border contrast against '+surface+
            ' must be >= 3:1, got '+ratio.toFixed(3)+':1');
        }
      }
      view.checks.push({page:'acervo-border-contrast',...contrastSamples,
        minimumContrast:Math.min(...contrastSamples.fields.flatMap(field=>[
          contrastRatio(field.border,contrastSamples.paper),
          contrastRatio(field.border,field.fill)]))});
      const counter=page.locator('#acervo-contagem');
      assert.equal((await counter.textContent()).trim(),files+' de '+files+' arquivos exibidos');
      const archiveUrl=page.url();
      await filter.focus();
      const focusRing=await filter.evaluate(element=>{
        const style=getComputedStyle(element);
        return {visible:element.matches(':focus-visible'),kind:style.outlineStyle,
          width:parseFloat(style.outlineWidth)};
      });
      assert.ok(focusRing.visible,'keyboard-ready filter must show :focus-visible');
      assert.equal(focusRing.kind,'solid','focus indicator must remain visible');
      assert.ok(focusRing.width>=3,'focus indicator must retain at least 3 CSS px');
      await page.keyboard.type('companies/0');
      const matches=await page.locator('#acervo-resultado .archive-file').count();
      assert.ok(matches>0 && matches<files,'filtro deve reduzir a lista localmente');
      assert.equal(await archiveRegion.locator('details.archive-file').count(),matches,
        'região acessível deve acompanhar os resultados filtrados');
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
      assert.equal(await archiveRegion.getByRole('link',{
        name:'Abrir arquivo indicado no catálogo: '+firstKey,
      }).count(),1,'link filtrado deve pertencer à região nomeada');
      assert.ok((await catalogLink.getAttribute('href')).startsWith('https://'));
      assert.equal(await catalogLink.getAttribute('target'),'_blank',
        'destino externo deve abrir em nova aba');
      assert.equal(await catalogLink.getAttribute('rel'),'noopener noreferrer',
        'nova aba externa precisa impedir acesso a window.opener');
      const afterFilter=await firstFiltered.evaluate(item=>({
        key:item.querySelector('summary strong')?.textContent.trim(),
        size:item.querySelector('summary span')?.textContent.trim(),
        digest:item.querySelector('.archive-detail code')?.textContent.trim()??null,
        url:item.querySelector('.archive-detail a')?.getAttribute('href')??null,
      }));
      assert.deepEqual(afterFilter,beforeFilter,
        'filtro não pode alterar chave, tamanho, digest ou URL declarados do arquivo');
      assert.equal(await page.locator('#acervo-resultado .stat-strip').textContent(),
        declaredSummary,'filtrar não pode alterar totais declarados do catálogo');
      // Opening <details> via Enter must permit natural keyboard traversal.
      // Programmatic .focus() would conceal a broken Tab order.
      await page.keyboard.press('Tab');
      assert.equal(await catalogLink.evaluate(el=>document.activeElement===el),true,
        'Tab a partir do summary deve alcançar o link do arquivo');
      if(viewport.width===320) {
        const dimensions=await page.evaluate(()=>({
          scrollWidth:document.documentElement.scrollWidth,
          viewportWidth:window.innerWidth,
        }));
        assert.ok(dimensions.scrollWidth<=dimensions.viewportWidth+1,
          'Detalhe aberto com digest e link deve reflow a 320 CSS px: '+JSON.stringify(dimensions));
      }
      await filter.fill('nenhum-arquivo-corresponde');
      assert.equal(await page.locator('#acervo-resultado .archive-file').count(),0);
      assert.match(await page.locator('#acervo-resultado').textContent(),/Nenhum arquivo corresponde ao filtro/);
      const clearButton=page.getByRole('button',{name:'Limpar filtro'});
      assert.equal(await clearButton.isEnabled(),true,'limpar deve estar habilitado com filtro ativo');
      if(viewport.textScale) {
        const clearLayout=await clearButton.evaluate(el=>{
          const r=el.getBoundingClientRect();
          return {left:r.left,right:r.right,viewportWidth:innerWidth,
            scrollWidth:document.documentElement.scrollWidth};
        });
        assert.ok(clearLayout.left>=0 && clearLayout.right<=clearLayout.viewportWidth+1 &&
          clearLayout.scrollWidth<=clearLayout.viewportWidth+1,
          'Limpar filtro ampliado deve caber sem rolagem horizontal: '+JSON.stringify(clearLayout));
        view.checks.push({page:'acervo-clear-button',...clearLayout});
      }
      await clearButton.focus();
      await page.keyboard.press('Enter');
      assert.equal(await page.locator('#acervo-resultado .archive-file').count(),files);
      if(viewport.textScale) {
        const afterClear=await page.evaluate(()=>({
          scrollWidth:document.documentElement.scrollWidth,viewportWidth:innerWidth,
        }));
        assert.ok(afterClear.scrollWidth<=afterClear.viewportWidth+1,
          'Limpar com fonte ampliada não deve causar rolagem horizontal: '+JSON.stringify(afterClear));
      }
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
        preservesCatalogIdentity:true,requestsDuringFilter:filterRequests.length});
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
        assert.equal(await outage.locator('#acervo-competencia').isDisabled(),true);
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
