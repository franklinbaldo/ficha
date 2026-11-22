// Fail if a change to the shared canonical decoder can bypass the SSG
// build or browser audit solely because of workflow path filters.
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import test from 'node:test';

function triggerPaths(workflow, event) {
  const text=readFileSync(new URL('../../.github/workflows/'+workflow,import.meta.url),'utf8');
  const paths=[];
  let inOn=false, active=false, inPaths=false;
  for(const line of text.split(/\r?\n/)){
    if(line==='on:'){inOn=true;continue;}
    if(!inOn)continue;
    if(/^\S/.test(line) && line.trim())break;
    const header=line.match(/^  ([a-z_]+):/);
    if(header){active=header[1]===event;inPaths=false;continue;}
    if(!active)continue;
    if(/^    paths:/.test(line)){inPaths=true;continue;}
    if(inPaths){
      const match=line.match(/^      - ['"]([^'"]+)['"]\s*$/);
      if(match)paths.push(match[1]);
    }
  }
  assert.ok(paths.length,workflow+' must declare on.'+event+'.paths');
  return paths;
}
function matches(patterns, file) {
  return patterns.some(pattern=>pattern===file ||
    (pattern.endsWith('/**') && file.startsWith(pattern.slice(0,-3)+'/')));
}

const requirements={
  'deploy.yml':{
    push:['site/static/js/atomic-browser.mjs','web/src/generated/company.pb.js',
      'web/package.json','web/bun.lock'],
    pull_request:['site/static/js/atomic-browser.mjs','web/src/generated/company.pb.js',
      'web/package.json','web/bun.lock'],
  },
  'visual-capture.yml':{
    pull_request:['site/static/js/atomic-browser.mjs',
      'web/src/generated/company.pb.js','web/scripts/check-atomic-ssg.mjs',
      'web/package.json','web/bun.lock'],
  },
};
for(const [workflow,events] of Object.entries(requirements)){
  for(const [event,needed] of Object.entries(events)){
    test(workflow+' on.'+event+' covers each SSG decoder dependency independently',()=>{
      const paths=triggerPaths(workflow,event);
      for(const file of needed){
        assert.ok(matches(paths,file),file+' does not activate '+workflow+' on '+event);
      }
    });
    test(workflow+' on.'+event+' does not activate for unrelated docs',()=>{
      assert.equal(matches(triggerPaths(workflow,event),'docs/unrelated-notes.md'),false);
    });
  }
}
