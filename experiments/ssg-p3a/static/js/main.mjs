import {createLookupController} from './consult.mjs';
const form=document.querySelector('#search-form');
const kind=document.querySelector('#search-kind');
const term=document.querySelector('#search-term');
const status=document.querySelector('#search-status');
const result=document.querySelector('#search-result');
const content=document.querySelector('#result-content');
// Mock seguro: NÃO consultar rede, IA ou dados cadastrais reais.
const controller=createLookupController(async()=>({state:'unavailable'}));
function render(s) {
  result.hidden=s.state!=='found';
  content.textContent=s.state==='found'?s.message:'';
  status.textContent=s.state==='found'?'Consulta concluída.':s.message;
  status.dataset.state=['error','invalid'].includes(s.state)?'error':s.state;
}
function reset(){controller.invalidate();render({state:'idle',message:'Nenhuma consulta realizada com este termo.'});}
if(form&&kind&&term&&status&&result&&content){
  term.addEventListener('input',reset);
  kind.addEventListener('change',reset);
  form.addEventListener('submit',async e=>{
    e.preventDefault();
    const k=kind.value,t=term.value;
    render({state:'loading',message:'Consultando…'});
    const response=await controller.submit(k,t);
    if(response.state!=='stale') render(response);
  });
}
