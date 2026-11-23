/* Undead Siege web build: room adapter over PeerJS (free signaling, peer-to-peer game traffic) */
(function(){
const CFG=window.USG_PEER||{};
const ICE={iceServers:[{urls:'stun:stun.l.google.com:19302'},{urls:'stun:stun1.l.google.com:19302'},{urls:'stun:global.stun.twilio.com:3478'}]};
function mkPeer(id){return new Promise((res,rej)=>{let p,done=0;try{p=id?new Peer(id,Object.assign({config:ICE,debug:0},CFG)):new Peer(Object.assign({config:ICE,debug:0},CFG))}catch(e){return rej(e)}
const t=setTimeout(()=>{if(!done){done=1;try{p.destroy()}catch(e){}rej(new Error('signal'))}},10000);
p.on('open',()=>{if(done)return;done=1;clearTimeout(t);res(p)});
p.on('error',e=>{if(done)return;done=1;clearTimeout(t);try{p.destroy()}catch(x){}rej(e)})})}
async function room(code,isHost){
const hs={},pp=[];let myId=isHost?'H':'?',pres={},links={},seq=0,hl=null,peer=null;pres[myId]={};
const list=()=>Object.keys(pres).map(id=>({peer:id,by:null,isMe:id==myId,sameTab:id==myId,kind:'viewer',guest:false,presence:pres[id]}));
const notify=()=>{const l=list();pp.forEach(f=>{try{f({peers:l,joined:[],left:[],updated:[]})}catch(e){}})};
const fire=(t,d,from)=>(hs[t]||[]).forEach(f=>{try{f({topic:t,data:d,peer:from,by:null,isMe:from==myId,sameTab:from==myId,kind:'viewer'})}catch(e){}});
const tx=(c,o)=>{try{if(c&&c.open)c.send(JSON.stringify(o))}catch(e){}};
const bc=(o,ex)=>{for(const id in links)if(id!=ex)tx(links[id].conn,o)};
function onMsg(from,raw){let o;try{o=JSON.parse(typeof raw=='string'?raw:String(raw))}catch(e){return}
if(o.t=='e'){fire(o.topic,o.d,isHost?from:'H')}
else if(o.t=='p'){if(isHost){pres[from]=Object.assign(pres[from]||{},o.p);bc({t:'p',id:from,p:pres[from]},from);notify()}else{pres[o.id]=o.p;notify()}}
else if(o.t=='w'&&!isHost){const mine=pres[myId]||{};myId=o.id;pres=o.pres;pres[myId]=Object.assign(pres[myId]||{},mine);tx(hl,{t:'p',p:pres[myId]});notify()}
else if(o.t=='x'&&!isHost){delete pres[o.id];notify()}}
function drop(id){if(!links[id])return;try{links[id].conn.close()}catch(e){}delete links[id];if(pres[id]){delete pres[id];bc({t:'x',id});notify()}}
function wire(id,c){const open=()=>{if(isHost){pres[id]=pres[id]||{};tx(c,{t:'w',id,pres});notify()}};
if(c.open)open();else c.on('open',open);
c.on('data',d=>onMsg(id,d));
c.on('close',()=>{if(isHost)drop(id);else{pres={};pres[myId]={};notify()}});c.on('error',()=>{})}
if(isHost){peer=await mkPeer('usg-'+code);
peer.on('connection',c=>{if(Object.keys(links).length>=5){try{c.close()}catch(e){}return}const id='c'+(++seq);links[id]={conn:c};wire(id,c)});
peer.on('disconnected',()=>{try{peer.reconnect()}catch(e){}})}
else{peer=await mkPeer();const c=peer.connect('usg-'+code,{reliable:true});hl=c;wire('H',c);
await new Promise((res,rej)=>{const t=setTimeout(()=>rej(new Error('nohost')),10000);c.on('open',()=>{clearTimeout(t);res()});c.on('error',e=>{clearTimeout(t);rej(e)});peer.on('error',e=>{clearTimeout(t);rej(e)})})}
return{
 emit:async(t,d)=>{const o={t:'e',topic:t,d:d};if(isHost)bc(o);else tx(hl,o)},
 on:(t,f)=>{(hs[t]=hs[t]||[]).push(f);return()=>{hs[t]=(hs[t]||[]).filter(x=>x!=f)}},
 presence:async p=>{pres[myId]=Object.assign(pres[myId]||{},p);if(isHost)bc({t:'p',id:myId,p:pres[myId]});else tx(hl,{t:'p',p})},
 peers:list,onPeers:f=>{pp.push(f);return()=>{const i=pp.indexOf(f);if(i>=0)pp.splice(i,1)}},
 leave:async()=>{for(const id in links)drop(id);try{if(hl)hl.close()}catch(e){}try{if(peer)peer.destroy()}catch(e){}pp.length=0;for(const k in hs)delete hs[k]}}}
const base={join:async(name,opts)=>{const code=String(name||'').replace(/^us-/,'').toLowerCase();return room(code,!!(opts&&opts.host))},peers:()=>[],onPeers:()=>()=>{},presence:async()=>{}};
window.claude={use:async n=>n=='room'?base:null,__web:1};
/* PWA + update check */
addEventListener('load',()=>{
if('serviceWorker' in navigator&&/^https?:/.test(location.protocol))navigator.serviceWorker.register('sw.js').catch(()=>{});
const chk=async()=>{try{const r=await fetch('version.json?'+Date.now(),{cache:'no-store'});const d=await r.json();const v=String(d.v||'');const nv=v.split('.').map(Number),cv=GV.split('.').map(Number);let nw=0;for(let i=0;i<3;i++){if((nv[i]||0)>(cv[i]||0)){nw=1;break}if((nv[i]||0)<(cv[i]||0))break}
const b=document.getElementById('upd');if(!b)return;
if(nw){b.textContent='New version v'+v+' is out - tap to reload';b.style.display='';b.onclick=()=>{(window.caches?caches.keys().then(k=>Promise.all(k.map(x=>caches.delete(x)))):Promise.resolve()).finally(()=>location.reload())};
if(state=='play'&&S&&window.__upn!=v){window.__upn=v;S.toast='Update v'+v+' ready - reload when you finish this game';S.tt=7}}else b.style.display='none'}catch(e){}};
setTimeout(chk,3000);setInterval(chk,120000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)chk()})});
})();
