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
 kick:id=>{if(isHost&&links[id])drop(id)},
 leave:async()=>{for(const id in links)drop(id);try{if(hl)hl.close()}catch(e){}try{if(peer)peer.destroy()}catch(e){}pp.length=0;for(const k in hs)delete hs[k]}}}
/* Public game list: tiny MQTT 3.1.1 client over WebSocket (public broker). Hosts of public games post a small retained listing; it is cleared when they stop or disconnect. */
const LST=(()=>{const URLS=window.USG_MQTT?[window.USG_MQTT]:['wss://broker.hivemq.com:8884/mqtt','wss://broker.emqx.io:8084/mqtt','wss://test.mosquitto.org:8081/mqtt'],PFX='undeadsiege-usg/v1/games/';
let ws=null,ui=0,ready=0,buf=new Uint8Array(0),games={},subs=[],mine=null,mineTopic='',pingT=0,refT=0,want=0,retryT=0;const te=new TextEncoder(),td=new TextDecoder();
const vl=n=>{const o=[];do{let b=n%128;n=Math.floor(n/128);if(n>0)b|=128;o.push(b)}while(n>0);return o};
const str=t=>{const b=te.encode(t);return[b.length>>8,b.length&255,...b]};
const pkt=(h,body)=>new Uint8Array([h,...vl(body.length),...body]);
const send=u=>{try{if(ws&&ws.readyState==1)ws.send(u)}catch(e){}};
const notify=()=>{const l=list();subs.forEach(f=>{try{f({peers:l})}catch(e){}})};
const list=()=>{const now=Date.now(),out=[];for(const c in games){const u=games[c];if(now-(u._rx||0)>110000)continue;if(mine&&mine.c==c)continue;out.push({peer:'pub-'+c,isMe:false,sameTab:false,presence:{us:u}})}if(mine)out.push({peer:'pub-me',isMe:true,sameTab:true,presence:{us:mine}});return out};
function onPub(topic,pl){if(!topic.startsWith(PFX))return;const c=topic.slice(PFX.length).toUpperCase();if(!/^[A-Z0-9]{4}$/.test(c))return;if(!pl.length){delete games[c];notify();return}
 try{const o=JSON.parse(td.decode(pl));if(!o||typeof o!='object')return;const u={v:String(o.v||'').slice(0,12),c,m:String(o.m||'').slice(0,24),d:String(o.d||'').slice(0,12),n:Math.max(1,Math.min(4,o.n|0)),r:Math.max(0,o.r|0),h:String(o.h||'').slice(0,14),hu:String(o.hu||'').slice(0,20),_rx:Date.now()};games[c]=u;notify()}catch(e){}}
function parse(){while(buf.length>=2){let m=1,len=0,i=1,b;do{if(i>=buf.length)return;b=buf[i++];len+=(b&127)*m;m*=128}while(b&128);if(buf.length<i+len)return;const h=buf[0],body=buf.subarray(i,i+len);buf=buf.slice(i+len);
 const t=h>>4;if(t==2){ready=1;send(pkt(0x82,[0,1,...str(PFX+'+'),0]));if(mine)pubMine()}else if(t==3){const tl=(body[0]<<8)|body[1],topic=td.decode(body.subarray(2,2+tl));let p=2+tl;if((h>>1)&3)p+=2;onPub(topic,body.subarray(p))}}}
function connect(){if(ws)return;want=1;const url=URLS[ui%URLS.length];let w;try{w=new WebSocket(url,'mqtt')}catch(e){ui++;retry();return}ws=w;w.binaryType='arraybuffer';ready=0;buf=new Uint8Array(0);
 w.onopen=()=>{const id='usg'+Math.random().toString(36).slice(2,10);let fl=0x02,tail=[];if(mineTopic){fl|=0x04|0x20;tail=[...str(mineTopic),0,0]}send(pkt(0x10,[...str('MQTT'),4,fl,0,45,...str(id),...tail]));clearInterval(pingT);pingT=setInterval(()=>send(new Uint8Array([0xC0,0])),25000)};
 w.onmessage=e=>{const u=new Uint8Array(e.data),nb=new Uint8Array(buf.length+u.length);nb.set(buf);nb.set(u,buf.length);buf=nb;parse()};
 w.onclose=w.onerror=()=>{if(ws!==w)return;ws=null;ready=0;clearInterval(pingT);if(!ready)ui++;retry()}}
function retry(){clearTimeout(retryT);if(!want)return;retryT=setTimeout(connect,4000)}
function pubMine(){if(!mine||!ready)return;const o=Object.assign({},mine,{t:Date.now()});send(pkt(0x31,[...str(PFX+mine.c),...te.encode(JSON.stringify(o))]))}
function clearTopic(t){if(t&&ready)send(pkt(0x31,[...str(t)]))}
return{peers:()=>{connect();return list()},onPeers:f=>{connect();subs.push(f);return()=>{subs=subs.filter(x=>x!=f)}},
 presence:async p=>{if(!p||!('us' in p))return;const u=p.us;if(u&&u.c){const c=String(u.c).toUpperCase().slice(0,4),t=PFX+c;let nick='';try{nick=lobNick()}catch(e){}mine={v:u.v,c,m:u.m,d:u.d,n:u.n,r:u.r,h:nick,hu:u.hu};
  if(mineTopic!=t){if(mineTopic)clearTopic(mineTopic);mineTopic=t;if(ws){const o=ws;ws=null;try{o.close()}catch(e){}}connect()}else pubMine();clearInterval(refT);refT=setInterval(()=>{try{if(typeof mpPublish=='function')mpPublish()}catch(e){}pubMine()},30000);notify()}
  else{clearInterval(refT);const t=mineTopic;mine=null;mineTopic='';clearTopic(t);notify()}}}})();
addEventListener('pagehide',()=>{try{LST.presence({us:null})}catch(e){}});
/* Friends presence + invites: own MQTT connection (public broker). Each player publishes a retained status on a topic named after their random id; the broker clears it via last-will on disconnect. */
const FRP=(()=>{const URLS=window.USG_MQTT?[window.USG_MQTT]:['wss://broker.hivemq.com:8884/mqtt','wss://broker.emqx.io:8084/mqtt','wss://test.mosquitto.org:8081/mqtt'],PFX='undeadsiege-usg/v1/';
let ws=null,ui=0,ready=0,buf=new Uint8Array(0),uid='',fr=[],sub={},st=null,on=1,pingT=0,retryT=0,want=0,pid=1,hb=0;const te=new TextEncoder(),td=new TextDecoder(),stat={},sSubs=[],iSubs=[];
const vl=n=>{const o=[];do{let b=n%128;n=Math.floor(n/128);if(n>0)b|=128;o.push(b)}while(n>0);return o},str=t=>{const b=te.encode(t);return[b.length>>8,b.length&255,...b]},pkt=(h,b)=>new Uint8Array([h,...vl(b.length),...b]);
const send=u=>{try{if(ws&&ws.readyState==1)ws.send(u)}catch(e){}};
const ok=u=>/^[A-Za-z0-9]{4,24}$/.test(u||'');
function subAll(){if(!ready)return;const add=fr.filter(u=>ok(u)&&!sub[u]);if(!sub['#inv']){add.push('#inv')}if(!add.length)return;const body=[(pid>>8)&255,pid&255];pid=pid%60000+1;for(const u of add){sub[u]=1;body.push(...str(u=='#inv'?PFX+'inv/'+uid:PFX+'p/'+u),0)}send(pkt(0x82,body))}
function pubSt(){if(!ready||!uid)return;const t=PFX+'p/'+uid;if(!on||!st)send(pkt(0x31,[...str(t)]));else send(pkt(0x31,[...str(t),...te.encode(JSON.stringify(Object.assign({},st,{t:Date.now()})))]))}
function onMsg(topic,pl){if(topic.startsWith(PFX+'p/')){const u=topic.slice(PFX.length+2);if(!ok(u))return;if(!pl.length){delete stat[u]}else try{const o=JSON.parse(td.decode(pl));if(!o||typeof o!='object')return;stat[u]={s:String(o.s||'on').slice(0,6),c:String(o.c||'').toUpperCase().slice(0,4),m:String(o.m||'').slice(0,24),n:Math.max(0,Math.min(4,o.n|0)),d:String(o.d||'').slice(0,12),_rx:Date.now(),t:+o.t||0}}catch(e){return}sSubs.forEach(f=>{try{f()}catch(e){}})}
 else if(topic==PFX+'inv/'+uid&&pl.length){try{const o=JSON.parse(td.decode(pl));if(!o||!ok(o.f)||!/^[A-Za-z0-9]{4}$/.test(String(o.c||'')))return;if(Math.abs(Date.now()-(+o.t||0))>120000)return;const v={f:o.f,n:String(o.n||'Friend').slice(0,14),c:String(o.c).toUpperCase(),m:String(o.m||'').slice(0,24)};iSubs.forEach(f=>{try{f(v)}catch(e){}})}catch(e){}}}
function parse(){while(buf.length>=2){let m=1,len=0,i=1,b;do{if(i>=buf.length)return;b=buf[i++];len+=(b&127)*m;m*=128}while(b&128);if(buf.length<i+len)return;const h=buf[0],body=buf.subarray(i,i+len);buf=buf.slice(i+len);const t=h>>4;
 if(t==2){ready=1;sub={};subAll();pubSt()}else if(t==3){const tl=(body[0]<<8)|body[1],topic=td.decode(body.subarray(2,2+tl));let p=2+tl;if((h>>1)&3)p+=2;onMsg(topic,body.subarray(p))}}}
function connect(){if(ws||!want||!uid)return;const url=URLS[ui%URLS.length];let w;try{w=new WebSocket(url,'mqtt')}catch(e){ui++;retry();return}ws=w;w.binaryType='arraybuffer';ready=0;buf=new Uint8Array(0);
 w.onopen=()=>{send(pkt(0x10,[...str('MQTT'),4,0x02|0x04|0x20,0,45,...str('usf'+Math.random().toString(36).slice(2,10)),...str(PFX+'p/'+uid),0,0]));clearInterval(pingT);pingT=setInterval(()=>{send(new Uint8Array([0xC0,0]));if(++hb%1==0&&hb%2==0)pubSt()},25000)};
 w.onmessage=e=>{const u=new Uint8Array(e.data),nb=new Uint8Array(buf.length+u.length);nb.set(buf);nb.set(u,buf.length);buf=nb;parse()};
 w.onclose=w.onerror=()=>{if(ws!==w)return;const was=ready;ws=null;ready=0;clearInterval(pingT);if(!was)ui++;for(const u in stat)delete stat[u];sSubs.forEach(f=>{try{f()}catch(e){}});retry()}}
function retry(){clearTimeout(retryT);if(!want)return;retryT=setTimeout(connect,5000)}
return{start:(u,f)=>{if(!ok(u))return;uid=u;want=1;fr=(f||[]).filter(ok);connect();subAll()},stop:()=>{want=0;clearTimeout(retryT);if(ws&&ready){try{send(pkt(0x31,[...str(PFX+'p/'+uid)]))}catch(e){}}try{ws&&ws.close()}catch(e){}ws=null;ready=0;for(const u in stat)delete stat[u]},
 friends:f=>{fr=(f||[]).filter(ok);subAll()},set:(o,show)=>{on=show!==0;st=o;pubSt()},get:u=>{const x=stat[u];return x&&Date.now()-x._rx<400000&&Date.now()-(x.t||x._rx)<120000?x:null},
 invite:(to,o)=>{if(!ready||!ok(to)||!o)return 0;send(pkt(0x30,[...str(PFX+'inv/'+to),...te.encode(JSON.stringify({f:uid,n:o.n,c:o.c,m:o.m,t:Date.now()}))]));return 1},
 onStatus:f=>{sSubs.push(f)},onInvite:f=>{iSubs.push(f)},up:()=>!!ready}})();
window.USG_FR=FRP;addEventListener('pagehide',()=>{try{FRP.stop()}catch(e){}});

const base={join:async(name,opts)=>{const code=String(name||'').replace(/^us-/,'').toLowerCase();return room(code,!!(opts&&opts.host))},peers:()=>LST.peers(),onPeers:f=>LST.onPeers(f),presence:p=>LST.presence(p)};
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
