const { WebSocketServer } = require("ws");
const http = require("http");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const server = http.createServer((req,res)=>{
  if (req.url === "/" || req.url === "/index.html") {
    const p = path.join(__dirname, "index.html");
    fs.readFile(p, (err,data)=>{
      if(err){ res.writeHead(500); return res.end("index.html missing"); }
      res.writeHead(200, {"content-type":"text/html; charset=utf-8"});
      res.end(data);
    });
    return;
  }
  res.writeHead(404); res.end("Not found");
});

const wss = new WebSocketServer({server});
const clients = new Map();
function safe(s,n=500){ return String(s??"").replace(/[\u0000-\u001f]/g,"").slice(0,n); }
function broadcast(room,data,except=null){
  const msg=JSON.stringify(data);
  for(const [ws,c] of clients) if(ws!==except && c.room===room && ws.readyState===1) ws.send(msg);
}
wss.on("connection", ws=>{
  const clientId=crypto.randomBytes(6).toString("hex");
  clients.set(ws,{room:"",name:"익명",clientId});
  ws.send(JSON.stringify({type:"hello",clientId}));
  ws.on("message",raw=>{
    let m; try{m=JSON.parse(raw)}catch{return}
    const c=clients.get(ws); if(!c)return;
    const room=safe(m.room,24).toUpperCase(), name=safe(m.name,16)||"익명";
    if(!room)return; c.room=room;c.name=name;
    if(m.type==="join"){ broadcast(room,{type:"system",text:`${name}님이 들어왔어.`},ws); return; }
    if(m.type==="chat"){ broadcast(room,{type:"chat",text:safe(m.text),name,clientId},null); return; }
    if(m.type==="state"){ broadcast(room,{type:"state",time:Number(m.time),paused:!!m.paused,rate:Number(m.rate)||1,name},ws); return; }
    if(m.type==="request_state") broadcast(room,{type:"request_state",name},ws);
  });
  ws.on("close",()=>{ const c=clients.get(ws); if(c?.room) broadcast(c.room,{type:"system",text:`${c.name}님이 나갔어.`},ws); clients.delete(ws); });
});
server.listen(process.env.PORT||10000);
