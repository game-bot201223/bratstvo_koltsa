#!/usr/bin/env python3
"""
Inject Admin Hub modal into /var/www/game/index.html.
Run: python3 /tmp/inject_admin_hub.py
"""
import sys, os, re, shutil
from datetime import datetime

TARGET = '/var/www/game/index.html'

GOD_BUTTON = (
    '<button class="btn" id="adminHubBtn" '
    'style="background:linear-gradient(135deg,#c084fc,#7c3aed);color:#fff;'
    'font-weight:900;font-size:15px;padding:14px;border:2px solid #7c3aed;'
    'margin-bottom:10px;text-transform:uppercase;width:100%;letter-spacing:1px;'
    'border-radius:12px;cursor:pointer;">'
    '\u26a1 \u0420\u0415\u0416\u0418\u041c \u0411\u041e\u0413\u0410</button>\n        '
)

ADMIN_HUB_HTML = r'''
<!-- ========== ADMIN HUB v2 ========== -->
<div class="modal" id="mAdminHub" style="z-index:10100">
<div class="modalContent" id="ahRoot" style="max-width:100%;width:100%;padding:0;background:#1a1a2e;color:#e0e0e0;min-height:100dvh;font-family:system-ui,-apple-system,sans-serif;font-size:14px;">

<div id="ahHeader" style="position:sticky;top:0;z-index:10;background:#1a1a2e;border-bottom:1px solid #2a2a4a;">
  <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 12px 6px;">
    <div style="font-size:18px;font-weight:900;color:#c084fc;letter-spacing:1px;">&#9889; ADMIN HUB</div>
    <button onclick="document.getElementById('mAdminHub').style.display='none'" style="background:none;border:none;color:#888;font-size:22px;cursor:pointer;">&times;</button>
  </div>
  <div id="ahTabs" style="display:flex;gap:0;overflow-x:auto;padding:0 8px 0;"></div>
</div>

<div id="ahBody" style="padding:10px 10px 80px;"></div>

</div>
</div>

<style>
#ahRoot *{box-sizing:border-box}
#ahRoot input,#ahRoot select,#ahRoot textarea{
  background:#16162a;color:#e0e0e0;border:1px solid #333;border-radius:8px;padding:8px 10px;font-size:13px;width:100%;margin-bottom:6px;outline:none;font-family:inherit;
}
#ahRoot input:focus,#ahRoot select:focus,#ahRoot textarea:focus{border-color:#c084fc}
#ahRoot textarea{min-height:100px;resize:vertical;font-family:'Courier New',monospace;font-size:12px}
.ah-btn{
  display:inline-flex;align-items:center;justify-content:center;gap:4px;
  padding:9px 14px;border-radius:8px;border:none;font-size:13px;font-weight:700;cursor:pointer;
  background:#c084fc;color:#fff;min-width:60px;transition:opacity .15s;
}
.ah-btn:active{opacity:.7}
.ah-btn-sm{padding:6px 10px;font-size:12px;min-width:40px}
.ah-btn-outline{background:transparent;border:1px solid #c084fc;color:#c084fc}
.ah-btn-red{background:#ef4444}
.ah-btn-green{background:#22c55e}
.ah-btn-orange{background:#f97316}
.ah-btn-gray{background:#444}
.ah-card{background:#16162a;border-radius:10px;padding:12px;margin-bottom:10px;border:1px solid #2a2a4a}
.ah-label{font-size:11px;color:#888;text-transform:uppercase;letter-spacing:.5px;margin-bottom:3px}
.ah-row{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.ah-grid2{display:grid;grid-template-columns:1fr 1fr;gap:6px}
.ah-grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px}
.ah-tab{
  padding:8px 14px;font-size:12px;font-weight:700;color:#888;cursor:pointer;border:none;
  background:none;border-bottom:2px solid transparent;white-space:nowrap;transition:color .15s;
}
.ah-tab.active{color:#c084fc;border-bottom-color:#c084fc}
.ah-sep{height:1px;background:#2a2a4a;margin:10px 0}
.ah-player-row{
  display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:8px;cursor:pointer;
  transition:background .15s;border:1px solid transparent;
}
.ah-player-row:hover{background:#1e1e3a;border-color:#333}
.ah-badge{display:inline-block;padding:2px 6px;border-radius:4px;font-size:10px;font-weight:700}
.ah-scroll{max-height:55vh;overflow-y:auto;-webkit-overflow-scrolling:touch}
.ah-mono{font-family:'Courier New',monospace;font-size:12px}
.ah-loader{display:inline-block;width:16px;height:16px;border:2px solid #444;border-top-color:#c084fc;border-radius:50%;animation:ahSpin .6s linear infinite}
@keyframes ahSpin{to{transform:rotate(360deg)}}
</style>

<script>
(function(){
"use strict";

var TABS = [
  {id:'players', label:'\u0418\u0413\u0420\u041e\u041a\u0418'},
  {id:'economy', label:'\u042d\u041a\u041e\u041d\u041e\u041c\u0418\u041a\u0410'},
  {id:'promo',   label:'\u041f\u0420\u041e\u041c\u041e'},
  {id:'clans',   label:'\u041a\u041b\u0410\u041d\u042b'},
  {id:'server',  label:'\u0421\u0415\u0420\u0412\u0415\u0420'},
];

var _tab = 'players';
var _playerCard = null;
var _playerList = [];
var _playerSearch = '';
var _promoList = [];
var _clanList = [];
var _auditLog = [];
var _loading = {};

function _$(id){return document.getElementById(id)}
function _h(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
function _fmt(n){return Number(n||0).toLocaleString('ru-RU')}

async function _api(fn, payload){
  try{
    var r = await sbFnCall(fn, Object.assign({initData:tgInitData()}, payload||{}));
    return r;
  }catch(e){
    console.error('AH api error',fn,e);
    toast('\u041e\u0448\u0438\u0431\u043a\u0430: '+String(e&&e.message||e),'bad');
    return {ok:false,error:String(e)};
  }
}

async function _dangerApi(action, fn, payload){
  var t = await _api('admin_danger_token_start',{action:action});
  if(!t||!t.ok||!t.token){toast('\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u044c danger token','bad');return {ok:false};}
  return await _api(fn, Object.assign({danger_token:t.token}, payload||{}));
}

function _renderTabs(){
  var el = _$('ahTabs');
  if(!el) return;
  el.innerHTML = TABS.map(function(t){
    return '<button class="ah-tab'+(_tab===t.id?' active':'')+'" data-tab="'+t.id+'">'+t.label+'</button>';
  }).join('');
  el.querySelectorAll('.ah-tab').forEach(function(b){
    b.onclick = function(){ _tab = b.dataset.tab; _renderTabs(); _renderBody(); };
  });
}

function _renderBody(){
  var el = _$('ahBody');
  if(!el) return;
  switch(_tab){
    case 'players': _renderPlayers(el); break;
    case 'economy': _renderEconomy(el); break;
    case 'promo':   _renderPromo(el);   break;
    case 'clans':   _renderClans(el);   break;
    case 'server':  _renderServer(el);  break;
  }
}

/* ===== PLAYERS TAB ===== */
function _renderPlayers(el){
  if(_playerCard){
    _renderPlayerCard(el);
    return;
  }
  var html = '<div class="ah-card">';
  html += '<div class="ah-label">\u041f\u043e\u0438\u0441\u043a \u0438\u0433\u0440\u043e\u043a\u0430 (\u0438\u043c\u044f / TG ID)</div>';
  html += '<div class="ah-row"><input id="ahPlayerQ" placeholder="\u0412\u0432\u0435\u0434\u0438\u0442\u0435 \u0438\u043c\u044f \u0438\u043b\u0438 ID..." value="'+_h(_playerSearch)+'" style="flex:1">';
  html += '<button class="ah-btn" id="ahPlayerSearch">\u041d\u0430\u0439\u0442\u0438</button></div>';
  html += '</div>';

  if(_loading.players){
    html += '<div style="text-align:center;padding:20px"><div class="ah-loader"></div></div>';
  } else if(_playerList.length){
    html += '<div class="ah-card"><div class="ah-label">\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u044b ('+_playerList.length+')</div>';
    html += '<div class="ah-scroll">';
    _playerList.forEach(function(p,i){
      html += '<div class="ah-player-row" data-idx="'+i+'">';
      html += '<div style="flex:1;min-width:0">';
      html += '<div style="font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+_h(p.name||'\u0411\u0435\u0437 \u0438\u043c\u0435\u043d\u0438')+'</div>';
      html += '<div style="font-size:11px;color:#888">ID: '+_h(p.tg_id)+' &middot; Lv.'+_fmt(p.level)+'</div>';
      html += '</div>';
      html += '<div style="text-align:right;font-size:11px;color:#888">';
      html += '<span style="color:#fbbf24">\u2B50'+_fmt(p.gold)+'</span> ';
      html += '<span style="color:#94a3b8">\u26aa'+_fmt(p.silver)+'</span>';
      html += '</div>';
      html += '</div>';
    });
    html += '</div></div>';
  }

  el.innerHTML = html;

  var searchBtn = _$('ahPlayerSearch');
  var searchInp = _$('ahPlayerQ');
  if(searchBtn) searchBtn.onclick = function(){ _doPlayerSearch(); };
  if(searchInp) searchInp.onkeydown = function(e){ if(e.key==='Enter') _doPlayerSearch(); };
}

async function _doPlayerSearch(){
  var q = (_$('ahPlayerQ')||{}).value || '';
  _playerSearch = q;
  _loading.players = true;
  _renderBody();
  var r = await _api('admin_list_players',{query:q,limit:100});
  _loading.players = false;
  _playerList = (r&&r.ok&&r.players)||[];
  _renderBody();
  var body = _$('ahBody');
  if(!body) return;
  body.querySelectorAll('.ah-player-row').forEach(function(row){
    row.onclick = function(){ _openPlayerCard(_playerList[parseInt(row.dataset.idx)]); };
  });
}

async function _openPlayerCard(p){
  if(!p) return;
  _loading.playerCard = true;
  _playerCard = p;
  _renderBody();
  var r = await _api('admin_get_player',{target_tg_id:String(p.tg_id),initData:tgInitData()});
  _loading.playerCard = false;
  if(r&&r.ok&&r.player) _playerCard = r.player;
  _renderBody();
}

function _renderPlayerCard(el){
  var p = _playerCard;
  if(!p){_renderPlayers(el);return;}
  var s = (p.state && typeof p.state === 'object') ? p.state : {};

  var html = '<div style="margin-bottom:8px">';
  html += '<button class="ah-btn ah-btn-sm ah-btn-gray" id="ahBackToList">&larr; \u041d\u0430\u0437\u0430\u0434</button>';
  html += '</div>';

  if(_loading.playerCard){
    html += '<div style="text-align:center;padding:30px"><div class="ah-loader"></div></div>';
    el.innerHTML = html;
    _$('ahBackToList').onclick = function(){_playerCard=null;_renderBody();};
    return;
  }

  html += '<div class="ah-card">';
  html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">';
  if(p.photo_url) html += '<img src="'+_h(p.photo_url)+'" style="width:40px;height:40px;border-radius:50%;object-fit:cover">';
  html += '<div>';
  html += '<div style="font-size:16px;font-weight:900;color:#c084fc">'+_h(p.name||'\u0411\u0435\u0437 \u0438\u043c\u0435\u043d\u0438')+'</div>';
  html += '<div style="font-size:11px;color:#888">TG ID: '+_h(p.tg_id)+' &middot; sv: '+(p.state_version||0)+'</div>';
  html += '</div></div>';

  html += '<div class="ah-grid3">';
  html += _fld('\u0423\u0440\u043e\u0432\u0435\u043d\u044c','ahPLevel',p.level||s.level||1);
  html += _fld('XP','ahPXp',p.xp||s.totalXp||s.xp||0);
  html += _fld('\u042d\u043d\u0435\u0440\u0433\u0438\u044f','ahPEnergy',s.energy||0);
  html += '</div>';
  html += '<div class="ah-grid3">';
  html += _fld('\u0417\u043e\u043b\u043e\u0442\u043e \u2B50','ahPGold',p.gold||s.gold||0);
  html += _fld('\u0421\u0435\u0440\u0435\u0431\u0440\u043e \u26aa','ahPSilver',p.silver||s.silver||0);
  html += _fld('\u0417\u0443\u0431\u044b \uD83E\uDDB7','ahPTooth',p.tooth||s.tooth||0);
  html += '</div>';
  html += '<div class="ah-grid2">';
  html += _fld('VIP','ahPVip',s.vip?'true':'false');
  html += _fld('\u0411\u043e\u0441\u0441\u043e\u0432 \u0443\u0431\u0438\u0442\u043e','ahPBossWins',p.boss_wins||0);
  html += '</div>';
  html += '</div>';

  html += '<div class="ah-card">';
  html += '<div class="ah-label">RAW STATE (JSON)</div>';
  html += '<textarea id="ahPState" style="min-height:140px">'+_h(JSON.stringify(s,null,2))+'</textarea>';
  html += '</div>';

  html += '<div class="ah-card">';
  html += '<div class="ah-row" style="gap:6px;flex-wrap:wrap">';
  html += '<button class="ah-btn ah-btn-green ah-btn-sm" id="ahPSave">\uD83D\uDCBE \u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c</button>';
  html += '<button class="ah-btn ah-btn-orange ah-btn-sm" id="ahPGod">\u26a1 GOD</button>';
  html += '<button class="ah-btn ah-btn-red ah-btn-sm" id="ahPReset">\uD83D\uDDD1 \u0421\u0431\u0440\u043e\u0441</button>';
  html += '<button class="ah-btn ah-btn-sm ah-btn-outline" id="ahPKick">\uD83D\uDEAA \u041a\u0438\u043a</button>';
  html += '<button class="ah-btn ah-btn-sm ah-btn-outline" id="ahPSnap">\uD83D\uDCF8 \u0421\u043d\u0430\u043f\u0448\u043e\u0442</button>';
  html += '</div>';
  html += '</div>';

  html += '<div class="ah-card" id="ahSnapsSection" style="display:none">';
  html += '<div class="ah-label">\u0421\u043d\u0430\u043f\u0448\u043e\u0442\u044b</div>';
  html += '<div id="ahSnapsList"></div>';
  html += '</div>';

  el.innerHTML = html;

  _$('ahBackToList').onclick = function(){_playerCard=null;_renderBody();};

  _$('ahPSave').onclick = async function(){
    var stTxt = (_$('ahPState')||{}).value||'{}';
    var stObj;
    try{stObj=JSON.parse(stTxt);}catch(e){toast('\u041d\u0435\u0432\u0430\u043b\u0438\u0434\u043d\u044b\u0439 JSON','bad');return;}
    stObj.level = parseInt((_$('ahPLevel')||{}).value)||1;
    stObj.totalXp = parseInt((_$('ahPXp')||{}).value)||0;
    stObj.xp = stObj.totalXp;
    stObj.energy = parseInt((_$('ahPEnergy')||{}).value)||0;
    stObj.gold = parseInt((_$('ahPGold')||{}).value)||0;
    stObj.silver = parseInt((_$('ahPSilver')||{}).value)||0;
    stObj.tooth = parseInt((_$('ahPTooth')||{}).value)||0;
    var vipVal = String((_$('ahPVip')||{}).value||'').trim().toLowerCase();
    stObj.vip = (vipVal==='true'||vipVal==='1');
    var r = await _api('admin_update_player',{
      target_tg_id:String(p.tg_id),
      state:stObj,
      admin_force_state_write:true,
      admin_replace_state:true,
      level:stObj.level,
      xp:stObj.totalXp,
      gold:stObj.gold,
      silver:stObj.silver,
      tooth:stObj.tooth,
      boss_wins:parseInt((_$('ahPBossWins')||{}).value)||0,
    });
    if(r&&r.ok) toast('\u2705 \u0421\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u043e (sv:'+r.state_version+')');
    else toast('\u274c \u041e\u0448\u0438\u0431\u043a\u0430: '+(r&&r.error||'unknown'),'bad');
  };

  _$('ahPGod').onclick = async function(){
    if(!confirm('\u0412\u043a\u043b\u044e\u0447\u0438\u0442\u044c GOD MODE \u0434\u043b\u044f '+p.name+'?')) return;
    var r = await _dangerApi('admin_player_godmode','admin_player_godmode',{target_tg_id:String(p.tg_id)});
    if(r&&r.ok) toast('\u26a1 God mode \u0430\u043a\u0442\u0438\u0432\u0438\u0440\u043e\u0432\u0430\u043d');
    else toast('\u274c '+(r&&r.error||'error'),'bad');
  };

  _$('ahPReset').onclick = async function(){
    if(!confirm('\u0421\u0411\u0420\u041e\u0421\u0418\u0422\u042c \u0418\u0413\u0420\u041e\u041a\u0410 '+p.name+'? \u042d\u0442\u043e \u043d\u0435\u043e\u0431\u0440\u0430\u0442\u0438\u043c\u043e!')) return;
    if(!confirm('\u0422\u043e\u0447\u043d\u043e \u0441\u0431\u0440\u043e\u0441\u0438\u0442\u044c? \u0412\u0441\u0435 \u0434\u0430\u043d\u043d\u044b\u0435 \u0431\u0443\u0434\u0443\u0442 \u0443\u0434\u0430\u043b\u0435\u043d\u044b!')) return;
    var r = await _dangerApi('admin_player_reset','admin_player_reset',{target_tg_id:String(p.tg_id)});
    if(r&&r.ok) toast('\uD83D\uDDD1 \u0418\u0433\u0440\u043e\u043a \u0441\u0431\u0440\u043e\u0448\u0435\u043d');
    else toast('\u274c '+(r&&r.error||'error'),'bad');
  };

  _$('ahPKick').onclick = async function(){
    if(!confirm('\u041a\u0438\u043a\u043d\u0443\u0442\u044c '+p.name+'?')) return;
    var r = await _api('admin_force_logout_player',{target_tg_id:String(p.tg_id)});
    if(r&&r.ok) toast('\uD83D\uDEAA \u0418\u0433\u0440\u043e\u043a \u043a\u0438\u043a\u043d\u0443\u0442');
    else toast('\u274c '+(r&&r.error||'error'),'bad');
  };

  _$('ahPSnap').onclick = async function(){
    var r = await _api('admin_snapshot_create',{target_tg_id:String(p.tg_id),note:'hub snapshot'});
    if(r&&r.ok){
      toast('\uD83D\uDCF8 \u0421\u043d\u0430\u043f\u0448\u043e\u0442 #'+(r.snapshot&&r.snapshot.id||'?'));
      _loadSnapshots(String(p.tg_id));
    } else toast('\u274c '+(r&&r.error||'error'),'bad');
  };
}

async function _loadSnapshots(tgId){
  var sec = _$('ahSnapsSection');
  var lst = _$('ahSnapsList');
  if(!sec||!lst) return;
  sec.style.display = 'block';
  lst.innerHTML = '<div class="ah-loader"></div>';
  var r = await _api('admin_snapshot_list',{target_tg_id:tgId,limit:20});
  if(!r||!r.ok){lst.innerHTML='<div style="color:#ef4444">\u041e\u0448\u0438\u0431\u043a\u0430</div>';return;}
  var snaps = r.snapshots||[];
  if(!snaps.length){lst.innerHTML='<div style="color:#888">\u041d\u0435\u0442 \u0441\u043d\u0430\u043f\u0448\u043e\u0442\u043e\u0432</div>';return;}
  var h2 = '';
  snaps.forEach(function(sn){
    h2 += '<div class="ah-row" style="padding:4px 0;border-bottom:1px solid #222">';
    h2 += '<span style="color:#888;font-size:11px">#'+sn.id+' sv:'+sn.state_version+' '+(sn.created_at||'').slice(0,19)+'</span>';
    h2 += '<button class="ah-btn ah-btn-sm ah-btn-outline" data-snap-id="'+sn.id+'" data-snap-tg="'+tgId+'">\u0412\u043e\u0441\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c</button>';
    h2 += '</div>';
  });
  lst.innerHTML = h2;
  lst.querySelectorAll('[data-snap-id]').forEach(function(btn){
    btn.onclick = async function(){
      if(!confirm('\u0412\u043e\u0441\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c \u0441\u043d\u0430\u043f\u0448\u043e\u0442 #'+btn.dataset.snapId+'?')) return;
      var r2 = await _api('admin_snapshot_restore',{target_tg_id:btn.dataset.snapTg,snapshot_id:parseInt(btn.dataset.snapId)});
      if(r2&&r2.ok) toast('\u2705 \u0412\u043e\u0441\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d\u043e');
      else toast('\u274c '+(r2&&r2.error||'error'),'bad');
    };
  });
}

function _fld(label,id,val){
  return '<div><div class="ah-label">'+label+'</div><input id="'+id+'" value="'+_h(String(val))+'"></div>';
}


/* ===== ECONOMY TAB ===== */
function _renderEconomy(el){
  var html = '<div class="ah-card">';
  html += '<div style="font-weight:900;color:#c084fc;margin-bottom:8px">\u041c\u0430\u0441\u0441\u043e\u0432\u043e\u0435 \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d\u0438\u0435</div>';
  html += '<div class="ah-label">\u0424\u0438\u043b\u044c\u0442\u0440\u044b</div>';
  html += '<div class="ah-grid2">';
  html += _fld('\u041c\u0438\u043d. \u0443\u0440\u043e\u0432\u0435\u043d\u044c','ahEcoMinLvl','');
  html += _fld('\u041c\u0430\u043a\u0441. \u0443\u0440\u043e\u0432\u0435\u043d\u044c','ahEcoMaxLvl','');
  html += '</div>';
  html += '<div class="ah-grid2">';
  html += _fld('Clan ID','ahEcoClan','');
  html += _fld('\u0410\u043a\u0442\u0438\u0432\u043d\u044b \u0437\u0430 N \u0447\u0430\u0441\u043e\u0432','ahEcoActive','');
  html += '</div>';
  html += '<div class="ah-sep"></div>';
  html += '<div class="ah-label">\u041d\u0430\u0447\u0438\u0441\u043b\u0438\u0442\u044c</div>';
  html += '<div class="ah-grid3">';
  html += _fld('\u0417\u043e\u043b\u043e\u0442\u043e +','ahEcoGold','0');
  html += _fld('\u0421\u0435\u0440\u0435\u0431\u0440\u043e +','ahEcoSilver','0');
  html += _fld('\u0417\u0443\u0431\u044b +','ahEcoTooth','0');
  html += '</div>';
  html += _fld('\u0423\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c \u0443\u0440\u043e\u0432\u0435\u043d\u044c','ahEcoLevelSet','');
  html += '<div class="ah-label">\u041f\u0440\u0435\u0441\u0435\u0442</div>';
  html += '<select id="ahEcoPreset"><option value="">- \u0411\u0435\u0437 \u043f\u0440\u0435\u0441\u0435\u0442\u0430 -</option>';
  html += '<option value="ECONOMY_BOOST">ECONOMY_BOOST</option>';
  html += '<option value="PVP_MAX">PVP_MAX</option>';
  html += '<option value="BOSS_MAX">BOSS_MAX</option></select>';
  html += '<div class="ah-row" style="margin-top:8px">';
  html += '<button class="ah-btn ah-btn-outline" id="ahEcoDry">\uD83D\uDD0D Dry Run</button>';
  html += '<button class="ah-btn ah-btn-green" id="ahEcoApply">\u2705 \u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c</button>';
  html += '</div>';
  html += '<div id="ahEcoResult" style="margin-top:8px"></div>';
  html += '</div>';
  el.innerHTML = html;

  function _getPayload(dryRun){
    var o = {dry_run:dryRun,confirm_token:dryRun?'':'APPLY'};
    function v(sid){return (_$(sid)||{}).value||'';}
    if(v('ahEcoMinLvl')) o.min_level = parseInt(v('ahEcoMinLvl'));
    if(v('ahEcoMaxLvl')) o.max_level = parseInt(v('ahEcoMaxLvl'));
    if(v('ahEcoClan')) o.clan_id = v('ahEcoClan');
    if(v('ahEcoActive')) o.active_within_hours = parseInt(v('ahEcoActive'));
    o.gold_add = parseInt(v('ahEcoGold'))||0;
    o.silver_add = parseInt(v('ahEcoSilver'))||0;
    o.tooth_add = parseInt(v('ahEcoTooth'))||0;
    if(v('ahEcoLevelSet')) o.level_set = parseInt(v('ahEcoLevelSet'));
    if(v('ahEcoPreset')) o.preset = v('ahEcoPreset');
    return o;
  }

  _$('ahEcoDry').onclick = async function(){
    var out = _$('ahEcoResult');
    out.innerHTML = '<div class="ah-loader"></div>';
    var r = await _api('admin_bulk_grant', _getPayload(true));
    if(r&&r.ok){
      var s2 = '<div style="color:#22c55e">\u041d\u0430\u0439\u0434\u0435\u043d\u043e: <b>'+r.matched+'</b> \u0438\u0433\u0440\u043e\u043a\u043e\u0432</div>';
      if(r.sample&&r.sample.length){
        s2 += '<div class="ah-scroll" style="max-height:200px;margin-top:6px">';
        r.sample.forEach(function(pp){ s2 += '<div style="font-size:12px;padding:2px 0">'+_h(pp.name)+' (Lv.'+pp.level+') ID:'+_h(pp.tg_id)+'</div>'; });
        s2 += '</div>';
      }
      out.innerHTML = s2;
    } else {
      out.innerHTML = '<div style="color:#ef4444">\u041e\u0448\u0438\u0431\u043a\u0430: '+(r&&r.error||'?')+'</div>';
    }
  };

  _$('ahEcoApply').onclick = async function(){
    if(!confirm('\u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c \u043c\u0430\u0441\u0441\u043e\u0432\u043e\u0435 \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d\u0438\u0435? \u042d\u0442\u043e \u043d\u0435\u043e\u0431\u0440\u0430\u0442\u0438\u043c\u043e!')) return;
    var out = _$('ahEcoResult');
    out.innerHTML = '<div class="ah-loader"></div> \u041f\u0440\u0438\u043c\u0435\u043d\u044f\u0435\u043c...';
    var r = await _dangerApi('admin_bulk_grant','admin_bulk_grant', _getPayload(false));
    if(r&&r.ok){
      out.innerHTML = '<div style="color:#22c55e">\u2705 \u041e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u043e: <b>'+r.updated+'</b> \u0438\u0437 '+r.matched+'</div>';
      toast('\u2705 Bulk grant: '+r.updated+' \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u043e');
    } else {
      out.innerHTML = '<div style="color:#ef4444">\u274c '+(r&&r.error||'error')+'</div>';
    }
  };
}


/* ===== PROMO TAB ===== */
function _renderPromo(el){
  var html = '<div class="ah-card">';
  html += '<div style="font-weight:900;color:#c084fc;margin-bottom:8px">\u0421\u043e\u0437\u0434\u0430\u0442\u044c / \u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u0442\u044c \u043f\u0440\u043e\u043c\u043e\u043a\u043e\u0434</div>';
  html += '<div class="ah-grid2">';
  html += _fld('\u041a\u043e\u0434','ahPrCode','');
  html += _fld('\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435','ahPrTitle','');
  html += '</div>';
  html += '<div class="ah-grid2">';
  html += _fld('\u041c\u0430\u043a\u0441. \u0438\u0441\u043f. \u0432\u0441\u0435\u0433\u043e','ahPrMaxTotal','0');
  html += _fld('\u041c\u0430\u043a\u0441. \u043d\u0430 \u0438\u0433\u0440\u043e\u043a\u0430','ahPrMaxPer','1');
  html += '</div>';
  html += '<div class="ah-label">\u041d\u0430\u0433\u0440\u0430\u0434\u044b</div>';
  html += '<div class="ah-grid2">';
  html += _fld('\u0417\u043e\u043b\u043e\u0442\u043e','ahPrGold','0');
  html += _fld('\u0421\u0435\u0440\u0435\u0431\u0440\u043e','ahPrSilver','0');
  html += '</div>';
  html += '<div class="ah-grid2">';
  html += _fld('\u0417\u0443\u0431\u044b','ahPrTooth','0');
  html += _fld('\u041a\u043e\u043b\u044c\u0446\u0430','ahPrRings','0');
  html += '</div>';
  html += '<div class="ah-grid2">';
  html += _fld('\u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u044f','ahPrCat','all');
  html += _fld('\u0420\u0435\u0436\u0438\u043c','ahPrMode','all');
  html += '</div>';
  html += '<div class="ah-row" style="margin-top:6px">';
  html += '<button class="ah-btn ah-btn-green" id="ahPrSave">\uD83D\uDCBE \u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c</button>';
  html += '<button class="ah-btn ah-btn-outline" id="ahPrLoad">\uD83D\uDD04 \u0417\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044c \u0441\u043f\u0438\u0441\u043e\u043a</button>';
  html += '</div>';
  html += '</div>';

  html += '<div class="ah-card" id="ahPromoListCard" style="display:none">';
  html += '<div class="ah-label">\u041f\u0440\u043e\u043c\u043e\u043a\u043e\u0434\u044b</div>';
  html += '<div id="ahPromoListBody" class="ah-scroll"></div>';
  html += '</div>';

  el.innerHTML = html;

  _$('ahPrSave').onclick = async function(){
    function v(sid){return (_$(sid)||{}).value||'';}
    var code = v('ahPrCode').trim();
    if(!code){toast('\u0412\u0432\u0435\u0434\u0438\u0442\u0435 \u043a\u043e\u0434','bad');return;}
    var r = await _api('admin_promo_upsert',{
      code:code,
      title:v('ahPrTitle'),
      max_total_uses:parseInt(v('ahPrMaxTotal'))||0,
      max_per_user:parseInt(v('ahPrMaxPer'))||1,
      rewards:{gold:parseInt(v('ahPrGold'))||0,silver:parseInt(v('ahPrSilver'))||0,tooth:parseInt(v('ahPrTooth'))||0,rings:parseInt(v('ahPrRings'))||0},
      category:v('ahPrCat')||'all',
      target_mode:v('ahPrMode')||'all',
      active:true,
    });
    if(r&&r.ok) toast('\u2705 \u041f\u0440\u043e\u043c\u043e\u043a\u043e\u0434 \u0441\u043e\u0445\u0440\u0430\u043d\u0451\u043d');
    else toast('\u274c '+(r&&r.error||'error'),'bad');
  };

  _$('ahPrLoad').onclick = async function(){
    var card = _$('ahPromoListCard');
    var body = _$('ahPromoListBody');
    card.style.display = 'block';
    body.innerHTML = '<div class="ah-loader"></div>';
    var r = await _api('admin_promo_list',{limit:200});
    if(!r||!r.ok){body.innerHTML='<div style="color:#ef4444">\u041e\u0448\u0438\u0431\u043a\u0430</div>';return;}
    _promoList = r.promos||[];
    if(!_promoList.length){body.innerHTML='<div style="color:#888">\u041d\u0435\u0442 \u043f\u0440\u043e\u043c\u043e\u043a\u043e\u0434\u043e\u0432</div>';return;}
    var s2 = '';
    _promoList.forEach(function(pp){
      var rew = pp.rewards||{};
      s2 += '<div class="ah-player-row" style="flex-direction:column;align-items:flex-start;gap:2px">';
      s2 += '<div style="font-weight:700;color:'+(pp.active?'#22c55e':'#ef4444')+'">'+_h(pp.code)+' '+(pp.active?'\u2705':'\u274c')+'</div>';
      s2 += '<div style="font-size:11px;color:#888">\u0418\u0441\u043f: '+(pp.used_total||0)+'/'+(pp.max_total_uses||'\u221e')+' | \u041d\u0430\u0433\u0440: \u2B50'+(rew.gold||0)+' \u26aa'+(rew.silver||0)+' \uD83E\uDDB7'+(rew.tooth||0)+' \uD83D\uDC8D'+(rew.rings||0)+'</div>';
      if(pp.title) s2 += '<div style="font-size:11px;color:#aaa">'+_h(pp.title)+'</div>';
      s2 += '</div>';
    });
    body.innerHTML = s2;
  };
}


/* ===== CLANS TAB ===== */
function _renderClans(el){
  var html = '<div class="ah-card">';
  html += '<div class="ah-row"><div style="font-weight:900;color:#c084fc">\u041a\u043b\u0430\u043d\u044b</div>';
  html += '<button class="ah-btn ah-btn-sm ah-btn-outline" id="ahClansLoad">\uD83D\uDD04 \u0417\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044c</button></div>';
  html += '</div>';
  html += '<div id="ahClansBody"></div>';
  el.innerHTML = html;

  _$('ahClansLoad').onclick = async function(){
    var body = _$('ahClansBody');
    body.innerHTML = '<div style="text-align:center;padding:20px"><div class="ah-loader"></div></div>';
    var r = await _api('list_clans',{limit:200});
    if(!r||!r.ok){body.innerHTML='<div class="ah-card" style="color:#ef4444">\u041e\u0448\u0438\u0431\u043a\u0430</div>';return;}
    _clanList = r.clans||[];
    if(!_clanList.length){body.innerHTML='<div class="ah-card" style="color:#888">\u041d\u0435\u0442 \u043a\u043b\u0430\u043d\u043e\u0432</div>';return;}
    var s2 = '';
    _clanList.forEach(function(c){
      var d = c.data||{};
      var members = Array.isArray(d.members) ? d.members : [];
      s2 += '<div class="ah-card">';
      s2 += '<div style="font-weight:700;color:#c084fc">'+_h(c.name||c.id)+'</div>';
      s2 += '<div style="font-size:12px;color:#888">ID: '+_h(c.id)+' &middot; \u0423\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u0438: '+members.length+'</div>';
      if(members.length){
        s2 += '<div style="margin-top:4px;font-size:11px">';
        members.slice(0,20).forEach(function(m){ s2 += '<span style="display:inline-block;background:#222;padding:2px 6px;border-radius:4px;margin:2px">'+_h(m.name||m.tg_id||'?')+'</span>'; });
        if(members.length>20) s2 += '<span style="color:#888">... +'+(members.length-20)+'</span>';
        s2 += '</div>';
      }
      s2 += '</div>';
    });
    body.innerHTML = s2;
  };
}


/* ===== SERVER TAB ===== */
function _renderServer(el){
  var html = '<div class="ah-card">';
  html += '<div style="font-weight:900;color:#c084fc;margin-bottom:8px">\u0421\u0435\u0440\u0432\u0435\u0440</div>';
  html += '<div class="ah-row">';
  html += '<button class="ah-btn ah-btn-outline ah-btn-sm" id="ahSrvPing">\uD83C\uDFD3 Ping</button>';
  html += '<button class="ah-btn ah-btn-outline ah-btn-sm" id="ahSrvAudit">\uD83D\uDCDC \u0410\u0443\u0434\u0438\u0442</button>';
  html += '</div>';
  html += '<div id="ahSrvPingResult" style="margin-top:8px"></div>';
  html += '</div>';

  html += '<div class="ah-card" id="ahAuditCard" style="display:none">';
  html += '<div class="ah-label">\u0410\u0443\u0434\u0438\u0442 \u043b\u043e\u0433 (\u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 100)</div>';
  html += '<div id="ahAuditBody" class="ah-scroll"></div>';
  html += '</div>';

  el.innerHTML = html;

  _$('ahSrvPing').onclick = async function(){
    var out = _$('ahSrvPingResult');
    var t0 = Date.now();
    var r = await _api('ping',{});
    var ms = Date.now()-t0;
    if(r&&r.ok) out.innerHTML = '<span style="color:#22c55e">\u2705 Pong! '+ms+'ms</span>';
    else out.innerHTML = '<span style="color:#ef4444">\u274c \u041d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d ('+ms+'ms)</span>';
  };

  _$('ahSrvAudit').onclick = async function(){
    var card = _$('ahAuditCard');
    var body = _$('ahAuditBody');
    card.style.display = 'block';
    body.innerHTML = '<div class="ah-loader"></div>';
    var r = await _api('admin_audit_list',{limit:100});
    if(!r||!r.ok){body.innerHTML='<div style="color:#ef4444">\u041e\u0448\u0438\u0431\u043a\u0430</div>';return;}
    _auditLog = r.events||[];
    if(!_auditLog.length){body.innerHTML='<div style="color:#888">\u041f\u0443\u0441\u0442\u043e</div>';return;}
    var s2 = '';
    _auditLog.forEach(function(ev){
      s2 += '<div style="padding:4px 0;border-bottom:1px solid #222;font-size:12px">';
      s2 += '<span style="color:#c084fc;font-weight:700">'+_h(ev.action)+'</span>';
      s2 += ' <span style="color:#888">\u0430\u043a\u0442\u043e\u0440:'+_h(ev.actor_tg_id||'-')+'</span>';
      if(ev.target_tg_id) s2 += ' <span style="color:#888">\u0446\u0435\u043b\u044c:'+_h(ev.target_tg_id)+'</span>';
      s2 += ' <span style="color:#555">'+(ev.created_at||'').slice(0,19)+'</span>';
      if(ev.details) s2 += '<div class="ah-mono" style="color:#666;max-height:40px;overflow:hidden">'+_h(JSON.stringify(ev.details))+'</div>';
      s2 += '</div>';
    });
    body.innerHTML = s2;
  };
}


/* ===== INIT ===== */
function _initHub(){
  _renderTabs();
  _renderBody();
}

var hubBtn = document.getElementById('adminHubBtn');
if(hubBtn){
  hubBtn.addEventListener('click', function(){
    var m = document.getElementById('mAdminHub');
    if(m){
      m.style.display = 'flex';
      _initHub();
    }
  });
}

})();
</script>
<!-- ========== /ADMIN HUB v2 ========== -->
'''

def main():
    if not os.path.isfile(TARGET):
        print(f'ERROR: {TARGET} not found')
        sys.exit(1)

    backup = TARGET + '.bak.' + datetime.now().strftime('%Y%m%d_%H%M%S')
    shutil.copy2(TARGET, backup)
    print(f'Backup: {backup}')

    with open(TARGET, 'r', encoding='utf-8') as f:
        content = f.read()

    changes = 0

    old_block = re.search(
        r'<!-- ========== ADMIN HUB v2 ========== -->.*?<!-- ========== /ADMIN HUB v2 ========== -->',
        content, re.DOTALL
    )
    if old_block:
        content = content[:old_block.start()] + content[old_block.end():]
        print('Removed old admin hub block')

    old_btn = re.search(r'<button[^>]*id="adminHubBtn"[^>]*>.*?</button>\s*', content)
    if old_btn:
        content = content[:old_btn.start()] + content[old_btn.end():]
        print('Removed old god button')

    marker = 'id="adminEditPlayerBtn"'
    idx = content.find(marker)
    if idx >= 0:
        bs = content.rfind('<button', max(0, idx - 300), idx)
        if bs >= 0:
            content = content[:bs] + GOD_BUTTON + content[bs:]
            changes += 1
            print('God button injected')
        else:
            print('WARNING: <button before adminEditPlayerBtn not found')
    else:
        print(f'WARNING: marker "{marker}" not found')

    body_close = content.rfind('</body>')
    if body_close >= 0:
        content = content[:body_close] + ADMIN_HUB_HTML + '\n' + content[body_close:]
        changes += 1
        print('Admin Hub modal injected')
    else:
        print('WARNING: </body> not found')

    with open(TARGET, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f'Done. Changes: {changes}. File: {TARGET}')

if __name__ == '__main__':
    main()
