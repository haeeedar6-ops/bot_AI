import os
import re
import time
import math
import json
import hmac
import logging
import urllib.request
import xml.etree.ElementTree as ET
from functools import wraps
from datetime import datetime
from threading import Thread, Lock

from flask import Flask, jsonify, request, Response
from binance.client import Client
from binance.exceptions import BinanceAPIException

try:
    from google import genai as genai_lib
    from google.genai import types as genai_types
except Exception:
    genai_lib = None
    genai_types = None

try:
    from groq import Groq
except Exception:
    Groq = None

# ==========================================
# 1. الإعدادات (كلها قابلة للتغيير من متغيرات Render)
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def env_int(name, default):
    try:
        return int(float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return int(default)


def env_list(name, default):
    raw = os.environ.get(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")

# نماذج الذكاء: بتجرب بالترتيب، أول واحد يرد بيُعتمد. أضف أو غيّر من Render بدون تعديل الكود
GEMINI_MODELS = env_list("GEMINI_MODELS", "gemini-4-argon,gemini-3.8-flash")
GROQ_MODELS = env_list("GROQ_MODELS", "llama-3.3-70b-versatile,llama-3.1-8b-instant")
QUOTA_BLOCK_MIN = env_int("QUOTA_BLOCK_MIN", 60)       # النموذج اللي خلص حده يتوقف هالقد دقيقة

# مصادر الأخبار (RSS). أي مصدر يفشل بيتجاهل بدون ما يوقف شي
NEWS_FEEDS = env_list(
    "NEWS_FEEDS",
    "https://www.coindesk.com/arc/outboundfeeds/rss/,https://cointelegraph.com/rss,https://decrypt.co/feed"
)

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
SYMBOL = os.environ.get("SYMBOL", "TAOUSDT")
COIN_ASSET = os.environ.get("COIN_ASSET", "TAO")

BUY_DROP_RATIO = env_float("BUY_DROP_RATIO", 0.99)       # شراء عند هبوط 1% عن أعلى سعر قريب
SELL_GAIN_RATIO = env_float("SELL_GAIN_RATIO", 1.025)    # جني ربح عند +2.5%
STOP_LOSS_RATIO = env_float("STOP_LOSS_RATIO", 0.98)     # وقف خسارة عند -2%
FEE_RATE = env_float("FEE_RATE", 0.001)

# حماية من الصفقات الكثيرة والخاسرة
MAX_TRADES_PER_DAY = env_int("MAX_TRADES_PER_DAY", 4)
LOSS_COOLDOWN_MIN = env_int("LOSS_COOLDOWN_MIN", 60)     # بعد أي صفقة خاسرة
STOP_COOLDOWN_MIN = env_int("STOP_COOLDOWN_MIN", 120)    # بعد وقف الخسارة
MAX_RSI_BUY = env_float("MAX_RSI_BUY", 65)               # ما نشتري إذا السعر طالع كتير

LOOP_SLEEP = 15
WAIT_COOLDOWN = 300
KLINE_LIMIT = 12
NEWS_CACHE_SEC = 1200

STATE_FILE = os.environ.get("STATE_FILE", "bot_state_dry.json" if DRY_RUN else "bot_state.json")

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

gclient = None
if genai_lib and GEMINI_API_KEY:
    try:
        gclient = genai_lib.Client(
            api_key=GEMINI_API_KEY,
            http_options=genai_types.HttpOptions(timeout=60000),
        )
    except Exception as e:
        logger.warning(f"تعذر تشغيل Gemini: {e}")

groq_client = None
if Groq and GROQ_API_KEY:
    try:
        groq_client = Groq(api_key=GROQ_API_KEY, timeout=45.0, max_retries=0)
    except Exception as e:
        logger.warning(f"تعذر تشغيل Groq: {e}")

if not gclient and not groq_client:
    logger.warning("ما في أي مزود ذكاء شغال. البوت رح يعتمد على القواعد الرياضية بس.")

# إعدادات يتحكم فيها الموقع
control = {
    "running": True,
    "sell_now": False,
    "buy_now": False,
    "analyze_now": False,
    "mode": os.environ.get("TRADE_MODE", "all"),
    "value": env_float("TRADE_VALUE", 10.0),
    "analysis_interval": env_int("ANALYSIS_INTERVAL_MIN", 15),
    "ai_block": True,
    "adopt": True,
    "stance": "unsure",
    "notes": "",
}
PERSIST_CONTROL = ("mode", "value", "analysis_interval", "ai_block", "adopt", "stance", "notes")
runtime = {"cooldown_until": 0.0, "last_rsi": None, "last_change": 0.0}
blocked_models = {}          # مفتاح النموذج -> وقت انتهاء الحظر المؤقت
analysis_lock = Lock()
news_cache = {"ts": 0.0, "items": []}

bot_status = {
    "mode": "DRY_RUN" if DRY_RUN else "LIVE",
    "symbol": SYMBOL,
    "coin": COIN_ASSET,
    "current_price": 0.0,
    "reference_price": 0.0,
    "buy_trigger": 0.0,
    "last_check": "",
    "last_event": "",
    "usdt_free": 0.0,
    "coin_free": 0.0,
    "total_value": 0.0,
    "next_trade_usdt": 0.0,
}

analysis = {
    "running": False, "outlook": "", "risk": "", "confidence": 0,
    "summary": "", "world_says": "", "points": [], "user_view": "",
    "time": "", "time_ts": 0.0, "duration": 0.0,
    "source": "", "attempts": [], "news": [], "rule_based": False,
    "error": "", "next_at": 0.0,
}
ANALYSIS_SAVE_KEYS = ("outlook", "risk", "confidence", "summary", "world_says", "points", "user_view",
                      "time", "time_ts", "duration", "source", "attempts", "news", "rule_based")

# ==========================================
# 2. الحالة (حفظ واسترجاع)
# ==========================================
state = {
    "in_position": False, "buy_price": 0.0, "qty": 0.0,
    "history": [], "total_profit": 0.0, "wins": 0, "losses": 0,
    "day": "", "trades_today": 0,
}


def load_state():
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
        state["in_position"] = bool(data.get("in_position", False))
        state["buy_price"] = float(data.get("buy_price", 0.0))
        state["qty"] = float(data.get("qty", 0.0))
        state["history"] = list(data.get("history", []))[-30:]
        state["total_profit"] = float(data.get("total_profit", 0.0))
        state["wins"] = int(data.get("wins", 0))
        state["losses"] = int(data.get("losses", 0))
        state["day"] = str(data.get("day", ""))
        state["trades_today"] = int(data.get("trades_today", 0))

        if data.get("c_mode") in ("all", "usdt", "percent"):
            control["mode"] = data["c_mode"]
        if data.get("c_value") is not None:
            control["value"] = float(data["c_value"])
        if data.get("c_analysis_interval"):
            control["analysis_interval"] = max(5, int(data["c_analysis_interval"]))
        for key in ("ai_block", "adopt"):
            if ("c_" + key) in data:
                control[key] = bool(data["c_" + key])
        if data.get("c_stance") in ("unsure", "drop", "stable"):
            control["stance"] = data["c_stance"]
        control["notes"] = str(data.get("c_notes", ""))[:1000]

        saved = data.get("analysis") or {}
        for k in ANALYSIS_SAVE_KEYS:
            if k in saved:
                analysis[k] = saved[k]
        if analysis["time_ts"]:
            analysis["next_at"] = analysis["time_ts"] + control["analysis_interval"] * 60
    except Exception as e:
        logger.warning(f"تعذر قراءة ملف الحالة: {e}")


def save_state():
    try:
        data = dict(state)
        for k in PERSIST_CONTROL:
            data["c_" + k] = control[k]
        data["analysis"] = {k: analysis[k] for k in ANALYSIS_SAVE_KEYS}
        with open(STATE_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"خطأ بحفظ الحالة: {e}")


def set_event(text):
    bot_status["last_event"] = f"{datetime.now().strftime('%H:%M:%S')} - {text}"
    logger.info(text)


def check_day():
    today = datetime.now().strftime("%Y-%m-%d")
    if state["day"] != today:
        state["day"] = today
        state["trades_today"] = 0


# ==========================================
# 3. الموقع (لوحة التحكم)
# ==========================================
app = Flask(__name__)

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>لوحة البوت</title>
<style>
*{box-sizing:border-box}
[hidden]{display:none!important}
body{margin:0;font-family:system-ui,-apple-system,"Segoe UI",Tahoma,sans-serif;background:#0f1419;color:#e6edf3;padding:12px;max-width:520px;margin:auto}
h1{font-size:18px;margin:6px 0 12px;display:flex;justify-content:space-between;align-items:center}
.card{background:#1a2230;border-radius:14px;padding:14px;margin-bottom:12px}
.big{font-size:30px;font-weight:700;direction:ltr;text-align:right}
.lbl{font-size:12px;color:#8b98a9;margin-bottom:4px}
.row{display:flex;gap:10px}
.row>div{flex:1}
.val{font-size:18px;font-weight:600;direction:ltr;text-align:right}
.green{color:#3fb950}.red{color:#f85149}.gray{color:#8b98a9}.gold{color:#e3b341}
.badge{display:inline-block;padding:6px 12px;border-radius:20px;font-size:14px;font-weight:600}
.b-pos{background:#12361f;color:#3fb950}
.b-wait{background:#2b2f3a;color:#8b98a9}
.b-stop{background:#3b1d1d;color:#f85149}
.b-cool{background:#3a3115;color:#e3b341}
.mode{font-size:12px;padding:3px 10px;border-radius:12px}
.m-dry{background:#3a3115;color:#e3b341}.m-live{background:#12361f;color:#3fb950}
button{width:100%;padding:13px;border:0;border-radius:10px;font-size:16px;font-weight:600;color:#fff;cursor:pointer}
.btn-start{background:#238636}.btn-stop{background:#b62324}.btn-sell{background:#9e6a03}.btn-buy{background:#238636}.btn-save{background:#1f6feb}
button:disabled{opacity:.4}
input,select,textarea{width:100%;padding:11px;border-radius:10px;border:1px solid #2d3a4f;background:#0f1419;color:#e6edf3;font-size:16px;font-family:inherit}
textarea{resize:vertical}
input[type=checkbox]{width:auto}
.chk{display:flex;gap:8px;align-items:center;margin-top:10px;font-size:14px}
table{width:100%;border-collapse:collapse;font-size:13px}
td,th{padding:6px 4px;text-align:right;border-bottom:1px solid #243044}
th{color:#8b98a9;font-weight:500}
.ltr{direction:ltr;display:inline-block}
.small{font-size:12px;color:#8b98a9;margin-top:6px}
ul{margin:6px 0 0;padding-right:18px;font-size:14px}
#err{color:#f85149;font-size:14px;margin-top:8px}
</style>
</head>
<body>

<div id="login" class="card" hidden>
  <h1>🔒 دخول</h1>
  <input id="pw" type="password" placeholder="كلمة السر" autocomplete="current-password">
  <div style="height:10px"></div>
  <button class="btn-save" onclick="doLogin()">دخول</button>
  <div id="err"></div>
</div>

<div id="app" hidden>
  <h1><span id="title">🤖 البوت</span><span id="modeBadge" class="mode"></span></h1>

  <div class="card">
    <div class="lbl">السعر اللحظي</div>
    <div class="big" id="price">-</div>
    <div class="small">حالة البوت: <span id="stateBadge" class="badge b-wait">-</span></div>
    <div class="small">صفقات اليوم: <span id="todayTrades">-</span></div>
  </div>

  <div class="card">
    <div class="lbl" id="posTitle">الصفقة الحالية</div>
    <div class="big" id="pnl">-</div>
    <div class="row" style="margin-top:10px">
      <div><div class="lbl">سعر الدخول</div><div class="val" id="buyP">-</div></div>
      <div><div class="lbl">الكمية</div><div class="val" id="qty">-</div></div>
    </div>
    <div class="row" style="margin-top:10px">
      <div><div class="lbl">هدف الربح</div><div class="val green" id="tp">-</div></div>
      <div><div class="lbl">وقف الخسارة</div><div class="val red" id="sl">-</div></div>
    </div>
  </div>

  <div class="card">
    <div class="row">
      <div><div class="lbl">USDT المتوفر</div><div class="val" id="usdt">-</div></div>
      <div><div class="lbl" id="coinLbl">العملة</div><div class="val" id="coin">-</div></div>
    </div>
    <div class="row" style="margin-top:10px">
      <div><div class="lbl">القيمة الكلية</div><div class="val" id="total">-</div></div>
      <div><div class="lbl">الشراء القادم</div><div class="val" id="next">-</div></div>
    </div>
  </div>

  <div class="card">
    <div class="row">
      <div><div class="lbl">إجمالي الربح</div><div class="val" id="tprofit">-</div></div>
      <div><div class="lbl">رابحة / خاسرة</div><div class="val" id="wl">-</div></div>
    </div>
    <div class="small">الأرباح محسوبة بعد خصم عمولة تقديرية</div>
  </div>

  <div class="card">
    <button id="toggleBtn" onclick="toggleRun()">-</button>
    <div style="height:10px"></div>
    <div class="row">
      <div><button class="btn-buy" id="buyBtn" onclick="buyNow()">شراء فوري</button></div>
      <div><button class="btn-sell" id="sellBtn" onclick="sellNow()">بيع فوري</button></div>
    </div>
    <div class="small">الإيقاف بيمنع الشراء التلقائي بس. الشراء الفوري بيتجاهل الفلاتر.</div>
  </div>

  <div class="card">
    <div class="lbl">🧠 تحليل السوق</div>
    <div id="aiStatus" class="small" style="margin-top:0">-</div>
    <div class="row" style="margin-top:10px">
      <div><div class="lbl">التوقع</div><div class="val" id="aiOutlook">-</div></div>
      <div><div class="lbl">خطر هبوط كبير</div><div class="val" id="aiRisk">-</div></div>
    </div>
    <div class="row" style="margin-top:10px">
      <div><div class="lbl">الثقة</div><div class="val" id="aiConf">-</div></div>
      <div><div class="lbl">مدة آخر تحليل</div><div class="val" id="aiDur">-</div></div>
    </div>
    <div class="lbl" style="margin-top:12px">الخلاصة</div>
    <div id="aiSummary" style="font-size:14px">-</div>
    <div class="lbl" style="margin-top:12px">شو بيحكي العالم</div>
    <div id="aiWorld" style="font-size:14px">-</div>
    <ul id="aiPoints"></ul>
    <div class="lbl" style="margin-top:12px">رد الذكاء على رأيك</div>
    <div id="aiUser" style="font-size:14px">-</div>
    <div class="lbl" style="margin-top:12px">أخبار استخدمها التحليل</div>
    <ul id="aiNews"></ul>
    <div class="lbl" style="margin-top:12px">محاولات المزودين</div>
    <div id="aiAttempts" class="small">-</div>
    <div class="small" id="aiNext"></div>
    <div style="height:10px"></div>
    <button class="btn-save" id="analyzeBtn" onclick="analyzeNow()">حلل الآن</button>
  </div>

  <div class="card">
    <div class="lbl">📝 رأيك وملاحظاتك عن السوق</div>
    <select id="aiStance">
      <option value="unsure">مو متأكد</option>
      <option value="drop">رأيي: رح ينزل كتير</option>
      <option value="stable">رأيي: ما رح ينزل كتير</option>
    </select>
    <div style="height:8px"></div>
    <textarea id="aiNotes" rows="4" maxlength="1000" placeholder="اكتب شو شايف بالسوق، أخبار، أسباب..."></textarea>
    <div class="lbl" style="margin-top:10px">كل كم دقيقة يحلل (5 لـ 1440)</div>
    <input id="aiInterval" type="number" min="5" max="1440" step="1">
    <label class="chk"><input type="checkbox" id="aiBlock"> الذكاء يمنع الشراء وقت خطر هبوط كبير</label>
    <label class="chk"><input type="checkbox" id="adopt"> استخدم عملات TAO الموجودة بالمحفظة</label>
    <div style="height:10px"></div>
    <button class="btn-save" onclick="saveAi()">حفظ</button>
    <div class="small" id="aiMsg">رأيك بيدخل بالتحليل كمعلومة، والذكاء ممكن يخالفك إذا الأرقام والأخبار تقول غير هيك.</div>
  </div>

  <div class="card">
    <div class="lbl">كم يشتري البوت بكل صفقة</div>
    <div class="row">
      <div><select id="setMode" onchange="onModeChange()"><option value="all">كامل الرصيد</option><option value="usdt">مبلغ ثابت (USDT)</option><option value="percent">نسبة من الرصيد (%)</option></select></div>
      <div><input id="setVal" type="number" step="any" min="1"></div>
    </div>
    <div style="height:10px"></div>
    <button class="btn-save" onclick="saveSettings()">حفظ</button>
    <div class="small" id="setMsg"></div>
  </div>

  <div class="card">
    <div class="lbl">آخر حدث</div>
    <div id="event" style="font-size:14px">-</div>
    <div class="small">آخر فحص: <span id="lastCheck">-</span></div>
  </div>

  <div class="card">
    <div class="lbl">آخر الصفقات</div>
    <table><thead><tr><th>الوقت</th><th>النتيجة</th><th>%</th><th>السبب</th></tr></thead><tbody id="hist"></tbody></table>
  </div>

  <button onclick="logout()" style="background:#2b2f3a">خروج</button>
</div>

<script>
let pw = localStorage.getItem('pw') || '';
let timer = null;
let lastData = null;
let formInit = false;
let aiInit = false;

function $(id){return document.getElementById(id)}
function fmt(n,d){ if(n===null||n===undefined||isNaN(n)) return '-'; return Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d}) }
function clsPn(n){ return n>0?'green':(n<0?'red':'gray') }
function fmtDur(s){ s=Math.round(s); if(s<60) return s+' ثانية'; const m=Math.floor(s/60); const r=s%60; return m+' د '+(r?r+' ث':''); }

async function api(path, body){
  const opts = {headers:{'X-Auth':pw,'Content-Type':'application/json'}};
  if(body){ opts.method='POST'; opts.body=JSON.stringify(body); }
  const r = await fetch(path, opts);
  const j = await r.json().catch(()=>({}));
  if(r.status===401 || r.status===503){ const e=new Error(j.error||'خطأ'); e.auth=true; throw e; }
  if(!r.ok){ throw new Error(j.error||'خطأ'); }
  return j;
}

async function doLogin(){
  pw = $('pw').value;
  $('err').textContent='';
  try{ await api('/api/status'); localStorage.setItem('pw', pw); start(); }
  catch(e){ $('err').textContent = e.message; }
}

function logout(){
  localStorage.removeItem('pw'); pw='';
  clearInterval(timer);
  $('app').hidden=true; $('login').hidden=false;
}

function start(){
  $('login').hidden=true; $('app').hidden=false;
  refresh();
  clearInterval(timer);
  timer = setInterval(refresh, 5000);
}

async function refresh(){
  try{ const d = await api('/api/status'); lastData = d; render(d); }
  catch(e){ if(e.auth){ logout(); $('err').textContent=e.message; } }
}

function renderAI(a){
  const ob={bullish:['صاعد 📈','green'],neutral:['محايد ➖','gray'],bearish:['هابط 📉','red'],crash_risk:['خطر انهيار ⚠️','red']};
  const rb={low:['منخفض','green'],medium:['متوسط','gold'],high:['عالي','red']};

  if(a.running){ $('aiStatus').textContent='⏳ جاري التحليل...'; }
  else if(a.time){ $('aiStatus').textContent='آخر تحليل: '+a.time+' - المصدر: '+(a.source||'-')+(a.rule_based?' ⚠️':''); }
  else { $('aiStatus').textContent='ما صار تحليل لسا'; }
  if(a.error){ $('aiStatus').textContent += ' | ⚠️ '+a.error; }

  const o=ob[a.outlook]; const r=rb[a.risk];
  $('aiOutlook').textContent=o?o[0]:'-'; $('aiOutlook').className='val '+(o?o[1]:'gray');
  $('aiRisk').textContent=r?r[0]:'-'; $('aiRisk').className='val '+(r?r[1]:'gray');
  $('aiConf').textContent=a.time?(a.confidence+'%'):'-';
  $('aiDur').textContent=a.duration?fmtDur(a.duration):'-';
  $('aiSummary').textContent=a.summary||'-';
  $('aiWorld').textContent=a.world_says||'-';
  $('aiUser').textContent=a.user_view||'-';
  $('aiAttempts').textContent=(a.attempts&&a.attempts.length)?a.attempts.join(' • '):'-';

  const ul=$('aiPoints'); ul.innerHTML='';
  (a.points||[]).forEach(p=>{ const li=document.createElement('li'); li.textContent=p; ul.appendChild(li); });

  const nl=$('aiNews'); nl.innerHTML='';
  (a.news||[]).forEach(n=>{ const li=document.createElement('li'); li.textContent=(n.rel?'⭐ ':'')+n.title+(n.src?' ('+n.src+')':''); nl.appendChild(li); });
  if(!(a.news||[]).length){ const li=document.createElement('li'); li.textContent='ما في أخبار جديدة'; nl.appendChild(li); }

  if(a.running){ $('aiNext').textContent=''; }
  else if(a.next_in!==null && a.next_in!==undefined){ $('aiNext').textContent='التحليل الجاي بعد '+fmtDur(a.next_in); }
  $('analyzeBtn').disabled=!!a.running;
}

function render(d){
  const s=d.status, st=d.state, c=d.control;
  $('title').textContent = '🤖 ' + s.symbol;
  const mb=$('modeBadge');
  mb.textContent = s.mode==='LIVE' ? 'حقيقي' : 'تجربة';
  mb.className = 'mode ' + (s.mode==='LIVE'?'m-live':'m-dry');

  $('price').textContent = fmt(s.current_price,4)+' $';
  $('todayTrades').textContent = (d.trades_today||0)+' / '+d.max_trades_day;

  const sb=$('stateBadge');
  const map={in_position:['داخل صفقة (بانتظار البيع)','b-pos'],
             stopped:['متوقف','b-stop'],
             cooldown:['استراحة (بعد صفقة)','b-cool'],
             waiting:['بانتظار فرصة شراء','b-wait']};
  const m=map[d.state_code]||map.waiting;
  sb.textContent=m[0]; sb.className='badge '+m[1];

  if(st.in_position){
    const pct=(s.current_price/st.buy_price-1)*100;
    $('posTitle').textContent = pct>=0 ? 'الصفقة الحالية (ربح)' : 'الصفقة الحالية (خسارة)';
    const p=$('pnl'); p.textContent=(pct>=0?'+':'')+fmt(pct,2)+' %'; p.className='big '+clsPn(pct);
    $('buyP').textContent=fmt(st.buy_price,4);
    $('qty').textContent=fmt(st.qty,4);
    $('tp').textContent=fmt(d.tp_price,4);
    $('sl').textContent=fmt(d.sl_price,4);
    $('sellBtn').disabled=false; $('buyBtn').disabled=true;
  }else{
    $('posTitle').textContent='ما في صفقة مفتوحة';
    const p=$('pnl'); p.textContent='-'; p.className='big gray';
    $('buyP').textContent='-'; $('qty').textContent='-'; $('tp').textContent='-'; $('sl').textContent='-';
    $('sellBtn').disabled=true; $('buyBtn').disabled=false;
  }

  $('usdt').textContent=fmt(s.usdt_free,2)+' $';
  $('coinLbl').textContent=s.coin;
  $('coin').textContent=fmt(s.coin_free,4);
  $('total').textContent=fmt(s.total_value,2)+' $';
  $('next').textContent=fmt(s.next_trade_usdt,2)+' $';

  const tp=st.total_profit;
  const t=$('tprofit'); t.textContent=(tp>=0?'+':'')+fmt(tp,4)+' $'; t.className='val '+clsPn(tp);
  $('wl').textContent=st.wins+' / '+st.losses;

  const tb=$('toggleBtn');
  if(c.running){ tb.textContent='⏸ إيقاف الشراء التلقائي'; tb.className='btn-stop'; }
  else { tb.textContent='▶ تشغيل البوت'; tb.className='btn-start'; }

  if(!formInit){ $('setMode').value=c.mode; $('setVal').value=c.value; onModeChange(); formInit=true; }
  if(!aiInit){
    $('aiStance').value=c.stance; $('aiNotes').value=c.notes||''; $('aiInterval').value=c.analysis_interval;
    $('aiBlock').checked=!!c.ai_block; $('adopt').checked=!!c.adopt; aiInit=true;
  }

  renderAI(d.analysis);

  $('event').textContent=s.last_event||'-';
  $('lastCheck').textContent=s.last_check||'-';

  const h=$('hist'); h.innerHTML='';
  (st.history||[]).slice().reverse().slice(0,10).forEach(x=>{
    const tr=document.createElement('tr');
    const cells=[x.time.slice(5,16), (x.pnl>=0?'+':'')+fmt(x.pnl,3)+'$', (x.pct>=0?'+':'')+fmt(x.pct,2)+'%', x.reason];
    cells.forEach((v,i)=>{
      const td=document.createElement('td'); td.textContent=v;
      if(i===1||i===2) td.className='ltr '+clsPn(x.pnl);
      tr.appendChild(td);
    });
    h.appendChild(tr);
  });
}

async function toggleRun(){
  if(!lastData) return;
  const action = lastData.control.running ? 'stop' : 'start';
  try{ await api('/api/control',{action}); refresh(); }catch(e){ alert(e.message); }
}

async function buyNow(){
  if(!confirm('أكيد بدك تشتري هلأ بسعر السوق؟')) return;
  try{ await api('/api/control',{action:'buy_now'}); $('setMsg').textContent='تم طلب الشراء، بينفذ خلال ثواني.'; refresh(); }catch(e){ alert(e.message); }
}

async function sellNow(){
  if(!confirm('أكيد بدك تبيع الصفقة هلأ؟')) return;
  try{ await api('/api/control',{action:'sell_now'}); $('setMsg').textContent='تم طلب البيع، بينفذ خلال ثواني.'; refresh(); }catch(e){ alert(e.message); }
}

async function analyzeNow(){
  try{ await api('/api/control',{action:'analyze_now'}); $('aiStatus').textContent='⏳ جاري التحليل...'; $('analyzeBtn').disabled=true; setTimeout(refresh,1500); }
  catch(e){ alert(e.message); }
}

function onModeChange(){ $('setVal').hidden = ($('setMode').value==='all'); }

async function saveSettings(){
  try{
    await api('/api/settings',{mode:$('setMode').value, value:parseFloat($('setVal').value)});
    $('setMsg').textContent='انحفظ ✅'; refresh();
  }catch(e){ $('setMsg').textContent=e.message; }
}

async function saveAi(){
  try{
    await api('/api/ai_settings',{
      stance:$('aiStance').value, notes:$('aiNotes').value,
      interval:parseInt($('aiInterval').value), ai_block:$('aiBlock').checked, adopt:$('adopt').checked
    });
    $('aiMsg').textContent='انحفظ ✅'; refresh();
  }catch(e){ $('aiMsg').textContent=e.message; }
}

if(pw){ api('/api/status').then(start).catch(()=>{ $('login').hidden=false; }); }
else { $('login').hidden=false; }
</script>
</body>
</html>
"""


def require_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not DASHBOARD_PASSWORD:
            return jsonify({"error": "حط DASHBOARD_PASSWORD بمتغيرات Render أول شي"}), 503
        supplied = request.headers.get("X-Auth", "")
        if not hmac.compare_digest(supplied.encode(), DASHBOARD_PASSWORD.encode()):
            time.sleep(1)
            return jsonify({"error": "كلمة السر غلط"}), 401
        return fn(*args, **kwargs)
    return wrapper


def current_state_code():
    if state["in_position"]:
        return "in_position"
    if not control["running"]:
        return "stopped"
    if time.time() < runtime["cooldown_until"]:
        return "cooldown"
    return "waiting"


@app.route('/')
def home():
    return Response(DASHBOARD_HTML, mimetype="text/html")


@app.route('/health')
def health():
    return "ok"


@app.route('/api/status')
@require_auth
def api_status():
    check_day()
    tp_price = state["buy_price"] * SELL_GAIN_RATIO if state["in_position"] else 0.0
    sl_price = state["buy_price"] * STOP_LOSS_RATIO if state["in_position"] else 0.0
    a = dict(analysis)
    a["next_in"] = max(0, int(analysis["next_at"] - time.time())) if analysis["next_at"] else None
    return jsonify({
        "status": bot_status,
        "state": state,
        "control": control,
        "analysis": a,
        "state_code": current_state_code(),
        "tp_price": tp_price,
        "sl_price": sl_price,
        "trades_today": state["trades_today"],
        "max_trades_day": MAX_TRADES_PER_DAY,
    })


@app.route('/api/control', methods=['POST'])
@require_auth
def api_control():
    action = (request.get_json(silent=True) or {}).get("action")
    if action == "start":
        control["running"] = True
        set_event("تم تشغيل البوت من الموقع")
    elif action == "stop":
        control["running"] = False
        set_event("تم إيقاف الشراء التلقائي من الموقع")
    elif action == "buy_now":
        if state["in_position"]:
            return jsonify({"error": "في صفقة مفتوحة أصلاً"}), 400
        control["buy_now"] = True
        set_event("طلب شراء فوري من الموقع")
    elif action == "sell_now":
        if not state["in_position"]:
            return jsonify({"error": "ما في صفقة مفتوحة"}), 400
        control["sell_now"] = True
        set_event("طلب بيع فوري من الموقع")
    elif action == "analyze_now":
        if analysis["running"]:
            return jsonify({"error": "التحليل شغّال هلأ"}), 400
        control["analyze_now"] = True
    else:
        return jsonify({"error": "أمر غير معروف"}), 400
    return jsonify({"ok": True})


@app.route('/api/settings', methods=['POST'])
@require_auth
def api_settings():
    data = request.get_json(silent=True) or {}
    mode = data.get("mode")
    if mode not in ("all", "usdt", "percent"):
        return jsonify({"error": "النوع غلط"}), 400
    if mode == "all":
        control["mode"] = "all"
        save_state()
        set_event("حجم الصفقة: كامل الرصيد المتوفر")
        return jsonify({"ok": True})
    try:
        value = float(data.get("value"))
    except (TypeError, ValueError):
        return jsonify({"error": "القيمة لازم تكون رقم"}), 400
    if mode == "percent" and not (1 <= value <= 95):
        return jsonify({"error": "النسبة لازم بين 1 و 95"}), 400
    if mode == "usdt" and not (1 <= value <= 100000):
        return jsonify({"error": "المبلغ لازم يكون 1 USDT على الأقل"}), 400
    control["mode"] = mode
    control["value"] = value
    save_state()
    set_event(f"تغيير حجم الصفقة: {value} {'USDT' if mode == 'usdt' else '% من الرصيد'}")
    return jsonify({"ok": True})


@app.route('/api/ai_settings', methods=['POST'])
@require_auth
def api_ai_settings():
    data = request.get_json(silent=True) or {}
    try:
        interval = int(data.get("interval"))
    except (TypeError, ValueError):
        return jsonify({"error": "مدة التحليل لازم تكون رقم (دقائق)"}), 400
    if not (5 <= interval <= 1440):
        return jsonify({"error": "مدة التحليل بين 5 و 1440 دقيقة"}), 400
    stance = data.get("stance")
    if stance not in ("unsure", "drop", "stable"):
        return jsonify({"error": "الرأي غلط"}), 400

    control["analysis_interval"] = interval
    control["stance"] = stance
    control["notes"] = str(data.get("notes", ""))[:1000]
    control["ai_block"] = bool(data.get("ai_block"))
    control["adopt"] = bool(data.get("adopt"))
    if analysis["time_ts"]:
        analysis["next_at"] = analysis["time_ts"] + interval * 60
    save_state()
    set_event("تم حفظ ملاحظاتك وإعدادات الذكاء")
    return jsonify({"ok": True})


def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)


# ==========================================
# 4. بيانات السوق وقواعد الزوج
# ==========================================
def get_market_data():
    ticker = client.get_ticker(symbol=SYMBOL)
    current_price = float(ticker['lastPrice'])
    avg_24h_price = float(ticker['weightedAvgPrice'])
    klines = client.get_klines(symbol=SYMBOL, interval=Client.KLINE_INTERVAL_15MINUTE, limit=KLINE_LIMIT)
    recent_closes = [float(k[4]) for k in klines]
    return current_price, avg_24h_price, recent_closes


def get_symbol_rules():
    info = client.get_symbol_info(SYMBOL)
    step, min_qty, min_notional = 0.0001, 0.0, 5.0
    for f in info['filters']:
        if f['filterType'] == 'LOT_SIZE':
            step = float(f['stepSize'])
            min_qty = float(f['minQty'])
        elif f['filterType'] in ('NOTIONAL', 'MIN_NOTIONAL'):
            min_notional = float(f.get('minNotional', min_notional))
    return step, min_qty, min_notional


def format_qty(qty, step):
    precision = max(0, int(round(-math.log10(step))))
    floored = math.floor(qty / step + 1e-9) * step
    return round(floored, precision)


def refresh_balances(price):
    try:
        acc = client.get_account()
        bal = {b['asset']: float(b['free']) for b in acc['balances']}
        bot_status["usdt_free"] = bal.get('USDT', 0.0)
        bot_status["coin_free"] = bal.get(COIN_ASSET, 0.0)
        bot_status["total_value"] = bot_status["usdt_free"] + bot_status["coin_free"] * price
    except Exception as e:
        logger.warning(f"تعذر جلب الرصيد: {e}")


def compute_trade_amount():
    usdt_free = bot_status["usdt_free"]
    if control["mode"] == "all":
        amount = usdt_free * 0.995
    elif control["mode"] == "percent":
        amount = usdt_free * control["value"] / 100.0
    else:
        amount = control["value"]
    if usdt_free > 0:
        amount = min(amount, usdt_free * 0.995)
    return amount


def calc_rsi(closes, period=14):
    if len(closes) <= period:
        return None
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0)
        losses += max(-d, 0)
    avg_g, avg_l = gains / period, losses / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_g = (avg_g * (period - 1) + max(d, 0)) / period
        avg_l = (avg_l * (period - 1) + max(-d, 0)) / period
    if avg_l == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_g / avg_l), 1)


def build_market_snapshot():
    t = client.get_ticker(symbol=SYMBOL)
    btc = client.get_ticker(symbol="BTCUSDT")
    k1h = client.get_klines(symbol=SYMBOL, interval=Client.KLINE_INTERVAL_1HOUR, limit=48)
    k1d = client.get_klines(symbol=SYMBOL, interval=Client.KLINE_INTERVAL_1DAY, limit=14)
    c1h = [float(k[4]) for k in k1h]
    c1d = [float(k[4]) for k in k1d]
    return {
        "price": float(t['lastPrice']),
        "change_24h_pct": float(t['priceChangePercent']),
        "high_24h": float(t['highPrice']),
        "low_24h": float(t['lowPrice']),
        "volume_24h_usdt": round(float(t['quoteVolume']), 0),
        "btc_change_24h_pct": float(btc['priceChangePercent']),
        "rsi_1h": calc_rsi(c1h),
        "closes_1h_last24": c1h[-24:],
        "closes_1d_last14": c1d,
    }


# ==========================================
# 5. الأخبار (من مصادر حقيقية RSS)
# ==========================================
def fetch_news():
    if time.time() - news_cache["ts"] < NEWS_CACHE_SEC and news_cache["items"]:
        return news_cache["items"]

    items = []
    for url in NEWS_FEEDS:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                root = ET.fromstring(resp.read())
            src = re.sub(r"^https?://(www\.)?", "", url).split("/")[0]
            for it in root.iter("item"):
                title = (it.findtext("title") or "").strip()
                desc = re.sub(r"<[^>]+>", "", it.findtext("description") or "")[:300]
                if title:
                    items.append({"title": title[:200], "desc": desc, "src": src,
                                  "pub": (it.findtext("pubDate") or "")[:30]})
        except Exception as e:
            logger.warning(f"تعذر جلب الأخبار من {url}: {str(e)[:100]}")

    rel_re = re.compile(r"bittensor|\bTAO\b", re.I)
    mkt_re = re.compile(r"bitcoin|crypto|ethereum|market", re.I)
    related = [i for i in items if rel_re.search(i["title"] + " " + i["desc"])]
    market = [i for i in items if i not in related and mkt_re.search(i["title"] + " " + i["desc"])]
    for i in related:
        i["rel"] = True
    for i in market:
        i["rel"] = False
    chosen = (related[:6] + market[:4])

    news_cache["ts"] = time.time()
    news_cache["items"] = chosen
    return chosen


# ==========================================
# 6. الذكاء الاصطناعي: سلسلة مزودين بدون فشل
# ==========================================
SYS_PROMPT = "أنت محلل أسواق كريبتو محترف. تجيب دائماً بصيغة JSON فقط بدون أي نص آخر، وما تخترع أخبار."


def classify_error(e):
    low = str(e).lower()
    if "429" in low or "resource_exhausted" in low or "rate limit" in low or "rate_limit" in low or "quota" in low:
        return "quota"
    if "503" in low or "unavailable" in low or "overloaded" in low or "timeout" in low or "timed out" in low:
        return "busy"
    if ("404" in low or "not_found" in low or "not found" in low or "decommission" in low
            or "model_not_found" in low or "403" in low or "permission" in low or "invalid model" in low):
        return "bad"
    return "other"


def call_gemini(prompt, model, search, max_tokens):
    cfg = genai_types.GenerateContentConfig(
        system_instruction=SYS_PROMPT,
        max_output_tokens=max_tokens,
        tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())] if search else None,
    )
    resp = gclient.models.generate_content(model=model, contents=prompt, config=cfg)
    return (getattr(resp, "text", None) or "").strip()


def call_groq(prompt, model, max_tokens):
    resp = groq_client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYS_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=max_tokens,
    )
    return (resp.choices[0].message.content or "").strip()


def provider_chain(quick):
    chain = []
    if gclient:
        for m in GEMINI_MODELS:
            if not quick:
                chain.append(("gemini", m, True))    # مع بحث بالإنترنت
            chain.append(("gemini", m, False))       # بدون بحث
    if groq_client:
        for m in GROQ_MODELS:
            chain.append(("groq", m, False))
    return chain


def ask_ai(prompt, max_tokens=1200, quick=False):
    """يرجع (النص, اسم المصدر, قائمة المحاولات). ما بيرمي خطأ أبداً."""
    attempts = []
    now = time.time()
    for prov, model, search in provider_chain(quick):
        key = f"{prov}:{model}"
        label = f"{model}{' +بحث' if search else ''}"
        if now < blocked_models.get(key, 0):
            continue
        if search and now < blocked_models.get(key + ":s", 0):
            continue
        try:
            if prov == "gemini":
                text = call_gemini(prompt, model, search, max_tokens * 2 if not quick else 1024)
            else:
                text = call_groq(prompt, model, max_tokens if not quick else 16)
            if not text:
                attempts.append(f"{label}: رد فاضي")
                continue
            return text, label, attempts
        except Exception as e:
            kind = classify_error(e)
            attempts.append(f"{label}: {kind}")
            logger.warning(f"فشل {label} ({kind}): {str(e)[:200]}")
            if kind == "quota":
                blocked_models[key] = time.time() + QUOTA_BLOCK_MIN * 60
            elif kind == "bad":
                blocked_models[key] = time.time() + 6 * 3600
            elif kind == "busy":
                blocked_models[key] = time.time() + 180
            elif search:
                blocked_models[key + ":s"] = time.time() + 1800
            else:
                blocked_models[key] = time.time() + 300
    return None, "", attempts


def parse_json_block(text):
    a, b = text.find("{"), text.rfind("}")
    if a == -1 or b == -1 or b <= a:
        raise ValueError("الرد ما فيه JSON")
    return json.loads(text[a:b + 1])


def build_analysis_prompt(snap, news):
    stance_text = {
        "drop": "المستخدم شايف إن السوق رح ينزل كتير.",
        "stable": "المستخدم شايف إن السوق ما رح ينزل كتير.",
        "unsure": "المستخدم ما عنده رأي محدد.",
    }[control["stance"]]
    notes = control["notes"].strip() or "(ما كتب ملاحظات)"
    news_lines = "\n".join(
        f"- [{n['src']}{' | يخص TAO' if n.get('rel') else ''}] {n['title']}: {n.get('desc', '')[:160]}"
        for n in news
    ) or "(ما وصلت أخبار من المصادر)"
    return (
        f"حلل العملة {COIN_ASSET} مقابل USDT (TAO = Bittensor) لتتوقع الـ 24 ساعة الجاية. "
        f"التاريخ اليوم {datetime.now().strftime('%Y-%m-%d')}.\n\n"
        f"أرقام السوق من Binance:\n{json.dumps(snap, ensure_ascii=False)}\n\n"
        f"أحدث الأخبار (من مصادر حقيقية، اعتمد عليها بس):\n{news_lines}\n\n"
        f"رأي المستخدم: {stance_text}\nملاحظاته: {notes}\n"
        "رأي المستخدم معلومة وحدة بس، وزنها مقابل الأرقام والأخبار، وخالفه بصراحة إذا الأدلة تقول غير هيك. "
        "إذا ما في أخبار واضحة، اكتب ذلك بـ world_says ولا تخترع.\n\n"
        "أجب بـ JSON فقط بدون أي نص قبله أو بعده وبدون علامات ```، بهالشكل:\n"
        '{"outlook": "bullish | neutral | bearish | crash_risk", '
        '"risk_big_drop": "low | medium | high", "confidence": 0-100, '
        '"summary": "جملتين بالعامية الشامية عن توقعك للـ 24 ساعة", '
        '"world_says": "جملة: شو بتحكي الأخبار اليوم عن العملة", '
        '"points": ["نقطة", "نقطة", "نقطة"], '
        '"user_view": "جملة: رأيك برأي المستخدم وملاحظاته"}'
    )


def rule_analysis(snap):
    """تحليل احتياطي بالأرقام لما كل الذكاء يفشل. ما بيفشل."""
    ch = snap["change_24h_pct"]
    rsi = snap["rsi_1h"]
    if ch <= -8 or (rsi is not None and rsi < 20):
        outlook, risk = "crash_risk", "high"
    elif ch <= -4:
        outlook, risk = "bearish", "medium"
    elif ch >= 4:
        outlook, risk = "bullish", "low"
    else:
        outlook, risk = "neutral", "medium"
    return {
        "outlook": outlook,
        "risk_big_drop": risk,
        "confidence": 30,
        "summary": f"التغير خلال 24 ساعة {ch:+.2f}%، ومؤشر RSI للساعة {rsi}. تحليل بالأرقام فقط.",
        "world_says": "ما قدرنا نجيب تحليل أخبار الآن، شوف الأخبار المعروضة تحت.",
        "points": [f"التغير 24 ساعة: {ch:+.2f}%", f"RSI ساعة: {rsi}", f"BTC 24 ساعة: {snap['btc_change_24h_pct']:+.2f}%"],
        "user_view": "",
    }


def apply_analysis(r, source, attempts, news, rule_based):
    outlook = r.get("outlook") if r.get("outlook") in ("bullish", "neutral", "bearish", "crash_risk") else "neutral"
    risk = r.get("risk_big_drop") if r.get("risk_big_drop") in ("low", "medium", "high") else "medium"
    try:
        conf = max(0, min(100, int(r.get("confidence", 0))))
    except (TypeError, ValueError):
        conf = 0
    analysis.update({
        "outlook": outlook,
        "risk": risk,
        "confidence": conf,
        "summary": str(r.get("summary", ""))[:400],
        "world_says": str(r.get("world_says", ""))[:400],
        "points": [str(p)[:200] for p in (r.get("points") or [])][:4],
        "user_view": str(r.get("user_view", ""))[:400],
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "time_ts": time.time(),
        "source": source,
        "attempts": attempts[-6:],
        "news": [{"title": n["title"], "src": n["src"], "rel": bool(n.get("rel"))} for n in news],
        "rule_based": rule_based,
        "error": "",
        "next_at": time.time() + control["analysis_interval"] * 60,
    })


def run_analysis():
    if not analysis_lock.acquire(blocking=False):
        return
    analysis["running"] = True
    started = time.time()
    try:
        snap = build_market_snapshot()
        runtime["last_rsi"] = snap["rsi_1h"]
        runtime["last_change"] = snap["change_24h_pct"]
        news = fetch_news()

        text, source, attempts = ask_ai(build_analysis_prompt(snap, news), max_tokens=1200)
        r = None
        if text:
            try:
                r = parse_json_block(text)
            except Exception as e:
                attempts.append(f"تنسيق الرد: {str(e)[:60]}")
        if r is None:
            r = rule_analysis(snap)
            apply_analysis(r, "قواعد داخلية (الذكاء ما رد)", attempts, news, True)
        else:
            apply_analysis(r, source, attempts, news, False)
        set_event(f"تحليل جديد: {analysis['outlook']} | خطر هبوط {analysis['risk']} | {analysis['source']}")
    except Exception as e:
        analysis["error"] = f"تعذر جلب بيانات Binance: {str(e)[:120]}"
        analysis["next_at"] = time.time() + 300
        logger.error(f"خطأ بالتحليل: {e}")
    finally:
        analysis["duration"] = round(time.time() - started, 1)
        analysis["running"] = False
        analysis_lock.release()
        save_state()


def analysis_loop():
    time.sleep(25)
    while True:
        try:
            if control["analyze_now"] or time.time() >= analysis["next_at"]:
                control["analyze_now"] = False
                run_analysis()
        except Exception as e:
            logger.error(f"خطأ بحلقة التحليل: {e}")
        time.sleep(3)


def ai_blocks_buy():
    if not control["ai_block"] or not analysis["time_ts"]:
        return False
    max_age = max(2 * control["analysis_interval"] * 60, 7200)
    if time.time() - analysis["time_ts"] > max_age:
        return False
    return analysis["risk"] == "high" or analysis["outlook"] == "crash_risk"


def math_filter_ok(current_price, avg_24h, recent):
    if current_price < avg_24h * 0.95:
        return False, "السعر أقل من متوسط 24 ساعة بأكثر من 5%"
    if len(recent) >= 4:
        last3 = recent[-3:]
        falling = last3[0] > last3[1] > last3[2]
        drop = (recent[-4] - current_price) / recent[-4]
        if falling and drop > 0.02:
            return False, "هبوط متواصل وسريع"
    return True, ""


def quick_buy_decision(price, avg_24h, recent):
    """يرجع (قرار, سبب). يعتمد على الذكاء، وإذا ما في ذكاء يعتمد على القواعد."""
    rsi = runtime.get("last_rsi")
    change = runtime.get("last_change", 0.0)
    ctx = ""
    if analysis["time_ts"]:
        ctx = f"آخر تحليل: {analysis['outlook']}، خطر الهبوط الكبير {analysis['risk']}. "
    prompt = (
        f"عملة {SYMBOL}. السعر {price}، متوسط 24 ساعة {avg_24h:.4f}، RSI ساعة {rsi}، "
        f"تغير 24 ساعة {change:+.2f}%، آخر إغلاقات 15 دقيقة {recent}. {ctx}"
        "السعر نزل قرب 1% من أعلى قريب. هل الشراء الارتدادي آمن الآن؟ "
        "أجب بكلمة وحدة فقط: BUY أو WAIT"
    )
    text, src, _ = ask_ai(prompt, max_tokens=16, quick=True)
    if text:
        up = text.upper()
        if "BUY" in up and "WAIT" not in up:
            return True, f"الذكاء ({src}) قال BUY"
        return False, f"الذكاء ({src}) قال WAIT"
    ok = rsi is not None and rsi < 40 and change > -8
    return ok, ("ما في ذكاء متاح، القواعد قالت " + ("شراء" if ok else "انتظار"))


def buy_allowed(price, avg_24h, recent):
    """يرجع (مسموح؟, سبب)."""
    check_day()
    if state["trades_today"] >= MAX_TRADES_PER_DAY:
        return False, f"وصلنا الحد اليومي ({MAX_TRADES_PER_DAY} صفقات)"
    if ai_blocks_buy():
        return False, "الذكاء شايف خطر هبوط كبير"
    ok, why = math_filter_ok(price, avg_24h, recent)
    if not ok:
        return False, f"الفلتر الرياضي: {why}"
    rsi = runtime.get("last_rsi")
    if rsi is not None and rsi > MAX_RSI_BUY:
        return False, f"RSI {rsi} مرتفع، السعر طالع كتير"
    return quick_buy_decision(price, avg_24h, recent)


# ==========================================
# 7. تنفيذ الأوامر
# ==========================================
def avg_fill_price_and_qty(order, fallback_price):
    fills = order.get('fills') or []
    if not fills:
        return fallback_price, float(order.get('executedQty', 0.0))
    total_qty = sum(float(f['qty']) for f in fills)
    total_cost = sum(float(f['qty']) * float(f['price']) for f in fills)
    commission_in_coin = sum(float(f['commission']) for f in fills if f.get('commissionAsset') == COIN_ASSET)
    avg_price = total_cost / total_qty if total_qty else fallback_price
    return avg_price, total_qty - commission_in_coin


def do_buy(usdt_amount, current_price, step, min_qty, min_notional):
    qty = format_qty(usdt_amount / current_price, step)
    if qty < min_qty or qty * current_price < min_notional:
        set_event(f"المبلغ {usdt_amount:.2f}$ صغير على الحد الأدنى للزوج. زيد الرصيد أو حجم الصفقة.")
        return None
    if DRY_RUN:
        set_event(f"[تجربة] شراء وهمي {qty} {COIN_ASSET} بسعر {current_price}")
        return current_price, qty
    try:
        order = client.order_market_buy(symbol=SYMBOL, quantity=qty)
    except BinanceAPIException as e:
        set_event(f"Binance رفض الشراء: {str(e.message)[:100]}")
        return None
    buy_price, net_qty = avg_fill_price_and_qty(order, current_price)
    set_event(f"تم الشراء: {net_qty:.6f} {COIN_ASSET} بمتوسط {buy_price:.4f}")
    return buy_price, net_qty


def do_sell(qty, current_price, step, min_qty):
    if DRY_RUN:
        set_event(f"[تجربة] بيع وهمي {qty} {COIN_ASSET} بسعر {current_price}")
        return current_price
    free_coin = float(client.get_asset_balance(asset=COIN_ASSET)['free'])
    sell_qty = format_qty(min(qty, free_coin), step)
    if sell_qty < min_qty or sell_qty <= 0:
        set_event("الكمية المتاحة للبيع صفر (يمكن انباعت يدوياً). تصفير الحالة.")
        return None
    try:
        order = client.order_market_sell(symbol=SYMBOL, quantity=sell_qty)
    except BinanceAPIException as e:
        set_event(f"Binance رفض البيع: {str(e.message)[:100]}")
        return None
    sell_price, _ = avg_fill_price_and_qty(order, current_price)
    set_event(f"تم البيع: {sell_qty} {COIN_ASSET} بمتوسط {sell_price:.4f}")
    return sell_price


def record_trade(buy_price, sell_price, qty, reason):
    gross = (sell_price - buy_price) * qty
    fees = FEE_RATE * (buy_price + sell_price) * qty
    pnl = gross - fees
    pct = pnl / (buy_price * qty) * 100 if buy_price * qty else 0.0
    state["history"].append({
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "buy": buy_price, "sell": sell_price, "qty": qty,
        "pnl": round(pnl, 4), "pct": round(pct, 3), "reason": reason,
    })
    state["history"] = state["history"][-30:]
    state["total_profit"] = round(state["total_profit"] + pnl, 4)
    if pnl >= 0:
        state["wins"] += 1
    else:
        state["losses"] += 1
    return pnl, pct


def open_position(buy_price, qty):
    check_day()
    state.update({"in_position": True, "buy_price": buy_price, "qty": qty})
    state["trades_today"] += 1
    save_state()


def close_position(buy_price, sell_price, qty, reason):
    pnl, pct = record_trade(buy_price, sell_price, qty, reason)
    set_event(f"{reason} | النتيجة: {pnl:+.4f} USDT ({pct:+.2f}%)")
    if reason == "وقف خسارة":
        runtime["cooldown_until"] = time.time() + STOP_COOLDOWN_MIN * 60
    elif pnl < 0:
        runtime["cooldown_until"] = time.time() + LOSS_COOLDOWN_MIN * 60
    state.update({"in_position": False, "buy_price": 0.0, "qty": 0.0})
    save_state()


def try_adopt(price, step, min_qty, min_notional):
    qty = format_qty(bot_status["coin_free"], step)
    if qty < min_qty or qty * price < max(min_notional, 5.0):
        return False
    open_position(price, qty)
    state["trades_today"] -= 1  # التبني مو صفقة جديدة
    save_state()
    set_event(f"البوت تبنّى {qty} {COIN_ASSET} الموجودة بالمحفظة. سعر الدخول المعتمد = {price}")
    return True


def verify_saved_position():
    if not state["in_position"] or DRY_RUN:
        return
    try:
        free_coin = float(client.get_asset_balance(asset=COIN_ASSET)['free'])
        if free_coin >= state["qty"] * 0.9:
            state["qty"] = min(state["qty"], free_coin)
            logger.info(f"استرجاع صفقة محفوظة بسعر {state['buy_price']}")
            return
        logger.warning("الصفقة المحفوظة ما إلها رصيد بالمحفظة. تصفير.")
        state.update({"in_position": False, "buy_price": 0.0, "qty": 0.0})
        save_state()
    except Exception as e:
        logger.warning(f"تعذر التحقق من الرصيد: {e}")


# ==========================================
# 8. المحرك الرئيسي
# ==========================================
def trading_bot_loop():
    logger.info(f"تشغيل البوت - الوضع: {'تجربة (DRY_RUN)' if DRY_RUN else 'حقيقي (LIVE)'}")

    while True:
        try:
            step, min_qty, min_notional = get_symbol_rules()
            get_market_data()
            break
        except Exception as e:
            logger.error(f"فشل الاتصال الأولي ({e}). إعادة المحاولة بعد 20 ثانية.")
            time.sleep(20)

    verify_saved_position()
    logger.info(f"انطلق البوت | داخل صفقة؟ {state['in_position']} | سعر الدخول: {state['buy_price']}")

    while True:
        try:
            current_price, avg_24h, recent = get_market_data()
            reference = max(recent + [current_price])

            refresh_balances(current_price)
            bot_status.update({
                "current_price": current_price,
                "reference_price": reference,
                "buy_trigger": reference * BUY_DROP_RATIO,
                "next_trade_usdt": compute_trade_amount(),
                "last_check": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

            if not state["in_position"]:
                control["sell_now"] = False
                adopted = control["adopt"] and not DRY_RUN and \
                    try_adopt(current_price, step, min_qty, min_notional)

                if adopted:
                    control["buy_now"] = False
                elif control["buy_now"]:
                    control["buy_now"] = False
                    result = do_buy(compute_trade_amount(), current_price, step, min_qty, min_notional)
                    if result:
                        open_position(result[0], result[1])
                elif control["running"] and time.time() >= runtime["cooldown_until"] \
                        and current_price <= reference * BUY_DROP_RATIO:
                    allowed, reason = buy_allowed(current_price, avg_24h, recent)
                    if allowed:
                        result = do_buy(compute_trade_amount(), current_price, step, min_qty, min_notional)
                        if result:
                            open_position(result[0], result[1])
                        else:
                            runtime["cooldown_until"] = time.time() + WAIT_COOLDOWN
                    else:
                        set_event(f"تأجيل الشراء: {reason}")
                        runtime["cooldown_until"] = time.time() + WAIT_COOLDOWN
            else:
                control["buy_now"] = False
                buy_price, qty = state["buy_price"], state["qty"]
                hit_tp = current_price >= buy_price * SELL_GAIN_RATIO
                hit_sl = current_price <= buy_price * STOP_LOSS_RATIO
                manual = control["sell_now"]

                if hit_tp or hit_sl or manual:
                    control["sell_now"] = False
                    sell_price = do_sell(qty, current_price, step, min_qty)
                    if sell_price is not None:
                        reason = "ربح" if hit_tp else ("وقف خسارة" if hit_sl else "بيع يدوي")
                        close_position(buy_price, sell_price, qty, reason)

            time.sleep(LOOP_SLEEP)

        except BinanceAPIException as e:
            if e.status_code in (418, 429) or e.code == -1003:
                logger.error("تجاوز حد الطلبات (Rate Limit). انتظار 90 ثانية.")
                time.sleep(90)
            else:
                logger.error(f"خطأ Binance: {e}")
                time.sleep(20)
        except Exception as e:
            logger.error(f"خطأ بالحلقة الرئيسية: {e}")
            time.sleep(20)


# ==========================================
# 9. التشغيل
# ==========================================
load_state()

if __name__ == "__main__":
    Thread(target=run_web_server, daemon=True).start()
    Thread(target=analysis_loop, daemon=True).start()
    trading_bot_loop()
