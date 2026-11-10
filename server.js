require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const axios = require('axios');
const Pusher = require('pusher-js');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const DATA_FILE = path.join(__dirname, 'database.json');

let db = {
  config: {
    botName: 'ab points',
    channelName: process.env.CHANNEL_NAME || 'abgaming_eg',
    chatroomId: process.env.KICK_CHATROOM_ID || '',
    channelId: process.env.KICK_CHANNEL_ID || '',
    sessionToken: process.env.BOT_SESSION_TOKEN || '',
    xsrfToken: process.env.BOT_XSRF_TOKEN || '',
    botEnabled: true,
    liveAnnounceEnabled: true,
    liveMessage: '🔥 البث المباشر بدأ الآن يا أبطال! متجر نقاط القناة متاح، استبدلوا نقاطكم الآن مع بوت ab points 💚',
    welcomeEnabled: true,
    welcomeMessage: 'منور البث يا @{user} 💚 نتمنى لك مشاهدة ممتعة!',
    autoReplyEnabled: true,
    replyTemplate: '🎉 شكراً يا @{user} على شراء [ {reward} ] مقابل {cost} نقطة! تم تسجيل طلبك بنجاح عند ab points.'
  },
  isLive: false,
  redemptions: [],
  welcomedUsers: [],
  customCommands: [
    { trigger: '!points', response: 'أهلاً بك يا @{user} ! يمكنك استبدال نقاط القناة من متجر النقاط أسفل البث.' },
    { trigger: '!store', response: 'متجر نقاط القناة متاح الآن يا @{user} ! استبدل نقاطك بجوائز فورية.' }
  ]
};

if (fs.existsSync(DATA_FILE)) {
  try {
    const saved = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    db = { ...db, ...saved, config: { ...db.config, ...saved.config } };
  } catch (e) {}
}

function saveDB() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(db, null, 2), 'utf8');
  } catch (e) {}
}

// ==========================================
// حماية ضد التكرار والرد على النفس (Anti-Loop)
// ==========================================
const BOT_SIGNATURE = '\u200B';
let lastSentTime = 0;
let processedEvents = new Set();
const messageQueue = [];
let isProcessingQueue = false;

function formatTemplate(template, data) {
  const cleanUser = (data.user || 'مشاهد').replace(/^@+/, '').trim();
  return (template || '')
    .replace(/@\{user\}/g, `@${cleanUser} `)
    .replace(/\{user\}/g, `@${cleanUser} `)
    .replace(/\{reward\}/g, data.reward || '')
    .replace(/\{cost\}/g, data.cost || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function queueChatMessage(text, isManual = false) {
  if (!db.config.botEnabled && !isManual) return;
  messageQueue.push({ text, isManual });
  processQueue();
}

async function processQueue() {
  if (isProcessingQueue || messageQueue.length === 0) return;
  isProcessingQueue = true;

  const item = messageQueue.shift();
  const now = Date.now();
  const wait = item.isManual ? 300 : Math.max(500, 4000 - (now - lastSentTime));

  setTimeout(async () => {
    await sendChatMessageNow(item.text);
    lastSentTime = Date.now();
    isProcessingQueue = false;
    processQueue();
  }, wait);
}

async function sendChatMessageNow(messageText) {
  const safeText = messageText.trim() + BOT_SIGNATURE;
  console.log(`[${db.config.botName}] -> الشات: ${messageText}`);

  io.emit('bot_log', {
    type: 'sent',
    text: `🤖 أرسل البوت للشات: ${messageText}`,
    time: new Date().toLocaleTimeString('ar-EG')
  });

  if (!db.config.chatroomId || !db.config.sessionToken) {
    io.emit('bot_log', {
      type: 'warn',
      text: `⚠️ تنبيه: يرجى وضع كود التوكن (session_token) ورقم الشات في الإعدادات لكي تظهر الرسالة داخل موقع Kick.`,
      time: new Date().toLocaleTimeString('ar-EG')
    });
    return { simulated: true };
  }

  try {
    const cleanToken = decodeURIComponent(db.config.sessionToken).replace(/^Bearer\s+/i, '').trim();
    const headers = {
      'Authorization': `Bearer ${cleanToken}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    };
    if (db.config.xsrfToken) {
      headers['X-XSRF-TOKEN'] = decodeURIComponent(db.config.xsrfToken);
    }

    const res = await axios.post(
      `https://kick.com/api/v2/messages/send/${db.config.chatroomId}`,
      { content: safeText, type: 'message' },
      { headers }
    );
    return { simulated: false, data: res.data };
  } catch (err) {
    console.error('Kick Send Error:', err.response?.data || err.message);
    io.emit('bot_log', {
      type: 'error',
      text: `❌ تعذر الإرسال لـ Kick (تأكد من تحديث كود session_token): ${err.response?.status || err.message}`,
      time: new Date().toLocaleTimeString('ar-EG')
    });
    return { error: err.message };
  }
}

// ==========================================
// فحص تلقائي للقناة وجلب الأرقام ومراقبة اللايف 24/7
// ==========================================
async function checkChannelStatusAndAutoIds() {
  if (!db.config.channelName) return;
  try {
    const res = await axios.get(`https://kick.com/api/v2/channels/${db.config.channelName}`, {
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    const data = res.data;
    if (data) {
      let updated = false;
      if (data.chatroom?.id && String(db.config.chatroomId) !== String(data.chatroom.id)) {
        db.config.chatroomId = String(data.chatroom.id);
        updated = true;
      }
      if (data.id && String(db.config.channelId) !== String(data.id)) {
        db.config.channelId = String(data.id);
        updated = true;
      }
      if (updated) {
        saveDB();
        connectPusherRadar();
      }

      const currentlyLive = Boolean(data.livestream && data.livestream.is_live);
      if (currentlyLive && !db.isLive) {
        db.isLive = true;
        db.welcomedUsers = [];
        saveDB();
        io.emit('live_status', { isLive: true });
        io.emit('bot_log', {
          type: 'live',
          text: `🔴 القناة فتحت لايف الآن! تم تشغيل الترحيب التلقائي وإرسال إعلان اللايف.`,
          time: new Date().toLocaleTimeString('ar-EG')
        });
        if (db.config.botEnabled && db.config.liveAnnounceEnabled) {
          queueChatMessage(db.config.liveMessage);
        }
      } else if (!currentlyLive && db.isLive) {
        db.isLive = false;
        saveDB();
        io.emit('live_status', { isLive: false });
      }
    }
  } catch (e) {
    // في حال حجب Cloudflare لطلب الـ API العام، يستمر Pusher بالعمل عبر Chatroom ID المحفوظ
  }
}

setInterval(checkChannelStatusAndAutoIds, 30000);

// ==========================================
// رادار Pusher السحابي 24 ساعة (يراقب الشات + اللايف + المتجر)
// ==========================================
let pusherClient = null;

function connectPusherRadar() {
  if (pusherClient) {
    try { pusherClient.disconnect(); } catch (e) {}
  }

  if (!db.config.chatroomId) {
    console.log('[Radar] بانتظار Chatroom ID للاتصال...');
    return;
  }

  pusherClient = new Pusher('32cbd69e4b950bf97679', {
    cluster: 'us2',
    wsHost: 'ws-us2.pusher.com',
    wsPort: 443,
    wssPort: 443,
    forceTLS: true,
    enabledTransports: ['ws', 'wss']
  });

  const channels = [`chatrooms.${db.config.chatroomId}.v2`, `chatrooms.${db.config.chatroomId}`];
  if (db.config.channelId) {
    channels.push(`channel.${db.config.channelId}`);
  }

  channels.forEach(chName => {
    const ch = pusherClient.subscribe(chName);
    ch.bind_global((eventName, payload) => {
      if (!eventName || eventName.startsWith('pusher:')) return;
      const data = typeof payload === 'string' ? JSON.parse(payload) : payload;
      handleIncomingKickEvent(eventName, data);
    });
  });

  console.log(`✅ [Radar 24/7] متصل الآن بغرفة الشات (${db.config.chatroomId}) ويراقب الكون كله!`);
}

function handleIncomingKickEvent(eventName, data) {
  const ev = (eventName || '').toLowerCase();
  const rawStr = JSON.stringify(data || {});

  // 1. حماية تامة ضد قراءة رسائل البوت نفسه
  if (rawStr.includes(BOT_SIGNATURE) || rawStr.includes('تم تسجيل طلبك') || rawStr.includes('تم تنفيذ وتسليم')) {
    return;
  }

  // 2. رصد بداية البث المباشر فورياً
  if (ev.includes('stream.start') || ev.includes('livestreamcreated') || ev.includes('islive')) {
    if (!db.isLive) {
      db.isLive = true;
      db.welcomedUsers = [];
      saveDB();
      io.emit('live_status', { isLive: true });
      if (db.config.botEnabled && db.config.liveAnnounceEnabled) {
        queueChatMessage(db.config.liveMessage);
      }
    }
    return;
  }

  // 3. رصد شراء نقاط القناة الرسمي (Channel Points Redemption)
  if (
    ev.includes('reward') ||
    ev.includes('redeem') ||
    ev.includes('point') ||
    (data && (data.reward || data.reward_title))
  ) {
    const user = data?.user?.username || data?.sender?.username || data?.username || data?.redeemer?.username || 'مشاهد';
    const reward = data?.reward?.title || data?.reward_title || data?.title || 'جائزة من متجر النقاط';
    const cost = data?.reward?.cost || data?.cost || '';
    const input = data?.user_input || data?.input || '';

    registerPurchase(user, reward, cost, input);
    return;
  }

  // 4. رصد رسائل الشات (للترحيب التلقائي باللي يدخل + الأوامر)
  if (ev.includes('chatmessage') || data?.content) {
    const sender = data?.sender?.username;
    const content = (data?.content || '').trim();
    if (!sender || !content) return;

    io.emit('bot_log', {
      type: 'chat',
      text: `💬 [${sender}]: ${content}`,
      time: new Date().toLocaleTimeString('ar-EG')
    });

    // ترحيب أوتوماتيك بكل شخص يدخل ويتفاعل في اللايف (مرة واحدة لكل مشاهد)
    const senderLower = sender.toLowerCase();
    if (
      db.config.botEnabled &&
      db.config.welcomeEnabled &&
      senderLower !== (db.config.channelName || '').toLowerCase() &&
      !db.welcomedUsers.includes(senderLower)
    ) {
      db.welcomedUsers.push(senderLower);
      saveDB();
      const welcomeMsg = formatTemplate(db.config.welcomeMessage, { user: sender });
      queueChatMessage(welcomeMsg);
    }

    // فحص الأوامر المخصصة (!points, !store)
    const matchedCmd = db.customCommands.find(c => content.toLowerCase() === c.trigger.toLowerCase());
    if (matchedCmd) {
      const cmdReply = formatTemplate(matchedCmd.response, { user: sender });
      queueChatMessage(cmdReply);
    }
  }
}

function registerPurchase(username, rewardTitle, cost = '', userInput = '') {
  const cleanUser = (username || 'مشاهد').replace(/^@+/, '').trim();
  const cleanReward = (rewardTitle || 'جائزة').replace(/\n+/g, ' ').trim();

  const dedupeKey = `${cleanUser}_${cleanReward}_${Math.floor(Date.now() / 8000)}`;
  if (processedEvents.has(dedupeKey)) return;
  processedEvents.add(dedupeKey);

  const item = {
    id: 'ORD-' + Date.now(),
    username: cleanUser,
    rewardTitle: cleanReward,
    cost: cost || 'نقاط',
    userInput: userInput || '',
    status: 'pending',
    timestamp: new Date().toISOString()
  };

  db.redemptions.unshift(item);
  saveDB();

  io.emit('new_redemption', item);
  io.emit('bot_log', {
    type: 'purchase',
    text: `🛒 عملية شراء جديدة! @${cleanUser} اشترى [${cleanReward}]`,
    time: new Date().toLocaleTimeString('ar-EG')
  });

  if (db.config.botEnabled && db.config.autoReplyEnabled) {
    const replyMsg = formatTemplate(db.config.replyTemplate, {
      user: cleanUser,
      reward: cleanReward,
      cost: cost || ''
    });
    queueChatMessage(replyMsg);
  }

  return item;
}

// ==========================================
// مسارات لوحة التحكم والموقع الخارجي (API)
// ==========================================
app.get('/ping', (req, res) => res.send('AB Points Bot is Awake 24/7!'));

app.get('/api/state', (req, res) => res.json(db));

app.post('/api/config', (req, res) => {
  const oldRoom = db.config.chatroomId;
  db.config = { ...db.config, ...req.body };
  saveDB();
  if (db.config.chatroomId !== oldRoom) {
    connectPusherRadar();
  }
  res.json({ success: true, config: db.config });
});

app.post('/api/send-message', (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: 'الرسالة مطلوبة' });
  queueChatMessage(message, true);
  res.json({ success: true });
});

app.post('/api/redemptions/trigger', (req, res) => {
  const item = registerPurchase(req.body.username, req.body.rewardTitle, req.body.cost, req.body.userInput);
  res.json({ success: true, item });
});

app.post('/api/redemptions/:id/status', (req, res) => {
  const item = db.redemptions.find(r => r.id === req.params.id);
  if (!item) return res.status(404).json({ error: 'Not found' });
  item.status = req.body.status;
  saveDB();
  io.emit('redemption_updated', item);

  if (req.body.notifyChatMessage) {
    queueChatMessage(req.body.notifyChatMessage, true);
  }
  res.json({ success: true, item });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 AB Points Cloud Bot v3 Running 24/7 on port ${PORT}`);
  checkChannelStatusAndAutoIds();
  connectPusherRadar();
});
