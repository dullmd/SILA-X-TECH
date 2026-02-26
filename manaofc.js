const axios = require('axios');
const yts = require('yt-search');
const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const fetch = require('node-fetch');
const cheerio = require("cheerio");
const bodyparser = require('body-parser');
const { Buffer } = require('buffer');
const FileType = require('file-type');
const { File } = require('megajs');
const mongoose = require('mongoose');
const songStore = new Map();

const {
    default: makeWASocket,
    useMultiFileAuthState,
    delay,
    makeCacheableSignalKeyStore,
    Browsers,
    jidNormalizedUser,
    downloadMediaMessage,
    getContentType
} = require('baileys');

// MongoDB Configuration
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://kxshrii:i7sgjXF6SO2cTJwU@kelumxz.zggub8h.mongodb.net/';

// Connect to MongoDB
mongoose.connect(MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  serverSelectionTimeoutMS: 30000,
  socketTimeoutMS: 45000,
}).then(() => {
  console.log('✅ Connected to MongoDB');
}).catch(err => {
  console.error('❌ MongoDB connection error:', err);
  process.exit(1);
});

// MongoDB Schemas
const sessionSchema = new mongoose.Schema({
  number: { type: String, required: true, unique: true },
  sessionId: { type: String },
  settings: { type: Object, default: {} },
  creds: { type: Object },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

const settingsSchema = new mongoose.Schema({
  number: { type: String, required: true, unique: true },
  settings: { type: Object, default: {} },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

// MongoDB Models
const Session = mongoose.model('Session', sessionSchema);
const Settings = mongoose.model('Settings', settingsSchema);

console.log('✅ Using MongoDB database system');

// Custom findOneAndUpdate for Session
Session.findOneAndUpdate = async function(query, update, options = {}) {
  try {
    const session = await this.findOne(query);
    
    if (session) {
      if (update.$set) {
        Object.assign(session, update.$set);
      } else {
        Object.assign(session, update);
      }
      session.updatedAt = new Date();
      await session.save();
      return session;
    } else if (options.upsert) {
      const newSession = new this({
        ...query,
        ...update.$set,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      await newSession.save();
      return newSession;
    }
    return null;
  } catch (error) {
    console.error('Error in findOneAndUpdate:', error);
    return null;
  }
};

// Custom findOneAndUpdate for Settings
Settings.findOneAndUpdate = async function(query, update, options = {}) {
  try {
    const settings = await this.findOne(query);
    
    if (settings) {
      if (update.$set) {
        Object.assign(settings.settings, update.$set);
      } else {
        Object.assign(settings.settings, update);
      }
      settings.updatedAt = new Date();
      await settings.save();
      return settings;
    } else if (options.upsert) {
      const newSettings = new this({
        ...query,
        settings: update.$set || update,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      await newSettings.save();
      return newSettings;
    }
    return null;
  } catch (error) {
    console.error('Error in Settings findOneAndUpdate:', error);
    return null;
  }
};

// Default config structure
const defaultConfig = {
    AUTO_VIEW_STATUS: 'false',
    AUTO_LIKE_STATUS: 'false',
    AUTO_RECORDING: 'true',
    AUTO_LIKE_EMOJI: ['💥', '👍', '😍', '💗', '🎈', '🎉', '🥳', '😎', '🚀', '🔥'],
    PREFIX: '.',
    MODE: 'private',
    MAX_RETRIES: 3,
    ADMIN_LIST_PATH: './admin.json',
    IMAGE_PATH: 'https://files.catbox.moe/jwmx1j.jpg',
    OWNER_NUMBER: '94759934522',
    NEWSLETTER_JIDS: ['120363422610520277@newsletter', '120363402325089913@newsletter'],
    CHANNEL_LINK: 'https://whatsapp.com/channel/0029VbBPxQTJUM2WCZLB6j28',
    BOT_NAME: '𝐒𝐈𝐋𝐀 𝐗 𝐓𝐄𝐂𝐇 𝐌𝐎𝐍𝐃𝐈𝐀𝐋',
    BOT_FOOTER: '> © ᴛʜɪꜱ ʙᴏᴛ ᴩᴏᴡᴇʀᴇᴅ ʙʏ 𝐒𝐈𝐋𝐀 𝐗 𝐓𝐄𝐂𝐇'
};

// Memory optimization: Use weak references for sockets
const activeSockets = new Map();
const socketCreationTime = new Map();
const SESSION_BASE_PATH = './session';
const NUMBER_LIST_PATH = './numbers.json';

// Memory optimization: Cache frequently used data
let adminCache = null;
let adminCacheTime = 0;
const ADMIN_CACHE_TTL = 86400000; // 24 hour

// Initialize directories
if (!fs.existsSync(SESSION_BASE_PATH)) {
    fs.mkdirSync(SESSION_BASE_PATH, { recursive: true });
}

// Memory optimization: Improved admin loading with caching
function loadAdmins() {
    try {
        const now = Date.now();
        if (adminCache && now - adminCacheTime < ADMIN_CACHE_TTL) {
            return adminCache;
        }
        
        if (fs.existsSync(defaultConfig.ADMIN_LIST_PATH)) {
            adminCache = JSON.parse(fs.readFileSync(defaultConfig.ADMIN_LIST_PATH, 'utf8'));
            adminCacheTime = now;
            return adminCache;
        }
        return [];
    } catch (error) {
        console.error('Failed to load admin list:', error);
        return [];
    }
}

function isGroup(jid) {
    return jid.endsWith('@g.us');
}

function isOwner(sender, sock) {
    const ownerNumber = sock.user.id.split(':')[0];
    return sender.split('@')[0] === ownerNumber;
}

function formatMessage(title, content, footer) {
    return `*${title}*\n\n${content}\n\n${footer}`;
}

function getSriLankaTimestamp() {
    return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss');
}

// Newsletter handlers
async function setupNewsletterHandlers(socket) {
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const message = messages[0];
        if (!message?.key) return;

        const newsletterJids = defaultConfig.NEWSLETTER_JIDS || [];
        const jid = message.key.remoteJid;

        if (!newsletterJids.includes(jid)) return;

        try {
            const emojis = ['💥', '👍', '😍', '💗', '🎈', '🎉', '🥳', '😎', '🚀', '🔥'];
            const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
            const messageId = message.newsletterServerId;

            if (!messageId) {
                console.warn('No newsletterServerId found in message');
                return;
            }

            let retries = 3;
            while (retries-- > 0) {
                try {
                    await socket.newsletterReactMessage(jid, messageId.toString(), randomEmoji);
                    console.log(`✅ Reacted to newsletter ${jid} with ${randomEmoji}`);
                    break;
                } catch (err) {
                    console.warn(`❌ Reaction attempt failed (${3 - retries}/3):`, err.message);
                    await delay(1500);
                }
            }
        } catch (error) {
            console.error('⚠️ Newsletter reaction handler failed:', error.message);
        }
    });
}

// Clean duplicate files in MongoDB
async function cleanDuplicateFiles(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        
        // Get sessions from MongoDB
        const sessions = await Session.find({ number: sanitizedNumber })
            .sort({ updatedAt: -1 }); // Latest first
        
        if (sessions.length > 1) {
            // Keep only the latest session
            const idsToDelete = sessions.slice(1).map(s => s._id);
            
            await Session.deleteMany({ 
                _id: { $in: idsToDelete } 
            });
            console.log(`Deleted ${idsToDelete.length} duplicate sessions for ${sanitizedNumber}`);
        }
    } catch (error) {
        console.error(`Failed to clean duplicate files for ${number}:`, error);
    }
}

// GDrive download function
async function GDriveDl(url) {
    let id;
    if (!(url && url.match(/drive\.google/i))) return { error: true };

    try {
        id = (url.match(/[-\w]{25,}/) || [null])[0];
        if (!id) return { error: true };

        const res = await fetch(`https://drive.google.com/uc?id=${id}&export=download`);
        const html = await res.text();

        if (html.includes("Quota exceeded")) {
            return { error: true, message: "⚠️ Download quota exceeded." };
        }

        const $ = cheerio.load(html);
        const fileName =
            $("title").text().replace(" - Google Drive", "").trim() || "Unknown";
        const fileSize =
            $("span.uc-name-size").text().replace(fileName, "").trim() || "Unknown";

        const downloadUrl = `https://drive.google.com/uc?export=download&id=${id}`;

        return { fileName, fileSize, downloadUrl };
    } catch (e) {
        return { error: true, message: e.message };
    }
}

// Send admin connect message
async function sendAdminConnectMessage(socket, number) {
    const admins = loadAdmins();
    const caption = formatMessage(
        '𝐁𝐎𝐓 𝐂𝐎𝐍𝐍𝐄𝐂𝐓𝐄𝐃',
        `📞 Number: ${number}\nBot: Connected\nTime: ${getSriLankaTimestamp()}`,
        defaultConfig.BOT_FOOTER
    );

    for (const admin of admins) {
        try {
            await socket.sendMessage(
                `${admin}@s.whatsapp.net`,
                {
                    image: { url: defaultConfig.IMAGE_PATH },
                    caption
                }
            );
            await delay(100);
        } catch (error) {
            console.error(`Failed to send connect message to admin ${admin}:`, error);
        }
    }
}

// Status handlers
function setupStatusHandlers(socket, userConfig) {
    let lastStatusInteraction = 0;
    const STATUS_INTERACTION_COOLDOWN = 10000; // 10 seconds
    
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const message = messages[0];
        if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;
        
        const now = Date.now();
        if (now - lastStatusInteraction < STATUS_INTERACTION_COOLDOWN) {
            return;
        }

        try {
            if (userConfig.AUTO_RECORDING === 'true' && message.key.remoteJid) {
                await socket.sendPresenceUpdate("recording", message.key.remoteJid);
            }

            if (userConfig.AUTO_VIEW_STATUS === 'true') {
                let retries = parseInt(userConfig.MAX_RETRIES) || 3;
                while (retries > 0) {
                    try {
                        await socket.readMessages([message.key]);
                        break;
                    } catch (error) {
                        retries--;
                        console.warn(`Failed to read status, retries left: ${retries}`, error);
                        if (retries === 0) throw error;
                        await delay(1000 * (parseInt(userConfig.MAX_RETRIES) || 3 - retries));
                    }
                }
            }

            if (userConfig.AUTO_LIKE_STATUS === 'true') {
                const emojis = Array.isArray(userConfig.AUTO_LIKE_EMOJI) ? 
                    userConfig.AUTO_LIKE_EMOJI : defaultConfig.AUTO_LIKE_EMOJI;
                const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
                let retries = parseInt(userConfig.MAX_RETRIES) || 3;
                while (retries > 0) {
                    try {
                        await socket.sendMessage(
                            message.key.remoteJid,
                            { react: { text: randomEmoji, key: message.key } },
                            { statusJidList: [message.key.participant] }
                        );
                        lastStatusInteraction = now;
                        console.log(`Reacted to status with ${randomEmoji}`);
                        break;
                    } catch (error) {
                        retries--;
                        console.warn(`Failed to react to status, retries left: ${retries}`, error);
                        if (retries === 0) throw error;
                        await delay(1000 * (parseInt(userConfig.MAX_RETRIES) || 3 - retries));
                    }
                }
            }
        } catch (error) {
            console.error('Status handler error:', error);
        }
    });
}

// Message revocation handler
async function handleMessageRevocation(socket, number) {
    socket.ev.on('messages.delete', async ({ keys }) => {
        if (!keys || keys.length === 0) return;

        const messageKey = keys[0];
        const userJid = jidNormalizedUser(socket.user.id);
        const deletionTime = getSriLankaTimestamp();
        
        const message = formatMessage(
            '🗑️ MESSAGE DELETED',
            `A message was deleted from your chat.\n🧚‍♂️ From: ${messageKey.remoteJid}\n🍁 Deletion Time: ${deletionTime}`,
            defaultConfig.BOT_FOOTER
        );

        try {
            await socket.sendMessage(userJid, {
                image: { url: defaultConfig.IMAGE_PATH },
                caption: message
            });
            console.log(`Notified ${number} about message deletion: ${messageKey.id}`);
        } catch (error) {
            console.error('Failed to send deletion notification:', error);
        }
    });
}

// Session management with MongoDB
async function deleteSessionFromMongoDB(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        
        // Delete session from MongoDB
        await Session.deleteMany({ number: sanitizedNumber });
        
        // Delete settings from MongoDB
        await Settings.deleteOne({ number: sanitizedNumber });
        
        console.log(`Deleted session for ${sanitizedNumber} from MongoDB`);
    } catch (error) {
        console.error('Failed to delete session:', error);
    }
}

async function restoreSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        
        // Get session from MongoDB
        const session = await Session.findOne({ number: sanitizedNumber })
            .sort({ updatedAt: -1 });
        
        if (!session) {
            console.log(`No session found in MongoDB for ${sanitizedNumber}`);
            return null;
        }
        
        return session.creds;
    } catch (error) {
        console.error('Session restore failed:', error);
        return null;
    }
}

async function loadUserConfig(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        
        // Get config from MongoDB
        const configDoc = await Settings.findOne({ number: sanitizedNumber });
        
        if (!configDoc) {
            console.warn(`No configuration found for ${number}, using default config`);
            return { ...defaultConfig };
        }
        
        return { ...defaultConfig, ...configDoc.settings };
    } catch (error) {
        console.error('Failed to load config:', error);
        return { ...defaultConfig };
    }
}

async function updateUserConfig(number, newConfig) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        
        // Update or create config in MongoDB
        await Settings.findOneAndUpdate(
            { number: sanitizedNumber },
            { $set: newConfig },
            { upsert: true, new: true }
        );
        
        console.log(`Updated config for ${sanitizedNumber} in MongoDB`);
    } catch (error) {
        console.error('Failed to update config:', error);
        throw error;
    }
}

// Command handlers
function setupCommandHandlers(socket, number, userConfig) {
    const commandCooldowns = new Map();
    const COMMAND_COOLDOWN = 1000; // 1 second per user
    
    socket.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        
        // Newsletter auto-react
        const newsletterJids = defaultConfig.NEWSLETTER_JIDS || [];
        const emojis = ["💥", "👍", "😍", "💗", "🎈", "🎉", "🥳", "😎", "🚀", "🔥"];

        if (msg.key && newsletterJids.includes(msg.key.remoteJid)) {
            try {
                const serverId = msg.newsletterServerId;
                if (serverId) {
                    const emoji = emojis[Math.floor(Math.random() * emojis.length)];
                    await socket.newsletterReactMessage(msg.key.remoteJid, serverId.toString(), emoji);
                }
            } catch (e) {
                console.error('Newsletter reaction error:', e);
            }
        }	  
        
        if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

        // Extract text from different message types
        let text = '';
        if (msg.message.conversation) {
            text = msg.message.conversation.trim();
        } else if (msg.message.extendedTextMessage?.text) {
            text = msg.message.extendedTextMessage.text.trim();
        } else if (msg.message.buttonsResponseMessage?.selectedButtonId) {
            text = msg.message.buttonsResponseMessage.selectedButtonId.trim();
        } else if (msg.message.imageMessage?.caption) {
            text = msg.message.imageMessage.caption.trim();
        } else if (msg.message.videoMessage?.caption) {
            text = msg.message.videoMessage.caption.trim();
        }

        // Check if it's a command
        const prefix = userConfig.PREFIX || '.';
        if (!text.startsWith(prefix)) return;
        
        // Rate limiting
        const sender = msg.key.remoteJid;
        const fromGroup = isGroup(sender);
        
        const now = Date.now();
        if (commandCooldowns.has(sender) && now - commandCooldowns.get(sender) < COMMAND_COOLDOWN) {
            return;
        }
        commandCooldowns.set(sender, now);

        const parts = text.slice(prefix.length).trim().split(/\s+/);
        const command = parts[0].toLowerCase();
        const args = parts.slice(1);

        // Mode check
        const mode = (userConfig.MODE || 'private').toLowerCase();
        if (mode === 'private' && !isOwner(sender, socket)) return;
        if (mode === 'group' && !fromGroup) return;
        if (mode === 'inbox' && fromGroup) return;

        try {
            switch (command) {
                // Main alive command
                case 'alive': {
                    const startTime = socketCreationTime.get(number) || Date.now();
                    const uptime = Math.floor((Date.now() - startTime) / 1000);
                    const hours = Math.floor(uptime / 3600);
                    const minutes = Math.floor((uptime % 3600) / 60);
                    const seconds = Math.floor(uptime % 60);

                    const caption = `
╭───『 🤖 ${defaultConfig.BOT_NAME} 』───╮
│ ⏰ *ᴜᴘᴛɪᴍᴇ:* ${hours}h ${minutes}m ${seconds}s
│ 🟢 *ᴀᴄᴛɪᴠᴇ sᴇssɪᴏɴs:* ${activeSockets.size}
│ 📱 *ʏᴏᴜʀ ɴᴜᴍʙᴇʀ:* ${number}
╰──────────────────╯

${defaultConfig.BOT_FOOTER}
`;

                    const buttons = [
                        {
                            buttonId: `${prefix}menu`,
                            buttonText: { displayText: '📜 MENU' },
                            type: 1
                        },
                        {
                            buttonId: `${prefix}ping`,
                            buttonText: { displayText: '⚡ PING' },
                            type: 1
                        },
                        {
                            buttonId: `${prefix}uptime`,
                            buttonText: { displayText: '⏰ UPTIME' },
                            type: 1
                        }
                    ];

                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: caption.trim(),
                        buttons: buttons,
                        headerType: 4
                    });

                    break;
                }

                // Song download
                case 'song': {
                    try {
                        const q = args.join(" ");
                        if (!q) {
                            return socket.sendMessage(sender, {
                                text: "❌ *Please provide a song name or YouTube URL!*"
                            });
                        }

                        const search = await yts(q);
                        if (!search.videos || search.videos.length === 0) {
                            return socket.sendMessage(sender, {
                                text: "⚠️ *No song results found!*"
                            });
                        }

                        const song = search.videos[0];

                        const apiUrl = `https://api-dark-shan-yt.koyeb.app/download/ytmp3-v2?url=${encodeURIComponent(song.url)}`;
                        const res = await axios.get(apiUrl, { timeout: 30000 });
                        const data = res.data;

                        if (!data.status || !data.data?.download) {
                            return socket.sendMessage(sender, {
                                text: "❌ *Failed to fetch song download link!*"
                            });
                        }

                        const downloadUrl = data.data.download;
                        songStore.set(sender, { song, downloadUrl });

                        const caption = `
╭───『 🎵 SONG DOWNLOADER 』───╮
│ 🎶 *Title:* ${song.title}
│ ⏱️ *Duration:* ${song.timestamp}
│ 👁️ *Views:* ${song.views}
│ 📅 *Uploaded:* ${song.ago}
│ 📺 *Channel:* ${song.author.name}
╰──────────────────────────╯
${defaultConfig.BOT_FOOTER}
                        `.trim();

                        const buttons = [
                            {
                                buttonId: `${prefix}song_audio`,
                                buttonText: { displayText: '🎧 AUDIO DOWNLOAD' },
                                type: 1
                            },
                            {
                                buttonId: `${prefix}song_doc`,
                                buttonText: { displayText: '📄 DOCUMENT DOWNLOAD' },
                                type: 1
                            }
                        ];

                        await socket.sendMessage(sender, {
                            image: { url: song.thumbnail },
                            caption,
                            buttons,
                            headerType: 4
                        });

                    } catch (err) {
                        console.error("SONG ERROR:", err);
                        await socket.sendMessage(sender, {
                            text: `❌ Error: ${err.message || "Failed to download song"}`
                        });
                    }
                    break;
                }

                case 'song_audio': {
                    try {
                        const data = songStore.get(sender);
                        if (!data) {
                            return socket.sendMessage(sender, {
                                text: "⚠️ *Song data expired. Please search again!*"
                            });
                        }

                        const { song, downloadUrl } = data;
                        const fileName = `${song.title}.mp3`.replace(/[^\w\s.-]/gi, '');

                        await socket.sendMessage(sender, {
                            audio: { url: downloadUrl },
                            mimetype: "audio/mpeg",
                            fileName
                        });

                    } catch (err) {
                        console.error("SONG AUDIO ERROR:", err);
                        await socket.sendMessage(sender, {
                            text: `❌ Error: ${err.message || "Failed to send audio"}`
                        });
                    }
                    break;
                }

                case 'song_doc': {
                    try {
                        const data = songStore.get(sender);
                        if (!data) {
                            return socket.sendMessage(sender, {
                                text: "⚠️ *Song data expired. Please search again!*"
                            });
                        }

                        const { song, downloadUrl } = data;
                        const fileName = `${song.title}.mp3`.replace(/[^\w\s.-]/gi, '');

                        await socket.sendMessage(sender, {
                            document: { url: downloadUrl },
                            mimetype: "audio/mpeg",
                            fileName
                        });

                    } catch (err) {
                        console.error("SONG DOC ERROR:", err);
                        await socket.sendMessage(sender, {
                            text: `❌ Error: ${err.message || "Failed to send document"}`
                        });
                    }
                    break;
                }

                // APK download
                case 'apk': {
                    try {
                        const q = args.join(" ");
                        if (!q) {
                            return socket.sendMessage(sender, {
                                text: "❌ *Please provide an app name to search!*"
                            });
                        }

                        const apiUrl = `http://ws75.aptoide.com/api/7/apps/search/query=${encodeURIComponent(q)}/limit=1`;
                        const res = await axios.get(apiUrl);
                        const data = res.data;

                        if (!data?.datalist?.list || data.datalist.list.length === 0) {
                            return socket.sendMessage(sender, {
                                text: "⚠️ *No results found for the given app name.*"
                            });
                        }

                        const app = data.datalist.list[0];
                        const appSize = (app.size / 1048576).toFixed(2); // MB
                        const thumb = app.icon;
                        const apkUrl = app.file?.path_alt;

                        if (!apkUrl) {
                            return socket.sendMessage(sender, {
                                text: "❌ *Failed to fetch APK download link.*"
                            });
                        }

                        const caption = `
╭───『 📱 APK DOWNLOADER 』───╮
│ 📦 *Name:* ${app.name}
│ 🆔 *Package:* ${app.package}
│ 💾 *Size:* ${appSize} MB
│ 👨‍💻 *Developer:* ${app.developer.name}
│ 🗓️ *Updated:* ${app.updated}
╰──────────────────────────╯
${defaultConfig.BOT_FOOTER}
                        `.trim();

                        await socket.sendMessage(sender, {
                            image: { url: thumb },
                            caption
                        });

                        await socket.sendMessage(sender, {
                            document: { url: apkUrl },
                            fileName: `${app.name}.apk`.replace(/[^\w\s.-]/gi, ''),
                            mimetype: "application/vnd.android.package-archive"
                        });

                    } catch (err) {
                        console.error("APK ERROR:", err);
                        await socket.sendMessage(sender, {
                            text: `❌ Error: ${err.message || "Failed to download APK"}`
                        });
                    }
                    break;
                }

                // Google Drive download
                case 'gdrive': {
                    try {
                        const q = args.join(" ");
                        if (!q || !q.startsWith("http")) {
                            return socket.sendMessage(sender, {
                                text: "❗ Please provide a valid Google Drive link."
                            });
                        }

                        const file = await GDriveDl(q);
                        if (file.error) {
                            return socket.sendMessage(sender, {
                                text: "❌ Failed: " + (file.message || "Unable to fetch file.")
                            });
                        }

                        let sizeMB = 0;
                        if (file.fileSize.includes("MB")) {
                            sizeMB = parseFloat(file.fileSize);
                        } else if (file.fileSize.includes("GB")) {
                            sizeMB = parseFloat(file.fileSize) * 1024;
                        }

                        if (sizeMB > 1900) {
                            return socket.sendMessage(sender, {
                                text:
`📄 ${file.fileName}
📦 Size: ${file.fileSize}

⚠️ File too large for WhatsApp.
⬇️ Download manually:
${file.downloadUrl}`
                            });
                        }

                        await socket.sendMessage(sender, {
                            document: { url: file.downloadUrl },
                            fileName: file.fileName,
                            mimetype: "application/octet-stream",
                            caption: `📄 ${file.fileName}\n📦 Size: ${file.fileSize}`
                        });

                    } catch (err) {
                        console.error("GDRIVE ERROR:", err);
                        socket.sendMessage(sender, {
                            text: "❌ Error while processing Google Drive link."
                        });
                    }
                    break;
                }

                // MEGA download
                case 'mega': {
                    try {
                        const q = args.join(" ");
                        if (!q) {
                            return socket.sendMessage(sender, {
                                text: "❗ Please provide a MEGA.nz link."
                            });
                        }

                        const file = File.fromURL(q);
                        await file.loadAttributes();

                        const maxSize = 4 * 1024 * 1024 * 1024; // 4GB
                        if (file.size > maxSize) {
                            return socket.sendMessage(sender, {
                                text:
`❌ File too large
Max: 4GB
Size: ${(file.size / (1024 ** 3)).toFixed(2)} GB`
                            });
                        }

                        await socket.sendMessage(sender, {
                            text: `⬇️ Downloading ${file.name} (${(file.size / (1024 ** 2)).toFixed(2)} MB)...`
                        });

                        const buffer = await file.downloadBuffer();
                        const mime = require('mime');
                        const mimeType = mime.lookup(file.name) || "application/octet-stream";

                        await socket.sendMessage(sender, {
                            document: buffer,
                            fileName: file.name,
                            mimetype: mimeType
                        });

                    } catch (err) {
                        console.error("MEGA ERROR:", err);
                        socket.sendMessage(sender, {
                            text: "❌ Failed to download MEGA file."
                        });
                    }
                    break;
                }

                // MediaFire download
                case 'mfire': {
                    try {
                        const q = args.join(" ");
                        if (!q || !q.startsWith("https://")) {
                            return socket.sendMessage(sender, {
                                text: "❗ Please provide a valid MediaFire link."
                            });
                        }

                        const res = await fetch(q);
                        const html = await res.text();
                        const $ = cheerio.load(html);

                        const fileName = $(".dl-info .filename").text().trim();
                        const downloadUrl = $("#downloadButton").attr("href");
                        const fileType = $(".dl-info .filetype").text().trim();
                        const fileSize = $(".dl-info ul li span").first().text().trim();
                        const fileDate = $(".dl-info ul li span").last().text().trim();

                        if (!fileName || !downloadUrl) {
                            return socket.sendMessage(sender, {
                                text: "⚠️ Failed to extract MediaFire info."
                            });
                        }

                        const ext = fileName.split(".").pop().toLowerCase();
                        const mimeTypes = {
                            zip: "application/zip",
                            pdf: "application/pdf",
                            mp4: "video/mp4",
                            mkv: "video/x-matroska",
                            mp3: "audio/mpeg",
                            "7z": "application/x-7z-compressed",
                            jpg: "image/jpeg",
                            jpeg: "image/jpeg",
                            png: "image/png",
                            rar: "application/x-rar-compressed"
                        };

                        await socket.sendMessage(sender, {
                            document: { url: downloadUrl },
                            fileName,
                            mimetype: mimeTypes[ext] || "application/octet-stream",
                            caption:
`📄 ${fileName}
📁 Type: ${fileType}
📦 Size: ${fileSize}
📅 Uploaded: ${fileDate}`
                        });

                    } catch (err) {
                        console.error("MEDIAFIRE ERROR:", err);
                        socket.sendMessage(sender, {
                            text: "❌ Error while processing MediaFire link."
                        });
                    }
                    break;
                }

                // BOOM COMMAND
                case 'boom': {
                    if (args.length < 2) {
                        return await socket.sendMessage(sender, { 
                            text: `📛 *Usage:* ${prefix}boom <count> <message>\n📌 *Example:* ${prefix}boom 100 Hello` 
                        });
                    }

                    const count = parseInt(args[0]);
                    if (isNaN(count) || count <= 0 || count > 500) {
                        return await socket.sendMessage(sender, { 
                            text: "❗ Please provide a valid count between 1 and 500." 
                        });
                    }

                    const message = args.slice(1).join(" ");
                    for (let i = 0; i < count; i++) {
                        await socket.sendMessage(sender, { text: message });
                        await new Promise(resolve => setTimeout(resolve, 500));
                    }

                    break;
                }

                // Bot settings
                case 'settings': {
                    if (!isOwner(sender, socket)) {
                        return await socket.sendMessage(sender, {
                            text: "*📛 Owner Only Command*"
                        });
                    }

                    if (!args[0]) {
                        const buttons = [
                            { buttonId: `${prefix}settings auto`, buttonText: { displayText: '⚙️ Auto Settings' }, type: 1 },
                            { buttonId: `${prefix}settings prefix`, buttonText: { displayText: '🔤 Prefix Settings' }, type: 1 },
                            { buttonId: `${prefix}settings mode`, buttonText: { displayText: '🧲 Mode Settings' }, type: 1 },
                            { buttonId: `${prefix}settings view`, buttonText: { displayText: '📋 View Settings' }, type: 1 },
                        ];

                        return await socket.sendMessage(sender, {
                            text: '*⚙️ Bot Settings Menu*\n\nSelect an option:',
                            buttons,
                            footer: defaultConfig.BOT_FOOTER,
                            headerType: 1
                        });
                    }

                    if (args[0] === 'auto') {
                        const buttons = [
                            { buttonId: `${prefix}settings_set AUTO_VIEW_STATUS true`, buttonText: { displayText: '👁️ Auto View Status ON' }, type: 1 },
                            { buttonId: `${prefix}settings_set AUTO_VIEW_STATUS false`, buttonText: { displayText: '🚫 Auto View Status OFF' }, type: 1 },
                            { buttonId: `${prefix}settings_set AUTO_LIKE_STATUS true`, buttonText: { displayText: '❤️ Auto Like Status ON' }, type: 1 },
                            { buttonId: `${prefix}settings_set AUTO_LIKE_STATUS false`, buttonText: { displayText: '💔 Auto Like Status OFF' }, type: 1 },
                            { buttonId: `${prefix}settings_set AUTO_RECORDING true`, buttonText: { displayText: '🎙️ Recording ON' }, type: 1 },
                            { buttonId: `${prefix}settings_set AUTO_RECORDING false`, buttonText: { displayText: '⏹️ Recording OFF' }, type: 1 },
                        ];

                        return await socket.sendMessage(sender, {
                            text: '*⚙️ Auto Settings*\n\nEnable or Disable:',
                            buttons,
                            footer: defaultConfig.BOT_FOOTER,
                            headerType: 1
                        });
                    }

                    if (args[0] === 'prefix') {
                        const buttons = [
                            { buttonId: `${prefix}settings_set PREFIX .`, buttonText: { displayText: '🔹 .' }, type: 1 },
                            { buttonId: `${prefix}settings_set PREFIX /`, buttonText: { displayText: '🔹 /' }, type: 1 },
                            { buttonId: `${prefix}settings_set PREFIX !`, buttonText: { displayText: '🔹 !' }, type: 1 },
                            { buttonId: `${prefix}settings_set PREFIX ?`, buttonText: { displayText: '🔹 ?' }, type: 1 },
                        ];

                        return await socket.sendMessage(sender, {
                            text: '*🔤 Select Bot Prefix*',
                            buttons,
                            footer: defaultConfig.BOT_FOOTER,
                            headerType: 1
                        });
                    }

                    if (args[0] === 'mode') {
                        const buttons = [
                            { buttonId: `${prefix}settings_set MODE private`, buttonText: { displayText: 'PRIVATE' }, type: 1 },
                            { buttonId: `${prefix}settings_set MODE inbox`, buttonText: { displayText: 'INBOX' }, type: 1 },
                            { buttonId: `${prefix}settings_set MODE group`, buttonText: { displayText: 'GROUP' }, type: 1 },
                            { buttonId: `${prefix}settings_set MODE public`, buttonText: { displayText: 'PUBLIC' }, type: 1 },
                        ];

                        return await socket.sendMessage(sender, {
                            text: '*🔤 Select Bot Mode*',
                            buttons,
                            footer: defaultConfig.BOT_FOOTER,
                            headerType: 1
                        });
                    }

                    if (args[0] === 'view') {
                        let text = '*📋 Current Bot Settings*\n\n';
                        for (const [key, value] of Object.entries(userConfig)) {
                            text += `• ${key}: ${value}\n`;
                        }

                        text += `\n${defaultConfig.BOT_FOOTER}`;

                        return await socket.sendMessage(sender, { text });
                    }

                    break;
                }

                case 'settings_set': {
                    if (!isOwner(sender, socket)) return;

                    const key = args[0]?.toUpperCase();
                    const value = args[1];

                    if (!key || value === undefined) return;

                    userConfig[key] = value;
                    await updateUserConfig(number, userConfig);

                    await socket.sendMessage(sender, {
                        text: `✅ Setting Updated\n\n• ${key} = ${value}\n\n${defaultConfig.BOT_FOOTER}`
                    });

                    break;
                }

                // Menu
                case 'menu': {
                    const startTime = socketCreationTime.get(number) || Date.now();
                    const uptime = Math.floor((Date.now() - startTime) / 1000);
                    const hours = Math.floor(uptime / 3600);
                    const minutes = Math.floor((uptime % 3600) / 60);
                    const seconds = Math.floor(uptime % 60);

                    const menuCaption = `
👋 *Hi ${number}*

╭───『 *${defaultConfig.BOT_NAME}* 』
│ 👾 *ʙᴏᴛ*: ${defaultConfig.BOT_NAME}
│ 📞 *ᴏᴡɴᴇʀ*: ᴍᴀɴᴀᴏꜰᴄ
│ ⏳ *ᴜᴘᴛɪᴍᴇ*: ${hours}h ${minutes}m ${seconds}s
│ ✏️ *ᴘʀᴇғɪx*: ${prefix}
│ 📺 *ᴄʜᴀɴɴᴇʟ*: ${defaultConfig.CHANNEL_LINK}
╰─────────────────────╯
`;

                    const buttons = [
                        {
                            buttonId: `${prefix}main_menu`,
                            buttonText: { displayText: 'MAIN MENU' },
                            type: 1
                        },
                        {
                            buttonId: `${prefix}download_menu`,
                            buttonText: { displayText: 'DOWNLOAD MENU' },
                            type: 1
                        },
                        {
                            buttonId: `${prefix}fun_menu`,
                            buttonText: { displayText: 'FUN MENU' },
                            type: 1
                        },
                        {
                            buttonId: `${prefix}settings_menu`,
                            buttonText: { displayText: 'SETTINGS MENU' },
                            type: 1
                        },
                        {
                            buttonId: `${prefix}owner_menu`,
                            buttonText: { displayText: 'OWNER MENU' },
                            type: 1
                        }
                    ];

                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: menuCaption.trim(),
                        buttons: buttons,
                        headerType: 4
                    });

                    break;
                }

                case 'main_menu': {
                    const menuCaption = `
MAIN COMMANDS:

${prefix}alive
${prefix}menu
${prefix}ping
${prefix}uptime
${prefix}owner

${defaultConfig.BOT_FOOTER}
 `;
                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: menuCaption.trim()
                    });
                    break;
                }

                case 'download_menu': {
                    const menuCaption = `
DOWNLOAD COMMANDS:

${prefix}song
${prefix}video
${prefix}ph
${prefix}mfire
${prefix}mega
${prefix}gdrive

${defaultConfig.BOT_FOOTER}
 `;
                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: menuCaption.trim()
                    });
                    break;
                }

                case 'fun_menu': {
                    const menuCaption = `
FUN COMMANDS:

${prefix}boom

${defaultConfig.BOT_FOOTER}
 `;
                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: menuCaption.trim()
                    });
                    break;
                }

                case 'settings_menu': {
                    const menuCaption = `
SETTINGS COMMANDS:

${prefix}settings

${defaultConfig.BOT_FOOTER}`;
                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: menuCaption.trim()
                    });
                    break;
                }

                case 'owner_menu': {
                    const menuCaption = `
OWNER COMMANDS:

${prefix}tagall
${prefix}deleteme / confirm
${prefix}getpp <number> - Get profile picture of any number

${defaultConfig.BOT_FOOTER}
`;
                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: menuCaption.trim()
                    });
                    break;
                }

                // Owner command
                case 'owner': {
                    const vcard = 'BEGIN:VCARD\n' +
                        'VERSION:3.0\n' +
                        'FN:MANAOFC\n' +
                        'ORG:MANAOFC\n' +
                        'TEL;type=CELL;type=VOICE;waid=94759934522:+94759934522\n' +
                        'EMAIL:manishasasmith27@gmail.com\n' +
                        'END:VCARD';

                    await socket.sendMessage(sender, {
                        contacts: {
                            displayName: "manaofc",
                            contacts: [{ vcard }]
                        }
                    });

                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: `${defaultConfig.BOT_NAME} OWNER DETAILS\n\n${defaultConfig.BOT_FOOTER}`,
                    });
                    break;
                }

                // Ping command
                case 'ping': {
                    const start = Date.now();
                    await socket.sendMessage(sender, { text: '🏓 Pong!' });
                    const latency = Date.now() - start;

                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: `⚡ *Latency:* ${latency}ms\n📶 *Connection:* ${latency < 500 ? 'Excellent' : latency < 1000 ? 'Good' : 'Poor'}\n\n${defaultConfig.BOT_FOOTER}`
                    });
                    break;
                }

                // Uptime
                case 'uptime': {
                    const startTime = socketCreationTime.get(number) || Date.now();
                    const uptime = Math.floor((Date.now() - startTime) / 1000);
                    const hours = Math.floor(uptime / 3600);
                    const minutes = Math.floor((uptime % 3600) / 60);
                    const seconds = Math.floor(uptime % 60);
                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: `⏰ *Uptime:* ${hours}h ${minutes}m ${seconds}s\n📊 *Active Sessions:* ${activeSockets.size}\n\n${defaultConfig.BOT_FOOTER}`
                    });

                    break;
                }

                // Tagall
                case 'tagall': {
                    if (!msg.key.remoteJid.endsWith('@g.us')) {
                        await socket.sendMessage(sender, { text: '❌ This command can only be used in groups.' });
                        return;
                    }
                    const groupMetadata = await socket.groupMetadata(sender);
                    const participants = groupMetadata.participants.map(p => p.id);
                    const tagMessage = `📢 *Tagging all members:*\n\n${participants.map(p => `@${p.split('@')[0]}`).join(' ')}`;

                    await socket.sendMessage(sender, {
                        text: tagMessage,
                        mentions: participants
                    });
                    break;
                }

                // Get profile picture
                case 'getpp': {
                    if (args.length === 0) {
                        await socket.sendMessage(sender, { 
                            text: `❌ Please provide a phone number.\nUsage: ${prefix}getpp <number>\nExample: ${prefix}getpp 94759934522\n\n${defaultConfig.BOT_FOOTER}` 
                        });
                        return;
                    }

                    let targetNumber = args[0].replace(/[^0-9]/g, '');
                    
                    if (!targetNumber.startsWith('92') && targetNumber.length === 10) {
                        targetNumber = '92' + targetNumber;
                    }
                    
                    const targetJid = targetNumber.includes('@') ? targetNumber : `${targetNumber}@s.whatsapp.net`;
                    
                    await socket.sendMessage(sender, { 
                        text: `🕵️ Stealing profile picture for ${targetNumber}...\n\n${defaultConfig.BOT_FOOTER}` 
                    });

                    try {
                        const profilePictureUrl = await socket.profilePictureUrl(targetJid, 'image');

                        if (profilePictureUrl) {
                            await socket.sendMessage(sender, {
                                image: { url: profilePictureUrl },
                                caption: `✅ Successfully stole profile picture!\n📱 Number: ${targetNumber}\n\n${defaultConfig.BOT_FOOTER}`
                            });
                        } else {
                            await socket.sendMessage(sender, { 
                                text: `❌ No profile picture found for ${targetNumber}\n\n${defaultConfig.BOT_FOOTER}` 
                            });
                        }

                    } catch (error) {
                        console.error('Profile picture steal error:', error);

                        if (error.message.includes('404') || error.message.includes('not found')) {
                            await socket.sendMessage(sender, { 
                                text: `❌ No profile picture found for ${targetNumber}\n\n${defaultConfig.BOT_FOOTER}` 
                            });
                        } else {
                            await socket.sendMessage(sender, { 
                                text: `❌ Error stealing profile picture: ${error.message}\n\n${defaultConfig.BOT_FOOTER}` 
                            });
                        }
                    }
                    break;
                }

                // Delete session
                case 'deleteme': {
                    const confirmationMessage = `⚠️ *Are you sure you want to delete your session?*\n\nThis action will:\n• Log out your bot\n• Delete all session data\n• Require re-pairing to use again\n\nReply with *${prefix}confirm* to proceed or ignore to cancel.`;

                    await socket.sendMessage(sender, {
                        image: { url: defaultConfig.IMAGE_PATH },
                        caption: confirmationMessage + `\n\n${defaultConfig.BOT_FOOTER}`
                    });
                    break;
                }

                // Confirm deletion
                case 'confirm': {
                    const sanitizedNumber = number.replace(/[^0-9]/g, '');

                    await socket.sendMessage(sender, {
                        text: `🗑️ Deleting your session...\n\n${defaultConfig.BOT_FOOTER}`
                    });

                    try {
                        const socket = activeSockets.get(sanitizedNumber);
                        if (socket) {
                            socket.ws.close();
                            activeSockets.delete(sanitizedNumber);
                            socketCreationTime.delete(sanitizedNumber);
                        }

                        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
                        if (fs.existsSync(sessionPath)) {
                            fs.removeSync(sessionPath);
                        }

                        await deleteSessionFromMongoDB(sanitizedNumber);

                        let numbers = [];
                        if (fs.existsSync(NUMBER_LIST_PATH)) {
                            numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
                        }
                        const index = numbers.indexOf(sanitizedNumber);
                        if (index !== -1) {
                            numbers.splice(index, 1);
                            fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
                        }

                        await socket.sendMessage(sender, {
                            text: `✅ Your session has been successfully deleted!\n\n${defaultConfig.BOT_FOOTER}`
                        });
                    } catch (error) {
                        console.error('Failed to delete session:', error);
                        await socket.sendMessage(sender, {
                            text: `❌ Failed to delete your session. Please try again later.\n\n${defaultConfig.BOT_FOOTER}`
                        });
                    }
                    break;
                }

                default: {
                    await socket.sendMessage(sender, {
                        text: `❌ Unknown command: ${command}\nUse ${prefix}menu to see available commands.\n\n${defaultConfig.BOT_FOOTER}`
                    });
                    break;
                }
            }
        } catch (error) {
            console.error('Command handler error:', error);
            await socket.sendMessage(sender, {
                text: `❌ An error occurred while processing your command. Please try again.\n\n${defaultConfig.BOT_FOOTER}`
            });
        }
    });
}

// Message handlers
function setupMessageHandlers(socket, userConfig) {
    let lastPresenceUpdate = 0;
    const PRESENCE_UPDATE_COOLDOWN = 5000; // 5 seconds

    socket.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

        const now = Date.now();
        if (now - lastPresenceUpdate < PRESENCE_UPDATE_COOLDOWN) {
            return;
        }

        if (userConfig.AUTO_RECORDING === 'true') {
            try {
                await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
                lastPresenceUpdate = now;
                console.log(`Set recording presence for ${msg.key.remoteJid}`);
            } catch (error) {
                console.error('Failed to set recording presence:', error);
            }
        }
    });
}

// Auto restart
function setupAutoRestart(socket, number) {
    let restartAttempts = 0;
    const MAX_RESTART_ATTEMPTS = 5;
    const RESTART_DELAY_BASE = 10000; // 10 seconds

    socket.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'close' && lastDisconnect?.error?.output?.statusCode !== 401) {
            await deleteSessionFromMongoDB(number);

            if (restartAttempts >= MAX_RESTART_ATTEMPTS) {
                console.log(`Max restart attempts reached for ${number}, giving up`);
                activeSockets.delete(number.replace(/[^0-9]/g, ''));
                socketCreationTime.delete(number.replace(/[^0-9]/g, ''));
                return;
            }

            restartAttempts++;
            const delayTime = RESTART_DELAY_BASE * Math.pow(2, restartAttempts - 1);

            console.log(`Connection lost for ${number}, attempting to reconnect in ${delayTime/1000} seconds (attempt ${restartAttempts}/${MAX_RESTART_ATTEMPTS})...`);

            await delay(delayTime);

            try {
                const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
                await EmpirePair(number, mockRes);
            } catch (error) {
                console.error(`Reconnection attempt ${restartAttempts} failed for ${number}:`, error);
            }
        } else if (connection === 'open') {
            restartAttempts = 0;
        }
    });
}

// Main pairing function
async function EmpirePair(number, res) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);

    if (activeSockets.has(sanitizedNumber)) {
        if (!res.headersSent) {
            res.send({ 
                status: 'already_connected',
                message: 'This number is already connected'
            });
        }
        return;
    }

    await cleanDuplicateFiles(sanitizedNumber);

    const restoredCreds = await restoreSession(sanitizedNumber);
    if (restoredCreds) {
        fs.ensureDirSync(sessionPath);
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(restoredCreds, null, 2));
        console.log(`Successfully restored session for ${sanitizedNumber}`);
    }

    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
    const logger = pino({ level: process.env.NODE_ENV === 'production' ? 'fatal' : 'debug' });

    try {
        const socket = makeWASocket({
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(state.keys, logger),
            },
            printQRInTerminal: false,
            logger,
            browser: Browsers.windows('Safari')
        });

        socketCreationTime.set(sanitizedNumber, Date.now());

        const userConfig = await loadUserConfig(sanitizedNumber);

        setupStatusHandlers(socket, userConfig);
        setupCommandHandlers(socket, sanitizedNumber, userConfig);
        setupMessageHandlers(socket, userConfig);
        setupAutoRestart(socket, sanitizedNumber);
        handleMessageRevocation(socket, sanitizedNumber);
        setupNewsletterHandlers(socket);

        if (!socket.authState.creds.registered) {
            let retries = parseInt(userConfig.MAX_RETRIES) || 3;
            let code;
            while (retries > 0) {
                try {
                    await delay(1500);
                    code = await socket.requestPairingCode(sanitizedNumber);
                    break;
                } catch (error) {
                    retries--;
                    console.warn(`Failed to request pairing code: ${retries}, ${error.message}`, retries);
                    await delay(2000 * ((parseInt(userConfig.MAX_RETRIES) || 3) - retries));
                }
            }
            if (!res.headersSent) {
                res.send({ code });
            }
        }

        socket.ev.on('creds.update', async () => {
            await saveCreds();
            const fileContent = await fs.readFile(path.join(sessionPath, 'creds.json'), 'utf8');
            const creds = JSON.parse(fileContent);

            await Session.findOneAndUpdate(
                { number: sanitizedNumber },
                { 
                    $set: {
                        creds: creds,
                        updatedAt: new Date()
                    }
                },
                { upsert: true }
            );

            console.log(`Updated creds for ${sanitizedNumber} in MongoDB`);
        });

        socket.ev.on('connection.update', async (update) => {
            const { connection } = update;
            if (connection === 'open') {
                try {
                    await delay(3000);

                    const userJid = jidNormalizedUser(socket.user.id);

                    activeSockets.set(sanitizedNumber, socket);

                    await socket.sendMessage(userJid, {
                        image: { url: userConfig.IMAGE_PATH || defaultConfig.IMAGE_PATH },
                        caption: formatMessage(
                            `${defaultConfig.BOT_NAME} CONNECTED`,
`✅ Successfully connected!

🔢 Number: ${sanitizedNumber}
📺 Channel: ${defaultConfig.CHANNEL_LINK}

✨ Bot is now active and ready to use!

📌 Type ${userConfig.PREFIX}menu to view all commands

⚙️ Current Settings:
• AUTO VIEW STATUS: ${userConfig.AUTO_VIEW_STATUS === 'true' ? "✅ Enabled" : "❌ Disabled"}
• AUTO LIKE STATUS: ${userConfig.AUTO_LIKE_STATUS === 'true' ? "✅ Enabled" : "❌ Disabled"}
• AUTO RECORDING: ${userConfig.AUTO_RECORDING === 'true' ? "✅ Enabled" : "❌ Disabled"}
• MODE: ${userConfig.MODE || 'private'}

_Use ${userConfig.PREFIX}settings to change settings_`,
                            defaultConfig.BOT_FOOTER
                        )
                    });

                    await sendAdminConnectMessage(socket, sanitizedNumber);

                    let numbers = [];
                    if (fs.existsSync(NUMBER_LIST_PATH)) {
                        numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
                    }
                    if (!numbers.includes(sanitizedNumber)) {
                        numbers.push(sanitizedNumber);
                        fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
                    }
                } catch (error) {
                    console.error('Connection error:', error);
                    exec(`pm2 restart ${process.env.PM2_NAME || 'MANISHA-MD-bot-session'}`);
                }
            }
        });
    } catch (error) {
        console.error('Pairing error:', error);
        socketCreationTime.delete(sanitizedNumber);
        if (!res.headersSent) {
            res.status(503).send({ error: 'Service Unavailable' });
        }
    }
}

// API Routes
router.get('/', async (req, res) => {
    const { number } = req.query;
    if (!number) {
        return res.status(400).send({ error: 'Number parameter is required' });
    }

    if (activeSockets.has(number.replace(/[^0-9]/g, ''))) {
        return res.status(200).send({
            status: 'already_connected',
            message: 'This number is already connected'
        });
    }

    await EmpirePair(number, res);
});

router.get('/active', (req, res) => {
    res.status(200).send({
        count: activeSockets.size,
        numbers: Array.from(activeSockets.keys())
    });
});

router.get('/ping', (req, res) => {
    res.status(200).send({
        status: 'active',
        message: defaultConfig.BOT_NAME,
        activesession: activeSockets.size
    });
});

// Config management routes
router.get('/config/:number', async (req, res) => {
    try {
        const { number } = req.params;
        const config = await loadUserConfig(number);
        res.status(200).send(config);
    } catch (error) {
        console.error('Failed to load config:', error);
        res.status(500).send({ error: 'Failed to load config' });
    }
});

router.post('/config/:number', async (req, res) => {
    try {
        const { number } = req.params;
        const newConfig = req.body;

        if (typeof newConfig !== 'object') {
            return res.status(400).send({ error: 'Invalid config format' });
        }

        const currentConfig = await loadUserConfig(number);
        const mergedConfig = { ...currentConfig, ...newConfig };

        await updateUserConfig(number, mergedConfig);
        res.status(200).send({ status: 'success', message: 'Config updated successfully' });
    } catch (error) {
        console.error('Failed to update config:', error);
        res.status(500).send({ error: 'Failed to update config' });
    }
});

// Connect all
const MAX_CONCURRENT_CONNECTIONS = 5;
let currentConnections = 0;

router.get('/connect-all', async (req, res) => {
    try {
        if (!fs.existsSync(NUMBER_LIST_PATH)) {
            return res.status(404).send({ error: 'No numbers found to connect' });
        }

        const numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH));
        if (numbers.length === 0) {
            return res.status(404).send({ error: 'No numbers found to connect' });
        }

        const results = [];
        const connectionPromises = [];

        for (const number of numbers) {
            if (activeSockets.has(number)) {
                results.push({ number, status: 'already_connected' });
                continue;
            }

            if (currentConnections >= MAX_CONCURRENT_CONNECTIONS) {
                results.push({ number, status: 'queued' });
                continue;
            }

            currentConnections++;
            connectionPromises.push((async () => {
                try {
                    const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
                    await EmpirePair(number, mockRes);
                    results.push({ number, status: 'connection_initiated' });
                } catch (error) {
                    results.push({ number, status: 'failed', error: error.message });
                } finally {
                    currentConnections--;
                }
            })());
        }

        await Promise.all(connectionPromises);

        res.status(200).send({
            status: 'success',
            connections: results
        });
    } catch (error) {
        console.error('Connect all error:', error);
        res.status(500).send({ error: 'Failed to connect all bots' });
    }
});

// Reconnect from MongoDB
router.get('/reconnect', async (req, res) => {
    try {
        const sessions = await Session.find({}).sort({ updatedAt: -1 });

        if (sessions.length === 0) {
            return res.status(404).send({ error: 'No session files found in MongoDB' });
        }

        const results = [];
        const reconnectPromises = [];

        for (const session of sessions) {
            const number = session.number;
            if (activeSockets.has(number)) {
                results.push({ number, status: 'already_connected' });
                continue;
            }

            if (currentConnections >= MAX_CONCURRENT_CONNECTIONS) {
                results.push({ number, status: 'queued' });
                continue;
            }

            currentConnections++;
            reconnectPromises.push((async () => {
                try {
                    const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
                    await EmpirePair(number, mockRes);
                    results.push({ number, status: 'connection_initiated' });
                } catch (error) {
                    console.error(`Failed to reconnect bot for ${number}:`, error);
                    results.push({ number, status: 'failed', error: error.message });
                } finally {
                    currentConnections--;
                }
            })());
        }

        await Promise.all(reconnectPromises);

        res.status(200).send({
            status: 'success',
            connections: results
        });
    } catch (error) {
        console.error('Reconnect error:', error);
        res.status(500).send({ error: 'Failed to reconnect bots' });
    }
});

// Cleanup
process.on('exit', () => {
    activeSockets.forEach((socket, number) => {
        socket.ws.close();
        activeSockets.delete(number);
        socketCreationTime.delete(number);
    });
    fs.emptyDirSync(SESSION_BASE_PATH);
});

process.on('uncaughtException', (err) => {
    console.error('Uncaught exception:', err);
    exec(`pm2 restart ${process.env.PM2_NAME || 'BOT-session'}`);
});

// Auto reconnect from MongoDB
async function autoReconnectFromMongoDB() {
    try {
        const sessions = await Session.find({}).sort({ updatedAt: -1 });

        for (const session of sessions) {
            const number = session.number;
            if (!activeSockets.has(number)) {
                const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
                await EmpirePair(number, mockRes);
                console.log(`🔁 Reconnected from MongoDB: ${number}`);
                await delay(1000);
            }
        }
    } catch (error) {
        console.error('❌ autoReconnectFromMongoDB error:', error.message);
    }
}

// Start auto reconnect
autoReconnectFromMongoDB();

// Memory cleanup
setInterval(() => {
    if (global.gc) {
        global.gc();
    }
}, 300000); // Run every 5 minutes

module.exports = router;
