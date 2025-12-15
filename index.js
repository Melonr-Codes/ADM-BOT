/**
 * Coin Moderation Bot
 * FULL FIXED & COMPLETE VERSION
 * Node.js >= 18 | discord.js v14
 */

require('dotenv').config();

const {
  Client,
  GatewayIntentBits,
  Partials,
  SlashCommandBuilder,
  PermissionFlagsBits,
  EmbedBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  AttachmentBuilder,
  ChannelType,
} = require('discord.js');

const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');
const fs = require('fs');
const path = require('path');

/* =========================
   CLIENT
========================= */
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.GuildVoiceStates,
    GatewayIntentBits.DirectMessages,
    GatewayIntentBits.MessageContent, // Required for message content listener
  ],
  partials: [Partials.Channel, Partials.GuildMember], // GuildMember for guildMemberRemove
});

/* =========================
   DATABASE
========================= */
const db = new sqlite3.Database('./database.db');

const run = (sql, params = []) =>
  new Promise((res, rej) => {
    db.run(sql, params, function (err) {
      if (err) rej(err);
      else res(this);
    });
  });

const get = (sql, params = []) =>
  new Promise((res, rej) => {
    db.get(sql, params, (err, row) => {
      if (err) rej(err);
      else res(row);
    });
  });

const all = (sql, params = []) =>
  new Promise((res, rej) => {
    db.all(sql, params, (err, rows) => {
      if (err) rej(err);
      else res(rows);
    });
  });

/* =========================
   INIT DATABASE
========================= */
async function initDatabase() {
  await run(`PRAGMA journal_mode=WAL`);

  await run(`
    CREATE TABLE IF NOT EXISTS guilds (
      guild_id TEXT PRIMARY KEY,
      log_channel TEXT,
      revoke_channel TEXT,
      panel_channel TEXT,
      bail_worth REAL DEFAULT 0,
      adv_worth REAL DEFAULT 0,
      timeout_worth REAL DEFAULT 0,
      unban_worth REAL DEFAULT 0,
      invite TEXT,
      coin_card TEXT
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS users (
      guild_id TEXT,
      user_id TEXT,
      join_timestamp INTEGER,
      reputation INTEGER DEFAULT 0,
      last_rep_message INTEGER DEFAULT 0,
      last_rep_voice INTEGER DEFAULT 0,
      PRIMARY KEY (guild_id, user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS advs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      guild_id TEXT,
      user_id TEXT,
      reason TEXT,
      is_denyword INTEGER DEFAULT 0,
      created_at INTEGER
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS adv_roles (
      guild_id TEXT PRIMARY KEY,
      adv1_role TEXT,
      adv2_role TEXT,
      adv3_role TEXT,
      ban_role TEXT
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS staff_roles (
      guild_id TEXT,
      role_id TEXT,
      UNIQUE(guild_id, role_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS invoices (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      guild_id TEXT,
      user_id TEXT,
      amount REAL,
      reason TEXT,
      created_at INTEGER,
      paid INTEGER DEFAULT 0
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS denywords (
      guild_id TEXT,
      word TEXT,
      PRIMARY KEY (guild_id, word)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      guild_id TEXT,
      channel_id TEXT,
      user_id TEXT,
      content TEXT,
      attachments_urls TEXT,
      created_at INTEGER
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS bans (
      guild_id TEXT,
      user_id TEXT,
      unban_worth REAL,
      reason TEXT,
      created_at INTEGER,
      PRIMARY KEY (guild_id, user_id)
    )
  `);
  
  await run(`
    CREATE TABLE IF NOT EXISTS rep_votes (
      guild_id TEXT,
      voter_id TEXT,
      target_id TEXT,
      last_vote_at INTEGER,
      PRIMARY KEY (guild_id, voter_id, target_id)
    )
  `);
}

/* =========================
   UTILITIES
========================= */
const trunc8 = n => Math.trunc(Number(n) * 1e8) / 1e8;
const clampRep = v => Math.max(-1000, Math.min(1000, v));
const msToTime = ms => {
  const s = Math.floor(ms / 1000);
  const d = Math.floor(s / (3600 * 24));
  const h = Math.floor((s % (3600 * 24)) / 3600);
  const m = Math.floor((s % 3600) / 60);
  const secs = Math.floor(s % 60);
  
  if (d > 0) return `${d}d ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m ${secs}s`;
  if (m > 0) return `${m}m ${secs}s`;
  return `${secs}s`;
};

async function ensureGuild(guildId) {
  await run(`INSERT OR IGNORE INTO guilds (guild_id) VALUES (?)`, [guildId]);
}

async function isStaff(member) {
  if (!member) return false;
  if (member.permissions.has(PermissionFlagsBits.Administrator)) return true;

  const roles = await all(
    `SELECT role_id FROM staff_roles WHERE guild_id=?`,
    [member.guild.id]
  );
  return roles.some(r => member.roles.cache.has(r.role_id));
}

async function updateReputation(guildId, userId, amount) {
  if (amount === 0) return;
  const row = await get(
    `SELECT reputation FROM users WHERE guild_id=? AND user_id=?`,
    [guildId, userId]
  );
  if (!row) return; // User must exist

  const newRep = clampRep(row.reputation + amount);
  await run(
    `UPDATE users SET reputation=? WHERE guild_id=? AND user_id=?`,
    [newRep, guildId, userId]
  );
}

async function sendToLogChannel(guild, embed) {
  const cfg = await get(`SELECT log_channel FROM guilds WHERE guild_id=?`, [guild.id]);
  if (!cfg?.log_channel) return;
  const channel = guild.channels.cache.get(cfg.log_channel);
  if (channel) await channel.send({ embeds: [embed] }).catch(() => {});
}

async function sendToRevokeChannel(guild, embed, attachment) {
  const cfg = await get(`SELECT revoke_channel FROM guilds WHERE guild_id=?`, [guild.id]);
  if (!cfg?.revoke_channel) return;
  const channel = guild.channels.cache.get(cfg.revoke_channel);
  if (channel) await channel.send({ embeds: [embed], files: attachment ? [attachment] : [] }).catch(() => {});
}

async function createInvoice(guildId, userId, amount, reason) {
  if (amount <= 0) return;
  const truncatedAmount = trunc8(amount);
  await run(
    `INSERT INTO invoices (guild_id,user_id,amount,reason,created_at)
     VALUES (?,?,?,?,?)`,
    [guildId, userId, truncatedAmount, reason, Date.now()]
  );
}

/* =========================
   ADV ROLE SYNC
========================= */
async function syncAdvRoles(guild, userId) {
  const cfg = await get(
    `SELECT * FROM adv_roles WHERE guild_id=?`,
    [guild.id]
  );
  if (!cfg) return;

  const member = await guild.members.fetch(userId).catch(() => null);
  if (!member) return;

  const row = await get(
    `SELECT COUNT(*) as c FROM advs WHERE guild_id=? AND user_id=?`,
    [guild.id, userId]
  );
  const count = row?.c || 0;

  const roles = [
    cfg.adv1_role,
    cfg.adv2_role,
    cfg.adv3_role,
    cfg.ban_role
  ].filter(Boolean);

  // Remove all ADV/BAN roles first
  for (const r of roles) {
    if (member.roles.cache.has(r)) {
      await member.roles.remove(r).catch(() => {});
    }
  }

  // Apply new roles
  if (count === 1 && cfg.adv1_role) await member.roles.add(cfg.adv1_role).catch(() => {});
  if (count === 2 && cfg.adv2_role) await member.roles.add(cfg.adv2_role).catch(() => {});
  if (count >= 3) {
    if (cfg.adv3_role) await member.roles.add(cfg.adv3_role).catch(() => {});
  }
  
  // Apply ban_role only if 3+ advs and no permban
  if (count >= 3 && cfg.ban_role) {
      const isPermBanned = await get(`SELECT 1 FROM bans WHERE guild_id=? AND user_id=?`, [guild.id, userId]);
      if (!isPermBanned) {
          await member.roles.add(cfg.ban_role).catch(() => {});
      }
  }
}

/* =========================
   COIN API
========================= */
async function payWithCard(fromCard, toCard, amount, reason) {
  // Ensure amount is truncated before sending
  const truncatedAmount = trunc8(amount);

  const res = await axios.post(
    `${process.env.COIN_API_BASE}/api/card/pay`,
    {
      from: fromCard,
      to: toCard,
      amount: truncatedAmount,
      reason
    },
    { timeout: 15000 }
  );

  if (!res.data?.success) {
    throw new Error(res.data?.error || 'Coin payment failed');
  }

  return res.data;
}

async function getGuildCard(guildId) {
  const row = await get(
    `SELECT coin_card FROM guilds WHERE guild_id=?`,
    [guildId]
  );
  return row?.coin_card || null;
}

/* =========================
   SLASH COMMAND DEFINITIONS
========================= */
const commands = [

  /* ===== ADVERTENCES ===== */
  new SlashCommandBuilder()
    .setName('adv')
    .setDescription('Give an advertence to a user')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true))
    .addStringOption(o =>
      o.setName('reason').setDescription('Reason').setRequired(false)),

  new SlashCommandBuilder()
    .setName('adv-remove')
    .setDescription('Remove one advertence from a user')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true)),
      
  new SlashCommandBuilder()
    .setName('advworth')
    .setDescription('Set the fine amount per ADV')
    .addNumberOption(o =>
      o.setName('value').setDescription('Coin value').setRequired(true)),

  new SlashCommandBuilder()
    .setName('advrole')
    .setDescription('Configure advertence roles')
    .addStringOption(o =>
      o.setName('level')
        .setDescription('Advertence level')
        .setRequired(true)
        .addChoices(
          { name: 'ADV 1', value: 'adv1' },
          { name: 'ADV 2', value: 'adv2' },
          { name: 'ADV 3', value: 'adv3' },
          { name: 'BAN', value: 'ban' }
        ))
    .addRoleOption(o =>
      o.setName('role')
        .setDescription('Role to apply')
        .setRequired(true)),

  /* ===== COIN / BAIL ===== */
  new SlashCommandBuilder()
    .setName('card')
    .setDescription('Set Coin Card ID for this server')
    .addStringOption(o =>
      o.setName('card').setDescription('Coin Card ID').setRequired(true)),

  new SlashCommandBuilder()
    .setName('bail')
    .setDescription('Pay bail to remove one advertence'),

  new SlashCommandBuilder()
    .setName('bailworth')
    .setDescription('Set bail price to remove one advertence')
    .addNumberOption(o =>
      o.setName('value').setDescription('Coin value').setRequired(true)),
      
  /* ===== INVOICES ===== */
  new SlashCommandBuilder()
    .setName('invoice')
    .setDescription('Create an invoice (fine)')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true))
    .addNumberOption(o =>
      o.setName('amount').setDescription('Coin amount').setRequired(true))
    .addStringOption(o =>
      o.setName('reason').setDescription('Reason').setRequired(false)),

  new SlashCommandBuilder()
    .setName('payinvoice')
    .setDescription('Pay your pending invoices'),

  /* ===== DENY WORDS ===== */
  new SlashCommandBuilder()
    .setName('denyword-add')
    .setDescription('Add a forbidden word')
    .addStringOption(o =>
      o.setName('word').setDescription('Word').setRequired(true)),

  new SlashCommandBuilder()
    .setName('denyword-remove')
    .setDescription('Remove a forbidden word')
    .addStringOption(o =>
      o.setName('word').setDescription('Word').setRequired(true)),
      
  /* ===== TIMEOUTS & BANS ===== */
  new SlashCommandBuilder()
    .setName('timeout')
    .setDescription('Apply a timeout to a user (and fine)')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true))
    .addStringOption(o =>
      o.setName('duration')
        .setDescription('Duration (e.g., 1h, 30m, 7d)')
        .setRequired(true))
    .addStringOption(o =>
      o.setName('reason').setDescription('Reason').setRequired(false)),

  new SlashCommandBuilder()
    .setName('timeout-worth')
    .setDescription('Set the fine amount for any timeout')
    .addNumberOption(o =>
      o.setName('value').setDescription('Coin value').setRequired(true)),
      
  new SlashCommandBuilder()
    .setName('permban')
    .setDescription('Ban a user permanently with a fine for unban')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true))
    .addNumberOption(o =>
      o.setName('unban_worth')
        .setDescription('Coin value for unban')
        .setRequired(true))
    .addStringOption(o =>
      o.setName('reason').setDescription('Reason').setRequired(false)),
      
  new SlashCommandBuilder()
    .setName('unbanworth')
    .setDescription('Set the default fine amount for permban')
    .addNumberOption(o =>
      o.setName('value').setDescription('Coin value').setRequired(true)),

  new SlashCommandBuilder()
    .setName('unban')
    .setDescription('Remove permban from a user')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true)),

  /* ===== INFO / LOGS ===== */
  new SlashCommandBuilder()
    .setName('view')
    .setDescription('View detailed information about a user')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true)),

  new SlashCommandBuilder()
    .setName('viewlog')
    .setDescription('Generate a message transcript for a user')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true))
    .addStringOption(o =>
      o.setName('time_from')
        .setDescription('Start time (e.g., 9h, 1/1/2023)')
        .setRequired(false))
    .addStringOption(o =>
      o.setName('time_to')
        .setDescription('End time (e.g., now, 2/1/2023)')
        .setRequired(false))
    .addChannelOption(o =>
      o.setName('channel')
        .setDescription('Filter by a specific channel')
        .addChannelTypes(ChannelType.GuildText)
        .setRequired(false)),

  new SlashCommandBuilder()
    .setName('rep')
    .setDescription('View a user\'s reputation or your own')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(false)),

  new SlashCommandBuilder()
    .setName('positive')
    .setDescription('Give a positive reputation point (+5)')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true)),

  new SlashCommandBuilder()
    .setName('negative')
    .setDescription('Give a negative reputation point (-5)')
    .addUserOption(o =>
      o.setName('user').setDescription('Target user').setRequired(true)),

  /* ===== CONFIG ===== */
  new SlashCommandBuilder()
    .setName('panel')
    .setDescription('Send moderation panel')
    .addChannelOption(o =>
      o.setName('channel').setDescription('Panel channel').setRequired(true)),

  new SlashCommandBuilder()
    .setName('staffrole-add')
    .setDescription('Add staff role')
    .addRoleOption(o =>
      o.setName('role').setDescription('Role').setRequired(true)),

  new SlashCommandBuilder()
    .setName('staffrole-remove')
    .setDescription('Remove staff role')
    .addRoleOption(o =>
      o.setName('role').setDescription('Role').setRequired(true)),

  new SlashCommandBuilder()
    .setName('log')
    .setDescription('Set moderation log channel')
    .addChannelOption(o =>
      o.setName('channel').setDescription('Log channel').setRequired(true)),

  new SlashCommandBuilder()
    .setName('revoke')
    .setDescription('Set deleted message log channel')
    .addChannelOption(o =>
      o.setName('channel').setDescription('Revoke channel').setRequired(true)),

  new SlashCommandBuilder()
    .setName('invite')
    .setDescription('Set guild invite link')
    .addStringOption(o =>
      o.setName('link').setDescription('Invite link').setRequired(true))
];

/* =========================
   REGISTER COMMANDS
========================= */
client.once('ready', async () => {
  await initDatabase();

  await client.application.commands.set(
    commands.map(c => c.toJSON())
  );

  console.log(`✅ Logged in as ${client.user.tag}`);
});

/* =========================
   PARSING TIME UTILITY
========================= */
function parseTime(input) {
  if (!input) return null;
  
  if (input.toLowerCase() === 'now') return Date.now();
  
  // Handle relative time like '9h', '30m', '7d'
  const match = input.match(/^(\d+)([hmd])$/i);
  if (match) {
    const value = parseInt(match[1]);
    const unit = match[2].toLowerCase();
    const now = Date.now();
    let ms = 0;
    
    if (unit === 'd') ms = value * 24 * 60 * 60 * 1000;
    else if (unit === 'h') ms = value * 60 * 60 * 1000;
    else if (unit === 'm') ms = value * 60 * 1000;
    
    return now - ms; // '9h' means 9 hours ago
  }

  // Handle date format like 'dd/mm/aaaa'
  const dateMatch = input.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (dateMatch) {
      const day = parseInt(dateMatch[1]);
      const month = parseInt(dateMatch[2]);
      const year = parseInt(dateMatch[3]);
      // Note: Month is 0-indexed in JS Date
      const date = new Date(year, month - 1, day);
      return date.getTime();
  }

  return null;
}

/* =========================
   TRANSCRIPT UTILITY
========================= */
async function generateTranscript(guildId, userId, channelId, timeFrom, timeTo) {
  let whereClauses = [`guild_id=?`, `user_id=?`];
  let params = [guildId, userId];

  if (channelId) {
    whereClauses.push(`channel_id=?`);
    params.push(channelId);
  }
  
  let fromTimestamp = parseTime(timeFrom);
  let toTimestamp = parseTime(timeTo) || Date.now();

  if (fromTimestamp) {
    whereClauses.push(`created_at >= ?`);
    params.push(fromTimestamp);
  }

  if (toTimestamp) {
    whereClauses.push(`created_at <= ?`);
    params.push(toTimestamp);
  }

  const messages = await all(
    `SELECT * FROM messages WHERE ${whereClauses.join(' AND ')} ORDER BY created_at ASC`,
    params
  );

  if (messages.length === 0) {
    return 'No messages found for this user in the specified range/channel.';
  }

  let transcript = '';
  let currentChannelId = null;

  for (const msg of messages) {
    const channelName = client.channels.cache.get(msg.channel_id)?.name || `Unknown Channel (${msg.channel_id})`;
    const date = new Date(msg.created_at).toLocaleString('pt-BR');

    if (msg.channel_id !== currentChannelId) {
      transcript += `\n--- CHANNEL: #${channelName} ---\n`;
      currentChannelId = msg.channel_id;
    }

    let content = msg.content || '';
    if (msg.attachments_urls) {
        content += `\n[Attachments: ${msg.attachments_urls.split(',').join(', ')}]`;
    }

    transcript += `[${date}] <@${msg.user_id}>: ${content}\n`;
  }
  
  return transcript;
}

/* =========================
   MODALS & BUTTON BUILDERS
========================= */
function bailModal() {
  return new ModalBuilder()
    .setCustomId('bail_pay')
    .setTitle('Pay Bail (Remove 1 ADV)')
    .addComponents(
      new ActionRowBuilder().addComponents(
        new TextInputBuilder()
          .setCustomId('card')
          .setLabel('Your Coin Card ID')
          .setPlaceholder('Enter your Coin Card ID to pay the bail worth.')
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
      )
    );
}

function payInvoiceModal() {
  return new ModalBuilder()
    .setCustomId('invoice_pay')
    .setTitle('Pay All Pending Invoices')
    .addComponents(
      new ActionRowBuilder().addComponents(
        new TextInputBuilder()
          .setCustomId('card')
          .setLabel('Your Coin Card ID')
          .setPlaceholder('Enter your Coin Card ID to pay total fine.')
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
      )
    );
}

function unbanModal() {
  return new ModalBuilder()
    .setCustomId('unban_pay')
    .setTitle('Pay Unban Fine')
    .addComponents(
      new ActionRowBuilder().addComponents(
        new TextInputBuilder()
          .setCustomId('card')
          .setLabel('Your Coin Card ID')
          .setPlaceholder('Enter your Coin Card ID to pay the unban fine.')
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
      )
    );
}

function panelButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId('panel_bail')
      .setLabel('Pay Bail (1 ADV)')
      .setStyle(ButtonStyle.Success),
    new ButtonBuilder()
      .setCustomId('panel_info')
      .setLabel('User Info')
      .setStyle(ButtonStyle.Primary),
    new ButtonBuilder()
      .setCustomId('panel_payinvoice')
      .setLabel('Pay Invoices (Fines)')
      .setStyle(ButtonStyle.Danger)
  );
}

/* =========================
   INTERACTION CREATE (UNIFIED)
========================= */
client.on('interactionCreate', async i => {
  try {
    if (!i.inGuild()) return i.reply({ content: '❌ This command must be used in a server.', ephemeral: true });

    const guildId = i.guild.id;
    await ensureGuild(guildId);
    
    // Defer for all non-modal-submit interactions to prevent timeout
    if (i.isChatInputCommand() || i.isButton()) {
        await i.deferReply({ ephemeral: true }).catch(() => {});
    }


    /* =====================
       SLASH COMMANDS
    ===================== */
    if (i.isChatInputCommand()) {
      const is_staff = await isStaff(i.member);
      
      // Staff Only Commands Check
      if (['adv', 'adv-remove', 'advrole', 'advworth', 'card', 'bailworth', 'invoice', 'denyword-add', 'denyword-remove', 'panel', 'staffrole-add', 'staffrole-remove', 'log', 'revoke', 'invite', 'timeout', 'timeout-worth', 'unbanworth', 'permban', 'unban'].includes(i.commandName)) {
        if (!is_staff) {
          return i.editReply({ content: '❌ No permission. Staff or Administrator role required.', ephemeral: true });
        }
      }
      
      /* ===== CONFIG COMMANDS (CARD, WORTHS, INVITE, LOG, REVOKE) ===== */
      if (i.commandName === 'card') {
        const card = i.options.getString('card');
        await run(`UPDATE guilds SET coin_card=? WHERE guild_id=?`, [card, guildId]);
        return i.editReply({ content: '✅ Coin Card configured.' });
      }

      if (i.commandName === 'bailworth') {
        const value = trunc8(i.options.getNumber('value'));
        await run(`UPDATE guilds SET bail_worth=? WHERE guild_id=?`, [value, guildId]);
        return i.editReply({ content: `✅ Bail worth set to ${value}` });
      }
      
      if (i.commandName === 'advworth') {
        const value = trunc8(i.options.getNumber('value'));
        await run(`UPDATE guilds SET adv_worth=? WHERE guild_id=?`, [value, guildId]);
        return i.editReply({ content: `✅ ADV fine worth set to ${value}` });
      }
      
      if (i.commandName === 'timeout-worth') {
        const value = trunc8(i.options.getNumber('value'));
        await run(`UPDATE guilds SET timeout_worth=? WHERE guild_id=?`, [value, guildId]);
        return i.editReply({ content: `✅ Timeout fine worth set to ${value}` });
      }
      
      if (i.commandName === 'unbanworth') {
        const value = trunc8(i.options.getNumber('value'));
        await run(`UPDATE guilds SET unban_worth=? WHERE guild_id=?`, [value, guildId]);
        return i.editReply({ content: `✅ Default Unban fine worth set to ${value}` });
      }
      
      if (i.commandName === 'invite') {
        const link = i.options.getString('link');
        await run(`UPDATE guilds SET invite=? WHERE guild_id=?`, [link, guildId]);
        return i.editReply({ content: '✅ Invite saved.' });
      }
      
      if (i.commandName === 'log') {
        const channel = i.options.getChannel('channel');
        await run(`UPDATE guilds SET log_channel=? WHERE guild_id=?`, [channel.id, guildId]);
        return i.editReply({ content: '✅ Moderation log channel set.' });
      }
      
      if (i.commandName === 'revoke') {
        const channel = i.options.getChannel('channel');
        await run(`UPDATE guilds SET revoke_channel=? WHERE guild_id=?`, [channel.id, guildId]);
        return i.editReply({ content: '✅ Deleted message log channel set.' });
      }

      /* ===== STAFF ROLE ===== */
      if (i.commandName === 'staffrole-add' || i.commandName === 'staffrole-remove') {
        if (!i.member.permissions.has(PermissionFlagsBits.Administrator))
          return i.editReply({ content: '❌ Admin only.' });

        const role = i.options.getRole('role');

        if (i.commandName === 'staffrole-add') {
          await run(
            `INSERT OR IGNORE INTO staff_roles (guild_id,role_id)
             VALUES (?,?)`,
            [guildId, role.id]
          );
          return i.editReply({ content: '✅ Staff role added.' });
        }

        await run(
          `DELETE FROM staff_roles WHERE guild_id=? AND role_id=?`,
          [guildId, role.id]
        );
        return i.editReply({ content: '🗑️ Staff role removed.' });
      }

      /* ===== ADV ===== */
      if (i.commandName === 'adv') {
        const user = i.options.getUser('user');
        const member = await i.guild.members.fetch(user.id).catch(() => null);
        const reason = i.options.getString('reason') || 'No reason';

        await run(
          `INSERT INTO advs (guild_id,user_id,reason,created_at)
           VALUES (?,?,?,?)`,
          [guildId, user.id, reason, Date.now()]
        );
        
        await updateReputation(guildId, user.id, -10); // -10 Rep per ADV [cite: 12]
        await syncAdvRoles(i.guild, user.id);
        
        // 5 min timeout 
        if (member) {
             const fiveMinMs = 5 * 60 * 1000;
             await member.timeout(fiveMinMs, `Received advertence: ${reason}`).catch(() => {});
        }

        // Apply ADV worth fine
        const cfg = await get(`SELECT adv_worth FROM guilds WHERE guild_id=?`, [guildId]);
        if (cfg?.adv_worth > 0) {
            await createInvoice(guildId, user.id, cfg.adv_worth, `Advertence Fine: ${reason}`);
        }
        
        // Log to channel
        await sendToLogChannel(i.guild, new EmbedBuilder()
            .setTitle('⚠️ User Advertised')
            .setColor('Yellow')
            .setDescription(`${user.tag} received an advertence.`)
            .addFields(
                { name: 'Target', value: `<@${user.id}>`, inline: true },
                { name: 'Staff', value: `<@${i.user.id}>`, inline: true },
                { name: 'Reason', value: reason }
            )
            .setTimestamp()
        );

        return i.editReply({ content: `⚠️ Advertence applied to ${user.tag}.` });
      }

      /* ===== ADV REMOVE ===== */
      if (i.commandName === 'adv-remove') {
        const user = i.options.getUser('user');

        const row = await get(
          `SELECT id FROM advs
           WHERE guild_id=? AND user_id=?
           ORDER BY created_at DESC LIMIT 1`,
          [guildId, user.id]
        );

        if (!row)
          return i.editReply({ content: '⚠️ User has no advertences.' });

        await run(`DELETE FROM advs WHERE id=?`, [row.id]);
        await syncAdvRoles(i.guild, user.id);
        
        await sendToLogChannel(i.guild, new EmbedBuilder()
            .setTitle('✅ Advertence Removed')
            .setColor('Green')
            .setDescription(`One advertence was removed from ${user.tag}.`)
            .addFields(
                { name: 'Target', value: `<@${user.id}>`, inline: true },
                { name: 'Staff', value: `<@${i.user.id}>`, inline: true }
            )
            .setTimestamp()
        );

        return i.editReply({ content: `✅ Advertence removed from ${user.tag}.` });
      }

      /* ===== ADV ROLE CONFIG ===== */
      if (i.commandName === 'advrole') {
        const level = i.options.getString('level');
        const role = i.options.getRole('role');

        const column =
          level === 'adv1' ? 'adv1_role' :
          level === 'adv2' ? 'adv2_role' :
          level === 'adv3' ? 'adv3_role' :
          'ban_role';

        await run(
          `INSERT INTO adv_roles (guild_id, ${column})
           VALUES (?,?)
           ON CONFLICT(guild_id)
           DO UPDATE SET ${column}=excluded.${column}`,
          [guildId, role.id]
        );

        return i.editReply({
          content: `✅ Role configured for ${level.toUpperCase()}`,
        });
      }

      /* ===== DENYWORD ADD/REMOVE ===== */
      if (i.commandName === 'denyword-add' || i.commandName === 'denyword-remove') {
        const word = i.options.getString('word').toLowerCase();

        if (i.commandName === 'denyword-add') {
          await run(
            `INSERT OR IGNORE INTO denywords (guild_id,word)
             VALUES (?,?)`,
            [guildId, word]
          );
          return i.editReply({ content: '🚫 Word added.' });
        }

        await run(
          `DELETE FROM denywords WHERE guild_id=? AND word=?`,
          [guildId, word]
        );

        return i.editReply({ content: '🗑️ Word removed.' });
      }

      /* ===== PANEL ===== */
      if (i.commandName === 'panel') {
        const channel = i.options.getChannel('channel');

        await run(
          `UPDATE guilds SET panel_channel=? WHERE guild_id=?`,
          [channel.id, guildId]
        );

        await channel.send({
          embeds: [
            new EmbedBuilder()
              .setTitle('🛡️ Moderation Panel')
              .setDescription('Use the buttons below to interact with your account and fines.')
              .setColor('Blurple')
          ],
          components: [panelButtons()]
        });

        return i.editReply({ content: '✅ Panel sent.' });
      }
      
      /* ===== TIMEOUT ===== */
      if (i.commandName === 'timeout') {
        const user = i.options.getUser('user');
        const member = await i.guild.members.fetch(user.id).catch(() => null);
        if (!member) return i.editReply({ content: '❌ User not found in this guild.' });
        
        const durationStr = i.options.getString('duration');
        const durationMs = parseTime(durationStr);
        if (!durationMs || durationMs < Date.now()) return i.editReply({ content: '❌ Invalid duration format (use 1h, 30m, 7d).' });

        const timeoutMs = durationMs - Date.now();
        const reason = i.options.getString('reason') || 'Manual timeout';
        
        await member.timeout(timeoutMs, reason).catch(() => {});
        
        // Apply timeout worth fine [cite: 14]
        const cfg = await get(`SELECT timeout_worth FROM guilds WHERE guild_id=?`, [guildId]);
        if (cfg?.timeout_worth > 0) {
            await createInvoice(guildId, user.id, cfg.timeout_worth, `Timeout Fine: ${reason}`);
        }
        
        await sendToLogChannel(i.guild, new EmbedBuilder()
            .setTitle('⌛ User Timed Out')
            .setColor('Orange')
            .setDescription(`${user.tag} was timed out for ${msToTime(timeoutMs)}.`)
            .addFields(
                { name: 'Target', value: `<@${user.id}>`, inline: true },
                { name: 'Staff', value: `<@${i.user.id}>`, inline: true },
                { name: 'Reason', value: reason }
            )
            .setTimestamp()
        );
        
        return i.editReply({ content: `✅ ${user.tag} timed out for ${msToTime(timeoutMs)}.` });
      }
      
      /* ===== PERMBAN ===== */
      if (i.commandName === 'permban') {
        const user = i.options.getUser('user');
        const unbanWorth = trunc8(i.options.getNumber('unban_worth'));
        const reason = i.options.getString('reason') || 'No reason specified';
        const guildCfg = await get(`SELECT invite FROM guilds WHERE guild_id=?`, [guildId]);
        
        // 1. Save ban details
        await run(
          `INSERT OR REPLACE INTO bans (guild_id,user_id,unban_worth,reason,created_at)
           VALUES (?,?,?,?,?)`,
          [guildId, user.id, unbanWorth, reason, Date.now()]
        );
        
        // 2. Send DM BEFORE ban [cite: 17]
        const dmEmbed = new EmbedBuilder()
            .setTitle(`You have been banned from ${i.guild.name}`)
            .setColor('Red')
            .setDescription(`**Reason:** ${reason}\n\nTo be unbanned, you must pay a fine of **${unbanWorth} COIN**.`)
            .setTimestamp();
            
        const unbanButton = new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId('unban_button')
                .setLabel('Pay Unban Fine')
                .setStyle(ButtonStyle.Success)
        );
        
        await user.send({
            embeds: [dmEmbed],
            components: [unbanButton]
        }).catch(() => {});
        
        // 3. Perform the ban
        await i.guild.members.ban(user.id, { reason }).catch(() => {});
        
        await sendToLogChannel(i.guild, new EmbedBuilder()
            .setTitle('⛔ User Permanently Banned')
            .setColor('Red')
            .setDescription(`${user.tag} was permanently banned.`)
            .addFields(
                { name: 'Target', value: `<@${user.id}>`, inline: true },
                { name: 'Staff', value: `<@${i.user.id}>`, inline: true },
                { name: 'Unban Worth', value: `${unbanWorth} COIN` },
                { name: 'Reason', value: reason }
            )
            .setTimestamp()
        );
        
        return i.editReply({ content: `✅ ${user.tag} permanently banned. DM sent with unban link.` });
      }
      
      /* ===== UNBAN (Staff) ===== */
      if (i.commandName === 'unban') {
          const user = i.options.getUser('user');
          
          await i.guild.bans.remove(user.id).catch(() => {});
          await run(`DELETE FROM bans WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
          await run(`DELETE FROM advs WHERE guild_id=? AND user_id=?`, [guildId, user.id]); // Remove advs too

          await sendToLogChannel(i.guild, new EmbedBuilder()
              .setTitle('🔓 User Unbanned (Staff)')
              .setColor('Blue')
              .setDescription(`${user.tag} was manually unbanned.`)
              .addFields(
                  { name: 'Target', value: `<@${user.id}>`, inline: true },
                  { name: 'Staff', value: `<@${i.user.id}>`, inline: true }
              )
              .setTimestamp()
          );

          return i.editReply({ content: `✅ ${user.tag} has been unbanned and removed from the ban list.` });
      }
      
      /* ===== INVOICE ===== */
      if (i.commandName === 'invoice') {
        const user = i.options.getUser('user');
        const amount = i.options.getNumber('amount');
        const reason = i.options.getString('reason') || 'Fine';
        
        await createInvoice(guildId, user.id, amount, reason);

        await sendToLogChannel(i.guild, new EmbedBuilder()
            .setTitle('💰 Invoice Created')
            .setColor('Gold')
            .setDescription(`An invoice was created for ${user.tag}.`)
            .addFields(
                { name: 'Target', value: `<@${user.id}>`, inline: true },
                { name: 'Staff', value: `<@${i.user.id}>`, inline: true },
                { name: 'Amount', value: `${trunc8(amount)} COIN` },
                { name: 'Reason', value: reason }
            )
            .setTimestamp()
        );
        
        return i.editReply({ content: `✅ Invoice for ${trunc8(amount)} COIN created for ${user.tag}.` });
      }
      
      /* ===== PAY INVOICE ===== */
      if (i.commandName === 'payinvoice') {
          // Open modal
          await i.editReply({ content: 'Opening payment modal...' }).catch(() => {});
          return i.showModal(payInvoiceModal());
      }
      
      /* ===== VIEW ===== */
      if (i.commandName === 'view') {
        const user = i.options.getUser('user');
        const member = await i.guild.members.fetch(user.id).catch(() => null);
        const userData = await get(`SELECT * FROM users WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
        
        if (!userData) return i.editReply({ content: '❌ User data not found in database.' });
        
        const advCountRow = await get(`SELECT COUNT(*) as c FROM advs WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
        const msgCountRow = await get(`SELECT COUNT(*) as c FROM messages WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
        const totalAdvCountRow = await get(`SELECT COUNT(*) as c FROM advs WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
        const invoiceTotalRow = await get(`SELECT SUM(amount) as s FROM invoices WHERE guild_id=? AND user_id=? AND paid=0`, [guildId, user.id]);
        
        const joinTimestamp = userData.join_timestamp || (member?.joinedTimestamp || Date.now());
        const advCount = advCountRow?.c || 0;
        const msgCount = msgCountRow?.c || 0;
        const totalAdvCount = totalAdvCountRow?.c || 0;
        const pendingInvoice = invoiceTotalRow?.s || 0;
        
        const embed = new EmbedBuilder()
            .setTitle(`👤 User Moderation Info: ${user.tag}`)
            .setColor(advCount > 0 ? 'Red' : 'Green')
            .setThumbnail(user.displayAvatarURL())
            .addFields(
                { name: 'Time in Guild', value: msToTime(Date.now() - joinTimestamp), inline: true },
                { name: 'Total Messages Logged', value: msgCount.toString(), inline: true },
                { name: 'Reputation Score', value: userData.reputation.toString(), inline: true },
                { name: 'Current Advertences', value: advCount.toString(), inline: true },
                { name: 'Total Advertences (Lifetime)', value: totalAdvCount.toString(), inline: true },
                { name: 'Pending Fine (COIN)', value: trunc8(pendingInvoice).toString(), inline: true }
            )
            .setFooter({ text: `User ID: ${user.id}` })
            .setTimestamp();
            
        return i.editReply({ embeds: [embed] });
      }
      
      /* ===== VIEWLOG (TRANSCRIPT) ===== */
      if (i.commandName === 'viewlog') {
        const user = i.options.getUser('user');
        const channel = i.options.getChannel('channel');
        const timeFrom = i.options.getString('time_from');
        const timeTo = i.options.getString('time_to');

        const transcript = await generateTranscript(
            guildId,
            user.id,
            channel?.id,
            timeFrom,
            timeTo
        );
        
        // Create file attachment (max 8MB check is implicitly handled by Discord) [cite: 4]
        const buffer = Buffer.from(transcript, 'utf-8');
        const attachment = new AttachmentBuilder(buffer, { name: `transcript_${user.id}_${Date.now()}.txt` });
        
        return i.editReply({ content: `✅ Transcript generated for ${user.tag}.`, files: [attachment] });
      }
      
      /* ===== REP ===== */
      if (i.commandName === 'rep') {
        const user = i.options.getUser('user') || i.user;
        const userData = await get(`SELECT reputation FROM users WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
        
        const rep = userData?.reputation || 0;
        
        return i.editReply({ content: `✨ **${user.tag}**'s reputation score is **${rep}**.` });
      }
      
      /* ===== POSITIVE / NEGATIVE ===== */
      if (i.commandName === 'positive' || i.commandName === 'negative') {
        const user = i.options.getUser('user');
        const voterId = i.user.id;
        const isPositive = i.commandName === 'positive';
        const repChange = isPositive ? 5 : -5;
        
        if (user.id === voterId) return i.editReply({ content: '❌ You cannot vote for yourself.' });

        // Check cooldown (1h) [cite: 11]
        const now = Date.now();
        const cooldown = 60 * 60 * 1000;
        const voteRow = await get(
            `SELECT last_vote_at FROM rep_votes WHERE guild_id=? AND voter_id=? AND target_id=?`,
            [guildId, voterId, user.id]
        );
        
        if (voteRow && now - voteRow.last_vote_at < cooldown) {
            const timeLeft = msToTime(cooldown - (now - voteRow.last_vote_at));
            return i.editReply({ content: `❌ You must wait ${timeLeft} before voting for ${user.tag} again.` });
        }
        
        // Update vote time
        await run(
            `INSERT OR REPLACE INTO rep_votes (guild_id, voter_id, target_id, last_vote_at)
             VALUES (?, ?, ?, ?)`,
            [guildId, voterId, user.id, now]
        );
        
        // Update reputation
        await updateReputation(guildId, user.id, repChange);
        
        return i.editReply({ content: `✅ Vote applied. **${user.tag}**'s reputation changed by ${repChange}.` });
      }

    }

    /* =====================
       BUTTONS
    ===================== */
    if (i.isButton()) {
      if (i.customId === 'panel_bail') {
        await i.editReply({ content: 'Opening bail payment modal...' }).catch(() => {});
        return i.showModal(bailModal());
      }
      if (i.customId === 'panel_payinvoice') {
        await i.editReply({ content: 'Opening invoice payment modal...' }).catch(() => {});
        return i.showModal(payInvoiceModal());
      }
      if (i.customId === 'panel_info') {
          // Re-use /view logic for the user who clicked the button
          const user = i.user;
          const member = i.member;
          const userData = await get(`SELECT * FROM users WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
        
          if (!userData) return i.editReply({ content: '❌ Your data not found in database.' });
        
          const advCountRow = await get(`SELECT COUNT(*) as c FROM advs WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
          const msgCountRow = await get(`SELECT COUNT(*) as c FROM messages WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
          const totalAdvCountRow = await get(`SELECT COUNT(*) as c FROM advs WHERE guild_id=? AND user_id=?`, [guildId, user.id]);
          const invoiceTotalRow = await get(`SELECT SUM(amount) as s FROM invoices WHERE guild_id=? AND user_id=? AND paid=0`, [guildId, user.id]);
          
          const joinTimestamp = userData.join_timestamp || (member?.joinedTimestamp || Date.now());
          const advCount = advCountRow?.c || 0;
          const msgCount = msgCountRow?.c || 0;
          const totalAdvCount = totalAdvCountRow?.c || 0;
          const pendingInvoice = invoiceTotalRow?.s || 0;
          
          const embed = new EmbedBuilder()
              .setTitle(`👤 Your Moderation Info: ${user.tag}`)
              .setColor(advCount > 0 ? 'Red' : 'Green')
              .setThumbnail(user.displayAvatarURL())
              .addFields(
                  { name: 'Time in Guild', value: msToTime(Date.now() - joinTimestamp), inline: true },
                  { name: 'Total Messages Logged', value: msgCount.toString(), inline: true },
                  { name: 'Reputation Score', value: userData.reputation.toString(), inline: true },
                  { name: 'Current Advertences', value: advCount.toString(), inline: true },
                  { name: 'Total Advertences (Lifetime)', value: totalAdvCount.toString(), inline: true },
                  { name: 'Pending Fine (COIN)', value: trunc8(pendingInvoice).toString(), inline: true }
              )
              .setFooter({ text: `User ID: ${user.id}` })
              .setTimestamp();
            
          return i.editReply({ embeds: [embed] });
      }
      if (i.customId === 'unban_button') {
        // This button is in the DM, so we only need to show the modal
        // We defer here to handle the reply in the modal submit
        return i.showModal(unbanModal());
      }
    }

    /* =====================
       MODALS
    ===================== */
    if (i.isModalSubmit()) {
      const userCard = i.fields.getTextInputValue('card');
      const guildCard = await getGuildCard(i.guild.id);
      
      await i.deferReply({ ephemeral: true }).catch(() => {});

      if (!guildCard)
        return i.editReply({ content: '❌ Server has no Coin Card configured. Cannot process payment.' });

      /* ===== BAIL PAY ===== */
      if (i.customId === 'bail_pay') {
        const cfg = await get(
          `SELECT bail_worth FROM guilds WHERE guild_id=?`,
          [i.guild.id]
        );

        if (!cfg || cfg.bail_worth <= 0)
          return i.editReply({ content: '❌ Bail worth not configured or is 0.' });

        const row = await get(
          `SELECT id FROM advs
           WHERE guild_id=? AND user_id=?
           ORDER BY created_at DESC LIMIT 1`,
          [i.guild.id, i.user.id]
        );
        
        if (!row) return i.editReply({ content: '❌ You have no advertences to remove.' });

        try {
            await payWithCard(userCard, guildCard, cfg.bail_worth, 'Bail payment (1 ADV removal)');
            
            await run(`DELETE FROM advs WHERE id=?`, [row.id]);
            await syncAdvRoles(i.guild, i.user.id);
            await updateReputation(i.guild.id, i.user.id, 5); // +5 rep for paying 

            return i.editReply({ content: '✅ Bail paid. One advertence removed. (+5 Rep)' });
            
        } catch(e) {
            return i.editReply({ content: `❌ Coin payment failed. Error: ${e.message}` });
        }
      }
      
      /* ===== INVOICE PAY ===== */
      if (i.customId === 'invoice_pay') {
        const pendingInvoices = await all(
            `SELECT id, amount, reason FROM invoices WHERE guild_id=? AND user_id=? AND paid=0`,
            [i.guild.id, i.user.id]
        );
        
        if (pendingInvoices.length === 0)
            return i.editReply({ content: '❌ You have no pending invoices (fines).' });
        
        const totalAmount = pendingInvoices.reduce((sum, inv) => sum + inv.amount, 0);
        
        try {
            await payWithCard(userCard, guildCard, totalAmount, `Invoice payment (Total: ${trunc8(totalAmount)})`);

            // Mark all as paid
            const invoiceIds = pendingInvoices.map(inv => inv.id).join(',');
            await run(`UPDATE invoices SET paid=1 WHERE id IN (${invoiceIds})`);
            
            await updateReputation(i.guild.id, i.user.id, 5); // +5 rep for paying 
            
            // Remove timeout [cite: 14]
            await i.member.timeout(null, 'Invoice paid, timeout removed.').catch(() => {});

            return i.editReply({ content: `✅ All pending invoices paid (${trunc8(totalAmount)} COIN). (+5 Rep)` });
            
        } catch(e) {
            return i.editReply({ content: `❌ Coin payment failed. Error: ${e.message}` });
        }
      }
      
      /* ===== UNBAN PAY (From DM) ===== */
      if (i.customId === 'unban_pay') {
        const banRow = await get(`SELECT * FROM bans WHERE user_id=?`, [i.user.id]);
        if (!banRow) return i.editReply({ content: '❌ You are not on a permanent ban list.' });
        
        const guild = client.guilds.cache.get(banRow.guild_id);
        const guildCfg = await get(`SELECT coin_card, invite FROM guilds WHERE guild_id=?`, [banRow.guild_id]);
        
        if (!guild) return i.editReply({ content: '❌ Ban originated from an unknown server.' });
        if (!guildCfg?.coin_card) return i.editReply({ content: '❌ Server has no Coin Card configured.' });
        
        const unbanWorth = banRow.unban_worth;
        
        try {
            await payWithCard(userCard, guildCfg.coin_card, unbanWorth, `Unban payment for ${guild.name}`);
            
            // Unban user, remove from table, remove all advs
            await guild.bans.remove(i.user.id).catch(() => {});
            await run(`DELETE FROM bans WHERE guild_id=? AND user_id=?`, [guild.id, i.user.id]);
            await run(`DELETE FROM advs WHERE guild_id=? AND user_id=?`, [guild.id, i.user.id]); 

            // Send invite back to user
            const inviteLink = guildCfg?.invite || 'No invite link configured for this server.';

            await i.editReply({ 
                content: `✅ Unban fine paid (${trunc8(unbanWorth)} COIN). You have been unbanned from **${guild.name}**.\n\n**Invite Link:** ${inviteLink}` 
            });
            
            // Log to channel
            await sendToLogChannel(guild, new EmbedBuilder()
                .setTitle('🔓 User Paid Unban Fine')
                .setColor('Blue')
                .setDescription(`${i.user.tag} paid the fine and was unbanned.`)
                .addFields(
                    { name: 'User', value: `<@${i.user.id}>`, inline: true },
                    { name: 'Amount Paid', value: `${trunc8(unbanWorth)} COIN` }
                )
                .setTimestamp()
            );

        } catch(e) {
            return i.editReply({ content: `❌ Coin payment failed. Error: ${e.message}` });
        }
      }
    }

  } catch (err) {
    console.error(err);
    // Use editReply if defered or replied, otherwise use reply
    if (i.replied || i.deferred) {
      i.editReply({ content: '❌ An internal error occurred.' }).catch(() => {});
    } else {
      i.reply({ content: '❌ An internal error occurred.', ephemeral: true }).catch(() => {});
    }
  }
});

/* =========================
   MESSAGE CREATE
========================= */
client.on('messageCreate', async msg => {
  if (!msg.guild || msg.author.bot) return;

  const guildId = msg.guild.id;
  const userId = msg.author.id;
  const now = Date.now();
  const member = msg.member;

  await ensureGuild(guildId);
  const cfg = await get(`SELECT * FROM guilds WHERE guild_id=?`, [guildId]);
  
  await run(
    `INSERT OR IGNORE INTO users (guild_id,user_id,join_timestamp)
     VALUES (?,?,?)`,
    [guildId, userId, now]
  );
  
  const attachmentUrls = msg.attachments.size > 0 
      ? msg.attachments.map(a => a.url).join(',')
      : null;

  await run(
    `INSERT INTO messages (guild_id,channel_id,user_id,content,attachments_urls,created_at)
     VALUES (?,?,?,?,?,?)`,
    [guildId, msg.channel.id, userId, msg.content || '', attachmentUrls, now]
  );

  /* ===== REPUTATION (MESSAGE) ===== */
  const repRow = await get(
    `SELECT last_rep_message,reputation FROM users WHERE guild_id=? AND user_id=?`,
    [guildId, userId]
  );
  
  // +2 rep only once every 5 minutes [cite: 11]
  const fiveMinMs = 5 * 60 * 1000;
  if (repRow && now - (repRow.last_rep_message || 0) >= fiveMinMs) {
      await run(
          `UPDATE users
           SET reputation=?, last_rep_message=?
           WHERE guild_id=? AND user_id=?`,
          [clampRep(repRow.reputation + 2), now, guildId, userId]
      );
  }

  /* ===== DENY WORD ===== */
  const denywords = await all(
    `SELECT word FROM denywords WHERE guild_id=?`,
    [guildId]
  );

  if (denywords.length && msg.content) {
    const lower = msg.content.toLowerCase();
    const hits = denywords.filter(w => lower.includes(w.word));
    
    if (hits.length > 0) {
      await msg.delete().catch(() => {});
      const hitWords = hits.map(h => h.word).join(', ');
      
      // 1. Loss of reputation: -5 per forbidden word [cite: 12]
      await updateReputation(guildId, userId, -5 * hits.length);
      
      // 2. 1 min timeout 
      const oneMinMs = 1 * 60 * 1000;
      await member.timeout(oneMinMs, 'Used forbidden word(s).').catch(() => {});
      
      // 3. Log to channel [cite: 9]
      await sendToLogChannel(msg.guild, new EmbedBuilder()
          .setTitle('🚫 Forbidden Word Used')
          .setColor('Red')
          .setDescription(`${msg.author.tag} used forbidden word(s) and message was deleted.`)
          .addFields(
              { name: 'User', value: `<@${userId}>`, inline: true },
              { name: 'Channel', value: `<#${msg.channel.id}>`, inline: true },
              { name: 'Words Used', value: hitWords }
          )
          .setTimestamp()
      );
      
      // 4. Send DM warning [cite: 10]
      await msg.author.send(`⚠️ **Warning:** Your message in ${msg.channel.name} was deleted for containing forbidden word(s): **${hitWords}**.\nYou have been timed out for 1 minute.`).catch(() => {});
      
      // 5. Save as ADV (with flag) and fine increase [cite: 10]
      await run(
          `INSERT INTO advs (guild_id,user_id,reason,is_denyword,created_at)
           VALUES (?,?,?,?,?)`,
          [guildId, userId, `Used forbidden word(s): ${hitWords}`, 1, now]
      );
      await syncAdvRoles(msg.guild, userId);
      
      // 6. 1% fine increase (only if user has pending fines) [cite: 10]
      const invoiceTotalRow = await get(`SELECT SUM(amount) as s FROM invoices WHERE guild_id=? AND user_id=? AND paid=0`, [guildId, userId]);
      const pendingInvoice = invoiceTotalRow?.s || 0;
      if (pendingInvoice > 0) {
          const fineIncrease = pendingInvoice * 0.01;
          await createInvoice(guildId, userId, fineIncrease, `1% fine increase for using forbidden word: ${hitWords}`);
      }
    }
  }
});

/* =========================
   MESSAGE DELETE (REVOKE LOG)
========================= */
client.on('messageDelete', async msg => {
  if (!msg.guild || !msg.author || msg.author.bot) return;

  const guildId = msg.guild.id;
  const userId = msg.author.id;
  
  // 1. Reputation loss: -1 rep per deleted message [cite: 12]
  await updateReputation(guildId, userId, -1);
  
  // 2. Revoke Log
  const cfg = await get(
    `SELECT revoke_channel FROM guilds WHERE guild_id=?`,
    [guildId]
  );

  if (!cfg?.revoke_channel) return;

  const channel = msg.guild.channels.cache.get(cfg.revoke_channel);
  if (!channel) return;
  
  const content = msg.content || '*No content*';
  let attachment = null;
  let attachmentValue = '*None*';
  
  if (msg.attachments.size > 0) {
    const firstAttachment = msg.attachments.first();
    // Discord handles file size limitations, we log the URL or a placeholder
    attachmentValue = `[Attachment: ${firstAttachment.url}]`; 
  } else {
      // Check database for attachment urls (if message was saved)
      const dbMsg = await get(`SELECT attachments_urls FROM messages WHERE guild_id=? AND channel_id=? AND user_id=? AND content=? ORDER BY created_at DESC LIMIT 1`, 
                                 [guildId, msg.channel.id, userId, msg.content || '']);
      if (dbMsg?.attachments_urls) {
          attachmentValue = `[Attachment URL(s): ${dbMsg.attachments_urls}]`;
      }
  }

  await channel.send({
    embeds: [
      new EmbedBuilder()
        .setTitle('🗑️ Message Deleted (Revoke)')
        .setColor('Greyple')
        .addFields(
          { name: 'User', value: `<@${userId}>`, inline: true },
          { name: 'Channel', value: `<#${msg.channel.id}>`, inline: true },
          { name: 'Content', value: content.substring(0, 1024) },
          { name: 'Attachments', value: attachmentValue }
        )
        .setTimestamp()
    ]
  }).catch(() => {});
});

/* =========================
   GUILD MEMBER REMOVE (REP LOSS)
========================= */
client.on('guildMemberRemove', async member => {
    if (member.user.bot) return;
    
    // -100 reputation on leaving guild 
    await updateReputation(member.guild.id, member.id, -100);
});

/* =========================
   VOICE STATE REPUTATION (UNCHANGED)
========================= */
client.on('voiceStateUpdate', async (oldState, newState) => {
  const member = newState.member;
  if (!member || member.user.bot) return;

  const guildId = member.guild.id;
  const userId = member.id;
  const now = Date.now();

  await ensureGuild(guildId);

  if (!oldState.channelId && newState.channelId) {
    // Joined a voice channel
    await run(
      `UPDATE users SET last_rep_voice=? WHERE guild_id=? AND user_id=?`,
      [now, guildId, userId]
    );
  }

  if (oldState.channelId && !newState.channelId) {
    // Left a voice channel
    const row = await get(
      `SELECT last_rep_voice,reputation FROM users WHERE guild_id=? AND user_id=?`,
      [guildId, userId]
    );

    if (row?.last_rep_voice) {
      // Rep gain is +2 per 5 minutes [cite: 12]
      const mins = Math.floor((now - row.last_rep_voice) / (5 * 60 * 1000));
      if (mins > 0) {
        await run(
          `UPDATE users
           SET reputation=?, last_rep_voice=0
           WHERE guild_id=? AND user_id=?`,
          [clampRep(row.reputation + mins * 2), guildId, userId]
        );
      }
    }
  }
});

/* =========================
   TIMERS
========================= */
setInterval(async () => {
  const now = Date.now();

  // 1. Expire advertences after 7 days [cite: 5]
  // Note: Only non-permanent bans (i.e., less than 3 advs) are affected by this.
  // The system only deletes the adv, the syncAdvRoles will handle role removal.
  await run(
    `DELETE FROM advs WHERE created_at < ?`,
    [now - 7 * 24 * 60 * 60 * 1000]
  );
  
  // 2. Prune messages after 30 days [cite: 8]
  await run(
    `DELETE FROM messages WHERE created_at < ?`,
    [now - 30 * 24 * 60 * 60 * 1000]
  );
  
  // Re-sync all roles (a more robust check after deletion)
  const allAdvUsers = await all(`SELECT DISTINCT guild_id, user_id FROM advs`);
  for (const { guild_id, user_id } of allAdvUsers) {
      const guild = client.guilds.cache.get(guild_id);
      if (guild) await syncAdvRoles(guild, user_id).catch(() => {});
  }
  
}, 5 * 60 * 1000); // Runs every 5 minutes 

/* =========================
   LOGIN
========================= */
client.login(process.env.DISCORD_TOKEN);