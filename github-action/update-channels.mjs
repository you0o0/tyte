#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.join(__dirname, '..', 'data');
const CHANNELS_FILE = path.join(DATA_DIR, 'channels.json');
const LOGS_DIR = path.join(__dirname, 'logs', 'channels');
const API_BASE = 'https://www.googleapis.com/youtube/v3';
const BATCH_SIZE = 50;

function getApiKey() {
    const keysEnv = process.env.YOUTUBE_API_KEYS || '';
    const keys = keysEnv.split(',').map(k => k.trim()).filter(k => k.length > 0);
    if (keys.length === 0) {
        console.error('❌ YOUTUBE_API_KEYS not set');
        process.exit(1);
    }
    return keys[0];
}

async function main() {
    console.log('=============================================');
    console.log('  YouTube Channels Info Updater');
    console.log('  ' + new Date().toISOString());
    console.log('=============================================\n');

    if (!fs.existsSync(CHANNELS_FILE)) {
        console.log('❌ channels.json not found');
        process.exit(1);
    }

    const channels = JSON.parse(fs.readFileSync(CHANNELS_FILE, 'utf-8'));
    if (channels.length === 0) {
        console.log('📭 No channels found.');
        return;
    }

    console.log(`📺 Found ${channels.length} channels\n`);

    const apiKey = getApiKey();
    let updated = 0;
    const logEntries = [];

    for (let i = 0; i < channels.length; i += BATCH_SIZE) {
        const batch = channels.slice(i, i + BATCH_SIZE);
        const ids = batch.map(c => c.id).join(',');

        const params = new URLSearchParams({
            part: 'snippet',
            id: ids,
            fields: 'items(id,snippet(title,thumbnails/high/url))',
            key: apiKey,
        });

        try {
            const response = await fetch(`${API_BASE}/channels?${params.toString()}`);
            if (!response.ok) {
                const err = await response.text().catch(() => response.statusText);
                console.log(`❌ API Error: ${err}`);
                continue;
            }

            const data = await response.json();
            await new Promise(r => setTimeout(r, 50));

            for (const item of data.items || []) {
                const channel = channels.find(c => c.id === item.id);
                if (!channel) continue;

                const newTitle = item.snippet?.title || '';
                const newThumb = item.snippet?.thumbnails?.high?.url || '';

                if (channel.title !== newTitle || channel.thumbnail !== newThumb) {
                    const changes = [];
                    if (channel.title !== newTitle) changes.push('Title changed');
                    if (channel.thumbnail !== newThumb) changes.push('Thumbnail changed');
                    console.log(`   🔄 ${channel.title} → ${newTitle}`);
                    logEntries.push({ id: channel.id, name: newTitle, change: changes.join(', ') });
                    channel.title = newTitle;
                    channel.thumbnail = newThumb;
                    updated++;
                }
            }
        } catch (error) {
            console.log(`❌ Fetch error: ${error.message}`);
        }
    }

    if (updated > 0) {
        fs.writeFileSync(CHANNELS_FILE, JSON.stringify(channels, null, 2));
        console.log(`\n✅ Updated ${updated} channel(s)`);
    } else {
        console.log('\n✅ All channels are up to date');
    }

    if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });
    const today = new Date().toISOString().split('T')[0];
    const logPath = path.join(LOGS_DIR, `${today}.md`);
    let content = '';
    if (fs.existsSync(logPath)) content = fs.readFileSync(logPath, 'utf-8') + '\n';
    content += `## ${new Date().toISOString().split('T')[1].split('.')[0]}\n\n`;
    content += `- Total: ${channels.length}\n`;
    content += `- Updated: ${updated}\n\n`;
    if (logEntries.length > 0) {
        content += `| ID | Name | Change |\n|---|---|---|\n`;
        for (const e of logEntries) {
            content += `| ${e.id} | ${e.name} | ${e.change} |\n`;
        }
    } else {
        content += `No changes.\n`;
    }
    fs.writeFileSync(logPath, content);
    console.log(`📝 Log saved: logs/channels/${today}.md`);
}

main().catch(error => {
    console.error('💥 Fatal error:', error.message);
    process.exit(1);
});
