import fs from 'fs-extra';
import path from 'path';
import { google } from 'googleapis';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.join(__dirname, '../server/data');
const CHANNELS_FILE = path.join(DATA_DIR, 'channels.json');

const API_KEYS = [
    process.env.YOUTUBE_API_KEY,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
    process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5,
].filter(key => key && key.trim());

if (API_KEYS.length === 0) {
    console.error('At least one YOUTUBE_API_KEY is required');
    console.error('Set it with: export YOUTUBE_API_KEY=your_key');
    process.exit(1);
}

let currentKeyIndex = 0;

function getYoutubeClient() {
    return google.youtube({ version: 'v3', auth: API_KEYS[currentKeyIndex] });
}

function rotateKey() {
    if (API_KEYS.length <= 1) return false;
    currentKeyIndex = (currentKeyIndex + 1) % API_KEYS.length;
    console.log(`Switched to API key ${currentKeyIndex + 1}/${API_KEYS.length}`);
    return true;
}

function isQuotaError(error) {
    if (error?.code === 403) {
        const reason = error?.errors?.[0]?.reason;
        return reason === 'quotaExceeded' || reason === 'dailyLimitExceeded';
    }
    return false;
}

async function fetchChannelDetails(channelIds) {
    const results = new Map();
    const BATCH_SIZE = 50;

    for (let i = 0; i < channelIds.length; i += BATCH_SIZE) {
        const batch = channelIds.slice(i, i + BATCH_SIZE);
        let retries = API_KEYS.length;

        while (retries > 0) {
            try {
                const youtube = getYoutubeClient();
                const response = await youtube.channels.list({
                    part: ['snippet', 'statistics'],
                    id: batch,
                });

                for (const item of response.data.items || []) {
                    results.set(item.id, {
                        id: item.id,
                        title: item.snippet?.title || '',
                        thumbnail: item.snippet?.thumbnails?.high?.url || '',
                    });
                }
                break;
            } catch (error) {
                if (isQuotaError(error) && rotateKey()) {
                    retries--;
                    continue;
                }
                console.error(`Failed to fetch channels: ${error.message}`);
                break;
            }
        }
    }

    return results;
}

async function main() {
    const args = process.argv.slice(2);
    const specificChannel = args.find(arg => arg.startsWith('--channel='))?.split('=')[1];

    console.log('Channel Update Script\n');

    if (!await fs.pathExists(CHANNELS_FILE)) {
        console.log('No channels.json file found');
        return;
    }

    let channels = await fs.readJson(CHANNELS_FILE);

    if (specificChannel) {
        channels = channels.filter(c => c.id === specificChannel);
        if (channels.length === 0) {
            console.log(`Channel ${specificChannel} not found`);
            return;
        }
    }

    console.log(`Found ${channels.length} channels to update\n`);

    const channelIds = channels.map(c => c.id);
    const updatedData = await fetchChannelDetails(channelIds);

    let updatedCount = 0;
    const allChannels = await fs.readJson(CHANNELS_FILE);

    for (let i = 0; i < allChannels.length; i++) {
        const channel = allChannels[i];
        const updated = updatedData.get(channel.id);

        if (updated) {
            const changed = channel.title !== updated.title ||
                channel.thumbnail !== updated.thumbnail;

            if (changed) {
                allChannels[i] = {
                    ...channel,
                    title: updated.title,
                    thumbnail: updated.thumbnail,
                };
                console.log(`Updated: ${updated.title}`);
                updatedCount++;
            } else {
                console.log(`No changes: ${channel.title}`);
            }
        }
    }

    await fs.writeJson(CHANNELS_FILE, allChannels, { spaces: 2 });

    console.log(`\nDone: ${updatedCount} channels updated`);
}

main().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});
