#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);


const DATA_DIR = path.join(__dirname, '..', 'server', 'data');
const LOGS_DIR = path.join(__dirname, 'logs', 'playlists');
const PATHS = {
    PLAYLISTS: path.join(DATA_DIR, 'playlists'),
    AUTO_UPDATE_LIST: path.join(DATA_DIR, 'auto-update.json'),
    INDICES: path.join(DATA_DIR, 'indices'),
    CATEGORIES_MAIN: path.join(DATA_DIR, 'indices', 'categories', 'main'),
    CATEGORIES_SUB: path.join(DATA_DIR, 'indices', 'categories', 'sub'),
    CHANNELS: path.join(DATA_DIR, 'indices', 'channels'),
    CHANNELS_FILE: path.join(DATA_DIR, 'channels.json'),
    CATEGORIES_FILE: path.join(DATA_DIR, 'categories.json'),
    PLAYLISTS_INDEX: path.join(DATA_DIR, 'playlists_index.json'),
};

const API_BASE = 'https://www.googleapis.com/youtube/v3';
const BATCH_SIZE = 50;


class ApiKeyManager {
    constructor() {
        const keysEnv = process.env.YOUTUBE_API_KEYS || '';
        this.keys = keysEnv.split(',').map(k => k.trim()).filter(k => k.length > 0);

        if (this.keys.length === 0) {
            throw new Error('❌ YOUTUBE_API_KEYS environment variable is not set or empty!');
        }

        this.currentIndex = 0;
        this.exhaustedKeys = new Set();
        console.log(`🔑 Loaded ${this.keys.length} API key(s)`);
    }

    get currentKey() {
        return this.keys[this.currentIndex];
    }

    get allExhausted() {
        return this.exhaustedKeys.size >= this.keys.length;
    }

    markExhausted() {
        console.log(`⚠️  API Key #${this.currentIndex + 1} quota exhausted`);
        this.exhaustedKeys.add(this.currentIndex);
        return this.rotateToNext();
    }

    rotateToNext() {
        for (let i = 0; i < this.keys.length; i++) {
            const nextIndex = (this.currentIndex + 1 + i) % this.keys.length;
            if (!this.exhaustedKeys.has(nextIndex)) {
                this.currentIndex = nextIndex;
                console.log(`🔄 Switched to API Key #${this.currentIndex + 1}`);
                return true;
            }
        }
        console.log('❌ All API keys exhausted!');
        return false;
    }
}


async function youtubeRequest(keyManager, endpoint, params) {
    const maxRetries = keyManager.keys.length;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
        if (keyManager.allExhausted) {
            throw new Error('ALL_KEYS_EXHAUSTED');
        }

        const searchParams = new URLSearchParams({
            ...params,
            key: keyManager.currentKey,
        });

        const url = `${API_BASE}/${endpoint}?${searchParams.toString()}`;

        try {
            const response = await fetch(url);

            if (response.ok) {
                const result = await response.json();
                await new Promise(r => setTimeout(r, 50));
                return result;
            }

            if (response.status === 403) {
                const errorData = await response.json().catch(() => ({}));
                const reason = errorData?.error?.errors?.[0]?.reason || '';

                if (reason === 'quotaExceeded' || reason === 'dailyLimitExceeded') {
                    const hasMore = keyManager.markExhausted();
                    if (!hasMore) throw new Error('ALL_KEYS_EXHAUSTED');
                    continue;
                }
            }

            const errorText = await response.text().catch(() => response.statusText);
            throw new Error(`API Error ${response.status}: ${errorText}`);

        } catch (error) {
            if (error.message === 'ALL_KEYS_EXHAUSTED') throw error;
            if (attempt === maxRetries - 1) throw error;
        }
    }
}


function readPlaylistsIndex() {
    if (fs.existsSync(PATHS.AUTO_UPDATE_LIST)) {
        try {
            const raw = JSON.parse(fs.readFileSync(PATHS.AUTO_UPDATE_LIST, 'utf-8'));
            return Object.entries(raw).map(([id, data]) => ({
                id,
                title: data.title,
                videoCount: data.videoCount,
                channelId: data.channelId,
            }));
        } catch (e) {
            console.log(`⚠️  Failed to read auto-update list: ${e.message}`);
        }
    }
    return [];
}

function updateAutoUpdateList(playlistId, title, videoCount, channelId) {
    let list = {};
    if (fs.existsSync(PATHS.AUTO_UPDATE_LIST)) {
        try {
            list = JSON.parse(fs.readFileSync(PATHS.AUTO_UPDATE_LIST, 'utf-8'));
        } catch (e) { }
    }
    // Only update existing entries
    if (list[playlistId]) {
        list[playlistId] = { title, videoCount, channelId };
        fs.writeFileSync(PATHS.AUTO_UPDATE_LIST, JSON.stringify(list, null, 2));
    }
}

async function getBatchedRemoteCounts(keyManager, playlistIds) {
    const results = new Map();

    for (let i = 0; i < playlistIds.length; i += BATCH_SIZE) {
        const batch = playlistIds.slice(i, i + BATCH_SIZE);
        const batchNum = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(playlistIds.length / BATCH_SIZE);

        console.log(`   📡 Scanning batch ${batchNum}/${totalBatches} (${batch.length} playlists)...`);

        try {
            const data = await youtubeRequest(keyManager, 'playlists', {
                part: 'contentDetails',
                id: batch.join(','),
                maxResults: '50',
                fields: 'items(id,contentDetails/itemCount)',
            });

            for (const item of data.items || []) {
                if (item.id) {
                    results.set(item.id, item.contentDetails?.itemCount || 0);
                }
            }
        } catch (error) {
            if (error.message === 'ALL_KEYS_EXHAUSTED') throw error;
            console.log(`   ⚠️  Batch scan failed: ${error.message}`);
        }
    }

    return results;
}

async function fetchPlaylistDetails(keyManager, playlistId) {
    const data = await youtubeRequest(keyManager, 'playlists', {
        part: 'snippet,contentDetails',
        id: playlistId,
        fields: 'items(id,snippet(title,description,thumbnails/high/url,channelId),contentDetails/itemCount)',
    });

    if (!data.items || data.items.length === 0) return null;

    const item = data.items[0];
    const snippet = item.snippet;

    return {
        id: item.id,
        title: snippet.title || '',
        description: snippet.description || '',
        thumbnail: snippet.thumbnails?.high?.url || '',
        videoCount: item.contentDetails?.itemCount || 0,
        channelId: snippet.channelId || '',
    };
}

async function fetchPlaylistVideos(keyManager, playlistId) {
    const videos = [];
    let pageToken = '';

    do {
        const params = {
            part: 'snippet,contentDetails',
            playlistId,
            maxResults: '50',
            fields: 'nextPageToken,items(snippet(title,description,publishedAt,thumbnails/high/url,resourceId/videoId),contentDetails/videoId)',
        };
        if (pageToken) params.pageToken = pageToken;

        const data = await youtubeRequest(keyManager, 'playlistItems', params);
        const items = data.items || [];

        for (const item of items) {
            const videoId = item.contentDetails?.videoId || item.snippet?.resourceId?.videoId;
            if (!videoId) continue;

            const snippet = item.snippet;
            const title = snippet.title || '';
            const thumbnail = snippet.thumbnails?.high?.url || '';

            if (
                title === 'Private video' ||
                title === 'Deleted video' ||
                title === 'فيديو خاص' ||
                title === 'فيديو محذوف' ||
                (!thumbnail && title.toLowerCase().includes('private'))
            ) {
                continue;
            }

            videos.push({
                id: videoId,
                title,
                description: snippet.description || '',
                date: snippet.publishedAt || '',
                thumbnail,
                url: `https://www.youtube.com/watch?v=${videoId}`,
            });
        }

        pageToken = data.nextPageToken || '';
    } while (pageToken);

    return videos;
}

function findCategoryFile(catName) {
    const mainPath = path.join(PATHS.CATEGORIES_MAIN, `${catName}.json`);
    const subPath = path.join(PATHS.CATEGORIES_SUB, `${catName}.json`);
    if (fs.existsSync(mainPath)) return mainPath;
    if (fs.existsSync(subPath)) return subPath;
    return null;
}

function updateIndexFiles(playlist) {
    // Get channelTitle from channels.json if possible
    let channelTitle = 'Unknown';
    if (fs.existsSync(PATHS.CHANNELS_FILE)) {
        try {
            const channels = JSON.parse(fs.readFileSync(PATHS.CHANNELS_FILE, 'utf-8'));
            const channel = channels.find(c => c.id === playlist.channelId);
            if (channel) channelTitle = channel.title;
        } catch (e) { }
    }

    const indexEntry = {
        id: playlist.id,
        title: playlist.title,
        thumbnail: playlist.thumbnail,
        videoCount: playlist.videoCount,
        channelId: playlist.channelId,
        channelTitle: channelTitle,
        categories: playlist.categories || [],
        path: `data/playlists/pl_${playlist.id}.json`,
    };


    for (const cat of (playlist.categories || [])) {
        const categoryFile = findCategoryFile(cat);
        if (!categoryFile) continue;

        try {
            let catIndex = JSON.parse(fs.readFileSync(categoryFile, 'utf-8'));
            const idx = catIndex.findIndex(p => p.id === playlist.id);
            if (idx > -1) {
                catIndex[idx] = indexEntry;
            } else {
                catIndex.push(indexEntry);
            }
            fs.writeFileSync(categoryFile, JSON.stringify(catIndex, null, 2));
        } catch (e) {
            console.log(`   ⚠️  Failed to update category index ${cat}: ${e.message}`);
        }
    }


    const channelFile = path.join(PATHS.CHANNELS, `ch_${playlist.channelId}.json`);
    if (fs.existsSync(channelFile)) {
        try {
            let chData = JSON.parse(fs.readFileSync(channelFile, 'utf-8'));
            if (chData.playlists) {
                const idx = chData.playlists.findIndex(p => p.id === playlist.id);
                if (idx > -1) {
                    chData.playlists[idx] = indexEntry;
                } else {
                    chData.playlists.push(indexEntry);
                }
                fs.writeFileSync(channelFile, JSON.stringify(chData, null, 2));
            }
        } catch (e) {
            console.log(`   ⚠️  Failed to update channel index: ${e.message}`);
        }
    }

    // 3. Update unified playlists index
    if (fs.existsSync(PATHS.PLAYLISTS_INDEX)) {
        try {
            let index = JSON.parse(fs.readFileSync(PATHS.PLAYLISTS_INDEX, 'utf-8'));
            const idx = index.findIndex(p => p.id === playlist.id);
            if (idx > -1) {
                index[idx] = indexEntry;
            } else {
                index.push(indexEntry);
            }
            fs.writeFileSync(PATHS.PLAYLISTS_INDEX, JSON.stringify(index, null, 2));
        } catch (e) {
            console.log(`   ⚠️  Failed to update unified index: ${e.message}`);
        }
    }
}


async function main() {
    console.log('=============================================');
    console.log('  YouTube Playlist Auto-Updater');
    console.log('  ' + new Date().toISOString());
    console.log('=============================================\n');


    const keyManager = new ApiKeyManager();


    const localPlaylists = readPlaylistsIndex();
    if (localPlaylists.length === 0) {
        console.log('📭 No playlists found. Nothing to do.');
        return;
    }
    console.log(`📚 Found ${localPlaylists.length} local playlists\n`);


    console.log('🔍 Scanning for updates...');
    const playlistIds = localPlaylists.map(p => p.id);

    let remoteCounts;
    try {
        remoteCounts = await getBatchedRemoteCounts(keyManager, playlistIds);
    } catch (error) {
        if (error.message === 'ALL_KEYS_EXHAUSTED') {
            console.log('\n❌ All API keys exhausted during scan phase. Exiting.');
            process.exit(1);
        }
        throw error;
    }


    const needsUpdate = [];
    for (const playlist of localPlaylists) {
        const remoteCount = remoteCounts.get(playlist.id);
        if (remoteCount !== undefined && remoteCount !== playlist.videoCount) {
            needsUpdate.push({
                id: playlist.id,
                title: playlist.title,
                localCount: playlist.videoCount,
                remoteCount,
                channelId: playlist.channelId,
            });
        }
    }

    if (needsUpdate.length === 0) {
        console.log('\n✅ All playlists are up to date! No changes needed.');
        if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });
        const today = new Date().toISOString().split('T')[0];
        const logPath = path.join(LOGS_DIR, `${today}.md`);
        let content = '';
        if (fs.existsSync(logPath)) content = fs.readFileSync(logPath, 'utf-8') + '\n';
        content += `## ${new Date().toISOString().split('T')[1].split('.')[0]}\n\n`;
        content += `- Total: ${localPlaylists.length}\n`;
        content += `- Needs update: 0\n- Updated: 0\n- Failed: 0\n\nNo changes.\n`;
        fs.writeFileSync(logPath, content);
        console.log(`📝 Log saved: logs/playlists/${today}.md`);
        return;
    }

    console.log(`\n📋 Found ${needsUpdate.length} playlist(s) needing updates:\n`);
    for (const p of needsUpdate) {
        const diff = p.remoteCount - p.localCount;
        const sign = diff > 0 ? '+' : '';
        console.log(`   • ${p.title} (${p.localCount} → ${p.remoteCount}, ${sign}${diff})`);
    }
    console.log('');


    let successCount = 0;
    let failCount = 0;
    const logEntries = [];

    for (let i = 0; i < needsUpdate.length; i++) {
        const pl = needsUpdate[i];
        console.log(`⬇️  [${i + 1}/${needsUpdate.length}] Updating "${pl.title}"...`);

        try {
            const existingData = JSON.parse(fs.readFileSync(path.join(PATHS.PLAYLISTS, `pl_${pl.id}.json`), 'utf-8'));

            const details = await fetchPlaylistDetails(keyManager, pl.id);
            if (!details) {
                console.log(`   ❌ Failed to fetch playlist details`);
                failCount++;
                continue;
            }


            const videos = await fetchPlaylistVideos(keyManager, pl.id);
            console.log(`   📹 Fetched ${videos.length} videos`);


            const updatedPlaylist = {
                id: pl.id,
                title: details.title,
                description: details.description,
                thumbnail: details.thumbnail,
                videoCount: videos.length,
                channelId: details.channelId || existingData.channelId,
                categories: existingData.categories || [],
                videos,
            };


            const playlistPath = path.join(PATHS.PLAYLISTS, `pl_${pl.id}.json`);
            fs.writeFileSync(playlistPath, JSON.stringify(updatedPlaylist, null, 2));


            updateIndexFiles(updatedPlaylist);
            updateAutoUpdateList(pl.id, updatedPlaylist.title, videos.length, updatedPlaylist.channelId);

            console.log(`   ✅ Updated successfully (${pl.localCount} → ${videos.length} videos)`);
            successCount++;
            logEntries.push({ id: pl.id, title: pl.title, before: pl.localCount, after: videos.length });

        } catch (error) {
            if (error.message === 'ALL_KEYS_EXHAUSTED') {
                console.log(`\n❌ All API keys exhausted. Stopping updates.`);
                console.log(`   Completed: ${successCount}, Remaining: ${needsUpdate.length - i}`);
                break;
            }
            console.log(`   ❌ Error: ${error.message}`);
            failCount++;
        }
    }


    console.log('\n=============================================');
    console.log('  Summary');
    console.log('=============================================');
    console.log(`  ✅ Updated: ${successCount}`);
    console.log(`  ❌ Failed:  ${failCount}`);
    console.log(`  📊 Total:   ${needsUpdate.length}`);
    console.log('=============================================\n');

    if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });
    const today = new Date().toISOString().split('T')[0];
    const logPath = path.join(LOGS_DIR, `${today}.md`);
    let content = '';
    if (fs.existsSync(logPath)) content = fs.readFileSync(logPath, 'utf-8') + '\n';
    content += `## ${new Date().toISOString().split('T')[1].split('.')[0]}\n\n`;
    content += `- Total: ${localPlaylists.length}\n`;
    content += `- Needs update: ${needsUpdate.length}\n`;
    content += `- Updated: ${successCount}\n`;
    content += `- Failed: ${failCount}\n\n`;
    if (logEntries.length > 0) {
        content += `| ID | Name | Before | After |\n|---|---|---|---|\n`;
        for (const e of logEntries) {
            content += `| ${e.id} | ${e.title} | ${e.before} | ${e.after} |\n`;
        }
    } else {
        content += `No changes.\n`;
    }
    fs.writeFileSync(logPath, content);
    console.log(`📝 Log saved: logs/playlists/${today}.md`);

    if (failCount > 0 && successCount === 0) {
        process.exit(1);
    }
}

main().catch(error => {
    console.error('💥 Fatal error:', error.message);
    process.exit(1);
});
