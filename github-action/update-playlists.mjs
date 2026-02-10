#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);


const DATA_DIR = path.join(__dirname, '..', 'server', 'data');
const PATHS = {
    PLAYLISTS: path.join(DATA_DIR, 'playlists'),
    INDICES: path.join(DATA_DIR, 'indices'),
    CATEGORIES_MAIN: path.join(DATA_DIR, 'indices', 'categories', 'main'),
    CATEGORIES_SUB: path.join(DATA_DIR, 'indices', 'categories', 'sub'),
    CHANNELS: path.join(DATA_DIR, 'indices', 'channels'),
    CHANNELS_FILE: path.join(DATA_DIR, 'channels.json'),
    CATEGORIES_FILE: path.join(DATA_DIR, 'categories.json'),
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
                return await response.json();
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


function readLocalPlaylists() {
    if (!fs.existsSync(PATHS.PLAYLISTS)) return [];

    const files = fs.readdirSync(PATHS.PLAYLISTS)
        .filter(f => f.startsWith('pl_') && f.endsWith('.json'));

    const playlists = [];
    for (const file of files) {
        try {
            const data = JSON.parse(fs.readFileSync(path.join(PATHS.PLAYLISTS, file), 'utf-8'));
            playlists.push(data);
        } catch (e) {
            console.log(`⚠️  Failed to read ${file}: ${e.message}`);
        }
    }
    return playlists;
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
        fields: 'items(id,snippet(title,description,thumbnails/high/url,channelId,channelTitle),contentDetails/itemCount)',
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
        channelTitle: snippet.channelTitle || '',
    };
}

function parseDuration(duration) {
    const match = duration.match(/PT(\d+H)?(\d+M)?(\d+S)?/);
    if (!match) return '00:00';

    const hours = (match[1] || '').replace('H', '');
    const minutes = (match[2] || '').replace('M', '');
    const seconds = (match[3] || '').replace('S', '');

    let result = '';
    if (hours) {
        result += `${hours}:`;
        result += `${minutes.padStart(2, '0')}:`;
    } else {
        result += `${(minutes || '0').padStart(2, '0')}:`;
    }
    result += (seconds || '0').padStart(2, '0');
    return result;
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


        const videoIds = items
            .map(item => item.contentDetails?.videoId || item.snippet?.resourceId?.videoId)
            .filter(Boolean);


        let durationsMap = new Map();
        if (videoIds.length > 0) {
            const videosData = await youtubeRequest(keyManager, 'videos', {
                part: 'contentDetails',
                id: videoIds.join(','),
                fields: 'items(id,contentDetails/duration)',
            });

            for (const video of videosData.items || []) {
                durationsMap.set(video.id, parseDuration(video.contentDetails?.duration || ''));
            }
        }


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
                duration: durationsMap.get(videoId) || '00:00',
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
    const indexEntry = {
        id: playlist.id,
        title: playlist.title,
        thumbnail: playlist.thumbnail,
        channelId: playlist.channelId,
        videoCount: playlist.videoCount,
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
}


async function main() {
    console.log('=============================================');
    console.log('  YouTube Playlist Auto-Updater');
    console.log('  ' + new Date().toISOString());
    console.log('=============================================\n');


    const keyManager = new ApiKeyManager();


    const localPlaylists = readLocalPlaylists();
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
                categories: playlist.categories || [],
                channelId: playlist.channelId,
                channelTitle: playlist.channelTitle || '',
                channelThumbnail: playlist.channelThumbnail || '',
            });
        }
    }

    if (needsUpdate.length === 0) {
        console.log('\n✅ All playlists are up to date! No changes needed.');
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

    for (let i = 0; i < needsUpdate.length; i++) {
        const pl = needsUpdate[i];
        console.log(`⬇️  [${i + 1}/${needsUpdate.length}] Updating "${pl.title}"...`);

        try {

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
                channelId: details.channelId || pl.channelId,
                channelTitle: details.channelTitle || pl.channelTitle,
                channelThumbnail: pl.channelThumbnail,
                categories: pl.categories,
                videos,
            };


            const playlistPath = path.join(PATHS.PLAYLISTS, `pl_${pl.id}.json`);
            fs.writeFileSync(playlistPath, JSON.stringify(updatedPlaylist, null, 2));


            updateIndexFiles(updatedPlaylist);

            console.log(`   ✅ Updated successfully (${pl.localCount} → ${videos.length} videos)`);
            successCount++;

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

    if (failCount > 0 && successCount === 0) {
        process.exit(1);
    }
}

main().catch(error => {
    console.error('💥 Fatal error:', error.message);
    process.exit(1);
});
