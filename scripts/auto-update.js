const fs = require('fs');
const path = require('path');
const { google } = require('googleapis');

const DATA_DIR = path.join(__dirname, '../data');
const PLAYLISTS_DIR = path.join(DATA_DIR, 'playlists');

const API_KEYS = [
    process.env.YOUTUBE_API_KEY,
    process.env.YOUTUBE_API_KEY_2,
    process.env.YOUTUBE_API_KEY_3,
    process.env.YOUTUBE_API_KEY_4,
    process.env.YOUTUBE_API_KEY_5,
].filter(key => key && key.trim());

if (API_KEYS.length === 0) {
    console.error('At least one YOUTUBE_API_KEY is required');
    process.exit(1);
}

console.log(`Loaded ${API_KEYS.length} API key(s)`);

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

function readJson(filePath) {
    return JSON.parse(fs.readFileSync(filePath, 'utf-8'));
}

function writeJson(filePath, data) {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf-8');
}

function parseDuration(duration) {
    if (!duration) return '00:00';
    const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
    if (!match) return '00:00';
    const hours = parseInt(match[1] || '0');
    const minutes = parseInt(match[2] || '0');
    const seconds = parseInt(match[3] || '0');
    if (hours > 0) {
        return `${hours}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
    }
    return `${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
}

async function getBatchedCounts(playlistIds) {
    const results = new Map();
    const BATCH_SIZE = 50;

    for (let i = 0; i < playlistIds.length; i += BATCH_SIZE) {
        const batch = playlistIds.slice(i, i + BATCH_SIZE);
        let retries = API_KEYS.length;

        while (retries > 0) {
            try {
                const youtube = getYoutubeClient();
                const response = await youtube.playlists.list({
                    part: ['contentDetails'],
                    id: batch,
                    maxResults: 50,
                });
                for (const item of response.data.items || []) {
                    results.set(item.id, item.contentDetails?.itemCount || 0);
                }
                break;
            } catch (error) {
                if (isQuotaError(error) && rotateKey()) {
                    retries--;
                    continue;
                }
                console.error(`Batch fetch failed: ${error.message}`);
                break;
            }
        }
    }
    return results;
}

async function fetchPlaylistVideos(playlistId) {
    const videos = [];
    let pageToken;

    do {
        let retries = API_KEYS.length;
        let response;

        while (retries > 0) {
            try {
                const youtube = getYoutubeClient();
                response = await youtube.playlistItems.list({
                    part: ['snippet', 'contentDetails'],
                    playlistId,
                    maxResults: 50,
                    pageToken,
                });
                break;
            } catch (error) {
                if (isQuotaError(error) && rotateKey()) {
                    retries--;
                    continue;
                }
                throw error;
            }
        }

        if (!response) break;

        const items = response.data.items || [];
        const videoIds = items.map(item => item.contentDetails?.videoId).filter(Boolean);

        let durationsMap = new Map();
        if (videoIds.length > 0) {
            let durationRetries = API_KEYS.length;
            while (durationRetries > 0) {
                try {
                    const youtube = getYoutubeClient();
                    const videosResponse = await youtube.videos.list({
                        part: ['contentDetails'],
                        id: videoIds,
                    });
                    for (const video of videosResponse.data.items || []) {
                        durationsMap.set(video.id, parseDuration(video.contentDetails?.duration));
                    }
                    break;
                } catch (error) {
                    if (isQuotaError(error) && rotateKey()) {
                        durationRetries--;
                        continue;
                    }
                    break;
                }
            }
        }

        for (const item of items) {
            const videoId = item.contentDetails?.videoId;
            if (!videoId) continue;

            const title = item.snippet?.title || '';
            const thumbnail = item.snippet?.thumbnails?.high?.url || '';

            if (title === 'Private video' || title === 'Deleted video' ||
                title === 'فيديو خاص' || title === 'فيديو محذوف') {
                continue;
            }

            videos.push({
                id: videoId,
                title,
                description: item.snippet?.description || '',
                date: item.snippet?.publishedAt || '',
                thumbnail,
                duration: durationsMap.get(videoId) || '00:00',
                url: `https://www.youtube.com/watch?v=${videoId}`,
            });
        }

        pageToken = response.data.nextPageToken;
    } while (pageToken);

    return videos;
}

async function main() {
    console.log('Starting playlist update scan...\n');

    if (!fs.existsSync(PLAYLISTS_DIR)) {
        console.log('No playlists directory found');
        return;
    }

    const files = fs.readdirSync(PLAYLISTS_DIR);
    const playlistFiles = files.filter(f => f.startsWith('pl_') && f.endsWith('.json'));

    if (playlistFiles.length === 0) {
        console.log('No playlists found');
        return;
    }

    console.log(`Found ${playlistFiles.length} playlists\n`);

    const localPlaylists = [];
    for (const file of playlistFiles) {
        try {
            const data = readJson(path.join(PLAYLISTS_DIR, file));
            localPlaylists.push({ file, data });
        } catch (error) {
            console.error(`Error reading ${file}: ${error.message}`);
        }
    }

    const playlistIds = localPlaylists.map(p => p.data.id);
    const remoteCounts = await getBatchedCounts(playlistIds);

    const needsUpdate = [];
    for (const { file, data } of localPlaylists) {
        const remoteCount = remoteCounts.get(data.id);
        if (remoteCount !== undefined && remoteCount !== data.videoCount) {
            needsUpdate.push({ file, data, localCount: data.videoCount, remoteCount });
            console.log(`${data.title}: ${data.videoCount} -> ${remoteCount}`);
        }
    }

    if (needsUpdate.length === 0) {
        console.log('\nAll playlists are up to date!');
        return;
    }

    console.log(`\nUpdating ${needsUpdate.length} playlists...\n`);

    let successCount = 0;
    for (const { file, data } of needsUpdate) {
        try {
            console.log(`Updating: ${data.title}...`);
            const videos = await fetchPlaylistVideos(data.id);

            data.videos = videos;
            data.videoCount = videos.length;

            writeJson(path.join(PLAYLISTS_DIR, file), data);
            console.log(`Updated: ${data.title} (${videos.length} videos)`);
            successCount++;
        } catch (error) {
            console.error(`Failed to update ${data.title}: ${error.message}`);
        }
    }

    console.log(`\nUpdate complete: ${successCount}/${needsUpdate.length} playlists updated`);
}

main().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});
