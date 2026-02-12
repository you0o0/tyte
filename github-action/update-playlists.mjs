#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.join(__dirname, '..', 'data');
const LOGS_DIR = path.join(__dirname, 'logs', 'playlists');
const PATHS = {
  PLAYLISTS: path.join(DATA_DIR, 'playlists'),
  AUTO_UPDATE_LIST: path.join(DATA_DIR, 'auto-update.json'),
  CATEGORIES_MAIN: path.join(DATA_DIR, 'indices', 'categories', 'main'),
  CATEGORIES_SUB: path.join(DATA_DIR, 'indices', 'categories', 'sub'),
  CHANNELS: path.join(DATA_DIR, 'indices', 'channels'),
  CHANNELS_FILE: path.join(DATA_DIR, 'channels.json'),
  PLAYLISTS_INDEX: path.join(DATA_DIR, 'playlists_index.json')
};

const API_BASE = 'https://www.googleapis.com/youtube/v3';
const BATCH_SIZE = 50;

class ApiKeyManager {
  constructor() {
    const keysEnv = process.env.YOUTUBE_API_KEYS || '';
    this.keys = keysEnv.split(',').map(k => k.trim()).filter(Boolean);
    if (this.keys.length === 0) {
      throw new Error('YOUTUBE_API_KEYS is not set');
    }

    this.currentIndex = 0;
    this.exhaustedKeys = new Set();
    console.log(`Loaded ${this.keys.length} API key(s)`);
  }

  get currentKey() {
    return this.keys[this.currentIndex];
  }

  get allExhausted() {
    return this.exhaustedKeys.size >= this.keys.length;
  }

  markExhausted() {
    this.exhaustedKeys.add(this.currentIndex);
    return this.rotateToNext();
  }

  rotateToNext() {
    for (let i = 0; i < this.keys.length; i++) {
      const nextIndex = (this.currentIndex + 1 + i) % this.keys.length;
      if (!this.exhaustedKeys.has(nextIndex)) {
        this.currentIndex = nextIndex;
        console.log(`Switched to API Key #${this.currentIndex + 1}`);
        return true;
      }
    }
    return false;
  }
}

async function youtubeRequest(keyManager, endpoint, params) {
  const maxRetries = keyManager.keys.length;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    if (keyManager.allExhausted) throw new Error('ALL_KEYS_EXHAUSTED');

    const searchParams = new URLSearchParams({ ...params, key: keyManager.currentKey });
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

  throw new Error('Unexpected API request failure');
}

function normalizeTitle(value) {
  return String(value || '').trim().toLowerCase();
}

function buildChannelLookup() {
  const map = new Map();
  if (!fs.existsSync(PATHS.CHANNELS_FILE)) return map;

  try {
    const channels = JSON.parse(fs.readFileSync(PATHS.CHANNELS_FILE, 'utf-8'));
    for (const channel of channels) {
      if (channel.youtubeId && channel.id) {
        map.set(String(channel.youtubeId), String(channel.id));
      }
    }
  } catch (e) {
    console.log(`Failed to build channel lookup: ${e.message}`);
  }

  return map;
}

function buildLocalPlaylistCache() {
  const out = [];
  if (!fs.existsSync(PATHS.PLAYLISTS)) return out;

  const files = fs.readdirSync(PATHS.PLAYLISTS).filter(f => f.endsWith('.json'));
  for (const file of files) {
    const filePath = path.join(PATHS.PLAYLISTS, file);
    try {
      const json = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
      out.push({
        id: String(json.id || ''),
        title: String(json.title || ''),
        channelId: String(json.channelId || ''),
        videoCount: Number(json.videoCount || 0),
        path: filePath
      });
    } catch (e) {
      console.log(`Failed to parse ${file}: ${e.message}`);
    }
  }

  return out;
}

function findLocalPlaylistMatch(cache, title, localChannelId, videoCount) {
  const normalizedTitle = normalizeTitle(title);
  const candidates = cache.filter(
    p => p.channelId === String(localChannelId) && normalizeTitle(p.title) === normalizedTitle
  );

  if (candidates.length === 0) return null;
  if (candidates.length === 1) return candidates[0];

  return candidates
    .slice()
    .sort((a, b) => Math.abs(a.videoCount - Number(videoCount || 0)) - Math.abs(b.videoCount - Number(videoCount || 0)))[0];
}

function readPlaylistsIndex() {
  if (!fs.existsSync(PATHS.AUTO_UPDATE_LIST)) return [];

  try {
    const raw = JSON.parse(fs.readFileSync(PATHS.AUTO_UPDATE_LIST, 'utf-8'));
    const channelLookup = buildChannelLookup();
    const playlistCache = buildLocalPlaylistCache();
    const resolved = [];

    for (const [youtubePlaylistId, data] of Object.entries(raw)) {
      const explicitLocalId = String(data.localId || '');
      const explicitLocalPath = String(data.localPath || '');
      if (explicitLocalId && explicitLocalPath && fs.existsSync(explicitLocalPath)) {
        const localChannelId = channelLookup.get(String(data.channelId || '')) || '';
        resolved.push({
          id: youtubePlaylistId,
          localId: explicitLocalId,
          localPath: explicitLocalPath,
          title: String(data.title || ''),
          videoCount: Number(data.videoCount || 0),
          channelId: String(data.channelId || ''),
          localChannelId
        });
        continue;
      }

      const localChannelId = channelLookup.get(String(data.channelId || ''));
      if (!localChannelId) {
        console.log(`Skipping ${youtubePlaylistId}: unknown channel mapping`);
        continue;
      }

      const local = findLocalPlaylistMatch(playlistCache, data.title, localChannelId, data.videoCount);
      if (!local) {
        console.log(`Skipping ${youtubePlaylistId}: local playlist not found`);
        continue;
      }

      resolved.push({
        id: youtubePlaylistId,
        localId: local.id,
        localPath: local.path,
        title: String(data.title || ''),
        videoCount: Number(data.videoCount || 0),
        channelId: String(data.channelId || ''),
        localChannelId
      });
    }

    return resolved;
  } catch (e) {
    console.log(`Failed to read auto-update list: ${e.message}`);
    return [];
  }
}

function updateAutoUpdateList(playlistId, title, videoCount, channelId) {
  let list = {};
  if (fs.existsSync(PATHS.AUTO_UPDATE_LIST)) {
    try {
      list = JSON.parse(fs.readFileSync(PATHS.AUTO_UPDATE_LIST, 'utf-8'));
    } catch {
      list = {};
    }
  }

  if (list[playlistId]) {
    list[playlistId] = {
      ...list[playlistId],
      title,
      videoCount,
      channelId
    };
    fs.writeFileSync(PATHS.AUTO_UPDATE_LIST, JSON.stringify(list));
  }
}

async function getBatchedRemoteSnapshots(keyManager, playlistIds) {
  const results = new Map();

  for (let i = 0; i < playlistIds.length; i += BATCH_SIZE) {
    const batch = playlistIds.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(playlistIds.length / BATCH_SIZE);

    console.log(`Scanning batch ${batchNum}/${totalBatches} (${batch.length} playlists)`);

    try {
      const data = await youtubeRequest(keyManager, 'playlists', {
        part: 'snippet,contentDetails',
        id: batch.join(','),
        maxResults: '50',
        fields: 'items(id,snippet(title,thumbnails/high/url,channelId),contentDetails/itemCount)'
      });

      for (const item of data.items || []) {
        if (!item.id) continue;
        results.set(item.id, {
          id: item.id,
          title: item.snippet?.title || '',
          thumbnail: item.snippet?.thumbnails?.high?.url || '',
          channelId: item.snippet?.channelId || '',
          videoCount: item.contentDetails?.itemCount || 0
        });
      }
    } catch (error) {
      if (error.message === 'ALL_KEYS_EXHAUSTED') throw error;
      console.log(`Batch failed: ${error.message}`);
    }
  }

  return results;
}

async function fetchPlaylistVideos(keyManager, playlistId) {
  const videos = [];
  let pageToken = '';

  do {
    const params = {
      part: 'snippet,contentDetails',
      playlistId,
      maxResults: '50',
      fields: 'nextPageToken,items(snippet(title,description,publishedAt,thumbnails/high/url,resourceId/videoId),contentDetails/videoId)'
    };
    if (pageToken) params.pageToken = pageToken;

    const data = await youtubeRequest(keyManager, 'playlistItems', params);
    const items = data.items || [];

    for (const item of items) {
      const videoId = item.contentDetails?.videoId || item.snippet?.resourceId?.videoId;
      if (!videoId) continue;

      const snippet = item.snippet || {};
      const title = snippet.title || '';
      const thumbnail = snippet.thumbnails?.high?.url || '';
      const normalizedTitle = title.toLowerCase();

      if (
        title === 'Private video' ||
        title === 'Deleted video' ||
        title === 'فيديو خاص' ||
        title === 'فيديو محذوف' ||
        (!thumbnail && normalizedTitle.includes('private'))
      ) {
        continue;
      }

      videos.push({
        id: videoId,
        title,
        description: snippet.description || '',
        date: snippet.publishedAt || '',
        thumbnail,
        url: `https://www.youtube.com/watch?v=${videoId}`
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
  let channelTitle = 'Unknown';
  if (fs.existsSync(PATHS.CHANNELS_FILE)) {
    try {
      const channels = JSON.parse(fs.readFileSync(PATHS.CHANNELS_FILE, 'utf-8'));
      const channel = channels.find(c => String(c.id) === String(playlist.channelId));
      if (channel) channelTitle = channel.title;
    } catch {
      // noop
    }
  }

  const indexEntry = {
    id: playlist.id,
    title: playlist.title,
    thumbnail: playlist.thumbnail,
    videoCount: playlist.videoCount,
    channelId: playlist.channelId,
    channelTitle,
    categories: playlist.categories || [],
    path: `data/playlists/${playlist.id}.json`
  };

  for (const cat of playlist.categories || []) {
    const categoryFile = findCategoryFile(cat);
    if (!categoryFile) continue;

    try {
      const catIndex = JSON.parse(fs.readFileSync(categoryFile, 'utf-8'));
      const idx = catIndex.findIndex(p => p.id === playlist.id);
      if (idx > -1) {
        catIndex[idx] = indexEntry;
      } else {
        catIndex.push(indexEntry);
      }
      fs.writeFileSync(categoryFile, JSON.stringify(catIndex));
    } catch (e) {
      console.log(`Failed to update category index ${cat}: ${e.message}`);
    }
  }

  const channelFile = path.join(PATHS.CHANNELS, `ch_${playlist.channelId}.json`);
  if (fs.existsSync(channelFile)) {
    try {
      const chData = JSON.parse(fs.readFileSync(channelFile, 'utf-8'));
      if (Array.isArray(chData.playlists)) {
        const idx = chData.playlists.findIndex(p => p.id === playlist.id);
        if (idx > -1) {
          chData.playlists[idx] = indexEntry;
        } else {
          chData.playlists.push(indexEntry);
        }
        fs.writeFileSync(channelFile, JSON.stringify(chData));
      }
    } catch (e) {
      console.log(`Failed to update channel index: ${e.message}`);
    }
  }

  if (fs.existsSync(PATHS.PLAYLISTS_INDEX)) {
    try {
      const index = JSON.parse(fs.readFileSync(PATHS.PLAYLISTS_INDEX, 'utf-8'));
      const idx = index.findIndex(p => p.id === playlist.id);
      if (idx > -1) {
        index[idx] = indexEntry;
      } else {
        index.push(indexEntry);
      }
      fs.writeFileSync(PATHS.PLAYLISTS_INDEX, JSON.stringify(index));
    } catch (e) {
      console.log(`Failed to update unified index: ${e.message}`);
    }
  }
}

async function main() {
  console.log('=============================================');
  console.log('YouTube Playlist Auto-Updater');
  console.log(new Date().toISOString());
  console.log('=============================================\n');

  const keyManager = new ApiKeyManager();

  const localPlaylists = readPlaylistsIndex();
  if (localPlaylists.length === 0) {
    console.log('No playlists found. Nothing to do.');
    return;
  }
  console.log(`Found ${localPlaylists.length} local playlists\n`);

  console.log('Scanning for updates...');
  const playlistIds = localPlaylists.map(p => p.id);

  let remoteSnapshots;
  try {
    remoteSnapshots = await getBatchedRemoteSnapshots(keyManager, playlistIds);
  } catch (error) {
    if (error.message === 'ALL_KEYS_EXHAUSTED') {
      console.log('All API keys exhausted during scan phase. Exiting.');
      process.exit(1);
    }
    throw error;
  }

  const needsUpdate = [];
  for (const playlist of localPlaylists) {
    const remote = remoteSnapshots.get(playlist.id);
    if (remote === undefined) continue;

    let existingData;
    try {
      existingData = JSON.parse(fs.readFileSync(playlist.localPath, 'utf-8'));
    } catch {
      continue;
    }

    const countChanged = remote.videoCount !== playlist.videoCount;
    const metadataChanged =
      String(existingData.title || '') !== String(remote.title || '') ||
      String(existingData.thumbnail || '') !== String(remote.thumbnail || '');

    if (countChanged || metadataChanged) {
      needsUpdate.push({
        id: playlist.id,
        localId: playlist.localId,
        localPath: playlist.localPath,
        title: playlist.title,
        localCount: playlist.videoCount,
        remoteCount: remote.videoCount,
        channelId: playlist.channelId,
        localChannelId: playlist.localChannelId,
        metadataChanged,
        countChanged,
        remote
      });
    }
  }

  if (needsUpdate.length === 0) {
    console.log('All playlists are up to date.');
    if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });
    const today = new Date().toISOString().split('T')[0];
    const logPath = path.join(LOGS_DIR, `${today}.md`);
    let content = '';
    if (fs.existsSync(logPath)) content = fs.readFileSync(logPath, 'utf-8') + '\n';
    content += `## ${new Date().toISOString().split('T')[1].split('.')[0]}\n\n`;
    content += `- Total: ${localPlaylists.length}\n`;
    content += '- Needs update: 0\n- Updated: 0\n- Failed: 0\n\nNo changes.\n';
    fs.writeFileSync(logPath, content);
    console.log(`Log saved: logs/playlists/${today}.md`);
    return;
  }

  console.log(`Found ${needsUpdate.length} playlist(s) needing updates\n`);
  for (const p of needsUpdate) {
    const diff = p.remoteCount - p.localCount;
    const sign = diff > 0 ? '+' : '';
    console.log(`- ${p.title} (${p.localCount} -> ${p.remoteCount}, ${sign}${diff})`);
  }
  console.log('');

  let successCount = 0;
  let failCount = 0;
  const logEntries = [];

  for (let i = 0; i < needsUpdate.length; i++) {
    const pl = needsUpdate[i];
    console.log(`[${i + 1}/${needsUpdate.length}] Updating "${pl.title}"...`);

    try {
      const existingData = JSON.parse(fs.readFileSync(pl.localPath, 'utf-8'));

      const details = pl.remote;
      let videos = existingData.videos || [];

      if (pl.countChanged) {
        videos = await fetchPlaylistVideos(keyManager, pl.id);
        console.log(`Fetched ${videos.length} videos`);
      } else {
        console.log('Metadata-only update (no video re-fetch)');
      }

      const updatedPlaylist = {
        id: existingData.id,
        title: details.title,
        thumbnail: details.thumbnail,
        videoCount: pl.countChanged ? videos.length : details.videoCount,
        channelId: existingData.channelId,
        categories: existingData.categories || [],
        videos
      };

      fs.writeFileSync(pl.localPath, JSON.stringify(updatedPlaylist));

      updateIndexFiles(updatedPlaylist);
      updateAutoUpdateList(pl.id, updatedPlaylist.title, videos.length, details.channelId || pl.channelId);

      const newCount = pl.countChanged ? videos.length : details.videoCount;
      console.log(`Updated successfully (${pl.localCount} -> ${newCount})`);
      successCount++;
      logEntries.push({ id: pl.id, title: pl.title, before: pl.localCount, after: newCount });
    } catch (error) {
      if (error.message === 'ALL_KEYS_EXHAUSTED') {
        console.log('All API keys exhausted. Stopping updates.');
        console.log(`Completed: ${successCount}, Remaining: ${needsUpdate.length - i}`);
        break;
      }
      console.log(`Error: ${error.message}`);
      failCount++;
    }
  }

  console.log('\n=============================================');
  console.log('Summary');
  console.log('=============================================');
  console.log(`Updated: ${successCount}`);
  console.log(`Failed: ${failCount}`);
  console.log(`Total: ${needsUpdate.length}`);
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
    content += '| ID | Name | Before | After |\n|---|---|---|---|\n';
    for (const e of logEntries) {
      content += `| ${e.id} | ${e.title} | ${e.before} | ${e.after} |\n`;
    }
  } else {
    content += 'No changes.\n';
  }

  fs.writeFileSync(logPath, content);
  console.log(`Log saved: logs/playlists/${today}.md`);

  if (failCount > 0 && successCount === 0) {
    process.exit(1);
  }
}

main().catch(error => {
  console.error('Fatal error:', error.message);
  process.exit(1);
});
