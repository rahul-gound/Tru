'use strict';

const express = require('express');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const { pipeline } = require('node:stream/promises');
const { Readable, PassThrough } = require('node:stream');

const app = express();
const PORT = Number(process.env.PORT || 7860);
const TMP_DIR = '/tmp';
const MOBILE_BASE_URL = process.env.MOBILE_ORIGIN_BASE_URL || '';

const RATE_LIMIT_WINDOW_MS = Number(process.env.RATE_LIMIT_WINDOW_MS || 60_000);
const RATE_LIMIT_MAX_REQUESTS = Number(process.env.RATE_LIMIT_MAX_REQUESTS || 120);
const rateLimitState = new Map();

let isDownloading = false;

function streamFromOrigin(responseBody) {
  return Readable.fromWeb(responseBody);
}

function setProxyHeaders(originResponse, res) {
  const passHeaders = ['content-type', 'content-length', 'accept-ranges', 'content-range', 'etag', 'last-modified'];
  for (const name of passHeaders) {
    const value = originResponse.headers.get(name);
    if (value) {
      res.setHeader(name, value);
    }
  }
}

function buildOriginUrl(movieId) {
  if (!MOBILE_BASE_URL) {
    throw new Error('Missing MOBILE_ORIGIN_BASE_URL');
  }

  return `${MOBILE_BASE_URL.replace(/\/$/, '')}/${encodeURIComponent(movieId)}.mp4`;
}

async function getOriginUrl(movieId) {
  if (MOBILE_BASE_URL) {
    return buildOriginUrl(movieId);
  }

  const endpoint = process.env.APPWRITE_ENDPOINT;
  const project = process.env.APPWRITE_PROJECT_ID;
  const databaseId = process.env.APPWRITE_DATABASE_ID;
  const collectionId = process.env.APPWRITE_MOVIES_COLLECTION_ID;
  const apiKey = process.env.APPWRITE_API_KEY;

  if (!endpoint || !project || !databaseId || !collectionId || !apiKey) {
    throw new Error('Missing Appwrite configuration and MOBILE_ORIGIN_BASE_URL');
  }

  const url = `${endpoint.replace(/\/$/, '')}/databases/${databaseId}/collections/${collectionId}/documents/${encodeURIComponent(movieId)}`;
  const response = await fetch(url, {
    headers: {
      'X-Appwrite-Project': project,
      'X-Appwrite-Key': apiKey,
      'Content-Type': 'application/json'
    }
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch movie metadata (${response.status})`);
  }

  const movie = await response.json();
  if (!movie.mobile_fallback_url) {
    throw new Error('movie.mobile_fallback_url missing in Appwrite movie document');
  }

  return movie.mobile_fallback_url;
}

function streamRateLimit(req, res, next) {
  const now = Date.now();
  const ip = req.ip || req.socket.remoteAddress || 'unknown';
  const current = rateLimitState.get(ip);

  if (!current || now >= current.resetAt) {
    rateLimitState.set(ip, { count: 1, resetAt: now + RATE_LIMIT_WINDOW_MS });
    next();
    return;
  }

  current.count += 1;
  if (current.count > RATE_LIMIT_MAX_REQUESTS) {
    const retryAfter = Math.ceil((current.resetAt - now) / 1000);
    res.setHeader('Retry-After', String(Math.max(retryAfter, 1)));
    res.status(429).json({ error: 'Too many requests' });
    return;
  }

  next();
}

async function streamLocalFile(req, res, filePath) {
  const stat = await fsp.stat(filePath);
  const fileSize = stat.size;
  const rangeHeader = req.headers.range;

  if (!rangeHeader) {
    res.writeHead(200, {
      'Content-Type': 'video/mp4',
      'Content-Length': fileSize,
      'Accept-Ranges': 'bytes'
    });
    return pipeline(fs.createReadStream(filePath), res);
  }

  const matches = /bytes=(\d*)-(\d*)/.exec(rangeHeader);
  if (!matches) {
    res.status(416).send('Invalid Range header');
    return;
  }

  const start = matches[1] ? Number(matches[1]) : 0;
  const end = matches[2] ? Number(matches[2]) : fileSize - 1;

  if (start > end || start >= fileSize) {
    res.status(416).setHeader('Content-Range', `bytes */${fileSize}`);
    res.end();
    return;
  }

  const chunkSize = end - start + 1;
  res.writeHead(206, {
    'Content-Range': `bytes ${start}-${end}/${fileSize}`,
    'Accept-Ranges': 'bytes',
    'Content-Length': chunkSize,
    'Content-Type': 'video/mp4'
  });

  return pipeline(fs.createReadStream(filePath, { start, end }), res);
}

async function proxyDirect(req, res, originUrl) {
  const upstream = await fetch(originUrl, {
    headers: req.headers.range ? { Range: req.headers.range } : undefined
  });

  if (!upstream.ok && upstream.status !== 206) {
    throw new Error(`Origin proxy failed (${upstream.status})`);
  }

  res.status(upstream.status);
  setProxyHeaders(upstream, res);

  if (!upstream.body) {
    res.status(502).json({ error: 'Origin stream body unavailable' });
    return;
  }

  return pipeline(streamFromOrigin(upstream.body), res);
}

async function cacheAndStream(req, res, localPath, originUrl) {
  const upstream = await fetch(originUrl);
  if (!upstream.ok || !upstream.body) {
    throw new Error(`Origin stream unavailable (${upstream.status})`);
  }

  const tempPath = `${localPath}.part`;
  const source = streamFromOrigin(upstream.body);
  const cacheBranch = new PassThrough();
  const responseBranch = new PassThrough();

  source.pipe(cacheBranch);
  source.pipe(responseBranch);

  res.status(200);
  setProxyHeaders(upstream, res);

  const writeCache = pipeline(cacheBranch, fs.createWriteStream(tempPath));
  const sendResponse = pipeline(responseBranch, res);

  req.on('close', () => {
    if (!res.writableEnded) {
      responseBranch.destroy(new Error('Client disconnected'));
    }
  });

  try {
    await Promise.all([writeCache, sendResponse]);
    await fsp.rename(tempPath, localPath);
  } catch (error) {
    await fsp.rm(tempPath, { force: true }).catch((cleanupError) => {
      console.error('Failed to clean partial cache:', cleanupError.message);
    });
    throw error;
  }
}

app.set('trust proxy', true);

app.get('/healthz', (_req, res) => {
  res.status(200).json({ ok: true, isDownloading });
});

app.get('/stream/:movieId', streamRateLimit, async (req, res) => {
  const movieId = String(req.params.movieId || '').trim();
  const safeMovieId = movieId.replace(/[^a-zA-Z0-9_-]/g, '');

  if (!safeMovieId) {
    res.status(400).json({ error: 'Invalid movieId' });
    return;
  }

  const localPath = path.join(TMP_DIR, `${safeMovieId}.mp4`);
  let startedDownload = false;

  try {
    if (fs.existsSync(localPath)) {
      await streamLocalFile(req, res, localPath);
      return;
    }

    const originUrl = await getOriginUrl(safeMovieId);

    if (isDownloading) {
      await proxyDirect(req, res, originUrl);
      return;
    }

    isDownloading = true;
    startedDownload = true;
    await cacheAndStream(req, res, localPath, originUrl);
  } catch (error) {
    if (!res.headersSent) {
      res.status(502).json({ error: 'Streaming failed', detail: error.message });
    } else {
      res.destroy(error);
    }
  } finally {
    if (startedDownload) {
      isDownloading = false;
    }
  }
});

app.use((_req, res) => {
  res.status(404).json({ error: 'Not found' });
});

app.listen(PORT, () => {
  console.log(`Edge proxy listening on ${PORT}`);
});
