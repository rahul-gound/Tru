import { Client, Databases, ID, Query } from 'https://cdn.jsdelivr.net/npm/appwrite@15.0.0/+esm';

const config = window.APPWRITE_CONFIG;
const client = new Client().setEndpoint(config.endpoint).setProject(config.projectId);
const databases = new Databases(client);

const searchInput = document.querySelector('#search');
const results = document.querySelector('#results');
const player = document.querySelector('#player');
const statusEl = document.querySelector('#status');

let searchTimer;
let activeMovie = null;
const trackedViews = new Set();

const setStatus = (message) => {
  statusEl.textContent = message;
};

async function loadMovies(term = '') {
  const queries = term ? [Query.search('title', term.trim())] : [];
  const response = await databases.listDocuments(config.databaseId, config.moviesCollectionId, queries);
  renderMovies(response.documents || []);
}

function renderMovies(movies) {
  results.innerHTML = '';

  for (const movie of movies) {
    const card = document.createElement('button');
    card.type = 'button';
    card.className = 'card';
    card.innerHTML = `
      <img src="${movie.poster_image}" alt="${movie.title}" loading="lazy" />
      <div class="meta">
        <strong>${movie.title}</strong><br />
        <small>Rating: ${Number(movie.rating).toFixed(1)}</small>
      </div>
    `;

    card.addEventListener('click', () => playWithFailover(movie));
    results.appendChild(card);
  }

  if (!movies.length) {
    setStatus('No movies found.');
  }
}

async function checkEdgeHealth(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 2500);
  try {
    const response = await fetch(url, { method: 'HEAD', signal: controller.signal });
    return response.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timeout);
  }
}

async function logCriticalIssue(errorType) {
  try {
    await databases.createDocument(config.databaseId, config.issuesCollectionId, ID.unique(), {
      error_type: errorType,
      severity: 'Critical',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Failed to log issue:', error);
  }
}

async function trackViewOnce(movie) {
  if (!movie || trackedViews.has(movie.$id)) {
    return;
  }

  trackedViews.add(movie.$id);

  if (!config.viewsCollectionId) {
    return;
  }

  try {
    await databases.createDocument(config.databaseId, config.viewsCollectionId, ID.unique(), {
      movie_id: movie.$id,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Failed to track view:', error);
  }
}

async function playWithFailover(movie) {
  const edgeIsHealthy = await checkEdgeHealth(movie.hf_stream_url);
  const streamUrl = edgeIsHealthy ? movie.hf_stream_url : movie.mobile_fallback_url;

  if (!edgeIsHealthy) {
    await logCriticalIssue('EDGE_UNAVAILABLE');
    setStatus('Edge unavailable. Using fallback stream.');
  } else {
    setStatus('Streaming from edge cache.');
  }

  activeMovie = movie;
  player.src = streamUrl;
  player.load();
  player.play().catch(() => {
    setStatus('Tap play to start streaming.');
  });
}

player.addEventListener('play', () => {
  void trackViewOnce(activeMovie);
});

player.addEventListener('error', async () => {
  if (!activeMovie) {
    return;
  }

  if (player.currentSrc !== activeMovie.mobile_fallback_url) {
    await logCriticalIssue('EDGE_STREAM_PLAYBACK_ERROR');
    player.src = activeMovie.mobile_fallback_url;
    player.load();
    player.play().catch(() => {
      setStatus('Playback failed. Please retry.');
    });
    return;
  }

  setStatus('Playback failed on fallback stream.');
});

searchInput.addEventListener('input', () => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => {
    void loadMovies(searchInput.value).catch((error) => {
      console.error(error);
      setStatus('Failed to fetch movies.');
    });
  }, 200);
});

void loadMovies().catch((error) => {
  console.error(error);
  setStatus('Failed to load movies.');
});
