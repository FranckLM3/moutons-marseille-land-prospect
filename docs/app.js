// ── Config ────────────────────────────────────────────────────────────────
// IGN WMTS public (pas de clé API requise, fonctionne depuis GitHub Pages)
// Note : wms-r/wms retourne 400 avec Origin cross-site (Kong API key requis)
// Couche : OCSGE.COUVERTURE.2021-2023 (millésime récent, couvre le dep. 13)
// TileMatrixSet PM_6_16 : zoom 6–16 (GetCapabilities geopf.fr)
const IGN_WMTS_URL = 'https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0'
  + '&LAYER=OCSGE.COUVERTURE.2021-2023&STYLE=normal&TILEMATRIXSET=PM_6_16'
  + '&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}&FORMAT=image/png';
const DEFAULT_FILTER = 'Marseille'; // pré-sélectionne toutes les communes contenant ce mot

// Couleur selon % pâturable (0 ou null = gris, >0 = vert clair→foncé)
function colorForPrairie(pct) {
  if (pct == null || pct === 0) return '#94a3b8'; // gris — 0% sol minéral/bâti/route
  if (pct < 25)  return '#bbf7d0'; // vert très pâle  — faible (1–24%)
  if (pct < 50)  return '#86efac'; // vert pâle       — moyen  (25–49%)
  if (pct < 75)  return '#4ade80'; // vert clair      — bon    (50–74%)
  return '#16a34a';                // vert foncé      — excellent (≥75%)
}

// Libellés OCS GE v2
const CS_LABELS = {
  'CS2.2.1':   'Formations herbacées',
  'CS2.1.2':   'Landes et maquis',
  'CS2.1.1.1': 'Peuplement de feuillus',
  'CS2.1.1.2': 'Peuplement de conifères',
  'CS2.1.1.3': 'Peuplement mixte',
  'CS2.1.3':   'Autres formations ligneuses',
  'CS2.2.2':   'Autres formations non ligneuses',
  'CS2.1.1':   'Formations arborées',
  'CS2.1':     'Végétation ligneuse',
  'CS2.2':     'Végétation non ligneuse',
};

// ── État ──────────────────────────────────────────────────────────────────
let allFeatures      = [];
let currentLayer     = null;
let selectedCommunes = new Set(); // multiselect Set — rempli par buildCommuneChips()
let allCommunes      = [];        // liste triée complète
let minAreaHa        = 0.5;       // surface pâturable min en ha (défaut 5 000 m²)
let showValidatedOnly = false;    // filtre sur parcelles validées par contributeurs
let showOwnerKnownOnly = false;   // filtre sur parcelles avec propriétaire connu
let selectedPropTypes = new Set(['public', 'semi-public', 'privé', 'indéterminé']); // filtre type propriétaire
let ocsWmsLayer = null;

// ── Avis contributeurs (localStorage) ───────────────────────────────────
const FEEDBACK_STORAGE_KEY = 'parcel-feedback-v1';
let parcelFeedback = loadParcelFeedback();
const SUPABASE_TABLE = 'parcel_feedback';
const SUPABASE_COMMENTS_TABLE = 'parcel_comments';
const supabaseClient = initSupabaseClient();
const pendingSupabaseFetch = new Set();

function loadParcelFeedback() {
  try {
    const raw = localStorage.getItem(FEEDBACK_STORAGE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch (_) {
    return {};
  }
}

function initSupabaseClient() {
  const url = window.SUPABASE_URL || '';
  const key = window.SUPABASE_ANON_KEY || '';
  if (!url || !key || url.includes('__SUPABASE_URL__') || key.includes('__SUPABASE_ANON_KEY__')) {
    return null;
  }
  if (!window.supabase || !window.supabase.createClient) return null;
  return window.supabase.createClient(url, key);
}

function supabaseEnabled() {
  return Boolean(supabaseClient);
}

async function fetchFeedbackFromSupabase(parcelId) {
  if (!supabaseEnabled() || pendingSupabaseFetch.has(parcelId)) return;
  pendingSupabaseFetch.add(parcelId);
  try {
    const { data, error } = await supabaseClient
      .from(SUPABASE_TABLE)
      .select('status, updated_at')
      .eq('parcel_id', parcelId)
      .maybeSingle();

    if (!error && data) {
      parcelFeedback[parcelId] = {
        status: data.status || 'unknown',
        comments: parcelFeedback[parcelId]?.comments || [],
        updatedAt: data.updated_at || null,
      };
      saveParcelFeedback();
      updateFeedbackUI(parcelId);
    }
  } catch (_) {
    // ignore
  } finally {
    pendingSupabaseFetch.delete(parcelId);
  }
}

async function upsertFeedbackToSupabase(parcelId) {
  if (!supabaseEnabled()) return;
  const feedback = getParcelFeedback(parcelId);
  try {
    await supabaseClient
      .from(SUPABASE_TABLE)
      .upsert({
        parcel_id: parcelId,
        status: feedback.status || 'unknown',
        updated_at: new Date().toISOString(),
      }, { onConflict: 'parcel_id' });
  } catch (_) {
    // ignore
  }
}

async function fetchCommentsFromSupabase(parcelId) {
  if (!supabaseEnabled()) return;
  try {
    const { data, error } = await supabaseClient
      .from(SUPABASE_COMMENTS_TABLE)
      .select('author, message, created_at')
      .eq('parcel_id', parcelId)
      .order('created_at', { ascending: false });

    if (!error && Array.isArray(data)) {
      const existing = parcelFeedback[parcelId] || { status: 'unknown', comments: [] };
      parcelFeedback[parcelId] = {
        ...existing,
        comments: data.map(row => ({
          author: row.author || 'Anonyme',
          message: row.message || '',
          createdAt: row.created_at || null,
        })),
      };
      saveParcelFeedback();
      updateCommentsUI(parcelId);
    }
  } catch (_) {
    // ignore
  }
}

function saveParcelFeedback() {
  try {
    localStorage.setItem(FEEDBACK_STORAGE_KEY, JSON.stringify(parcelFeedback));
  } catch (_) {}
}

function getLocalFeedbackStatus(parcelId) {
  return parcelFeedback[parcelId]?.status || 'unknown';
}

function getParcelId(feature) {
  const p = feature.properties || {};
  if (p.id) return String(p.id);
  const c = centroid(feature);
  const lat = c ? c.lat.toFixed(6) : '0';
  const lng = c ? c.lng.toFixed(6) : '0';
  return `${p.nom_commune || 'comm'}-${p.area_m2 || 0}-${lat}-${lng}`;
}

function getParcelFeedback(parcelId) {
  if (!parcelFeedback[parcelId]) {
    fetchFeedbackFromSupabase(parcelId);
    fetchCommentsFromSupabase(parcelId);
  }
  return parcelFeedback[parcelId] || { status: 'unknown', comments: [] };
}

function escapeForAttr(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function makeDomId(value) {
  return encodeURIComponent(String(value)).replace(/%/g, '_');
}

function setParcelStatus(parcelId, status) {
  const existing = parcelFeedback[parcelId] || {};
  parcelFeedback[parcelId] = { ...existing, status, updatedAt: new Date().toISOString() };
  saveParcelFeedback();
  updateFeedbackUI(parcelId);
  upsertFeedbackToSupabase(parcelId);
  if (showValidatedOnly) applyFilters();
}

function addParcelComment(parcelId) {
  const domId = makeDomId(parcelId);
  const nameInput = document.getElementById(`commenter-${domId}`);
  const textarea = document.getElementById(`comment-${domId}`);
  if (!textarea) return;
  const author = nameInput?.value.trim() || 'Anonyme';
  const message = textarea.value.trim();
  if (!message) return;

  const existing = parcelFeedback[parcelId] || { status: 'unknown', comments: [] };
  const newComment = { author, message, createdAt: new Date().toISOString() };
  parcelFeedback[parcelId] = {
    ...existing,
    comments: [newComment, ...(existing.comments || [])],
    updatedAt: new Date().toISOString(),
  };
  saveParcelFeedback();
  updateCommentsUI(parcelId);
  updateFeedbackUI(parcelId);
  textarea.value = '';

  if (supabaseEnabled()) {
    supabaseClient
      .from(SUPABASE_COMMENTS_TABLE)
      .insert({ parcel_id: parcelId, author, message })
      .then(() => fetchCommentsFromSupabase(parcelId))
      .catch(() => {});
  }
}

function formatCommentDate(value) {
  if (!value) return '';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleString('fr', { dateStyle: 'short', timeStyle: 'short' });
}

function renderCommentsHtml(parcelId) {
  const feedback = getParcelFeedback(parcelId);
  const comments = feedback.comments || [];
  if (!comments.length) {
    return '<div class="popup-comment-empty">Aucun commentaire pour le moment.</div>';
  }
  return comments.map(comment => `
    <div class="popup-comment-item">
      <div class="popup-comment-head">
        <span class="popup-comment-author">${comment.author || 'Anonyme'}</span>
        <span class="popup-comment-date">${formatCommentDate(comment.createdAt)}</span>
      </div>
      <div class="popup-comment-text">${comment.message}</div>
    </div>
  `).join('');
}

function updateCommentsUI(parcelId) {
  const domId = makeDomId(parcelId);
  const listEl = document.getElementById(`comments-${domId}`);
  if (listEl) {
    listEl.innerHTML = renderCommentsHtml(parcelId);
  }
}

function updateFeedbackUI(parcelId) {
  const domId = makeDomId(parcelId);
  const feedback = getParcelFeedback(parcelId);
  const statusEl = document.getElementById(`status-${domId}`);
  if (statusEl) {
    const label = feedback.status === 'yes' ? '✅ Pâturable' : feedback.status === 'no' ? '🚫 Non pâturable' : '⏺️ Avis non défini';
    statusEl.textContent = label;
  }
  const tagEl = document.getElementById(`tag-${domId}`);
  if (tagEl) {
    tagEl.textContent = feedback.status === 'yes' ? '✅ Pâturable (contrib)' : feedback.status === 'no' ? '🚫 Non pâturable' : '⏺️ Avis non défini';
    tagEl.className = `tag ${feedback.status === 'yes' ? 'tag-green' : feedback.status === 'no' ? 'tag-red' : 'tag-gray'}`;
  }
  document.querySelectorAll(`[data-feedback-id="${domId}"]`).forEach(btn => {
    const val = btn.getAttribute('data-status');
    btn.classList.toggle('active', val === feedback.status);
  });
  if (showValidatedOnly) applyFilters();
}

// ── Carte ─────────────────────────────────────────────────────────────────
const map = L.map('map', { zoomControl: false }).setView([43.3, 5.4], 12);
L.control.zoom({ position: 'bottomright' }).addTo(map);
L.control.scale({ metric: true, imperial: false, position: 'bottomright' }).addTo(map);

// Pane intermédiaire pour la couche OCS GE : au-dessus du fond de carte (200)
// mais en-dessous des parcelles cadastrales (400) et des markers (600)
map.createPane('ocsPane');
map.getPane('ocsPane').style.zIndex = 300;
map.getPane('ocsPane').style.pointerEvents = 'none';

const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© <a href="https://openstreetmap.org">OpenStreetMap</a>',
  maxZoom: 19,
});
const satellite = L.tileLayer(
  'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
  { attribution: '© Esri', maxZoom: 19 }
);
satellite.addTo(map);
L.control.layers({ 'Carte': osmLayer, 'Satellite': satellite }, {}, { position: 'bottomright' }).addTo(map);

// ── Chargement ────────────────────────────────────────────────────────────
// Les données sont chargées depuis Supabase à la demande par commune via RPC

// Cache des features déjà chargées par commune (évite les double-fetch)
const communeCache = {};  // { nomCommune: [feature, ...] }

async function fetchParcellesByCommunes(communes) {
  if (!supabaseEnabled()) return [];
  const toFetch = communes.filter(c => !(c in communeCache));
  if (toFetch.length > 0) {
    try {
      const { data, error } = await supabaseClient.rpc('parcelles_by_communes', {
        communes:    toFetch,
        min_prairie: 0,
      });
      if (error) throw new Error(error.message);
      // Regrouper par commune et convertir en features GeoJSON
      toFetch.forEach(c => { communeCache[c] = []; });
      (data || []).forEach(row => {
        const { geojson, ...props } = row;
        const feature = {
          type: 'Feature',
          geometry: JSON.parse(geojson),
          properties: { ...props, cs_detail: props.cs_detail },
        };
        communeCache[row.nom_commune] = communeCache[row.nom_commune] || [];
        communeCache[row.nom_commune].push(feature);
      });
    } catch (err) {
      console.error('fetchParcellesByCommunes error:', err);
    }
  }
  // Retourner toutes les features des communes demandées
  return communes.flatMap(c => communeCache[c] || []);
}

async function loadCommuneList() {
  // Charge la liste des communes distinctes via RPC (évite la limite de 1000 lignes)
  if (!supabaseEnabled()) return [];
  const { data, error } = await supabaseClient.rpc('liste_communes');
  if (error) { console.error('loadCommuneList error:', error); return []; }
  return (data || []).map(r => r.nom_commune).filter(Boolean);
}

async function initData() {
  document.getElementById('loading').style.display = 'flex';

  if (!supabaseEnabled()) {
    document.getElementById('loading').innerHTML = `
      <div style="color:#f87171;font-size:20px;">⚠️</div>
      <p style="color:#666;max-width:320px;text-align:center;line-height:1.6">
        Connexion Supabase non configurée.<br>
        Vérifiez les variables <code style="color:#4ade80">SUPABASE_URL</code>
        et <code style="color:#4ade80">SUPABASE_ANON_KEY</code>.
      </p>`;
    return;
  }

  try {
    allCommunes = await loadCommuneList();
    if (!allCommunes.length) throw new Error('Aucune commune trouvée dans Supabase');

    // Pré-sélectionner communes Marseille
    allCommunes.forEach(name => {
      if (name.toLowerCase().includes(DEFAULT_FILTER.toLowerCase())) selectedCommunes.add(name);
    });

    // Charger les features des communes sélectionnées
    allFeatures = await fetchParcellesByCommunes([...selectedCommunes]);

    document.getElementById('loading').style.display = 'none';
    buildCommuneChips(true); // true = liste déjà chargée, ne pas re-extraire
    applyFilters();
  } catch (err) {
    console.error('Erreur chargement Supabase :', err.message);
    document.getElementById('loading').innerHTML = `
      <div style="color:#f87171;font-size:20px;">⚠️</div>
      <p style="color:#666;max-width:320px;text-align:center;line-height:1.6">
        Impossible de charger les données Supabase.<br><br>
        <span style="color:#f87171;font-size:12px">${err.message}</span>
      </p>`;
  }
}

// ── Démarrage ─────────────────────────────────────────────────────────────
initData();

// ── Chips communes (multiselect) ──────────────────────────────────────────
let showAllCommunes = false; // false = n'affiche que les actives (+ résultats recherche)

function buildCommuneChips(skipExtract = false) {
  const container = document.getElementById('commune-chips');

  if (!skipExtract) {
    // Mode GeoJSON : extraire les communes depuis allFeatures
    const seen = new Set();
    allFeatures.forEach(f => {
      const c = (f.properties?.nom_commune || '').trim();
      if (c && c !== 'null' && c !== 'None') seen.add(c);
    });
    allCommunes = [...seen].sort((a, b) => a.localeCompare(b, 'fr'));
    // Pré-sélectionner les communes Marseille
    allCommunes.forEach(name => {
      if (name.toLowerCase().includes(DEFAULT_FILTER.toLowerCase())) selectedCommunes.add(name);
    });
  }
  // Mode Supabase : allCommunes et selectedCommunes déjà remplis par initData()

  // Actives en premier, puis inactives
  const sorted = [
    ...allCommunes.filter(n => selectedCommunes.has(n)),
    ...allCommunes.filter(n => !selectedCommunes.has(n)),
  ];

  sorted.forEach(name => {
    const chip = document.createElement('div');
    const active = selectedCommunes.has(name);
    chip.className = 'comm-chip ' + (active ? 'active' : 'inactive');
    chip.dataset.commune = name;
    chip.innerHTML = `<span class="comm-dot"></span>${name}`;
    chip.addEventListener('click', async () => {
      selectedCommunes.has(name) ? selectedCommunes.delete(name) : selectedCommunes.add(name);
      refreshCommuneChips();
      await applyFilters();
    });
    container.appendChild(chip);
  });

  _applyChipVisibility('');
  _updateToggleBtn();
}

function refreshCommuneChips() {
  // Reconstruire l'ordre : actives d'abord
  const container = document.getElementById('commune-chips');
  const chips = [...container.querySelectorAll('.comm-chip')];
  chips.forEach(chip => {
    const active = selectedCommunes.has(chip.dataset.commune);
    chip.className = 'comm-chip ' + (active ? 'active' : 'inactive');
  });
  // Trier dans le DOM : actives en premier
  const sorted = chips.sort((a, b) => {
    const aA = selectedCommunes.has(a.dataset.commune) ? 0 : 1;
    const bA = selectedCommunes.has(b.dataset.commune) ? 0 : 1;
    return aA - bA;
  });
  sorted.forEach(c => container.appendChild(c));

  const query = (document.getElementById('commune-search')?.value || '');
  _applyChipVisibility(query);
  _updateToggleBtn();
}

function _applyChipVisibility(query) {
  const q = query.trim().toLowerCase();
  document.querySelectorAll('#commune-chips .comm-chip').forEach(chip => {
    const nameMatch = !q || chip.dataset.commune.toLowerCase().includes(q);
    const isActive  = selectedCommunes.has(chip.dataset.commune);
    // Visible si : correspond à la recherche ET (actif OU showAllCommunes)
    chip.style.display = (nameMatch && (isActive || showAllCommunes || q)) ? '' : 'none';
  });
}

function _updateToggleBtn() {
  const btn = document.getElementById('commune-toggle-btn');
  if (!btn) return;
  const inactiveCount = allCommunes.filter(c => !selectedCommunes.has(c)).length;
  if (inactiveCount === 0) { btn.style.display = 'none'; return; }
  btn.style.display = '';
  btn.textContent = showAllCommunes
    ? `▲ Masquer les non sélectionnées (${inactiveCount})`
    : `▼ Voir toutes les communes (${inactiveCount} de plus)`;
}

function toggleShowAllCommunes() {
  showAllCommunes = !showAllCommunes;
  const query = (document.getElementById('commune-search')?.value || '');
  _applyChipVisibility(query);
  _updateToggleBtn();
}

function selectAllCommunes()   { allCommunes.forEach(c => selectedCommunes.add(c)); refreshCommuneChips(); applyFilters(); }
function deselectAllCommunes() { selectedCommunes.clear(); refreshCommuneChips(); applyFilters(); }

function filterCommuneChips(query) {
  _applyChipVisibility(query);
}

function getGeometryBounds(geometry) {
  if (!geometry || !geometry.coordinates) return null;
  let minLat = 90, minLng = 180, maxLat = -90, maxLng = -180;
  const walk = coords => {
    if (!Array.isArray(coords)) return;
    if (typeof coords[0] === 'number' && typeof coords[1] === 'number') {
      const lng = coords[0];
      const lat = coords[1];
      minLat = Math.min(minLat, lat);
      maxLat = Math.max(maxLat, lat);
      minLng = Math.min(minLng, lng);
      maxLng = Math.max(maxLng, lng);
      return;
    }
    coords.forEach(walk);
  };
  walk(geometry.coordinates);
  if (minLat > maxLat || minLng > maxLng) return null;
  return L.latLngBounds([minLat, minLng], [maxLat, maxLng]);
}

function getSelectedCommunesBounds() {
  if (!selectedCommunes.size) return null;
  let bounds = null;
  allFeatures.forEach(f => {
    const commune = (f.properties?.nom_commune || '').trim();
    if (!selectedCommunes.has(commune)) return;
    const geomBounds = getGeometryBounds(f.geometry);
    if (!geomBounds) return;
    bounds = bounds ? bounds.extend(geomBounds) : geomBounds;
  });
  return bounds;
}

// ── Sliders ───────────────────────────────────────────────────────────────
document.getElementById('area-slider').addEventListener('input', function() {
  const m2 = parseInt(this.value);
  minAreaHa = m2 / 10000;
  const label = m2 === 0 ? '0 m²'
    : m2 < 10000 ? `${m2.toLocaleString('fr')} m²`
    : `${(m2 / 10000).toLocaleString('fr', { minimumFractionDigits: 1, maximumFractionDigits: 1 })} ha`;
  document.getElementById('area-value').textContent = label;
  applyFilters();
});

const validatedOnlyEl = document.getElementById('validated-only');
if (validatedOnlyEl) {
  validatedOnlyEl.addEventListener('change', () => {
    showValidatedOnly = validatedOnlyEl.checked;
    applyFilters();
  });
}

const ownerKnownOnlyEl = document.getElementById('owner-known-only');
if (ownerKnownOnlyEl) {
  ownerKnownOnlyEl.addEventListener('change', () => {
    showOwnerKnownOnly = ownerKnownOnlyEl.checked;
    applyFilters();
  });
}

// Filtres type de propriétaire
document.querySelectorAll('.prop-type-check').forEach(checkbox => {
  checkbox.addEventListener('change', () => {
    selectedPropTypes.clear();
    document.querySelectorAll('.prop-type-check:checked').forEach(el => {
      selectedPropTypes.add(el.value);
    });
    applyFilters();
  });
});

function selectAllPropTypes() {
  document.querySelectorAll('.prop-type-check').forEach(el => { el.checked = true; selectedPropTypes.add(el.value); });
  applyFilters();
}
function deselectAllPropTypes() {
  document.querySelectorAll('.prop-type-check').forEach(el => { el.checked = false; });
  selectedPropTypes.clear();
  applyFilters();
}

const toggleOcsEl = document.getElementById('toggle-ocs');
const ocsLegendList = document.getElementById('ocs-legend-list');
if (toggleOcsEl) {
  toggleOcsEl.addEventListener('change', () => {
    if (toggleOcsEl.checked) {
      showOcsLayer();
    } else {
      hideOcsLayer();
    }
  });
}

function showOcsLayer() {
  if (!ocsWmsLayer) {
    ocsWmsLayer = L.tileLayer(IGN_WMTS_URL, {
      attribution: '© IGN',
      opacity: 0.7,
      crossOrigin: 'anonymous',
      minZoom: 6,
      maxZoom: 16,
      pane: 'ocsPane',
    });
  }
  map.addLayer(ocsWmsLayer);
  updateLegendFromWms();
}

function hideOcsLayer() {
  if (ocsWmsLayer && map.hasLayer(ocsWmsLayer)) {
    map.removeLayer(ocsWmsLayer);
  }
}

function hasIgnWmtsConfig() {
  return Boolean(IGN_WMTS_URL);
}

function updateLegendFromWms() {
  if (!ocsLegendList) return;
  if (!hasIgnWmtsConfig()) return;
  const legendUrl = 'https://data.geopf.fr/annexes/ressources/legendes/OCSGE.COUVERTURE-legend.png';
  ocsLegendList.innerHTML = `
    <div class="ocs-legend-row">
      <img class="ocs-legend-image" src="${legendUrl}" alt="Légende OCS GE couverture"
           onerror="this.parentElement.textContent='Légende OCS GE — voir geoservices.ign.fr'" />
    </div>
  `;
}

function hasKnownOwner(props) {
  if (!props) return false;
  const denom = (props.denomination || '').trim();
  const siren = (props.siren || '').toString().trim();
  return Boolean(denom) || Boolean(siren);
}

// ── Filtrage ──────────────────────────────────────────────────────────────
function getFiltered() {
  if (selectedCommunes.size === 0) return [];
  return allFeatures.filter(f => {
    const p = f.properties || {};
    const c = (p.nom_commune || '').trim();
    if (!selectedCommunes.has(c)) return false;
    if (minAreaHa > 0 && (p.prairie_m2 || 0) < minAreaHa * 10000) return false;
    if (showValidatedOnly) {
      const parcelId = getParcelId(f);
      if (getLocalFeedbackStatus(parcelId) !== 'yes') return false;
    }
    if (showOwnerKnownOnly && !hasKnownOwner(p)) return false;
    // Filtre type de propriétaire (si pas tous sélectionnés)
    if (selectedPropTypes.size < 4) {
      const ptype = p.proprietaire_type || 'indéterminé';
      if (!selectedPropTypes.has(ptype)) return false;
    }
    return true;
  });
}

async function applyFilters() {
  // Charger les features manquantes depuis le cache ou l'API Supabase
  if (supabaseEnabled()) {
    allFeatures = await fetchParcellesByCommunes([...selectedCommunes]);
  }

  const filtered = getFiltered();

  if (currentLayer) map.removeLayer(currentLayer);

  currentLayer = L.geoJSON({ type: 'FeatureCollection', features: filtered }, {
    style: feature => {
      const color = colorForPrairie(feature.properties?.pct_prairie);
      return { fillColor: color, fillOpacity: 0.45, color: color, weight: 1.2, opacity: 0.8 };
    },
    onEachFeature: (feature, layer) => {
      layer.bindPopup(() => buildPopup(feature), { maxWidth: 300, minWidth: 260 });
      layer.on('mouseover', function() { this.setStyle({ fillOpacity: 0.7, weight: 2 }); });
      layer.on('mouseout',  function() { currentLayer && currentLayer.resetStyle(this); });
    },
  }).addTo(map);

  if (filtered.length > 0) {
    try { map.fitBounds(currentLayer.getBounds(), { padding: [20, 20], maxZoom: 14 }); } catch(_) {}
  }

  const totalHa   = filtered.reduce((s, f) => s + (f.properties?.prairie_m2 || 0), 0) / 10000;
  const withOwner = filtered.filter(f => f.properties?.denomination).length;
  const pct       = filtered.length ? Math.round(withOwner / filtered.length * 100) : 0;

  document.getElementById('count-total').textContent   = allFeatures.length.toLocaleString('fr');
  document.getElementById('count-visible').textContent = filtered.length.toLocaleString('fr');
  document.getElementById('stat-ha').textContent       = totalHa.toLocaleString('fr', { maximumFractionDigits: 0 }) + ' ha';
  document.getElementById('stat-pct').textContent      = pct + '%';
}

// ── Réinitialisation ──────────────────────────────────────────────────────
async function resetFilters() {
  // Réinitialise surface pâturable
  document.getElementById('area-slider').value = 5000;
  minAreaHa = 0.5;
  document.getElementById('area-value').textContent = '5 000 m²';
  if (validatedOnlyEl) validatedOnlyEl.checked = false;
  showValidatedOnly = false;
  if (ownerKnownOnlyEl) ownerKnownOnlyEl.checked = false;
  showOwnerKnownOnly = false;
  // Réinitialise type propriétaire → tous cochés
  selectedPropTypes = new Set(['public', 'semi-public', 'privé', 'indéterminé']);
  document.querySelectorAll('.prop-type-check').forEach(el => { el.checked = true; });
  // Réinitialise communes → Marseille
  selectedCommunes.clear();
  allCommunes.forEach(name => {
    if (name.toLowerCase().includes(DEFAULT_FILTER.toLowerCase())) selectedCommunes.add(name);
  });
  showAllCommunes = false;
  const communeSearch = document.getElementById('commune-search');
  if (communeSearch) communeSearch.value = '';
  refreshCommuneChips();
  await applyFilters();
}

// ── Popup ─────────────────────────────────────────────────────────────────
function buildPopup(feature) {
  const p = feature.properties || {};
  const totalM2   = p.area_m2    != null ? `${Number(p.area_m2).toLocaleString('fr')} m²` : '—';
  const prairieM2 = p.prairie_m2 != null ? `${Number(p.prairie_m2).toLocaleString('fr')} m²` : '0 m²';
  const pct       = p.pct_prairie != null ? `${p.pct_prairie} %` : '0 %';
  const own       = p.denomination || '—';
  const commune   = p.nom_commune  || '—';

  const parcelId = getParcelId(feature);
  const parcelIdEsc = escapeForAttr(parcelId);
  const domId = makeDomId(parcelId);
  const feedback = getParcelFeedback(parcelId);
  const feedbackLabel = feedback.status === 'yes' ? '✅ Pâturable' : feedback.status === 'no' ? '🚫 Non pâturable' : '⏺️ Avis non défini';
  const feedbackTagClass = feedback.status === 'yes' ? 'tag-green' : feedback.status === 'no' ? 'tag-red' : 'tag-gray';

  // Détail par type de couverture
  let csRows = '';
  try {
    const detail = typeof p.cs_detail === 'string' ? JSON.parse(p.cs_detail) : (p.cs_detail || {});
    const entries = Object.entries(detail).sort((a, b) => b[1] - a[1]);
    if (entries.length) {
      csRows = entries.map(([code, m2]) => {
        const label = CS_LABELS[code] || code;
        // CS2.1.1.2 = conifères (pins) : sol pauvre, risque incendie — non comptabilisé dans % pâturable
        const note = code === 'CS2.1.1.2'
          ? ' <span style="color:#f97316;font-size:0.8em">(non pâturable)</span>'
          : '';
        return `<span class="k" style="padding-left:16px;color:#555">${label}${note}</span><span class="v" style="color:#aaa">${Number(m2).toLocaleString('fr')} m²</span>`;
      }).join('');
    }
  } catch(_) {}

  let gmaps = '';
  try {
    const c = L.geoJSON(feature).getBounds().getCenter();
    gmaps = `https://www.google.com/maps?q=${c.lat.toFixed(6)},${c.lng.toFixed(6)}`;
  } catch(_) {}

  const sirenTag = p.siren ? `<span class="tag tag-blue">SIREN&nbsp;${p.siren}</span>` : '';
  const feedbackTag = `<span id="tag-${domId}" class="tag ${feedbackTagClass}">${feedbackLabel}</span>`;

  const PROP_TYPE_CLASSES = { 'public': 'tag-green', 'semi-public': 'tag-orange', 'privé': 'tag-gray', 'indéterminé': 'tag-gray' };
  const PROP_TYPE_LABELS  = { 'public': '🏛️ Public', 'semi-public': '🏢 Semi-public', 'privé': '🔒 Privé', 'indéterminé': '❓ Inconnu' };
  const propType = p.proprietaire_type || null;
  const propTypeTag = propType
    ? `<span class="tag ${PROP_TYPE_CLASSES[propType] || 'tag-gray'}">${PROP_TYPE_LABELS[propType] || propType}</span>`
    : '';

  const links = [
    p.siren ? `<a class="popup-link" href="https://annuaire-entreprises.data.gouv.fr/entreprise/${p.siren}" target="_blank" rel="noopener">🔍 Fiche entreprise →</a>` : '',
    gmaps   ? `<a class="popup-link" href="${gmaps}" target="_blank" rel="noopener">📍 Google Maps →</a>` : '',
  ].filter(Boolean).join('');

  return `
    <div class="popup-body">
      <div class="popup-title">Terrain pâturable</div>
      <div class="popup-grid">
        <span class="k">Commune</span>       <span class="v">${commune}</span>
        <span class="k">Surface totale</span><span class="v">${totalM2}</span>
        <span class="k">Végét. pâturable</span>  <span class="v">${prairieM2} · ${pct}</span>
        ${csRows}
        <span class="k">Propriétaire</span>  <span class="v">${own}</span>
      </div>
      <div class="popup-tags">${feedbackTag}${propTypeTag}${sirenTag}</div>
      <div class="popup-feedback">
        <div class="popup-feedback-title">Avis contributeurs</div>
        <div class="popup-feedback-status" id="status-${domId}">${feedbackLabel}</div>
        <div class="popup-feedback-actions">
          <button class="popup-feedback-btn ${feedback.status === 'yes' ? 'active' : ''}" data-feedback-id="${domId}" data-status="yes" onclick="setParcelStatus('${parcelIdEsc}', 'yes')">✅ Pâturable</button>
          <button class="popup-feedback-btn ${feedback.status === 'no' ? 'active' : ''}" data-feedback-id="${domId}" data-status="no" onclick="setParcelStatus('${parcelIdEsc}', 'no')">🚫 Non</button>
          <button class="popup-feedback-btn ${feedback.status === 'unknown' ? 'active' : ''}" data-feedback-id="${domId}" data-status="unknown" onclick="setParcelStatus('${parcelIdEsc}', 'unknown')">⏺️ Indécis</button>
        </div>
        <div class="popup-comment-list" id="comments-${domId}">${renderCommentsHtml(parcelId)}</div>
        <div class="popup-comment-form">
          <input id="commenter-${domId}" class="popup-comment-input" placeholder="Nom du contributeur" />
          <textarea id="comment-${domId}" class="popup-feedback-text" rows="3" placeholder="Commentaire (terrain, accès, clôture…)"></textarea>
        <button class="popup-feedback-save" onclick="addParcelComment('${parcelIdEsc}')">Ajouter</button>
        </div>
      </div>
    </div>
    ${links ? `<div class="popup-footer">${links}</div>` : ''}`;
}

// ── Onglets sidebar ───────────────────────────────────────────────────────
function switchTab(tab) {
  document.querySelectorAll('.sidebar-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tab));
  document.querySelectorAll('.sidebar-pane').forEach(p => p.classList.toggle('active', p.id === 'pane-' + tab));
  // Recadre la carte sur la sélection courante à chaque changement d'onglet
  if (currentLayer) {
    try { map.fitBounds(currentLayer.getBounds(), { padding: [20, 20], maxZoom: 14 }); } catch(_) {}
  }
}

// ── Mobile sidebar toggle ───────────────────────────────────────────────
const sidebarEl = document.getElementById('sidebar');
const overlayEl = document.getElementById('sidebar-overlay');
const mobileToggle = document.getElementById('mobile-toggle');

function closeSidebar() {
  if (!sidebarEl) return;
  sidebarEl.classList.remove('open');
  overlayEl && overlayEl.classList.remove('active');
}

function openSidebar() {
  if (!sidebarEl) return;
  sidebarEl.classList.add('open');
  overlayEl && overlayEl.classList.add('active');
}

if (mobileToggle) {
  mobileToggle.addEventListener('click', () => {
    if (sidebarEl && sidebarEl.classList.contains('open')) {
      closeSidebar();
    } else {
      openSidebar();
    }
  });
}
overlayEl && overlayEl.addEventListener('click', closeSidebar);
map.on('click', () => {
  if (window.innerWidth <= 768) closeSidebar();
});

// ── Info modal ──────────────────────────────────────────────────────────
const infoBtn = document.getElementById('map-info-btn');
const infoOverlay = document.getElementById('map-info-overlay');
const infoModal = document.getElementById('map-info-modal');
const infoClose = document.getElementById('map-info-close');

function openInfoModal() {
  if (!infoModal || !infoOverlay) return;
  infoModal.classList.add('active');
  infoOverlay.classList.add('active');
  infoModal.setAttribute('aria-hidden', 'false');
  infoOverlay.setAttribute('aria-hidden', 'false');
}

function closeInfoModal() {
  if (!infoModal || !infoOverlay) return;
  infoModal.classList.remove('active');
  infoOverlay.classList.remove('active');
  infoModal.setAttribute('aria-hidden', 'true');
  infoOverlay.setAttribute('aria-hidden', 'true');
}

if (infoBtn) infoBtn.addEventListener('click', openInfoModal);
if (infoClose) infoClose.addEventListener('click', closeInfoModal);
if (infoOverlay) infoOverlay.addEventListener('click', closeInfoModal);
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') closeInfoModal();
});

// ── Itinéraire ────────────────────────────────────────────────────────────
let routeLayer        = null;
let routeParcelsLayer = null;
let routeMarkers      = [];
let routeCoords       = [];   // polyligne du tracé ORS [{lat,lng},...] — utilisée pour le corridor
let geocodeCache      = {};
let lastPtA           = null;
let lastPtB           = null;
let selectedParcels   = [];   // [{feature, center, id}] — ajoutées par clic
let candidateParcels  = [];   // toutes les parcelles candidates visibles
let _wpCount          = 0;    // compteur IDs champs étapes

async function geocode(address) {
  if (geocodeCache[address]) return geocodeCache[address];
  const url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(address)}`;
  const res = await fetch(url, { headers: { 'Accept-Language': 'fr' } });
  const data = await res.json();
  if (!data.length) throw new Error(`Adresse introuvable : "${address}"`);
  const result = { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon), label: data[0].display_name };
  geocodeCache[address] = result;
  return result;
}

function haversine(a, b) {
  const R = 6371;
  const dLat = (b.lat - a.lat) * Math.PI / 180;
  const dLng = (b.lng - a.lng) * Math.PI / 180;
  const s = Math.sin(dLat/2)**2 + Math.cos(a.lat*Math.PI/180) * Math.cos(b.lat*Math.PI/180) * Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(s), Math.sqrt(1-s));
}

function distToSegment(p, a, b) {
  const dx = b.lng - a.lng, dy = b.lat - a.lat;
  if (dx === 0 && dy === 0) return haversine(p, a);
  const t = Math.max(0, Math.min(1, ((p.lng - a.lng)*dx + (p.lat - a.lat)*dy) / (dx*dx + dy*dy)));
  return haversine(p, { lat: a.lat + t*dy, lng: a.lng + t*dx });
}

// Distance minimale d'un point à une polyligne (tableau de {lat,lng})
function distToPolyline(p, coords) {
  let minD = Infinity;
  for (let i = 0; i < coords.length - 1; i++) {
    const d = distToSegment(p, coords[i], coords[i + 1]);
    if (d < minD) minD = d;
  }
  return minD;
}

function projectionOnSegment(p, a, b) {
  const dx = b.lng - a.lng, dy = b.lat - a.lat;
  if (dx === 0 && dy === 0) return 0;
  return Math.max(0, Math.min(1, ((p.lng - a.lng)*dx + (p.lat - a.lat)*dy) / (dx*dx + dy*dy)));
}

// Position cumulative d'un point projeté sur une polyligne (0 = départ, 1 = arrivée)
function projectionOnPolyline(p, coords) {
  if (coords.length < 2) return 0;
  let bestT = 0, bestSeg = 0, bestDist = Infinity;
  const segLengths = [];
  let totalLen = 0;
  for (let i = 0; i < coords.length - 1; i++) {
    const l = haversine(coords[i], coords[i + 1]);
    segLengths.push(l);
    totalLen += l;
  }
  let cumLen = 0;
  for (let i = 0; i < coords.length - 1; i++) {
    const t = projectionOnSegment(p, coords[i], coords[i + 1]);
    const proj = { lat: coords[i].lat + t*(coords[i+1].lat - coords[i].lat), lng: coords[i].lng + t*(coords[i+1].lng - coords[i].lng) };
    const d = haversine(p, proj);
    if (d < bestDist) {
      bestDist = d;
      bestSeg  = i;
      bestT    = t;
    }
  }
  let cumBefore = 0;
  for (let i = 0; i < bestSeg; i++) cumBefore += segLengths[i];
  return totalLen > 0 ? (cumBefore + bestT * segLengths[bestSeg]) / totalLen : 0;
}

function centroid(feature) {
  try {
    const b = L.geoJSON(feature).getBounds().getCenter();
    return { lat: b.lat, lng: b.lng };
  } catch(_) { return null; }
}

function orderAlongRoute(ptA, ptB, parcelles) {
  // Si le tracé ORS est disponible, ordonner le long de la vraie polyligne
  if (routeCoords.length >= 2) {
    return [...parcelles].sort((a, b) =>
      projectionOnPolyline(a.center, routeCoords) - projectionOnPolyline(b.center, routeCoords)
    );
  }
  // Fallback : ligne droite A→B
  return [...parcelles].sort((a, b) =>
    projectionOnSegment(a.center, ptA, ptB) - projectionOnSegment(b.center, ptA, ptB)
  );
}

function _clearRouteLayers() {
  if (routeLayer)        { map.removeLayer(routeLayer);        routeLayer = null; }
  if (routeParcelsLayer) { map.removeLayer(routeParcelsLayer); routeParcelsLayer = null; }
  map.eachLayer(l => { if (l._routeSelected) map.removeLayer(l); });
  routeMarkers.forEach(m => map.removeLayer(m));
  routeMarkers = [];
}

async function computeRoute(keepSelected = false) {
  const startAddr = document.getElementById('route-start').value.trim();
  const endAddr   = document.getElementById('route-end').value.trim();
  if (!startAddr || !endAddr) {
    setRouteStatus('Renseignez le départ et l\'arrivée.', 'err'); return;
  }

  if (!keepSelected) selectedParcels = [];

  const btn = document.getElementById('btn-route');
  btn.disabled = true; btn.textContent = '⏳ Calcul…';
  setRouteStatus('Géocodage des adresses…', '');

  try {
    // 1. Géocodage A, étapes fixes, B
    const [ptA, ptB] = await Promise.all([
      geocode(startAddr).then(r => { document.getElementById('status-start').className='route-status ok'; document.getElementById('status-start').textContent='✓ '+r.label.split(',')[0]; return r; }),
      geocode(endAddr).then(r   => { document.getElementById('status-end').className='route-status ok';   document.getElementById('status-end').textContent='✓ '+r.label.split(',')[0];   return r; }),
    ]);

    const wpInputs = [...document.querySelectorAll('.waypoint-input')];
    const fixedWaypoints = [];
    for (const inp of wpInputs) {
      const addr = inp.value.trim();
      const statusEl = inp.nextElementSibling;
      if (!addr) continue;
      try {
        const r = await geocode(addr);
        statusEl.className = 'route-status ok';
        statusEl.textContent = '✓ ' + r.label.split(',')[0];
        fixedWaypoints.push(r);
      } catch(e) {
        statusEl.className = 'route-status err';
        statusEl.textContent = '✗ Adresse introuvable';
      }
    }

    lastPtA = ptA; lastPtB = ptB;

    const minArea  = parseInt(document.getElementById('rte-area').value);
    const radiusKm = parseInt(document.getElementById('rte-radius').value);
    const orsKey   = window.ORS_API_KEY || '';
    const selectedIds = new Set(selectedParcels.map(p => p.id));

    // 2. Calcul du tracé de base A → étapes fixes → B pour obtenir la polyligne réelle
    setRouteStatus('Calcul du tracé de base…', '');
    const baseWaypoints = [ptA, ...fixedWaypoints, ptB];
    const baseRes = await fetch('https://api.openrouteservice.org/v2/directions/foot-hiking/geojson', {
      method: 'POST',
      headers: { 'Authorization': orsKey, 'Content-Type': 'application/json; charset=utf-8', 'Accept': 'application/json, application/geo+json' },
      body: JSON.stringify({ coordinates: baseWaypoints.map(p => [p.lng, p.lat]) })
    });
    const baseData = await baseRes.json();
    if (!baseRes.ok) throw new Error('ORS : ' + (baseData.error?.message || baseRes.status));

    // Stocker la polyligne du tracé de base comme référence pour le corridor
    routeCoords = baseData.features[0].geometry.coordinates.map(([lng, lat]) => ({ lat, lng }));

    // 3. Parcelles candidates dans le corridor autour du tracé réel
    // Requête Supabase sur toute la PACA via ST_DWithin (pas seulement les communes filtrées)
    setRouteStatus('Recherche des parcelles sur le trajet…', '');
    const routeLineGeoJSON = JSON.stringify(baseData.features[0].geometry);
    const { data: corridorRows, error: corridorErr } = await supabaseClient.rpc(
      'parcelles_dans_corridor',
      { route_geojson: routeLineGeoJSON, radius_km: radiusKm, min_prairie: minArea }
    );
    if (corridorErr) throw new Error('Corridor Supabase : ' + corridorErr.message);

    candidateParcels = (corridorRows || [])
      .filter(row => !selectedIds.has(row.id || ''))
      .map(row => {
        const feature = { type: 'Feature', properties: row, geometry: JSON.parse(row.geojson) };
        return { feature, center: centroid(feature), id: row.id || '' };
      })
      .filter(p => p.center !== null);

    // 4. Waypoints finaux : A → étapes fixes → parcelles sélectionnées ordonnées → B
    const orderedSelected = orderAlongRoute(ptA, ptB, selectedParcels);
    const waypoints = [ptA, ...fixedWaypoints, ...orderedSelected.map(p => p.center), ptB];

    // 5. Appel ORS final avec toutes les étapes
    setRouteStatus('Calcul de l\'itinéraire final…', '');
    const orsRes = await fetch('https://api.openrouteservice.org/v2/directions/foot-hiking/geojson', {
      method: 'POST',
      headers: {
        'Authorization': orsKey,
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json, application/geo+json'
      },
      body: JSON.stringify({ coordinates: waypoints.map(p => [p.lng, p.lat]) })
    });
    const orsData = await orsRes.json();
    if (!orsRes.ok) throw new Error('ORS : ' + (orsData.error?.message || orsRes.status));

    // Mettre à jour routeCoords avec le tracé final (incluant les étapes sélectionnées)
    routeCoords = orsData.features[0].geometry.coordinates.map(([lng, lat]) => ({ lat, lng }));

    const routeGeojson = orsData.features[0].geometry;
    const distKm = (orsData.features[0].properties.summary.distance / 1000).toFixed(1);
    const durMin = Math.round(orsData.features[0].properties.summary.duration / 60);
    const durH   = Math.floor(durMin / 60);
    const durStr = durH > 0 ? `${durH}h${String(durMin % 60).padStart(2,'0')}` : `${durMin} min`;

    // 5. Affichage carte
    _clearRouteLayers();
    if (currentLayer) map.removeLayer(currentLayer);

    routeLayer = L.geoJSON(routeGeojson, {
      style: { color: '#4ade80', weight: 4, opacity: 0.85, dashArray: '8 4' }
    }).addTo(map);

    // Parcelles candidates en orange — popup complète + bouton ajouter
    routeParcelsLayer = L.geoJSON(
      { type: 'FeatureCollection', features: candidateParcels.map(p => p.feature) },
      {
        style: { fillColor: '#fb923c', fillOpacity: 0.45, color: '#fb923c', weight: 1.5, opacity: 0.8 },
        onEachFeature: (feature, layer) => {
          const fid    = feature.properties?.id || '';
          const fidEsc = fid.replace(/'/g, "\\'");
          layer.bindPopup(() => {
            const fidEsc = fid.replace(/'/g, "\\'");
            const btn = `<div style="padding:0 0 6px">
              <button onclick="addParcelToRoute('${fidEsc}')"
                style="width:100%;padding:8px 0;background:#fb923c;color:#111;border:none;border-radius:7px;font-size:12px;font-weight:700;cursor:pointer">
                ➕ Ajouter à l'itinéraire
              </button>
            </div>`;
            return btn + buildPopup(feature);
          }, { maxWidth: 340, minWidth: 280 });
          layer.on('mouseover', function() { this.setStyle({ fillOpacity: 0.75, weight: 2.5 }); });
          layer.on('mouseout',  function() { routeParcelsLayer && routeParcelsLayer.resetStyle(this); });
        },
      }
    ).addTo(map);

    // Parcelles sélectionnées en vert
    if (orderedSelected.length) {
      const selLayer = L.geoJSON(
        { type: 'FeatureCollection', features: orderedSelected.map(p => p.feature) },
        {
          style: { fillColor: '#4ade80', fillOpacity: 0.7, color: '#fff', weight: 2, opacity: 0.9 },
          onEachFeature: (feature, layer) => {
            layer._routeSelected = true;
            layer.bindPopup(() => buildPopup(feature), { maxWidth: 300, minWidth: 260 });
            layer.on('mouseover', function() { this.setStyle({ fillOpacity: 0.9, weight: 3 }); });
          },
        }
      ).addTo(map);
      selLayer.eachLayer(l => { l._routeSelected = true; });
    }

    // Marqueurs
    const iconFor = (color, label) => L.divIcon({
      html: `<div style="background:${color};color:#111;width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;border:2px solid rgba(255,255,255,0.5);box-shadow:0 2px 8px rgba(0,0,0,.6)">${label}</div>`,
      className: '', iconSize: [28,28], iconAnchor: [14,14]
    });
    routeMarkers.push(L.marker([ptA.lat, ptA.lng], { icon: iconFor('#60a5fa','A') }).addTo(map).bindPopup('Départ'));
    fixedWaypoints.forEach((wp, i) => {
      routeMarkers.push(L.marker([wp.lat, wp.lng], { icon: iconFor('#a78bfa', i+1) }).addTo(map).bindPopup(wp.label.split(',')[0]));
    });
    orderedSelected.forEach((p, i) => {
      routeMarkers.push(
        L.marker([p.center.lat, p.center.lng], { icon: iconFor('#4ade80', fixedWaypoints.length + i + 1) })
          .addTo(map).bindPopup(buildPopup(p.feature), { maxWidth: 300 })
      );
    });
    routeMarkers.push(L.marker([ptB.lat, ptB.lng], { icon: iconFor('#f87171','B') }).addTo(map).bindPopup('Arrivée'));

    map.fitBounds(routeLayer.getBounds(), { padding: [30, 30] });

    // 6. Résumé sidebar
    const fixedStepsHtml = fixedWaypoints.map((wp, i) => `
      <div class="route-step">
        <div class="route-step-num" style="background:#a78bfa">${i+1}</div>
        <div class="route-step-info"><div class="route-step-name">${wp.label.split(',')[0]}</div><div class="route-step-meta">Étape fixe</div></div>
      </div>`).join('');

    const stepsHtml = orderedSelected.map((p, i) => {
      const props = p.feature.properties || {};
      const name  = props.denomination || 'Parcelle sans propriétaire';
      const comm  = props.nom_commune  || '';
      const pct   = props.pct_prairie  != null ? `${props.pct_prairie}% pâturable` : '';
      const area  = props.prairie_m2   != null ? `${Number(props.prairie_m2).toLocaleString('fr')} m² pât.` : '';
      const fid   = (p.id || '').replace(/'/g, "\\'");
      const num   = fixedWaypoints.length + i + 1;
      return `<div class="route-step" onclick="map.setView([${p.center.lat},${p.center.lng}],15)">
        <div class="route-step-num">${num}</div>
        <div class="route-step-info">
          <div class="route-step-name">${name}</div>
          <div class="route-step-meta">${[comm,pct,area].filter(Boolean).join(' · ')}</div>
        </div>
        <button class="route-step-exclude" title="Retirer de l'itinéraire" onclick="event.stopPropagation();removeParcelFromRoute('${fid}')">✕</button>
      </div>`;
    }).join('');

    const candidateHint = candidateParcels.length
      ? `<div style="font-size:10px;color:#fb923c;margin:6px 0 2px;text-align:center">🟠 ${candidateParcels.length} parcelle${candidateParcels.length>1?'s':''} disponible${candidateParcels.length>1?'s':''} — cliquer sur la carte pour ajouter</div>`
      : '';

    document.getElementById('route-result').innerHTML = `
      <div class="route-summary">
        <span><strong>${distKm} km</strong>distance</span>
        <span><strong>${durStr}</strong>durée est.</span>
        <span><strong>${fixedWaypoints.length + orderedSelected.length}</strong>étape${(fixedWaypoints.length+orderedSelected.length)!==1?'s':''}</span>
      </div>
      ${candidateHint}
      <div class="route-steps">
        <div class="route-step">
          <div class="route-step-num start">A</div>
          <div class="route-step-info"><div class="route-step-name">Départ</div><div class="route-step-meta">${startAddr}</div></div>
        </div>
        ${fixedStepsHtml}
        ${stepsHtml}
        <div class="route-step">
          <div class="route-step-num end">B</div>
          <div class="route-step-info"><div class="route-step-name">Arrivée</div><div class="route-step-meta">${endAddr}</div></div>
        </div>
      </div>`;

    const msg = orderedSelected.length
      ? `${orderedSelected.length} étape${orderedSelected.length>1?'s':''} · ${distKm} km · ${durStr}`
      : `Itinéraire direct · ${distKm} km · ${durStr}`;
    setRouteStatus(msg, 'ok');

  } catch(err) {
    setRouteStatus('Erreur : ' + err.message, 'err');
  } finally {
    btn.disabled = false; btn.textContent = '🐑 Calculer';
  }
}

function addParcelToRoute(fid) {
  const found = candidateParcels.find(p => p.id === fid);
  if (!found) return;
  selectedParcels.push(found);
  map.closePopup();
  computeRoute(true);
}

function removeParcelFromRoute(fid) {
  selectedParcels = selectedParcels.filter(p => p.id !== fid);
  computeRoute(true);
}

function addWaypointField() {
  _wpCount++;
  const id = 'wp-' + _wpCount;
  const div = document.createElement('div');
  div.id = id;
  div.style.cssText = 'display:flex;gap:4px;align-items:flex-start;margin-bottom:6px';
  div.innerHTML = `
    <div style="flex:1">
      <input class="route-input waypoint-input" placeholder="Étape : adresse ou lieu…" style="width:100%" />
      <div class="route-status"></div>
    </div>
    <button onclick="removeWaypointField('${id}')" style="background:none;border:none;color:#555;font-size:16px;cursor:pointer;padding:5px 2px;line-height:1;margin-top:1px" title="Supprimer">✕</button>`;
  document.getElementById('waypoints-list').appendChild(div);
}

function removeWaypointField(id) {
  const el = document.getElementById(id);
  if (el) el.remove();
}

function clearRoute() {
  _clearRouteLayers();
  selectedParcels  = [];
  candidateParcels = [];
  lastPtA = null; lastPtB = null;
  document.getElementById('route-result').innerHTML = '';
  setRouteStatus('', '');
  if (currentLayer && !map.hasLayer(currentLayer)) currentLayer.addTo(map);
}

function setRouteStatus(msg, cls) {
  const el = document.getElementById('status-route');
  if (!el) return;
  el.textContent = msg;
  el.className = 'route-status' + (cls ? ' ' + cls : '');
}

// ── Export CSV ────────────────────────────────────────────────────────────
function exportExcel() {
  const filtered = getFiltered();
  if (!filtered.length) { alert('Aucune zone à exporter.'); return; }

  const cols = ['commune', 'surface_m2', 'prairie_m2', 'pct_prairie', 'proprietaire', 'siren', 'type_proprietaire'];
  const rows = filtered.map(f => {
    const p = f.properties || {};
    return [
      p.nom_commune         || '',
      p.area_m2             || '',
      p.prairie_m2          ?? '',
      p.pct_prairie         ?? '',
      p.denomination        || '',
      p.siren               || '',
      p.proprietaire_type   || '',
    ];
  });

  const csv = [cols, ...rows].map(r =>
    r.map(v => `"${String(v).replace(/"/g, '""')}"`).join(';')
  ).join('\n');

  const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  const suffix = selectedCommunes.size === 1 ? `_${[...selectedCommunes][0]}` : '';
  a.download = `terrains_paturables${suffix}.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
}
