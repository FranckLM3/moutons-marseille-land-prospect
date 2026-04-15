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
    console.warn('[supabase] variables non configurées');
    return null;
  }
  if (!window.supabase || !window.supabase.createClient) {
    console.warn('[supabase] window.supabase non disponible — CDN non chargé ?', window.supabase);
    return null;
  }
  try {
    return window.supabase.createClient(url, key);
  } catch(e) {
    console.error('[supabase] createClient erreur:', e);
    return null;
  }
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
  // Pour les contextes onclick="func('${val}')": JS-escape d'abord (protège le
  // string literal JS), puis encode < et > pour l'attribut HTML.
  // NE PAS utiliser &#39; ici : le parser HTML décode les entités avant que le
  // moteur JS n'exécute l'handler, ce qui annule la protection.
  return String(value)
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'")
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
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
        <span class="popup-comment-author">${escapeHtml(comment.author || 'Anonyme')}</span>
        <span class="popup-comment-date">${formatCommentDate(comment.createdAt)}</span>
      </div>
      <div class="popup-comment-text">${escapeHtml(comment.message)}</div>
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

// ── Contrôle de visibilité des couches ───────────────────────────────────
(function _initLayerControl() {
  const LAYERS_CONFIG = [
    { key: 'terrains',   color: '#fb923c', label: 'Terrains pâturables' },
    { key: 'route',      color: '#f43f5e', label: 'Itinéraire'          },
    { key: 'communes',   color: '#60a5fa', label: 'Communes'            },
    { key: 'poi',        color: '#a78bfa', label: 'Points d\'intérêt'   },
    { key: 'vegetation', color: '#4ade80', label: 'Végétation OCS GE'   },
  ];

  const Ctrl = L.Control.extend({
    options: { position: 'topright' },
    onAdd() {
      const el = L.DomUtil.create('div', 'map-layer-control');
      L.DomEvent.disableClickPropagation(el);
      L.DomEvent.disableScrollPropagation(el);

      const header = L.DomUtil.create('div', 'mlc-header', el);
      header.innerHTML = '<span>🗺 Couches</span><span class="mlc-chevron">▾</span>';

      const body = L.DomUtil.create('div', 'mlc-body', el);
      body.innerHTML = LAYERS_CONFIG.map(({ key, color, label }) => {
        const isOn   = layerVisible[key] !== false;
        const opac   = isOn ? '1' : '0.45';
        const chk    = isOn ? 'checked' : '';
        return `<label class="layer-toggle-row" style="opacity:${opac}" title="Afficher/masquer ${label}">
          <span class="layer-dot" style="background:${color}"></span>
          <span style="flex:1">${label}</span>
          <input type="checkbox" class="layer-toggle-cb" data-key="${key}" ${chk} />
        </label>`;
      }).join('');

      // Toggle panel open/close
      let open = true;
      header.addEventListener('click', () => {
        open = !open;
        body.style.display       = open ? 'block' : 'none';
        header.querySelector('.mlc-chevron').textContent = open ? '▾' : '▸';
      });

      // Checkbox listeners
      body.querySelectorAll('.layer-toggle-cb').forEach(cb => {
        cb.addEventListener('change', () => toggleMapLayer(cb.dataset.key));
      });

      return el;
    },
  });
  new Ctrl().addTo(map);
})();

// ── Chargement ────────────────────────────────────────────────────────────
// Les données sont servies comme GeoJSON statiques par commune (docs/data/communes/)

// Cache des features déjà chargées par commune (évite les double-fetch)
const communeCache = {};   // { nomCommune: [feature, ...] }
let   communeIndex = [];   // [{ name, file, bbox: [minLng, minLat, maxLng, maxLat] }, ...]

async function loadCommuneList() {
  try {
    const res = await fetch('data/communes/index.json');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    communeIndex = await res.json();
    return communeIndex.map(e => e.name);
  } catch (err) {
    console.error('[communes] erreur chargement index.json:', err.message);
    return [];
  }
}

async function fetchParcellesByCommunes(communes) {
  const toFetch = communes.filter(c => !(c in communeCache));

  for (const name of toFetch) {
    communeCache[name] = [];  // pré-remplir pour éviter double-fetch concurrent
    const entry = communeIndex.find(e => e.name === name);
    if (!entry) { console.warn(`[communes] commune inconnue dans l'index: ${name}`); continue; }
    try {
      const res = await fetch(`data/communes/${entry.file}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const fc = await res.json();
      communeCache[name] = fc.features || [];
      console.log(`[communes] ${name}: ${communeCache[name].length} parcelles`);
    } catch (err) {
      console.error(`[communes] erreur chargement ${entry.file}:`, err.message);
    }
  }

  return communes.flatMap(c => communeCache[c] || []);
}

async function initData() {
  document.getElementById('loading').style.display = 'flex';

  try {
    allCommunes = await loadCommuneList();
    if (!allCommunes.length) throw new Error('index.json vide ou introuvable');

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
    console.error('Erreur chargement données :', err.message);
    document.getElementById('loading').innerHTML = `
      <div style="max-width:340px;text-align:center">
        <div style="font-size:32px;margin-bottom:12px;color:#f87171">⚑</div>
        <p style="color:#f87171;font-weight:700;font-size:14px;margin-bottom:8px">Données introuvables</p>
        <p style="color:#64748b;font-size:12px;line-height:1.6;margin-bottom:16px">
          Lancez <code>python scripts/build.py --split-communes</code> pour générer les fichiers communes.
        </p>
        <button onclick="location.reload()"
          style="padding:9px 24px;background:#4ade80;color:#111;border:none;border-radius:8px;font-size:13px;font-weight:700;cursor:pointer">
          Réessayer
        </button>
      </div>`;
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
    chip.addEventListener('click', () => {
      selectedCommunes.has(name) ? selectedCommunes.delete(name) : selectedCommunes.add(name);
      refreshCommuneChips();
      _scheduleApplyFilters(); // debounce 400ms — évite les rafales
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

function selectAllCommunes()   { allCommunes.forEach(c => selectedCommunes.add(c)); refreshCommuneChips(); _scheduleApplyFilters(); }
function deselectAllCommunes() { selectedCommunes.clear(); refreshCommuneChips(); _renderFilters(); } // deselect = données déjà en cache ou vide

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
  _renderFilters(); // pas de réseau — filtre local uniquement
});

const validatedOnlyEl = document.getElementById('validated-only');
if (validatedOnlyEl) {
  validatedOnlyEl.addEventListener('change', () => {
    showValidatedOnly = validatedOnlyEl.checked;
    _renderFilters();
  });
}

const ownerKnownOnlyEl = document.getElementById('owner-known-only');
if (ownerKnownOnlyEl) {
  ownerKnownOnlyEl.addEventListener('change', () => {
    showOwnerKnownOnly = ownerKnownOnlyEl.checked;
    _renderFilters();
  });
}

// Filtres type de propriétaire
document.querySelectorAll('.prop-type-check').forEach(checkbox => {
  checkbox.addEventListener('change', () => {
    selectedPropTypes.clear();
    document.querySelectorAll('.prop-type-check:checked').forEach(el => {
      selectedPropTypes.add(el.value);
    });
    _renderFilters();
  });
});

function selectAllPropTypes() {
  document.querySelectorAll('.prop-type-check').forEach(el => { el.checked = true; selectedPropTypes.add(el.value); });
  _renderFilters();
}
function deselectAllPropTypes() {
  document.querySelectorAll('.prop-type-check').forEach(el => { el.checked = false; });
  selectedPropTypes.clear();
  _renderFilters();
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
  layerVisible.vegetation = true;
  const cb = document.querySelector('.layer-toggle-cb[data-key="vegetation"]');
  if (cb) { cb.checked = true; cb.closest('.layer-toggle-row').style.opacity = '1'; }
  map.addLayer(ocsWmsLayer);
  updateLegendFromWms();
}

function hideOcsLayer() {
  layerVisible.vegetation = false;
  const cb = document.querySelector('.layer-toggle-cb[data-key="vegetation"]');
  if (cb) { cb.checked = false; cb.closest('.layer-toggle-row').style.opacity = '0.45'; }
  if (ocsWmsLayer && map.hasLayer(ocsWmsLayer)) map.removeLayer(ocsWmsLayer);
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
    // prairie_m2 peut être absent (colonne supprimée en DB) → calcul local
    const prairie_m2 = p.prairie_m2 != null ? p.prairie_m2 : (p.area_m2 || 0) * (p.pct_prairie || 0) / 100;
    if (minAreaHa > 0 && prairie_m2 < minAreaHa * 10000) return false;
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

// ── Rendu filtre (Leaflet uniquement, sans réseau) ───────────────────────
function _renderFilters() {
  const filtered = getFiltered();

  if (currentLayer) map.removeLayer(currentLayer);

  currentLayer = L.geoJSON({ type: 'FeatureCollection', features: filtered }, {
    style: feature => {
      const color = colorForPrairie(feature.properties?.pct_prairie);
      return { fillColor: color, fillOpacity: 0.45, color: color, weight: 1.2, opacity: 0.8 };
    },
    onEachFeature: (feature, layer) => {
      layer.bindPopup(() => buildPopup(feature), { maxWidth: 440, minWidth: 340 });
      layer.on('mouseover', function() { this.setStyle({ fillOpacity: 0.7, weight: 2 }); });
      layer.on('mouseout',  function() { currentLayer && currentLayer.resetStyle(this); });
    },
  });
  _addLayerIfVisible(currentLayer, 'terrains');

  if (filtered.length > 0) {
    const activeTab = document.querySelector('.sidebar-tab.active')?.dataset?.tab;
    if (activeTab !== 'route') {
      try { map.fitBounds(currentLayer.getBounds(), { padding: [20, 20], maxZoom: 14 }); } catch(_) {}
    }
  }

  const totalHa   = filtered.reduce((s, f) => {
    const p = f.properties || {};
    const pm2 = p.prairie_m2 != null ? p.prairie_m2 : (p.area_m2 || 0) * (p.pct_prairie || 0) / 100;
    return s + pm2;
  }, 0) / 10000;
  const withOwner = filtered.filter(f => f.properties?.denomination).length;
  const pct       = filtered.length ? Math.round(withOwner / filtered.length * 100) : 0;

  document.getElementById('count-total').textContent   = allFeatures.length.toLocaleString('fr');
  document.getElementById('count-visible').textContent = filtered.length.toLocaleString('fr');
  document.getElementById('stat-ha').textContent       = totalHa.toLocaleString('fr', { maximumFractionDigits: 0 }) + ' ha';
  document.getElementById('stat-pct').textContent      = pct + '%';

  // État vide
  const emptyEl = document.getElementById('empty-state');
  if (emptyEl) emptyEl.classList.toggle('visible', filtered.length === 0 && allFeatures.length > 0);
}

// Debounce pour applyFilters (évite les rafales lors du clic sur plusieurs communes)
let _applyFiltersTimer = null;
function _scheduleApplyFilters() {
  clearTimeout(_applyFiltersTimer);
  _applyFiltersTimer = setTimeout(() => applyFilters(), 400);
}

async function applyFilters() {
  clearTimeout(_applyFiltersTimer);
  // Charger uniquement les communes absentes du cache — sans réseau si déjà en cache
  allFeatures = await fetchParcellesByCommunes([...selectedCommunes]);
  _renderFilters();
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
  // prairie_m2 peut être absent après migration DB — calcul local depuis area_m2 × pct_prairie
  const rawPrairieM2 = p.prairie_m2 != null
    ? Number(p.prairie_m2)
    : Math.round((p.area_m2 || 0) * (p.pct_prairie || 0) / 100);
  const prairieM2 = `${rawPrairieM2.toLocaleString('fr')} m²`;
  const own       = p.denomination || '—';
  const commune   = p.nom_commune  || '—';
  // Titre contextuel : propriétaire si connu, sinon commune
  const popupTitle = p.denomination ? escapeHtml(p.denomination) : escapeHtml(commune);

  const parcelId = getParcelId(feature);
  const parcelIdEsc = escapeForAttr(parcelId);
  const domId = makeDomId(parcelId);
  const feedback = getParcelFeedback(parcelId);
  const feedbackLabel = feedback.status === 'yes' ? 'Pâturable' : feedback.status === 'no' ? 'Non pâturable' : 'Non évalué';
  const feedbackTagClass = feedback.status === 'yes' ? 'tag-green' : feedback.status === 'no' ? 'tag-red' : 'tag-gray';

  // Barre de prairie
  const pctNum = p.pct_prairie != null ? Math.round(p.pct_prairie) : 0;
  const pctLabel = `${pctNum} %`;

  // Détail végétation (masqué par défaut)
  let csDetailRows = '';
  try {
    const detail = typeof p.cs_detail === 'string' ? JSON.parse(p.cs_detail) : (p.cs_detail || {});
    const entries = Object.entries(detail).sort((a, b) => b[1] - a[1]);
    if (entries.length) {
      csDetailRows = entries.map(([code, m2]) => {
        const label = CS_LABELS[code] || code;
        const note = code === 'CS2.1.1.2'
          ? ' <span style="color:#f97316;font-size:0.85em">(non pâturable)</span>'
          : '';
        return `<span class="k" style="padding-left:12px;font-size:11px">${label}${note}</span><span class="v" style="font-size:11px;color:#94a3b8">${Number(m2).toLocaleString('fr')} m²</span>`;
      }).join('');
    }
  } catch(_) {}

  let gmaps = '';
  try {
    const c = L.geoJSON(feature).getBounds().getCenter();
    gmaps = `https://www.google.com/maps?q=${c.lat.toFixed(6)},${c.lng.toFixed(6)}`;
  } catch(_) {}

  const feedbackTag = `<span id="tag-${domId}" class="tag ${feedbackTagClass}">${feedbackLabel}</span>`;

  const PROP_TYPE_CLASSES = { 'public': 'tag-green', 'semi-public': 'tag-orange', 'privé': 'tag-gray', 'indéterminé': 'tag-gray' };
  const PROP_TYPE_LABELS  = { 'public': 'Public', 'semi-public': 'Semi-public', 'privé': 'Privé', 'indéterminé': 'Inconnu' };
  const propType = p.proprietaire_type || null;
  const propTypeTag = propType
    ? `<span class="tag ${PROP_TYPE_CLASSES[propType] || 'tag-gray'}">${PROP_TYPE_LABELS[propType] || propType}</span>`
    : '';

  // Détails techniques repliables
  const technicalDetails = [
    p.siren ? `<span class="k">SIREN</span><span class="v" style="color:#94a3b8">${escapeHtml(String(p.siren))}</span>` : '',
    csDetailRows,
  ].filter(Boolean).join('');

  const detailsToggle = technicalDetails ? `
    <button class="popup-details-toggle" onclick="
      const d=document.getElementById('pd-${domId}');
      d.classList.toggle('open');
      this.textContent=d.classList.contains('open')?'Masquer les détails':'Voir les détails techniques';
    ">Voir les détails techniques</button>
    <div class="popup-details" id="pd-${domId}">
      <div class="popup-grid" style="margin-top:6px">${technicalDetails}</div>
      ${p.siren ? `<div style="margin-top:8px"><a class="popup-link" href="https://annuaire-entreprises.data.gouv.fr/entreprise/${p.siren}" target="_blank" rel="noopener">Fiche entreprise →</a></div>` : ''}
    </div>` : '';

  const gmapsLink = gmaps ? `<a class="popup-link" href="${gmaps}" target="_blank" rel="noopener">Voir sur Google Maps →</a>` : '';

  return `
    <div class="popup-header">
      <div class="popup-title">${popupTitle}</div>
      <div class="popup-commune">${escapeHtml(commune)}</div>
    </div>
    <div class="popup-prairie-bar">
      <div class="popup-prairie-pct">${pctLabel}</div>
      <div class="popup-prairie-detail">
        <div class="popup-prairie-m2">${prairieM2} de prairie</div>
        <div class="popup-prairie-sub">sur ${totalM2} de surface totale</div>
        <div class="popup-prairie-bar-track">
          <div class="popup-prairie-bar-fill" style="width:${Math.min(pctNum,100)}%"></div>
        </div>
      </div>
    </div>
    <div class="popup-body">
      <div class="popup-grid">
        <span class="k">Propriétaire</span><span class="v">${escapeHtml(own)}</span>
      </div>
      <div class="popup-tags" style="margin-top:8px">${feedbackTag}${propTypeTag}</div>
      ${detailsToggle}
      <div class="popup-feedback">
        <div class="popup-feedback-title">Mon avis</div>
        <div class="popup-feedback-status" id="status-${domId}">${feedbackLabel}</div>
        <div class="popup-feedback-actions">
          <button class="popup-feedback-btn ${feedback.status === 'yes' ? 'active' : ''}" onclick="setParcelStatus('${parcelIdEsc}', 'yes')">Pâturable</button>
          <button class="popup-feedback-btn ${feedback.status === 'no' ? 'active' : ''}" onclick="setParcelStatus('${parcelIdEsc}', 'no')">Non</button>
          <button class="popup-feedback-btn ${feedback.status === 'unknown' ? 'active' : ''}" onclick="setParcelStatus('${parcelIdEsc}', 'unknown')">Indécis</button>
        </div>
        <div class="popup-comment-list" id="comments-${domId}">${renderCommentsHtml(parcelId)}</div>
        <div class="popup-comment-form">
          <input id="commenter-${domId}" class="popup-comment-input" placeholder="Votre nom (facultatif)" />
          <textarea id="comment-${domId}" class="popup-feedback-text" rows="3" placeholder="Accès, clôture, état du terrain…" maxlength="500"></textarea>
          <button class="popup-feedback-save" onclick="addParcelComment('${parcelIdEsc}')">Enregistrer</button>
        </div>
      </div>
    </div>
    ${gmapsLink ? `<div class="popup-footer">${gmapsLink}</div>` : ''}`;
}

// ── Onglets sidebar ───────────────────────────────────────────────────────
function switchTab(tab) {
  document.querySelectorAll('.sidebar-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tab));
  document.querySelectorAll('.sidebar-pane').forEach(p => p.classList.toggle('active', p.id === 'pane-' + tab));
  // Remonter en haut de la sidebar au changement d'onglet
  const scroll = document.getElementById('sidebar-scroll');
  if (scroll) scroll.scrollTop = 0;
  // Recadre sur la bonne couche selon l'onglet actif
  if (tab === 'route' && routeLayer) {
    try { map.fitBounds(routeLayer.getBounds(), { padding: [30, 30] }); } catch(_) {}
  } else if (tab === 'filters' && currentLayer) {
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

// ── Autocomplete adresse (API adresse data.gouv.fr) ───────────────────────
function setupAddressAutocomplete(input) {
  let _acTimer = null;
  let _acList  = null;

  function removeDropdown() {
    if (_acList) { _acList.remove(); _acList = null; }
  }

  function buildDropdown(items) {
    removeDropdown();
    if (!items.length) return;
    const ul = document.createElement('ul');
    ul.style.cssText = [
      'position:absolute','z-index:9999','background:#1e293b',
      'border:1px solid #334155','border-radius:6px','list-style:none',
      'margin:2px 0 0','padding:0','min-width:100%','max-height:220px',
      'overflow-y:auto','box-shadow:0 4px 16px rgba(0,0,0,.6)',
      'font-size:12px',
    ].join(';');

    items.forEach(item => {
      const li = document.createElement('li');
      li.textContent = item.label;
      li.style.cssText = 'padding:7px 10px;cursor:pointer;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis';
      li.addEventListener('mouseenter', () => li.style.background = '#334155');
      li.addEventListener('mouseleave', () => li.style.background = '');
      li.addEventListener('mousedown', e => {
        e.preventDefault(); // évite blur avant click
        input.value = item.label;
        // Pré-remplir le cache geocode avec les coords BAN → évite un appel Nominatim
        if (item.lat != null && item.lng != null) {
          geocodeCache[item.label] = { lat: item.lat, lng: item.lng, label: item.label };
        }
        removeDropdown();
      });
      ul.appendChild(li);
    });

    // Positionner sous l'input
    const wrap = input.closest('.ac-wrap') || input.parentElement;
    wrap.style.position = 'relative';
    wrap.appendChild(ul);
    _acList = ul;
  }

  input.addEventListener('input', () => {
    const q = input.value.trim();
    clearTimeout(_acTimer);
    if (q.length < 2) { removeDropdown(); return; }
    _acTimer = setTimeout(async () => {
      try {
        const url = `https://api-adresse.data.gouv.fr/search/?q=${encodeURIComponent(q)}&limit=6&autocomplete=1`;
        const r = await fetch(url);
        const d = await r.json();
        const items = (d.features || []).map(f => ({
          label: f.properties.label,
          lat: f.geometry.coordinates[1],
          lng: f.geometry.coordinates[0],
        }));
        buildDropdown(items);
      } catch(_) { removeDropdown(); }
    }, 250);
  });

  input.addEventListener('blur', () => {
    // Léger délai pour laisser le mousedown se déclencher
    setTimeout(removeDropdown, 150);
  });

  input.addEventListener('keydown', e => {
    if (!_acList) return;
    const items = [..._acList.querySelectorAll('li')];
    const cur   = items.findIndex(li => li.style.background === 'rgb(51, 65, 85)');
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      const next = items[(cur + 1) % items.length];
      items.forEach(li => li.style.background = '');
      next.style.background = '#334155';
      input.value = next.textContent;
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      const prev = items[(cur - 1 + items.length) % items.length];
      items.forEach(li => li.style.background = '');
      prev.style.background = '#334155';
      input.value = prev.textContent;
    } else if (e.key === 'Enter' || e.key === 'Tab') {
      removeDropdown();
    } else if (e.key === 'Escape') {
      removeDropdown();
    }
  });
}

// ── Welcome overlay ──────────────────────────────────────────────────────
function closeWelcome() {
  const el = document.getElementById('welcome-overlay');
  if (el) el.classList.add('hidden');
  try { localStorage.setItem('welcome-seen-v1', '1'); } catch(_) {}
}

function _initWelcome() {
  let seen = false;
  try { seen = Boolean(localStorage.getItem('welcome-seen-v1')); } catch(_) {}
  const el = document.getElementById('welcome-overlay');
  if (!el) return;
  if (seen) {
    el.classList.add('hidden');
  }
  // Fermer en cliquant sur le fond
  el.addEventListener('click', e => { if (e.target === el) closeWelcome(); });
}

// Appliquer l'autocomplete aux champs fixes au démarrage de la page
document.addEventListener('DOMContentLoaded', () => {
  _initWelcome();

  // Autocomplete adresses
  ['route-start', 'route-end'].forEach(id => {
    const el = document.getElementById(id);
    if (el) setupAddressAutocomplete(el);
  });

  // Forcer les sliders à leur valeur par défaut (évite restauration navigateur)
  const areaSlider = document.getElementById('area-slider');
  if (areaSlider) {
    areaSlider.value = 5000;
    minAreaHa = 0.5;
    const m2 = 5000;
    document.getElementById('area-value').textContent = `${m2.toLocaleString('fr')} m²`;
  }
  const rteArea = document.getElementById('rte-area');
  if (rteArea) {
    rteArea.value = 2000;
    document.getElementById('rte-area-val').textContent = '2 000 m²';
  }
});


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
  // Calcul géométrique direct — évite de créer un layer Leaflet pour chaque feature
  try {
    const geom = feature?.geometry;
    if (!geom?.coordinates) return null;
    let ring;
    if (geom.type === 'Polygon')           ring = geom.coordinates[0];
    else if (geom.type === 'MultiPolygon') ring = geom.coordinates[0][0];
    else return null;
    if (!ring?.length) return null;
    let sumLng = 0, sumLat = 0;
    for (const [lng, lat] of ring) { sumLng += lng; sumLat += lat; }
    return { lat: sumLat / ring.length, lng: sumLng / ring.length };
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

// ── Corridor in-memory ────────────────────────────────────────────────────

function bboxIntersects(bboxA, bboxB) {
  // bboxA, bboxB = [minLng, minLat, maxLng, maxLat]
  return bboxA[0] <= bboxB[2] && bboxA[2] >= bboxB[0] &&
         bboxA[1] <= bboxB[3] && bboxA[3] >= bboxB[1];
}

function _routeBbox(coords) {
  // coords = [[lng, lat], ...] (format ORS)
  let minLng = Infinity, minLat = Infinity, maxLng = -Infinity, maxLat = -Infinity;
  for (const [lng, lat] of coords) {
    if (lng < minLng) minLng = lng;
    if (lat < minLat) minLat = lat;
    if (lng > maxLng) maxLng = lng;
    if (lat > maxLat) maxLat = lat;
  }
  const pad = 0.05; // ~5 km de marge
  return [minLng - pad, minLat - pad, maxLng + pad, maxLat + pad];
}

async function _loadCommunesForRouteBbox(coords) {
  const routeBbox = _routeBbox(coords);
  const toLoad = communeIndex.filter(c => bboxIntersects(c.bbox, routeBbox) && !(c.name in communeCache));
  if (!toLoad.length) return;

  console.log(`[corridor] chargement ${toLoad.length} communes dans le corridor…`);
  await Promise.all(toLoad.map(async c => {
    communeCache[c.name] = [];  // pré-remplir pour éviter double-fetch concurrent
    try {
      const res = await fetch(`data/communes/${c.file}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const fc = await res.json();
      communeCache[c.name] = fc.features || [];
      console.log(`[corridor] ${c.name}: ${communeCache[c.name].length} parcelles`);
    } catch (err) {
      console.warn(`[corridor] erreur chargement ${c.file}:`, err.message);
    }
  }));
}

// Sous-échantillonne uniformément une polyligne (garde premier + dernier + N-2 intermédiaires)
function _downsampleCoords(coords, maxPts = 30) {
  if (coords.length <= maxPts) return coords;
  const result = [coords[0]];
  const step = (coords.length - 1) / (maxPts - 1);
  for (let i = 1; i < maxPts - 1; i++) result.push(coords[Math.round(i * step)]);
  result.push(coords[coords.length - 1]);
  return result;
}

async function _findCorridorInMemory(allCoords, radiusKm, minArea) {
  // 1. Charger les communes dont la bbox intersecte le corridor
  await _loadCommunesForRouteBbox(allCoords);

  // 2. Polyligne simplifiée à 30 pts pour distToPolyline (réduction ~16x du nb de segments)
  const simplCoords  = _downsampleCoords(allCoords, 30);
  const polyline     = simplCoords.map(([lng, lat]) => ({ lat, lng }));

  // 3. Bbox du corridor élargie de radiusKm (pré-filtre bbox O(1) avant distance O(n))
  const DEG_PER_KM   = 1 / 111;
  const pad          = radiusKm * DEG_PER_KM;
  const rb           = _routeBbox(allCoords);
  const corridorBbox = [rb[0] - pad, rb[1] - pad, rb[2] + pad, rb[3] + pad];

  // 4. Filtrer toutes les features du cache
  const seen    = new Set();
  const results = [];

  for (const features of Object.values(communeCache)) {
    for (const feature of features) {
      const id = feature.properties?.id;
      if (!id || seen.has(id)) continue;
      if ((feature.properties?.area_m2 || 0) < minArea) continue;

      const c = centroid(feature);
      if (!c) continue;

      // Pré-filtre bbox : élimine ~80 % des features sans calcul de distance
      if (c.lng < corridorBbox[0] || c.lng > corridorBbox[2] ||
          c.lat < corridorBbox[1] || c.lat > corridorBbox[3]) continue;

      if (distToPolyline(c, polyline) <= radiusKm) {
        seen.add(id);
        results.push(feature);
      }
    }
  }

  console.log(`[corridor] in-memory: ${results.length} parcelles dans ${radiusKm} km (polyline ${allCoords.length}→${simplCoords.length} pts)`);
  return results;
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
    console.log('[computeRoute] démarrage', { startAddr, endAddr, keepSelected });
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

    const minArea   = parseInt(document.getElementById('rte-area').value);
    const radiusKm  = parseFloat(document.getElementById('rte-radius').value);
    const orsProfile = 'foot-hiking'; // transhumance à pied
    const orsKey   = window.ORS_API_KEY || '';
    const selectedIds = new Set(selectedParcels.map(p => p.id));

    // 2. Calcul du tracé de base A → étapes fixes → B pour obtenir la polyligne réelle
    setRouteStatus('Calcul du tracé de base…', '');
    const baseWaypoints = [ptA, ...fixedWaypoints, ptB];
    const baseRes = await fetch(`https://api.openrouteservice.org/v2/directions/${orsProfile}/geojson`, {
      method: 'POST',
      headers: { 'Authorization': orsKey, 'Content-Type': 'application/json; charset=utf-8', 'Accept': 'application/json, application/geo+json' },
      body: JSON.stringify({ coordinates: baseWaypoints.map(p => [p.lng, p.lat]) })
    });
    const baseData = await baseRes.json();
    if (!baseRes.ok) throw new Error('ORS : ' + (baseData.error?.message || baseRes.status));
    console.log('[computeRoute] ORS tracé de base OK,', baseData.features[0].geometry.coordinates.length, 'points');

    // Stocker la polyligne du tracé de base comme référence pour le corridor
    routeCoords = baseData.features[0].geometry.coordinates.map(([lng, lat]) => ({ lat, lng }));

    // 3. Parcelles candidates dans le corridor autour du tracé réel
    // Si recalcul après ajout d'une étape, on réutilise le corridor déjà chargé (évite timeout RPC)
    if (!keepSelected) {
      setRouteStatus('Recherche des parcelles sur le trajet…', '');

      const allCoords = baseData.features[0].geometry.coordinates;
      const corridorFeatures = await _findCorridorInMemory(allCoords, radiusKm, minArea);

      candidateParcels = corridorFeatures
        .map(feature => ({ feature, center: centroid(feature), id: feature.properties?.id || '' }))
        .filter(p => p.center !== null);
    }
    // Exclure les parcelles déjà sélectionnées + filtrer par type de propriétaire
    const displayCandidates = candidateParcels.filter(p => !selectedIds.has(p.id));

    // 4. Waypoints finaux : A → étapes fixes → parcelles sélectionnées ordonnées → B
    const orderedSelected = orderAlongRoute(ptA, ptB, selectedParcels);
    const waypoints = [ptA, ...fixedWaypoints, ...orderedSelected.map(p => p.center), ptB];

    // 5. Appel ORS final avec toutes les étapes
    setRouteStatus('Calcul de l\'itinéraire final…', '');
    const orsRes = await fetch(`https://api.openrouteservice.org/v2/directions/${orsProfile}/geojson`, {
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
    const distKm  = (orsData.features[0].properties.summary.distance / 1000).toFixed(1);
    // Vitesse troupeau ~3 km/h (vs 5 km/h randonneur solo)
    const troupeauDurMin = Math.round((orsData.features[0].properties.summary.distance / 1000) / 3 * 60);
    const troupeauDays   = (orsData.features[0].properties.summary.distance / 1000 / 20).toFixed(1); // ~20 km/jour
    const durH  = Math.floor(troupeauDurMin / 60);
    const durStr = durH > 0 ? `${durH}h${String(troupeauDurMin % 60).padStart(2,'0')}` : `${troupeauDurMin} min`;
    // Dénivelé depuis ORS (ascent/descent en mètres)
    const ascent  = Math.round(orsData.features[0].properties.summary.ascent  || 0);
    const descent = Math.round(orsData.features[0].properties.summary.descent || 0);

    // 5. Affichage carte
    _clearRouteLayers();
    // Effacer les parcelles du filtre (onglet carte) pour ne garder que l'itinéraire
    if (currentLayer) { map.removeLayer(currentLayer); currentLayer = null; }

    routeLayer = _addLayerIfVisible(L.geoJSON(routeGeojson, {
      style: { color: '#f43f5e', weight: 4, opacity: 0.9, dashArray: '8 4' }
    }), 'route');

    // Parcelles candidates en orange — popup complète + bouton ajouter
    routeParcelsLayer = _addLayerIfVisible(L.geoJSON(
      { type: 'FeatureCollection', features: displayCandidates.map(p => p.feature) },
      {
        style: { fillColor: '#fb923c', fillOpacity: 0.45, color: '#fb923c', weight: 1.5, opacity: 0.8 },
        onEachFeature: (feature, layer) => {
          const fid = feature.properties?.id || '';
          layer.bindPopup(() => {
            const fidEsc = escapeForAttr(fid);
            const btn = `<div style="padding:0 0 6px">
              <button onclick="addParcelToRoute('${fidEsc}')"
                style="width:100%;padding:8px 0;background:#fb923c;color:#111;border:none;border-radius:7px;font-size:12px;font-weight:700;cursor:pointer">
                ➕ Ajouter à l'itinéraire
              </button>
            </div>`;
            return btn + buildPopup(feature);
          }, { maxWidth: 440, minWidth: 340 });
          layer.on('mouseover', function() { this.setStyle({ fillOpacity: 0.75, weight: 2.5 }); });
          layer.on('mouseout',  function() { routeParcelsLayer && routeParcelsLayer.resetStyle(this); });
        },
      }
    ), 'terrains');

    // Parcelles sélectionnées en vert
    if (orderedSelected.length) {
      const selLayer = L.geoJSON(
        { type: 'FeatureCollection', features: orderedSelected.map(p => p.feature) },
        {
          style: { fillColor: '#4ade80', fillOpacity: 0.7, color: '#fff', weight: 2, opacity: 0.9 },
          onEachFeature: (feature, layer) => {
            layer._routeSelected = true;
            layer.bindPopup(() => buildPopup(feature), { maxWidth: 440, minWidth: 340 });
            layer.on('mouseover', function() { this.setStyle({ fillOpacity: 0.9, weight: 3 }); });
          },
        }
      );
      _addLayerIfVisible(selLayer, 'terrains');
      selLayer.eachLayer(l => { l._routeSelected = true; });
    }

    // Marqueurs
    const iconFor = (color, label) => L.divIcon({
      html: `<div style="background:${color};color:#111;width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;border:2px solid rgba(255,255,255,0.5);box-shadow:0 2px 8px rgba(0,0,0,.6)">${label}</div>`,
      className: '', iconSize: [28,28], iconAnchor: [14,14]
    });
    const _addMarker = (m) => { _addLayerIfVisible(m, 'route'); routeMarkers.push(m); };
    _addMarker(L.marker([ptA.lat, ptA.lng], { icon: iconFor('#60a5fa','A') }).bindPopup('Départ'));
    fixedWaypoints.forEach((wp, i) => {
      _addMarker(L.marker([wp.lat, wp.lng], { icon: iconFor('#a78bfa', i+1) }).bindPopup(wp.label.split(',')[0]));
    });
    orderedSelected.forEach((p, i) => {
      _addMarker(
        L.marker([p.center.lat, p.center.lng], { icon: iconFor('#4ade80', fixedWaypoints.length + i + 1) })
          .bindPopup(buildPopup(p.feature), { maxWidth: 440 })
      );
    });
    _addMarker(L.marker([ptB.lat, ptB.lng], { icon: iconFor('#f87171','B') }).bindPopup('Arrivée'));

    map.fitBounds(routeLayer.getBounds(), { padding: [30, 30] });

    // 6. Résumé sidebar
    const fixedStepsHtml = fixedWaypoints.map((wp, i) => `
      <div class="route-step">
        <div class="route-step-num" style="background:#a78bfa">${i+1}</div>
        <div class="route-step-info"><div class="route-step-name">${escapeHtml(wp.label.split(',')[0])}</div><div class="route-step-meta">Étape fixe</div></div>
      </div>`).join('');

    // Distances cumulées entre étapes le long du tracé ORS
    const allWaypts = [ptA, ...fixedWaypoints, ...orderedSelected.map(p => p.center), ptB];
    const stepDistKm = allWaypts.slice(0,-1).map((_, i) => {
      if (routeCoords.length < 2) return null;
      const projA = projectionOnPolyline(allWaypts[i],   routeCoords);
      const projB = projectionOnPolyline(allWaypts[i+1], routeCoords);
      return Math.abs(projB - projA) * parseFloat(distKm);
    });

    const stepsHtml = orderedSelected.map((p, i) => {
      const props   = p.feature.properties || {};
      const name    = escapeHtml(props.denomination || 'Parcelle sans propriétaire');
      const comm    = escapeHtml(props.nom_commune  || '');
      const pct     = props.pct_prairie != null ? `${props.pct_prairie}%` : '';
      const pm2raw  = props.prairie_m2 != null ? props.prairie_m2
                    : Math.round((props.area_m2 || 0) * (props.pct_prairie || 0) / 100);
      // Capacité : 1 mouton ≈ 150 m²/jour en prairie
      const moutons = pm2raw > 0 ? `~${Math.floor(pm2raw / 150)} moutons/j` : '';
      const stepIdx = fixedWaypoints.length + i; // index dans allWaypts (après ptA)
      const dkm     = stepDistKm[stepIdx] != null ? `${stepDistKm[stepIdx].toFixed(1)} km` : '';
      const fid     = escapeForAttr(p.id || '');
      const num     = fixedWaypoints.length + i + 1;
      return `<div class="route-step" onclick="map.setView([${p.center.lat},${p.center.lng}],15)">
        <div class="route-step-num">${num}</div>
        <div class="route-step-info">
          <div class="route-step-name">${name}</div>
          <div class="route-step-meta">${[comm, pct, moutons, dkm].filter(Boolean).join(' · ')}</div>
        </div>
        <button class="route-step-exclude" title="Retirer de l'itinéraire" onclick="event.stopPropagation();removeParcelFromRoute('${fid}')">✕</button>
      </div>`;
    }).join('');

    const candidateHint = displayCandidates.length
      ? `<div style="font-size:11px;color:var(--orange,#fb923c);margin:8px 0 2px;text-align:center;font-weight:600">${displayCandidates.length} terrain${displayCandidates.length>1?'s':''} disponible${displayCandidates.length>1?'s':''} — cliquez sur la carte pour ajouter</div>`
      : '';

    // Stocker le tracé pour export GPX
    window._lastRouteCoords = orsData.features[0].geometry.coordinates;
    window._lastRouteInfo   = { distKm, durStr, startAddr, endAddr };

    document.getElementById('route-result').innerHTML = `
      <div class="route-summary">
        <span><strong>${distKm} km</strong>distance</span>
        <span><strong>~${troupeauDays} j</strong>troupeau</span>
        <span><strong>${ascent > 0 ? '+'+ascent+'m' : durStr}</strong>${ascent > 0 ? 'dénivelé' : 'durée'}</span>
      </div>
      <div style="font-size:11px;color:var(--text-lo,#64748b);text-align:center;margin-bottom:6px">+${ascent} m / −${descent} m · 3 km/h (troupeau)</div>
      <button onclick="exportGPX()" style="width:100%;margin:4px 0 6px;padding:9px 0;background:rgba(96,165,250,0.1);border:1px solid rgba(96,165,250,0.3);color:#60a5fa;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer">Exporter GPX</button>
      ${candidateHint}
      <div class="route-steps">
        <div class="route-step">
          <div class="route-step-num start">A</div>
          <div class="route-step-info"><div class="route-step-name">Départ</div><div class="route-step-meta">${escapeHtml(startAddr)}</div></div>
        </div>
        ${fixedStepsHtml}
        ${stepsHtml}
        <div class="route-step">
          <div class="route-step-num end">B</div>
          <div class="route-step-info"><div class="route-step-name">Arrivée</div><div class="route-step-meta">${escapeHtml(endAddr)}</div></div>
        </div>
      </div>`;

    const msg = orderedSelected.length
      ? `${orderedSelected.length} étape${orderedSelected.length>1?'s':''} · ${distKm} km · ${durStr}`
      : `Itinéraire direct · ${distKm} km · ${durStr}`;
    setRouteStatus(msg, 'ok');

    // Auto-scroll vers les résultats
    const resultEl = document.getElementById('route-result');
    if (resultEl) setTimeout(() => resultEl.scrollIntoView({ behavior: 'smooth', block: 'start' }), 100);

  } catch(err) {
    console.error('[computeRoute] erreur:', err);
    setRouteStatus('Erreur : ' + err.message, 'err');
  } finally {
    btn.disabled = false; btn.textContent = "Calculer l'itinéraire";
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
    <div style="flex:1" class="ac-wrap">
      <input class="route-input waypoint-input" placeholder="Étape : adresse ou lieu…" style="width:100%" />
      <div class="route-status"></div>
    </div>
    <button onclick="removeWaypointField('${id}')" style="background:none;border:none;color:#555;font-size:16px;cursor:pointer;padding:5px 2px;line-height:1;margin-top:1px" title="Supprimer">✕</button>`;
  document.getElementById('waypoints-list').appendChild(div);
  // Activer l'autocomplete sur le nouvel input
  const inp = div.querySelector('.waypoint-input');
  if (inp) setupAddressAutocomplete(inp);
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
}

function setRouteStatus(msg, cls) {
  const el = document.getElementById('status-route');
  if (!el) return;
  el.textContent = msg;
  el.className = 'route-status' + (cls ? ' ' + cls : '');
}

// ── Export GPX ───────────────────────────────────────────────────────────
function exportGPX() {
  const coords = window._lastRouteCoords;
  if (!coords || !coords.length) { alert('Aucun itinéraire à exporter.'); return; }
  const info = window._lastRouteInfo || {};
  const trkpts = coords.map(([lng, lat, ele]) =>
    `    <trkpt lat="${lat}" lon="${lng}">${ele != null ? `<ele>${ele}</ele>` : ''}</trkpt>`
  ).join('\n');
  const gpx = `<?xml version="1.0" encoding="UTF-8"?>
<gpx version="1.1" creator="Moutons Marseillais" xmlns="http://www.topografix.com/GPX/1/1">
  <metadata><name>${escapeHtml(info.startAddr || 'Départ')} → ${escapeHtml(info.endAddr || 'Arrivée')}</name></metadata>
  <trk>
    <name>Moutons Marseillais — ${escapeHtml(info.distKm || '')} km</name>
    <trkseg>
${trkpts}
    </trkseg>
  </trk>
</gpx>`;
  const blob = new Blob([gpx], { type: 'application/gpx+xml' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'moutons-marseillais.gpx';
  a.click();
  URL.revokeObjectURL(a.href);
}

// ── Export Excel (.xlsx) via SheetJS ─────────────────────────────────────

/**
 * Génère et télécharge un fichier .xlsx depuis un tableau 2D (header + rows).
 * @param {Array<Array>} headerRow  - ligne d'en-tête
 * @param {Array<Array>} dataRows   - lignes de données
 * @param {string}       filename   - nom du fichier (sans extension)
 * @param {string}       sheetName  - nom de l'onglet
 */
function _exportXlsx(headerRow, dataRows, filename, sheetName = 'Données') {
  if (!window.XLSX) {
    alert('Bibliothèque Excel non chargée. Utilisez l\'export CSV.');
    return;
  }
  const aoa = [headerRow, ...dataRows];
  const ws  = XLSX.utils.aoa_to_sheet(aoa);

  // Largeur automatique : max(longueur header, max des valeurs) + 2
  const colWidths = headerRow.map((h, ci) => {
    const maxData = dataRows.reduce((m, r) => Math.max(m, String(r[ci] ?? '').length), 0);
    return { wch: Math.max(String(h).length, maxData) + 2 };
  });
  ws['!cols'] = colWidths;

  // En-tête en gras (style via SheetJS mini — limité mais fonctionnel)
  headerRow.forEach((_, ci) => {
    const cellAddr = XLSX.utils.encode_cell({ r: 0, c: ci });
    if (ws[cellAddr]) ws[cellAddr].s = { font: { bold: true } };
  });

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, sheetName);
  XLSX.writeFile(wb, `${filename}.xlsx`);
}

function exportExcel() {
  const filtered = getFiltered();
  if (!filtered.length) { alert('Aucune zone à exporter.'); return; }

  const header = ['Commune', 'Surface (m²)', 'Prairie (m²)', '% Prairie', 'Propriétaire', 'SIREN', 'Type propriétaire'];
  const rows   = filtered.map(f => {
    const p = f.properties || {};
    const pm2 = p.prairie_m2 ?? Math.round((p.area_m2 || 0) * (p.pct_prairie || 0) / 100);
    return [
      p.nom_commune       || '',
      p.area_m2           ?? '',
      pm2                 || '',
      p.pct_prairie       ?? '',
      p.denomination      || '',
      p.siren             || '',
      p.proprietaire_type || '',
    ];
  });

  const suffix = selectedCommunes.size === 1 ? `_${[...selectedCommunes][0]}` : '';
  _exportXlsx(header, rows, `terrains_paturables${suffix}`, 'Terrains');
}

// ══════════════════════════════════════════════════════════════════════════════
// Feature KML/GPX — Communes du trajet
// ══════════════════════════════════════════════════════════════════════════════

// ── État global ───────────────────────────────────────────────────────────────
let kmlLoadedFiles   = [];   // [{ name, coords: [[lng,lat],...] }]
let kmlLocalities    = [];   // résultat final
let kmlPoiData       = null; // { eau: [...], haltes: [...], ravitaillement: [...] }
let kmlMarkersLayer  = null; // LayerGroup Leaflet pour les marqueurs communes
let kmlRouteLayer    = null; // Polyline de prévisualisation du tracé fusionné
let kmlPoiLayer      = null; // LayerGroup Leaflet pour les marqueurs POI
let kmlParcelLayer   = null; // Parcelles corridor KML
let kmlAbortFlag     = false;

// ── Cache polygones communes (chargé une fois depuis docs/data/geo/communes-paca.geojson) ─
let _communesGeoCache = null;   // FeatureCollection complète
let _communesGeoLoading = null; // Promise en cours (évite double-fetch)

async function loadCommunesGeo() {
  if (_communesGeoCache) return _communesGeoCache;
  if (_communesGeoLoading) return _communesGeoLoading;
  _communesGeoLoading = fetch('data/geo/communes-paca.geojson')
    .then(r => r.ok ? r.json() : null)
    .then(data => { _communesGeoCache = data; _communesGeoLoading = null; return data; })
    .catch(() => { _communesGeoLoading = null; return null; });
  return _communesGeoLoading;
}

// Ray-casting point-in-polygon (coordonnées GeoJSON [lng, lat])
function _pointInRing(lat, lng, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i], [xj, yj] = ring[j];
    if (((yi > lat) !== (yj > lat)) && (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi))
      inside = !inside;
  }
  return inside;
}

function _pointInGeometry(lat, lng, geometry) {
  if (!geometry) return false;
  if (geometry.type === 'Polygon')
    return _pointInRing(lat, lng, geometry.coordinates[0]);
  if (geometry.type === 'MultiPolygon')
    return geometry.coordinates.some(poly => _pointInRing(lat, lng, poly[0]));
  return false;
}

// ── Source de l'itinéraire ────────────────────────────────────────────────────
let routeMode = 'ors'; // 'ors' | 'kml'

// ── Visibilité des couches cartographiques ────────────────────────────────────
const layerVisible = { terrains: false, route: true, communes: true, poi: true, vegetation: true };

function _getLayerRefs(key) {
  const refs = [];
  switch(key) {
    case 'terrains':
      if (currentLayer)       refs.push(currentLayer);
      if (routeParcelsLayer)  refs.push(routeParcelsLayer);
      if (kmlParcelLayer)     refs.push(kmlParcelLayer);
      map.eachLayer(l => { if (l._routeSelected) refs.push(l); });
      break;
    case 'route':
      if (routeLayer)     refs.push(routeLayer);
      if (kmlRouteLayer)  refs.push(kmlRouteLayer);
      routeMarkers.forEach(m => refs.push(m));
      break;
    case 'communes':
      if (kmlMarkersLayer)        refs.push(kmlMarkersLayer);
      if (kmlCommunePolygonLayer) refs.push(kmlCommunePolygonLayer);
      break;
    case 'poi':
      if (kmlPoiLayer) refs.push(kmlPoiLayer);
      break;
    case 'vegetation':
      if (ocsWmsLayer) refs.push(ocsWmsLayer);
      break;
  }
  return refs;
}

function _addLayerIfVisible(layer, key) {
  if (layerVisible[key] !== false) layer.addTo(map);
  return layer;
}

function toggleMapLayer(key) {
  layerVisible[key] = !layerVisible[key];
  _getLayerRefs(key).forEach(l => {
    if (layerVisible[key]) { if (!map.hasLayer(l)) map.addLayer(l); }
    else                   { if (map.hasLayer(l))  map.removeLayer(l); }
  });
  const cb = document.querySelector(`.layer-toggle-cb[data-key="${key}"]`);
  if (cb) cb.checked = layerVisible[key];
  const row = cb && cb.closest('.layer-toggle-row');
  if (row) row.style.opacity = layerVisible[key] ? '1' : '0.45';
}

function switchRouteMode(mode) {
  if (routeMode === mode) return;
  routeMode = mode;
  document.querySelectorAll('.route-mode-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.mode === mode);
  });
  document.getElementById('route-ors-section').style.display = mode === 'ors' ? '' : 'none';
  document.getElementById('route-kml-section').style.display = mode === 'kml' ? '' : 'none';
  // Masquer les parcelles du filtre quand on entre dans le mode itinéraire
  if (mode === 'kml' && currentLayer) {
    map.removeLayer(currentLayer); currentLayer = null;
  }
}

// ── Parsing KML ───────────────────────────────────────────────────────────────
function parseKml(text) {
  const doc = new DOMParser().parseFromString(text, 'application/xml');
  const coords = [];
  doc.querySelectorAll('LineString coordinates, MultiGeometry LineString coordinates').forEach(el => {
    const raw = el.textContent.trim();
    raw.split(/\s+/).forEach(triplet => {
      const parts = triplet.split(',');
      if (parts.length >= 2) {
        const lng = parseFloat(parts[0]), lat = parseFloat(parts[1]);
        if (!isNaN(lng) && !isNaN(lat)) coords.push([lng, lat]);
      }
    });
  });
  return coords;
}

// ── Parsing GPX ───────────────────────────────────────────────────────────────
function parseGpx(text) {
  const doc = new DOMParser().parseFromString(text, 'application/xml');
  const coords = [];

  // 1. Route points
  const rtepts = doc.querySelectorAll('rte rtept');
  if (rtepts.length) {
    rtepts.forEach(pt => {
      const lat = parseFloat(pt.getAttribute('lat'));
      const lng = parseFloat(pt.getAttribute('lon'));
      if (!isNaN(lat) && !isNaN(lng)) coords.push([lng, lat]);
    });
    return coords;
  }

  // 2. Track points (multi-segments concaténés dans l'ordre)
  const trkpts = doc.querySelectorAll('trk trkseg trkpt');
  if (trkpts.length) {
    trkpts.forEach(pt => {
      const lat = parseFloat(pt.getAttribute('lat'));
      const lng = parseFloat(pt.getAttribute('lon'));
      if (!isNaN(lat) && !isNaN(lng)) coords.push([lng, lat]);
    });
    return coords;
  }

  // 3. Waypoints en dernier recours
  doc.querySelectorAll('wpt').forEach(pt => {
    const lat = parseFloat(pt.getAttribute('lat'));
    const lng = parseFloat(pt.getAttribute('lon'));
    if (!isNaN(lat) && !isNaN(lng)) coords.push([lng, lat]);
  });
  return coords;
}

// ── Merge et ordonnancement multi-fichiers ────────────────────────────────────
function mergeAndOrderCoords(filesData) {
  if (filesData.length === 1) return filesData[0].coords;

  const result = [...filesData[0].coords];
  for (let i = 1; i < filesData.length; i++) {
    const next = filesData[i].coords;
    if (!next.length) continue;
    const lastPt   = result[result.length - 1];
    const firstPt  = next[0];
    const lastPt2  = next[next.length - 1];
    const dFirst   = haversineMeters(lastPt[0], lastPt[1], firstPt[0], firstPt[1]);
    const dLast    = haversineMeters(lastPt[0], lastPt[1], lastPt2[0], lastPt2[1]);
    // Si le dernier point du tronçon est plus proche → inverser
    const ordered  = dLast < dFirst ? [...next].reverse() : next;
    result.push(...ordered);
  }
  return result;
}

// ── Haversine ─────────────────────────────────────────────────────────────────
function haversineMeters(lng1, lat1, lng2, lat2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 +
            Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

function totalRouteKm(coords) {
  let dist = 0;
  for (let i = 1; i < coords.length; i++) {
    dist += haversineMeters(coords[i-1][0], coords[i-1][1], coords[i][0], coords[i][1]);
  }
  return Math.round(dist / 100) / 10;
}

// ── Échantillonnage du tracé ──────────────────────────────────────────────────
function sampleRoutePoints(coords, stepMeters = 500) {
  if (!coords.length) return [];
  const result = [coords[0]];
  let accumulated = 0;
  for (let i = 1; i < coords.length; i++) {
    const d = haversineMeters(coords[i-1][0], coords[i-1][1], coords[i][0], coords[i][1]);
    accumulated += d;
    if (accumulated >= stepMeters) {
      result.push(coords[i]);
      accumulated = 0;
    }
  }
  const last = coords[coords.length - 1];
  if (result[result.length - 1] !== last) result.push(last);
  return result;
}

// ── Reverse geocoding (api-adresse.data.gouv.fr — France uniquement, CORS OK) ─
async function reverseGeocodePoint(lng, lat) {
  try {
    const url = `https://api-adresse.data.gouv.fr/reverse/?lon=${lng}&lat=${lat}&limit=3`;
    const res = await fetch(url);
    if (res.ok) {
      const json = await res.json();
      const feats = json.features || [];
      // Prefer locality then municipality over street/housenumber
      const best  = feats.find(f => f.properties.type === 'locality')
                 || feats.find(f => f.properties.type === 'municipality')
                 || feats[0];
      if (best) {
        const p    = best.properties;
        const type = p.type || 'municipality';
        const insee = p.citycode || '';
        const dept  = _parseDept(p.postcode || insee);
        const name  = (type === 'locality') ? (p.name || p.city) : (p.city || p.municipality || p.name);
        if (name) return { name, insee, dept, type };
      }
    }
  } catch(_) {}
  return null;
}

function _parseDept(code) {
  if (!code) return '';
  const s = String(code).replace(/\s/g, '');
  if (s.startsWith('2A') || s.startsWith('2B')) return s.slice(0, 2);
  const n = s.slice(0, 2);
  return n || '';
}

// ── Détection des communes via point-in-polygon sur données pré-chargées ──────
async function extractLocalities(allCoords, onProgress) {
  if (kmlAbortFlag) return [];
  onProgress(1, 3);

  // Chargement du cache communes (une seule fois, ensuite instantané)
  const geo = await loadCommunesGeo();
  onProgress(2, 3);

  if (!geo || kmlAbortFlag) {
    // Fallback si le fichier statique est absent
    return _extractLocalitiesFallback(allCoords, onProgress);
  }

  // Pré-filtre bbox : on ne garde que les communes dont la bbox intersecte le tracé élargi
  const routeBbox  = routeBboxWithMargin(allCoords, 0.01);
  const candidates = geo.features.filter(f => {
    const b = f.properties?.bbox;
    return b && b[0] <= routeBbox.maxLng && b[2] >= routeBbox.minLng &&
               b[1] <= routeBbox.maxLat && b[3] >= routeBbox.minLat;
  });

  // Échantillonnage du tracé + point-in-polygon (purement en mémoire, zéro requête réseau)
  const sampled   = sampleRoutePoints(allCoords, 400);
  const seenCode  = new Set();
  const results   = [];

  for (const [lng, lat] of sampled) {
    if (kmlAbortFlag) break;
    for (const feature of candidates) {
      const code = feature.properties?.code;
      if (!code || seenCode.has(code)) continue;
      if (_pointInGeometry(lat, lng, feature.geometry)) {
        seenCode.add(code);
        results.push({
          name:     feature.properties.nom,
          insee:    code,
          dept:     _parseDept(code),
          type:     'municipality',
          lat, lng,
          geometry: feature.geometry,   // déjà en mémoire — zéro requête API
        });
      }
    }
  }

  onProgress(3, 3);
  return results;
}

// Fallback si communes-paca.geojson absent (reverse geocoding api-adresse)
async function _extractLocalitiesFallback(allCoords, onProgress) {
  const sampled = sampleRoutePoints(allCoords, 500);
  const seen    = new Map();
  const results = [];
  for (let i = 0; i < sampled.length; i++) {
    if (kmlAbortFlag) break;
    const [lng, lat] = sampled[i];
    onProgress(i + 1, sampled.length);
    const loc = await reverseGeocodePoint(lng, lat);
    if (loc) {
      const key = `${loc.insee}_${loc.name.toLowerCase()}`;
      if (!seen.has(key)) { seen.set(key, 1); results.push({ ...loc, lat, lng }); }
    }
    await new Promise(r => setTimeout(r, 90));
  }
  return results;
}

// ── Rendu résultats ───────────────────────────────────────────────────────────
function kmlRenderResults(localities, totalKm, terrainCount = 0) {
  if (kmlMarkersLayer) { map.removeLayer(kmlMarkersLayer); kmlMarkersLayer = null; }
  if (kmlCommunePolygonLayer) { map.removeLayer(kmlCommunePolygonLayer); kmlCommunePolygonLayer = null; }
  kmlLocalities   = localities;
  kmlMarkersLayer = L.layerGroup();
  _addLayerIfVisible(kmlMarkersLayer, 'communes');

  localities.forEach(loc => {
    const marker = L.circleMarker([loc.lat, loc.lng], {
      radius: 5, color: '#4ade80', fillColor: '#4ade80',
      fillOpacity: 0.85, weight: 2,
    }).bindPopup(`<b>${loc.name}</b><br><small>${loc.insee || ''} · ${loc.dept || ''}</small>`);
    kmlMarkersLayer.addLayer(marker);
    loc._marker = marker;
  });

  // Stat update
  const communes = localities.filter(l => l.type === 'municipality' || l.type === 'commune').length;
  const communesEl  = document.getElementById('kml-stat-communes');
  const terrainsEl  = document.getElementById('kml-stat-terrains');
  const kmEl        = document.getElementById('kml-stat-km');
  const statsEl     = document.getElementById('kml-stats');
  if (communesEl)  communesEl.textContent = communes || localities.length;
  if (terrainsEl)  terrainsEl.textContent = terrainCount;
  if (kmEl)        kmEl.textContent       = totalKm;
  if (statsEl)     statsEl.style.display  = 'block';

  // Liste sidebar
  const container = document.getElementById('kml-results');
  if (!container) return;
  container.innerHTML = '<h2 style="margin-bottom:6px">Communes du trajet</h2>';
  localities.forEach((loc, i) => {
    const item = document.createElement('div');
    item.className = 'kml-locality-item';
    item.style.animationDelay = `${i * 30}ms`;
    item.innerHTML = `
      <div class="kml-locality-main">
        <span class="kml-locality-name">${escapeHtml(loc.name)}</span>
        ${loc.dept ? `<span class="kml-badge">${escapeHtml(loc.dept)}</span>` : ''}
      </div>
      <div class="kml-locality-meta">
        ${loc.insee ? `<span style="color:var(--text-lo);font-size:10px">${escapeHtml(loc.insee)}</span>` : ''}
        ${loc.type  ? `<span style="color:var(--text-lo);font-size:10px;text-transform:capitalize">${escapeHtml(loc.type)}</span>` : ''}
      </div>`;
    item.addEventListener('mouseenter', () => {
      item.classList.add('hover');
      if (loc._marker) loc._marker.openPopup();
      if (loc._polygon) loc._polygon.setStyle({ fillOpacity: 0.45, weight: 2.5 });
      map.panTo([loc.lat, loc.lng], { animate: true, duration: 0.4 });
    });
    item.addEventListener('mouseleave', () => {
      item.classList.remove('hover');
      if (loc._polygon) loc._polygon.setStyle({ fillOpacity: 0.12, weight: 1 });
    });
    container.appendChild(item);
  });

  // Fetch commune boundaries from geo.api.gouv.fr and display on map
  kmlFetchAndRenderCommunePolygons(localities);

  if (kmlRouteLayer) {
    try { map.fitBounds(kmlRouteLayer.getBounds(), { padding: [30, 30] }); } catch(_) {}
  }
}

// ── Contours communes (geo.api.gouv.fr) ──────────────────────────────────────
let kmlCommunePolygonLayer = null;

function kmlFetchAndRenderCommunePolygons(localities) {
  // Utilise la géométrie déjà en mémoire — zéro requête réseau
  const withGeom = localities.filter(l => l.geometry);
  if (!withGeom.length) return;

  if (kmlCommunePolygonLayer) { map.removeLayer(kmlCommunePolygonLayer); kmlCommunePolygonLayer = null; }
  kmlCommunePolygonLayer = L.layerGroup();
  _addLayerIfVisible(kmlCommunePolygonLayer, 'communes');

  const PALETTE = ['#60a5fa','#4ade80','#fb923c','#a78bfa','#f472b6','#34d399','#fbbf24'];

  withGeom.forEach((loc, idx) => {
    try {
      const color = PALETTE[idx % PALETTE.length];
      const poly  = L.geoJSON(loc.geometry, {
        style: { color, weight: 1.5, opacity: 0.75, fillColor: color, fillOpacity: 0.13 },
      });
      poly.bindPopup(`<b>${escapeHtml(loc.name)}</b><br><small>${loc.insee} · ${loc.dept ? 'Dép. ' + loc.dept : ''}</small>`);
      poly.addTo(kmlCommunePolygonLayer);
      loc._polygon = poly.getLayers()[0];
    } catch(_) {}
  });
}

// ── Wiring UI ─────────────────────────────────────────────────────────────────
function kmlUpdateFileUI() {
  const list   = document.getElementById('kml-file-list');
  const chips  = document.getElementById('kml-file-chips');
  const runBtn = document.getElementById('kml-run-btn');
  list.style.display   = kmlLoadedFiles.length ? 'block' : 'none';
  runBtn.style.display = kmlLoadedFiles.length ? 'block' : 'none';
  chips.innerHTML = kmlLoadedFiles.map((f, i) => `
    <div class="kml-file-chip">
      <span>${escapeHtml(f.name)}</span>
      <span class="kml-file-chip-count">${f.coords.length} pts</span>
      <button onclick="kmlRemoveFile(${i})" title="Retirer">✕</button>
    </div>`).join('');
}

async function kmlLoadFile(file) {
  const text  = await file.text();
  const ext   = file.name.split('.').pop().toLowerCase();
  let   coords = [];
  if      (ext === 'kml') coords = parseKml(text);
  else if (ext === 'gpx') coords = parseGpx(text);
  if (!coords.length) { alert(`Aucun tracé trouvé dans ${file.name}`); return; }
  kmlLoadedFiles.push({ name: file.name, coords });
  kmlUpdateFileUI();
}

function kmlRemoveFile(index) {
  kmlLoadedFiles.splice(index, 1);
  kmlUpdateFileUI();
  if (!kmlLoadedFiles.length) kmlClearAll();
}

function kmlClearAll() {
  kmlLoadedFiles = [];
  kmlLocalities  = [];
  kmlPoiData     = null;
  kmlAbortFlag   = true;
  if (kmlMarkersLayer)       { map.removeLayer(kmlMarkersLayer);       kmlMarkersLayer = null; }
  if (kmlRouteLayer)         { map.removeLayer(kmlRouteLayer);         kmlRouteLayer   = null; }
  if (kmlPoiLayer)           { map.removeLayer(kmlPoiLayer);           kmlPoiLayer     = null; }
  if (kmlParcelLayer)        { map.removeLayer(kmlParcelLayer);        kmlParcelLayer  = null; }
  if (kmlCommunePolygonLayer){ map.removeLayer(kmlCommunePolygonLayer);kmlCommunePolygonLayer = null; }
  const fileList = document.getElementById('kml-file-list');
  const runBtn   = document.getElementById('kml-run-btn');
  const progress = document.getElementById('kml-progress-wrap');
  const stats    = document.getElementById('kml-stats');
  const results  = document.getElementById('kml-results');
  const chips    = document.getElementById('kml-file-chips');
  const poi      = document.getElementById('kml-poi-section');
  if (fileList) fileList.style.display = 'none';
  if (runBtn)   runBtn.style.display   = 'none';
  if (progress) progress.style.display = 'none';
  if (stats)    stats.style.display    = 'none';
  if (results)  results.innerHTML      = '';
  if (chips)    chips.innerHTML        = '';
  if (poi)      poi.innerHTML          = '';
}

async function kmlRunExtraction() {
  if (!kmlLoadedFiles.length) return;
  kmlAbortFlag = false;

  const allCoords = mergeAndOrderCoords(kmlLoadedFiles);
  const totalKm   = totalRouteKm(allCoords);

  // Afficher le tracé sur la carte
  if (kmlRouteLayer) map.removeLayer(kmlRouteLayer);
  kmlRouteLayer = _addLayerIfVisible(L.polyline(
    allCoords.map(([lng, lat]) => [lat, lng]),
    { color: '#f43f5e', weight: 4, opacity: 0.9, dashArray: '8 5' }
  ), 'route');
  map.fitBounds(kmlRouteLayer.getBounds(), { padding: [30, 30] });

  const progressWrap  = document.getElementById('kml-progress-wrap');
  const progressLabel = document.getElementById('kml-progress-label');
  const progressBar   = document.getElementById('kml-progress-bar');
  const runBtn        = document.getElementById('kml-run-btn');
  progressWrap.style.display = 'block';
  runBtn.disabled            = true;
  runBtn.textContent         = '⏳ Analyse…';
  document.getElementById('kml-results').innerHTML    = '';
  document.getElementById('kml-stats').style.display  = 'none';
  const poiSection = document.getElementById('kml-poi-section');
  if (poiSection) poiSection.innerHTML = '';

  const radiusKm = parseFloat(document.getElementById('kml-radius')?.value || '2') || 2;
  const minArea  = parseInt(document.getElementById('kml-area')?.value    || '2000') || 2000;

  // ── Étape 1 : terrains ────────────────────────────────────────────────────
  progressLabel.textContent = 'Recherche des terrains…';
  progressBar.style.width   = '5%';

  const corridorFeatures = await _findCorridorInMemory(allCoords, radiusKm, minArea);
  _kmlRenderParcelLayer(corridorFeatures);

  // ── Étape 2 : communes ────────────────────────────────────────────────────
  const localityLabels = ['Chargement communes…', 'Analyse du tracé…', 'Terminé'];
  const localities = await extractLocalities(allCoords, (current, total) => {
    const label = localityLabels[current - 1] || `${current}/${total}`;
    progressLabel.textContent = `Communes — ${label}`;
    progressBar.style.width   = `${Math.round(20 + current / total * 60)}%`;
  });

  if (localities.length) kmlRenderResults(localities, totalKm, corridorFeatures.length);

  // ── Étape 3 : POI Overpass ────────────────────────────────────────────────
  progressLabel.textContent = 'Recherche des points d\'intérêt…';
  progressBar.style.width   = '85%';

  try {
    kmlPoiData = await fetchAllPoi(allCoords);
    kmlRenderPoi(kmlPoiData);
    kmlRenderPoiSidebar(kmlPoiData);
    const poiTotal = Object.values(kmlPoiData).reduce((s, arr) => s + arr.length, 0);
    const poiEl = document.getElementById('kml-stat-poi');
    if (poiEl) poiEl.textContent = poiTotal;
  } catch(e) {
    console.warn('[kml] POI fetch failed:', e);
    if (poiSection) poiSection.innerHTML =
      '<div style="font-size:11px;color:var(--text-lo);text-align:center;padding:8px">Points d\'intérêt indisponibles (Overpass)</div>';
  }

  progressBar.style.width   = '100%';
  setTimeout(() => { progressWrap.style.display = 'none'; }, 400);
  runBtn.disabled   = false;
  runBtn.textContent = '↺ Relancer l\'analyse';

  if (!localities.length) alert('Aucune commune trouvée. Vérifiez que vos fichiers contiennent des tracés linéaires.');
}

function _kmlRenderParcelLayer(features) {
  if (kmlParcelLayer) { map.removeLayer(kmlParcelLayer); kmlParcelLayer = null; }
  // Masquer les parcelles du filtre pour éviter la pollution visuelle
  if (currentLayer) { map.removeLayer(currentLayer); currentLayer = null; }
  if (!features.length) return;
  kmlParcelLayer = _addLayerIfVisible(L.geoJSON(
    { type: 'FeatureCollection', features },
    {
      style: { fillColor: '#fb923c', fillOpacity: 0.40, color: '#fb923c', weight: 1.2, opacity: 0.75 },
      onEachFeature: (feature, layer) => {
        layer.bindPopup(() => buildPopup(feature), { maxWidth: 440, minWidth: 340 });
        layer.on('mouseover', function() { this.setStyle({ fillOpacity: 0.7, weight: 2 }); });
        layer.on('mouseout',  function() { kmlParcelLayer && kmlParcelLayer.resetStyle(this); });
      },
    }
  ), 'terrains');
}

function kmlCopyList() {
  const text = kmlLocalities.map(l => l.name).join('\n');
  navigator.clipboard.writeText(text)
    .then(() => alert(`${kmlLocalities.length} localités copiées !`))
    .catch(() => alert('Copie non disponible — utilisez le CSV'));
}

function kmlExportCsv() {
  if (!kmlLocalities.length && !kmlPoiData) return;
  const header = 'type_donnee,nom,code_insee,departement,type_lieu,lat,lng,dist_km_trace,tags_osm';
  const rows   = [];

  // Communes
  for (const l of kmlLocalities) {
    rows.push([
      'commune', l.name, l.insee || '', l.dept || '', l.type || 'commune',
      l.lat, l.lng, '', ''
    ].map(v => `"${String(v).replace(/"/g, '""')}"`).join(','));
  }

  // POI
  if (kmlPoiData) {
    for (const [cat, list] of Object.entries(kmlPoiData)) {
      for (const p of list) {
        const tagsStr = Object.entries(p.tags || {}).map(([k,v]) => `${k}=${v}`).join(' ');
        rows.push([
          'poi_' + cat, p.name, '', '', p.typeLabel,
          p.lat, p.lng, p.distKm, tagsStr
        ].map(v => `"${String(v).replace(/"/g, '""')}"`).join(','));
      }
    }
  }

  const blob = new Blob([[header, ...rows].join('\n')], { type: 'text/csv;charset=utf-8;' });
  const url  = URL.createObjectURL(blob);
  const a    = Object.assign(document.createElement('a'), {
    href: url, download: 'transhumance-communes-poi.csv',
  });
  document.body.appendChild(a); a.click();
  document.body.removeChild(a); URL.revokeObjectURL(url);
}

function kmlExportXlsx() {
  if (!kmlLocalities.length && !kmlPoiData) { alert('Aucune donnée à exporter.'); return; }

  const wb = window.XLSX ? XLSX.utils.book_new() : null;
  if (!wb) { kmlExportCsv(); return; } // fallback CSV si SheetJS absent

  // ── Onglet Communes ──
  if (kmlLocalities.length) {
    const header = ['Nom', 'Code INSEE', 'Département', 'Type', 'Latitude', 'Longitude'];
    const rows   = kmlLocalities.map(l => [
      l.name, l.insee || '', l.dept || '', l.type || 'commune', l.lat, l.lng
    ]);
    const ws = XLSX.utils.aoa_to_sheet([header, ...rows]);
    ws['!cols'] = header.map((h, ci) => ({
      wch: Math.max(h.length, ...rows.map(r => String(r[ci] ?? '').length)) + 2,
    }));
    XLSX.utils.book_append_sheet(wb, ws, 'Communes');
  }

  // ── Onglet POI (une feuille par catégorie) ──
  if (kmlPoiData) {
    const catLabels = { eau: '💧 Eau', haltes: '🏡 Haltes', ravitaillement: '🛒 Ravitaillement' };
    for (const [cat, list] of Object.entries(kmlPoiData)) {
      if (!list.length) continue;
      const header = ['Nom', 'Type', 'Dist. tracé (km)', 'Latitude', 'Longitude', 'Tags OSM'];
      const rows   = list.map(p => [
        p.name, p.typeLabel, p.distKm, p.lat, p.lng,
        Object.entries(p.tags || {}).map(([k,v]) => `${k}=${v}`).join('; ')
      ]);
      const ws = XLSX.utils.aoa_to_sheet([header, ...rows]);
      ws['!cols'] = header.map((h, ci) => ({
        wch: Math.max(h.length, ...rows.map(r => String(r[ci] ?? '').length)) + 2,
      }));
      XLSX.utils.book_append_sheet(wb, ws, catLabels[cat] || cat);
    }
  }

  XLSX.writeFile(wb, 'transhumance.xlsx');
}

// ══════════════════════════════════════════════════════════════════════════════
// Feature POI — Points d'intérêt à 2 km du tracé (Overpass API)
// ══════════════════════════════════════════════════════════════════════════════

const POI_CATEGORIES = {
  eau: {
    emoji: '💧', label: 'Points d\'eau', color: '#60a5fa',
    query: (bbox) => `[out:json][timeout:30];
(
 node["natural"="spring"](${bbox});
 node["amenity"="drinking_water"](${bbox});
 node["amenity"="watering_place"](${bbox});
 node["amenity"="fountain"](${bbox});
 node["amenity"="water_point"](${bbox});
 node["man_made"="water_well"](${bbox});
 node["man_made"="water_tap"](${bbox});
 node["man_made"="water_tower"](${bbox});
 node["natural"="water"]["name"](${bbox});
 way["natural"="water"]["name"](${bbox});
);out center;`,
  },
  haltes: {
    emoji: '🏡', label: 'Haltes / Partenaires', color: '#fb923c',
    query: (bbox) => `[out:json][timeout:30];
(node["tourism"="farm"](${bbox});
 node["tourism"="guest_house"](${bbox});
 node["tourism"="hostel"](${bbox});
 node["tourism"="camp_site"](${bbox});
 node["tourism"="caravan_site"](${bbox});
 node["tourism"="picnic_site"](${bbox});
 node["amenity"="shelter"](${bbox});
 node["landuse"="farmyard"](${bbox}););out center;`,
  },
  ravitaillement: {
    emoji: '🛒', label: 'Ravitaillement', color: '#4ade80',
    query: (bbox) => `[out:json][timeout:30];
(node["shop"="supermarket"](${bbox});
 node["shop"="convenience"](${bbox});
 node["shop"="greengrocer"](${bbox});
 node["shop"="bakery"](${bbox});
 node["shop"="butcher"](${bbox});
 node["shop"="farm"](${bbox});
 node["amenity"="marketplace"](${bbox}););out center;`,
  },
};

function routeBboxWithMargin(coords, marginDeg = 0.025) {
  let minLat = Infinity, minLng = Infinity, maxLat = -Infinity, maxLng = -Infinity;
  for (const [lng, lat] of coords) {
    if (lat < minLat) minLat = lat;
    if (lng < minLng) minLng = lng;
    if (lat > maxLat) maxLat = lat;
    if (lng > maxLng) maxLng = lng;
  }
  return {
    minLat: minLat - marginDeg, minLng: minLng - marginDeg,
    maxLat: maxLat + marginDeg, maxLng: maxLng + marginDeg,
    overpassStr: `${(minLat - marginDeg).toFixed(6)},${(minLng - marginDeg).toFixed(6)},${(maxLat + marginDeg).toFixed(6)},${(maxLng + marginDeg).toFixed(6)}`,
  };
}

async function fetchOverpassPoi(overpassQuery) {
  const ENDPOINTS = [
    'https://overpass-api.de/api/interpreter',
    'https://overpass.kumi.systems/api/interpreter',
  ];
  const body = 'data=' + encodeURIComponent(overpassQuery);
  for (let attempt = 0; attempt < 4; attempt++) {
    const url = ENDPOINTS[attempt % ENDPOINTS.length];
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body,
      });
      if (res.status === 429 || res.status === 504) {
        // Rate-limited or gateway timeout — backoff before retry
        await new Promise(r => setTimeout(r, 3000 * (attempt + 1)));
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      return (json.elements || []).map(el => ({
        id:   el.id,
        lat:  el.lat  ?? el.center?.lat,
        lng:  el.lon  ?? el.center?.lon,
        tags: el.tags || {},
      })).filter(el => el.lat != null && el.lng != null);
    } catch(e) {
      if (attempt < 3) { await new Promise(r => setTimeout(r, 2000 * (attempt + 1))); continue; }
      throw e;
    }
  }
  return [];
}

function filterPoiByDistance(poiList, polyline, maxKm = 2.0) {
  const simplPoly = _downsampleCoords(
    polyline.map(p => [p.lng, p.lat]), 60
  ).map(([lng, lat]) => ({ lat, lng }));

  // Pre-compute segment lengths once — reused by projectionOnPolyline sort below
  const segLengths = [];
  let totalLen = 0;
  for (let i = 0; i < simplPoly.length - 1; i++) {
    const l = haversine(simplPoly[i], simplPoly[i + 1]);
    segLengths.push(l);
    totalLen += l;
  }

  // Inline projection using cached lengths — avoids O(n) haversine loop per POI
  function projFast(p) {
    let bestT = 0, bestSeg = 0, bestDist = Infinity;
    for (let i = 0; i < simplPoly.length - 1; i++) {
      const t    = projectionOnSegment(p, simplPoly[i], simplPoly[i + 1]);
      const proj = { lat: simplPoly[i].lat + t*(simplPoly[i+1].lat - simplPoly[i].lat),
                     lng: simplPoly[i].lng + t*(simplPoly[i+1].lng - simplPoly[i].lng) };
      const d    = haversine(p, proj);
      if (d < bestDist) { bestDist = d; bestSeg = i; bestT = t; }
    }
    let cum = 0;
    for (let i = 0; i < bestSeg; i++) cum += segLengths[i];
    return totalLen > 0 ? (cum + bestT * segLengths[bestSeg]) / totalLen : 0;
  }

  return poiList
    .map(poi => {
      const dist = distToPolyline({ lat: poi.lat, lng: poi.lng }, simplPoly);
      return { ...poi, distKm: Math.round(dist * 10) / 10 };
    })
    .filter(poi => poi.distKm <= maxKm)
    .sort((a, b) => projFast({ lat: a.lat, lng: a.lng }) - projFast({ lat: b.lat, lng: b.lng }));
}

function poiDisplayName(tags) {
  if (tags.name)        return tags.name;
  if (tags['name:fr'])  return tags['name:fr'];
  if (tags.ref)         return tags.ref;
  // Fallback par type
  if (tags.natural === 'spring')            return 'Source';
  if (tags.amenity === 'drinking_water')    return 'Eau potable';
  if (tags.amenity === 'watering_place')    return 'Abreuvoir';
  if (tags.amenity === 'fountain')          return 'Fontaine';
  if (tags.amenity === 'water_point')       return 'Point d\'eau';
  if (tags.man_made === 'water_well')       return 'Puits';
  if (tags.man_made === 'water_tap')        return 'Robinet d\'eau';
  if (tags.man_made === 'water_tower')      return 'Château d\'eau';
  if (tags.natural === 'water')             return 'Plan d\'eau';
  if (tags.tourism === 'farm')              return tags.operator || 'Ferme';
  if (tags.tourism === 'camp_site')         return 'Camping';
  if (tags.tourism === 'caravan_site')      return 'Aire camping-car';
  if (tags.tourism === 'hostel')            return 'Auberge';
  if (tags.tourism === 'guest_house')       return 'Chambre d\'hôtes';
  if (tags.tourism === 'picnic_site')       return 'Aire de pique-nique';
  if (tags.amenity === 'shelter')           return 'Abri / refuge';
  if (tags.landuse === 'farmyard')          return 'Exploitation agricole';
  if (tags.shop === 'supermarket')          return 'Supermarché';
  if (tags.shop === 'convenience')          return 'Épicerie';
  if (tags.shop === 'greengrocer')          return 'Primeur';
  if (tags.shop === 'bakery')               return 'Boulangerie';
  if (tags.shop === 'butcher')              return 'Boucherie';
  if (tags.shop === 'farm')                 return 'Vente à la ferme';
  if (tags.amenity === 'marketplace')       return 'Marché';
  return 'Point d\'intérêt';
}

// Reconstruit une adresse lisible depuis les tags OSM addr:*
function poiAddress(tags) {
  const parts = [];
  const num    = tags['addr:housenumber'];
  const street = tags['addr:street'];
  const city   = tags['addr:city'];
  const post   = tags['addr:postcode'];
  if (num && street)  parts.push(`${num} ${street}`);
  else if (street)    parts.push(street);
  if (post && city)   parts.push(`${post} ${city}`);
  else if (city)      parts.push(city);
  else if (post)      parts.push(post);
  return parts.join(', ');
}

function poiTypeLabel(tags) {
  if (tags.natural === 'spring')            return 'Source';
  if (tags.amenity === 'drinking_water')    return 'Eau potable';
  if (tags.amenity === 'watering_place')    return 'Abreuvoir';
  if (tags.amenity === 'fountain')          return 'Fontaine';
  if (tags.amenity === 'water_point')       return 'Point d\'eau';
  if (tags.man_made === 'water_well')       return 'Puits';
  if (tags.man_made === 'water_tap')        return 'Robinet';
  if (tags.man_made === 'water_tower')      return 'Château d\'eau';
  if (tags.natural === 'water')             return 'Eau';
  if (tags.tourism === 'farm')              return 'Ferme';
  if (tags.tourism === 'camp_site')         return 'Camping';
  if (tags.tourism === 'caravan_site')      return 'Camping-car';
  if (tags.tourism === 'hostel')            return 'Auberge';
  if (tags.tourism === 'guest_house')       return 'Gîte';
  if (tags.tourism === 'picnic_site')       return 'Pique-nique';
  if (tags.amenity === 'shelter')           return 'Refuge';
  if (tags.landuse === 'farmyard')          return 'Ferme';
  if (tags.shop === 'supermarket')          return 'Supermarch\xe9';
  if (tags.shop === 'convenience')          return '\xc9picerie';
  if (tags.shop === 'greengrocer')          return 'Primeur';
  if (tags.shop === 'bakery')               return 'Boulangerie';
  if (tags.shop === 'butcher')              return 'Boucherie';
  if (tags.shop === 'farm')                 return 'Vente ferme';
  if (tags.amenity === 'marketplace')       return 'March\xe9';
  return 'POI';
}

async function fetchAllPoi(coords) {
  const { overpassStr } = routeBboxWithMargin(coords);
  const polyline = coords.map(([lng, lat]) => ({ lat, lng }));

  const [eauRaw, haltesRaw, ravRaw] = await Promise.all([
    fetchOverpassPoi(POI_CATEGORIES.eau.query(overpassStr)).catch(() => []),
    fetchOverpassPoi(POI_CATEGORIES.haltes.query(overpassStr)).catch(() => []),
    fetchOverpassPoi(POI_CATEGORIES.ravitaillement.query(overpassStr)).catch(() => []),
  ]);

  function enrich(list, cat) {
    return filterPoiByDistance(list, polyline, 2.0).map(p => ({
      ...p,
      name:      poiDisplayName(p.tags),
      typeLabel: poiTypeLabel(p.tags),
      address:   poiAddress(p.tags),
      phone:     p.tags?.phone || p.tags?.['contact:phone'] || '',
      website:   p.tags?.website || p.tags?.['contact:website'] || '',
      hours:     p.tags?.opening_hours || '',
      category:  cat,
    }));
  }

  return {
    eau:            enrich(eauRaw,    'eau'),
    haltes:         enrich(haltesRaw, 'haltes'),
    ravitaillement: enrich(ravRaw,    'ravitaillement'),
  };
}

function kmlRenderPoi(poiData) {
  if (kmlPoiLayer) { map.removeLayer(kmlPoiLayer); kmlPoiLayer = null; }
  kmlPoiLayer = L.layerGroup();
  _addLayerIfVisible(kmlPoiLayer, 'poi');

  const catConfig = {
    eau:            { color: '#60a5fa', emoji: '💧' },
    haltes:         { color: '#fb923c', emoji: '🏡' },
    ravitaillement: { color: '#4ade80', emoji: '🛒' },
  };

  for (const [cat, list] of Object.entries(poiData)) {
    const { color, emoji } = catConfig[cat] || { color: '#fff', emoji: '📍' };
    for (const poi of list) {
      const marker = L.circleMarker([poi.lat, poi.lng], {
        radius: 6, color, fillColor: color,
        fillOpacity: 0.9, weight: 2,
      }).bindPopup(() => {
        const showContact = (cat === 'haltes' || cat === 'ravitaillement');
        const addrHtml    = (showContact && poi.address)
          ? `<div class="poi-popup-addr">${escapeHtml(poi.address)}</div>` : '';
        const phoneHtml   = (showContact && poi.phone)
          ? `<div class="poi-popup-contact"><a href="tel:${escapeHtml(poi.phone)}">${escapeHtml(poi.phone)}</a></div>` : '';
        const webHtml     = (showContact && poi.website)
          ? `<div class="poi-popup-contact"><a href="${escapeHtml(poi.website)}" target="_blank" rel="noopener">Site web</a></div>` : '';
        const hoursHtml   = (showContact && poi.hours)
          ? `<div class="poi-popup-hours">${escapeHtml(poi.hours)}</div>` : '';
        return `<div class="poi-popup">
          <div class="poi-popup-title">${emoji} ${escapeHtml(poi.name)}</div>
          <div class="poi-popup-meta">${escapeHtml(poi.typeLabel)}</div>
          ${addrHtml}${phoneHtml}${webHtml}${hoursHtml}
          <div class="poi-popup-dist">${poi.distKm} km du tracé</div>
        </div>`;
      }, { maxWidth: 360, minWidth: 220 });
      kmlPoiLayer.addLayer(marker);
      poi._marker = marker;
    }
  }
}

function kmlRenderPoiSidebar(poiData) {
  const section = document.getElementById('kml-poi-section');
  if (!section) return;

  const catConfig = {
    eau:            { emoji: '💧', label: 'Points d\'eau',      color: '#60a5fa' },
    haltes:         { emoji: '🏡', label: 'Haltes / Partenaires', color: '#fb923c' },
    ravitaillement: { emoji: '🛒', label: 'Ravitaillement',     color: '#4ade80' },
  };

  const total = Object.values(poiData).reduce((s, arr) => s + arr.length, 0);
  if (!total) {
    section.innerHTML = '<div class="kml-poi-empty">Aucun point d\'intérêt trouvé à 2 km du tracé</div>';
    return;
  }

  section.innerHTML = '<h2 style="margin:12px 0 8px">Points d\'intérêt à 2 km</h2>';

  for (const [cat, list] of Object.entries(poiData)) {
    const cfg = catConfig[cat];
    const group = document.createElement('div');
    group.className = 'kml-poi-group';

    const header = document.createElement('div');
    header.className = 'kml-poi-group-header';
    header.innerHTML = `
      <span>${cfg.emoji} ${escapeHtml(cfg.label)}</span>
      <span class="kml-poi-count">${list.length} résultat${list.length !== 1 ? 's' : ''}</span>
      <span class="kml-poi-chevron">▾</span>`;
    header.addEventListener('click', () => {
      const listEl = group.querySelector('.kml-poi-list');
      const open   = listEl.style.display !== 'none';
      listEl.style.display = open ? 'none' : 'block';
      header.querySelector('.kml-poi-chevron').textContent = open ? '▸' : '▾';
    });
    group.appendChild(header);

    const listEl = document.createElement('div');
    listEl.className = 'kml-poi-list';

    if (!list.length) {
      listEl.innerHTML = '<div style="font-size:11px;color:var(--text-lo);padding:8px 10px">Aucun résultat</div>';
    } else {
      for (const poi of list) {
        const item = document.createElement('div');
        item.className = 'kml-poi-item';
        const showAddr = (cat === 'haltes' || cat === 'ravitaillement');
        const addrLine = (showAddr && poi.address)
          ? `<span class="kml-poi-item-addr">${escapeHtml(poi.address)}</span>` : '';
        item.innerHTML = `
          <span class="kml-poi-dot" style="background:${cfg.color}"></span>
          <span class="kml-poi-item-main">
            <span class="kml-poi-item-name">${escapeHtml(poi.name)}</span>
            <span class="kml-poi-item-type">${escapeHtml(poi.typeLabel)}</span>
            ${addrLine}
          </span>
          <span class="kml-poi-dist">${poi.distKm} km</span>`;
        item.addEventListener('click', () => {
          map.panTo([poi.lat, poi.lng], { animate: true, duration: 0.5 });
          if (poi._marker) poi._marker.openPopup();
        });
        listEl.appendChild(item);
      }
    }
    group.appendChild(listEl);
    section.appendChild(group);
  }

  // Attribution OSM
  const attr = document.createElement('div');
  attr.style.cssText = 'font-size:10px;color:var(--text-lo);text-align:center;margin-top:10px;padding-bottom:6px';
  attr.textContent = '© OpenStreetMap contributors (ODbL)';
  section.appendChild(attr);
}

// ── Drop zone ─────────────────────────────────────────────────────────────────
(function initKmlDropZone() {
  const zone  = document.getElementById('kml-drop-zone');
  const input = document.getElementById('kml-file-input');
  if (!zone || !input) return;

  zone.addEventListener('click', e => { if (e.target.tagName !== 'BUTTON') input.click(); });
  input.addEventListener('change', async () => {
    for (const f of input.files) await kmlLoadFile(f);
    input.value = '';
  });
  zone.addEventListener('dragover',  e => { e.preventDefault(); zone.classList.add('drag-over'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
  zone.addEventListener('drop', async e => {
    e.preventDefault(); zone.classList.remove('drag-over');
    for (const f of e.dataTransfer.files) {
      if (['kml','gpx'].includes(f.name.split('.').pop().toLowerCase())) await kmlLoadFile(f);
    }
  });
})();
