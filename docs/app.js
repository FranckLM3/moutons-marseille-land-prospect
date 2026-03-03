// ── Config ────────────────────────────────────────────────────────────────
const DATA_FILE    = 'pasture_zones.geojson';
const DEFAULT_FILTER = 'Marseille'; // pré-sélectionne toutes les communes contenant ce mot

// Couleur selon % prairie (0=gris foncé → gris clair → vert clair → vert vif)
function colorForPrairie(pct) {
  if (pct == null) return '#94a3b8';
  if (pct < 5)   return '#64748b'; // gris foncé  — quasi nul
  if (pct < 20)  return '#94a3b8'; // gris clair  — faible
  if (pct < 40)  return '#86efac'; // vert pâle   — moyen
  if (pct < 65)  return '#4ade80'; // vert clair  — bon
  return '#16a34a';                // vert vif    — excellent
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
let minAreaHa        = 0;         // surface pâturable min en ha (0 = pas de filtre)

// ── Carte ─────────────────────────────────────────────────────────────────
const map = L.map('map', { zoomControl: false }).setView([43.3, 5.4], 12);
L.control.zoom({ position: 'bottomright' }).addTo(map);
L.control.scale({ metric: true, imperial: false, position: 'bottomright' }).addTo(map);

const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© <a href="https://openstreetmap.org">OpenStreetMap</a>',
  maxZoom: 19,
});
const satellite = L.tileLayer(
  'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
  { attribution: '© Esri', maxZoom: 19 }
);
osmLayer.addTo(map);
L.control.layers({ 'Carte': osmLayer, 'Satellite': satellite }, {}, { position: 'bottomright' }).addTo(map);

// ── Chargement ────────────────────────────────────────────────────────────
fetch(DATA_FILE)
  .then(r => {
    if (!r.ok) throw new Error(`Fichier ${DATA_FILE} introuvable (HTTP ${r.status})`);
    return r.json();
  })
  .then(data => {
    document.getElementById('loading').style.display = 'none';
    allFeatures = data.features || [];

    buildCommuneChips();
    applyFilters();
  })
  .catch(err => {
    document.getElementById('loading').innerHTML = `
      <div style="color:#f87171;font-size:20px;">⚠️</div>
      <p style="color:#666;max-width:320px;text-align:center;line-height:1.6">
        ${err.message}<br><br>
        Générez d'abord les données :<br>
        <code style="color:#4ade80">python scripts/build.py</code>
      </p>`;
  });

// ── Chips communes (multiselect) ──────────────────────────────────────────
function buildCommuneChips() {
  const container = document.getElementById('commune-chips');
  const seen = new Set();
  allFeatures.forEach(f => {
    const c = (f.properties?.nom_commune || '').trim();
    if (c && c !== 'null' && c !== 'None') seen.add(c);
  });
  allCommunes = [...seen].sort((a, b) => a.localeCompare(b, 'fr'));

  // Pré-sélectionner toutes les communes dont le nom contient DEFAULT_FILTER
  allCommunes.forEach(name => {
    if (name.toLowerCase().includes(DEFAULT_FILTER.toLowerCase())) selectedCommunes.add(name);
  });

  allCommunes.forEach(name => {
    const chip = document.createElement('div');
    const active = selectedCommunes.has(name);
    chip.className = 'comm-chip ' + (active ? 'active' : 'inactive');
    chip.dataset.commune = name;
    chip.innerHTML = `<span class="comm-dot"></span>${name}`;
    chip.addEventListener('click', () => {
      selectedCommunes.has(name) ? selectedCommunes.delete(name) : selectedCommunes.add(name);
      refreshCommuneChips();
      applyFilters();
    });
    container.appendChild(chip);
  });
}

function refreshCommuneChips() {
  document.querySelectorAll('#commune-chips .comm-chip').forEach(chip => {
    const active = selectedCommunes.has(chip.dataset.commune);
    chip.className = 'comm-chip ' + (active ? 'active' : 'inactive');
  });
}

function selectAllCommunes()   { allCommunes.forEach(c => selectedCommunes.add(c)); refreshCommuneChips(); applyFilters(); }
function deselectAllCommunes() { selectedCommunes.clear(); refreshCommuneChips(); applyFilters(); }

function filterCommuneChips(query) {
  const q = query.trim().toLowerCase();
  document.querySelectorAll('#commune-chips .comm-chip').forEach(chip => {
    const match = chip.dataset.commune.toLowerCase().includes(q);
    chip.style.display = match ? '' : 'none';
  });
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

// ── Filtrage ──────────────────────────────────────────────────────────────
function getFiltered() {
  return allFeatures.filter(f => {
    const p = f.properties || {};
    if (selectedCommunes.size > 0) {
      const c = (p.nom_commune || '').trim();
      if (!selectedCommunes.has(c)) return false;
    }
    if (minAreaHa > 0 && (p.prairie_m2 || 0) < minAreaHa * 10000) return false;
    return true;
  });
}

function applyFilters() {
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
function resetFilters() {
  // Réinitialise surface pâturable
  document.getElementById('area-slider').value = 0;
  minAreaHa = 0;
  document.getElementById('area-value').textContent = '0 m²';
  // Réinitialise communes → Marseille
  selectedCommunes.clear();
  allCommunes.forEach(name => {
    if (name.toLowerCase().includes(DEFAULT_FILTER.toLowerCase())) selectedCommunes.add(name);
  });
  refreshCommuneChips();
  applyFilters();
}

// ── Popup ─────────────────────────────────────────────────────────────────
function buildPopup(feature) {
  const p = feature.properties || {};
  const totalM2   = p.area_m2    != null ? `${Number(p.area_m2).toLocaleString('fr')} m²` : '—';
  const prairieM2 = p.prairie_m2 != null ? `${Number(p.prairie_m2).toLocaleString('fr')} m²` : '—';
  const pct       = p.pct_prairie != null ? `${p.pct_prairie} %` : '—';
  const own       = p.denomination || '—';
  const commune   = p.nom_commune  || '—';

  // Détail par type de couverture
  let csRows = '';
  try {
    const detail = typeof p.cs_detail === 'string' ? JSON.parse(p.cs_detail) : (p.cs_detail || {});
    const entries = Object.entries(detail).sort((a, b) => b[1] - a[1]);
    if (entries.length) {
      csRows = entries.map(([code, m2]) => {
        const label = CS_LABELS[code] || code;
        return `<span class="k" style="padding-left:16px;color:#555">${label}</span><span class="v" style="color:#aaa">${Number(m2).toLocaleString('fr')} m²</span>`;
      }).join('');
    }
  } catch(_) {}

  let gmaps = '';
  try {
    const c = L.geoJSON(feature).getBounds().getCenter();
    gmaps = `https://www.google.com/maps?q=${c.lat.toFixed(6)},${c.lng.toFixed(6)}`;
  } catch(_) {}

  const sirenTag = p.siren ? `<span class="tag tag-blue">SIREN&nbsp;${p.siren}</span>` : '';

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
        <span class="k">Pâturable</span>  <span class="v">${prairieM2} · ${pct}</span>
        ${csRows}
        <span class="k">Propriétaire</span>  <span class="v">${own}</span>
      </div>
      <div class="popup-tags">${sirenTag}</div>
    </div>
    ${links ? `<div class="popup-footer">${links}</div>` : ''}`;
}

// ── Onglets sidebar ───────────────────────────────────────────────────────
function switchTab(tab) {
  document.querySelectorAll('.sidebar-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tab));
  document.querySelectorAll('.sidebar-pane').forEach(p => p.classList.toggle('active', p.id === 'pane-' + tab));
}

// ── Itinéraire ────────────────────────────────────────────────────────────
let routeLayer        = null;
let routeParcelsLayer = null;
let routeMarkers      = [];
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

function projectionOnSegment(p, a, b) {
  const dx = b.lng - a.lng, dy = b.lat - a.lat;
  if (dx === 0 && dy === 0) return 0;
  return Math.max(0, Math.min(1, ((p.lng - a.lng)*dx + (p.lat - a.lat)*dy) / (dx*dx + dy*dy)));
}

function centroid(feature) {
  try {
    const b = L.geoJSON(feature).getBounds().getCenter();
    return { lat: b.lat, lng: b.lng };
  } catch(_) { return null; }
}

function orderAlongRoute(ptA, ptB, parcelles) {
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

    // 2. Parcelles candidates dans le périmètre
    const minArea  = parseInt(document.getElementById('rte-area').value);
    const radiusKm = parseInt(document.getElementById('rte-radius').value);
    const selectedIds = new Set(selectedParcels.map(p => p.id));

    candidateParcels = allFeatures
      .filter(f => {
        const p = f.properties || {};
        if ((p.prairie_m2 || 0) < minArea) return false;
        if (selectedIds.has(p.id || '')) return false;
        const c = centroid(f);
        if (!c) return false;
        if (distToSegment(c, ptA, ptB) > radiusKm) return false;
        return true;
      })
      .map(f => ({ feature: f, center: centroid(f), id: f.properties?.id || '' }));

    // 3. Waypoints : A → étapes fixes → parcelles sélectionnées ordonnées → B
    const orderedSelected = orderAlongRoute(ptA, ptB, selectedParcels);
    const waypoints = [ptA, ...fixedWaypoints, ...orderedSelected.map(p => p.center), ptB];

    // 4. Appel ORS foot-hiking
    setRouteStatus('Calcul de l\'itinéraire…', '');
    const orsKey = window.ORS_API_KEY || '';
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

    // Parcelles candidates en orange — cliquables pour ajouter
    routeParcelsLayer = L.geoJSON(
      { type: 'FeatureCollection', features: candidateParcels.map(p => p.feature) },
      {
        style: { fillColor: '#fb923c', fillOpacity: 0.45, color: '#fb923c', weight: 1.5, opacity: 0.8 },
        onEachFeature: (feature, layer) => {
          const fid = feature.properties?.id || '';
          layer.bindTooltip('➕ Cliquer pour ajouter à l\'itinéraire', { sticky: true, className: 'route-tooltip' });
          layer.on('click', () => addParcelToRoute(fid));
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
      const pct   = props.pct_prairie  != null ? `${props.pct_prairie}% prairie` : '';
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

  const cols = ['commune', 'surface_m2', 'prairie_m2', 'pct_prairie', 'proprietaire', 'siren'];
  const rows = filtered.map(f => {
    const p = f.properties || {};
    return [
      p.nom_commune  || '',
      p.area_m2      || '',
      p.prairie_m2   ?? '',
      p.pct_prairie  ?? '',
      p.denomination || '',
      p.siren        || '',
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
