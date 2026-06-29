(function () {
  var apiBase = window.SIGNALMAKER_API_BASE || '';

  function text(value) {
    if (value === null || value === undefined || value === '') return '-';
    return String(value).replace(/[&<>'"]/g, function (char) {
      return {'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char];
    });
  }

  function setHtml(id, html) {
    var element = document.getElementById(id);
    if (element) element.innerHTML = html;
  }

  function operatorHeaders() {
    var headers = { Accept: 'application/json' };
    try {
      var key = window.localStorage.getItem('signalmaker_operator_key') || '';
      if (key) headers['x-operator-key'] = key;
    } catch (error) {}
    return headers;
  }

  function fetchJson(path, options) {
    options = options || {};
    var headers = operatorHeaders();
    if (options.body) headers['Content-Type'] = 'application/json';

    return fetch(apiBase + path, {
      method: options.method || 'GET',
      headers: headers,
      body: options.body
    }).then(function (response) {
      if (!response.ok) throw new Error(response.status + ' ' + response.statusText);
      return response.json();
    });
  }

  function table(rows, columns) {
    if (!rows || !rows.length) return '<p class="muted">Aucune donnée.</p>';
    return '<div class="scroll"><table><thead><tr>' + columns.map(function (col) {
      return '<th>' + text(col.label) + '</th>';
    }).join('') + '</tr></thead><tbody>' + rows.map(function (row) {
      return '<tr>' + columns.map(function (col) {
        var value = typeof col.value === 'function' ? col.value(row) : row[col.key];
        return '<td>' + text(value) + '</td>';
      }).join('') + '</tr>';
    }).join('') + '</tbody></table></div>';
  }

  function asRows(payload) {
    if (Array.isArray(payload)) return payload;
    return payload && (payload.rows || payload.candidates || payload.items || payload.data) || [];
  }


  var adminSettingsPayload = null;

  function parseAdminValue(raw, original) {
    if (typeof original === 'boolean') return raw === 'true' || raw === '1' || raw === 'on';
    if (typeof original === 'number') {
      var number = Number(raw);
      return Number.isNaN(number) ? original : number;
    }
    if (Array.isArray(original)) return raw.split(',').map(function (item) { return item.trim(); }).filter(Boolean);
    return raw;
  }

  function renderAdminSettings(payload) {
    adminSettingsPayload = payload || {};
    var preferred = ['general', 'binance', 'kraken', 'strategy', 'notifications', 'bot', 'live', 'momentum', 'admin/security'];
    var seen = {};
    var sections = preferred.concat(Object.keys(adminSettingsPayload)).filter(function (section) {
      if (seen[section]) return false;
      seen[section] = true;
      return true;
    });
    return sections.map(function (section) {
      var values = adminSettingsPayload[section] || {};
      var keys = Object.keys(values);
      var rows = keys.map(function (key) {
        var value = values[key];
        var inputType = typeof value === 'number' ? 'number' : 'text';
        if (typeof value === 'boolean') {
          return '<tr><td><code>' + text(key) + '</code></td><td><select data-admin-section="' + text(section) + '" data-admin-key="' + text(key) + '"><option value="true"' + (value ? ' selected' : '') + '>true</option><option value="false"' + (!value ? ' selected' : '') + '>false</option></select></td></tr>';
        }
        return '<tr><td><code>' + text(key) + '</code></td><td><input type="' + inputType + '" data-admin-section="' + text(section) + '" data-admin-key="' + text(key) + '" value="' + text(Array.isArray(value) ? value.join(',') : value) + '"></td></tr>';
      }).join('');
      if (!rows) rows = '<tr><td colspan="2" class="muted">Section vide.</td></tr>';
      return '<details open><summary>' + text(section) + '</summary><div class="scroll"><table><tbody>' + rows + '</tbody></table></div></details>';
    }).join('');
  }

  function loadOperatorToken() {
    var input = document.getElementById('operator-key-input');
    if (!input) return;
    if (document.activeElement === input || input.getAttribute('data-loaded') === 'true') return;
    try { input.value = window.localStorage.getItem('signalmaker_operator_key') || ''; input.setAttribute('data-loaded', 'true'); } catch (error) {}
  }

  function saveOperatorToken() {
    var input = document.getElementById('operator-key-input');
    if (!input) return;
    try {
      window.localStorage.setItem('signalmaker_operator_key', input.value || '');
      setHtml('operator-key-result', 'Token admin local sauvegardé.');
    } catch (error) {
      setHtml('operator-key-result', 'Impossible de sauvegarder le token local : ' + text(error.message));
    }
  }

  function loadAdminSettings() {
    if (!document.getElementById('admin-settings-content')) return Promise.resolve();
    return fetchJson('/api/v1/admin/settings').then(function (payload) {
      setHtml('admin-settings-content', renderAdminSettings(payload));
    }).catch(function (error) { setHtml('admin-settings-content', '<p class="warn">Réglages indisponibles : ' + text(error.message) + '</p>'); });
  }

  function saveAdminSettings() {
    if (!adminSettingsPayload) return;
    var payload = JSON.parse(JSON.stringify(adminSettingsPayload));
    document.querySelectorAll('[data-admin-section][data-admin-key]').forEach(function (input) {
      var section = input.getAttribute('data-admin-section');
      var key = input.getAttribute('data-admin-key');
      var original = adminSettingsPayload[section] && adminSettingsPayload[section][key];
      payload[section][key] = parseAdminValue(input.value, original);
    });
    fetchJson('/api/v1/admin/settings', { method: 'PUT', body: JSON.stringify(payload) }).then(function (updated) {
      setHtml('admin-save-result', 'Réglages sauvegardés.');
      setHtml('admin-settings-content', renderAdminSettings(updated));
    }).catch(function (error) { setHtml('admin-save-result', 'Erreur sauvegarde : ' + text(error.message)); });
  }

  function loadAdminWorkers() {
    if (!document.getElementById('admin-workers-content')) return Promise.resolve();
    return fetchJson('/api/v1/admin/workers').then(function (payload) {
      setHtml('admin-workers-content', '<pre>' + text(JSON.stringify(payload, null, 2)) + '</pre>');
    }).catch(function (error) { setHtml('admin-workers-content', '<p class="warn">Workers indisponibles : ' + text(error.message) + '</p>'); });
  }

  function postAdminAction(path, resultId) {
    fetchJson(path, { method: 'POST' }).then(function (payload) {
      setHtml(resultId, '<pre>' + text(JSON.stringify(payload, null, 2)) + '</pre>');
      refresh();
    }).catch(function (error) { setHtml(resultId, 'Erreur : ' + text(error.message)); });
  }

  function loadHealth() {
    return fetchJson('/api/v1/health').catch(function () { return fetchJson('/healthz'); }).then(function (data) {
      var health = document.getElementById('health');
      if (!health) return;
      health.className = 'ok';
      health.textContent = (data.service || 'SignalMaker') + ' : ' + (data.status || 'ok');
    }).catch(function (error) {
      var health = document.getElementById('health');
      if (!health) return;
      health.className = 'bad';
      health.textContent = 'API indisponible : ' + error.message;
    });
  }

  function loadPositions() {
    return fetchJson('/api/v1/positions?limit=100').then(function (rows) {
      setHtml('positions-content', table(asRows(rows), [
        {key:'status', label:'Status'}, {key:'symbol', label:'Symbol'}, {key:'side', label:'Side'},
        {key:'quantity', label:'Qty'}, {key:'entry_price', label:'Entry'}, {key:'mark_price', label:'Mark'},
        {key:'unrealized_pnl', label:'PnL'}, {key:'opened_at', label:'Ouvert'}
      ]));
    }).catch(function (error) { setHtml('positions-content', '<p class="warn">Positions indisponibles : ' + text(error.message) + '</p>'); });
  }

  function loadCandidates() {
    return fetchJson('/api/v1/trade-candidates?limit=100').then(function (payload) {
      setHtml('candidates-content', table(asRows(payload), [
        {key:'candidate_id', label:'ID'}, {key:'symbol', label:'Symbol'}, {key:'stage', label:'Stage'},
        {key:'score', label:'Score'}, {key:'status', label:'Status'}, {key:'created_at', label:'Créé'}
      ]));
    }).catch(function (error) { setHtml('candidates-content', '<p class="warn">Candidats indisponibles : ' + text(error.message) + '</p>'); });
  }

  function loadMomentum() {
    return fetchJson('/api/v1/trade-candidates?limit=100&stage=momentum').then(function (payload) {
      setHtml('momentum-content', table(asRows(payload), [
        {key:'candidate_id', label:'ID'}, {key:'symbol', label:'Symbol'}, {key:'score', label:'Score'},
        {key:'status', label:'Status'}, {key:'target_pct', label:'Target %'}, {key:'created_at', label:'Créé'}
      ]));
    }).catch(function (error) { setHtml('momentum-content', '<p class="warn">Momentum indisponible : ' + text(error.message) + '</p>'); });
  }

  function loadAssets() {
    return fetchJson('/api/v1/assets?limit=100&sort_by=updated_at').then(function (payload) {
      setHtml('assets-content', table(asRows(payload), [
        {key:'symbol', label:'Symbol'}, {key:'state', label:'State'}, {key:'bias', label:'Bias'},
        {key:'score', label:'Score'}, {key:'rsi_15m', label:'RSI 15m'}, {key:'updated_at', label:'MAJ'}
      ]));
    }).catch(function (error) { setHtml('assets-content', '<p class="warn">Dashboard indisponible : ' + text(error.message) + '</p>'); });
  }

  function loadOps() {
    if (document.getElementById('services-content')) fetchJson('/api/v1/services').then(function (p) { setHtml('services-content', table(asRows(p), [{key:'name', label:'Service'}, {key:'status', label:'Status'}, {key:'detail', label:'Detail'}])); }).catch(function (e) { setHtml('services-content', '<p class="warn">Services indisponibles : '+text(e.message)+'</p>'); });
    if (document.getElementById('fills-content')) fetchJson('/api/v1/fills?limit=50').then(function (p) { setHtml('fills-content', table(asRows(p), [{key:'symbol', label:'Symbol'}, {key:'side', label:'Side'}, {key:'quantity', label:'Qty'}, {key:'price', label:'Prix'}, {key:'created_at', label:'Date'}])); }).catch(function (e) { setHtml('fills-content', '<p class="warn">Fills indisponibles : '+text(e.message)+'</p>'); });
    if (document.getElementById('live-runs-content')) fetchJson('/api/v1/live-runs?limit=20').then(function (p) { setHtml('live-runs-content', table(asRows(p), [{key:'id', label:'ID'}, {key:'status', label:'Status'}, {key:'started_at', label:'Début'}, {key:'finished_at', label:'Fin'}])); }).catch(function (e) { setHtml('live-runs-content', '<p class="warn">Runs indisponibles : '+text(e.message)+'</p>'); });
  }

  function loadLogs() {
    if (document.getElementById('workers-content')) fetchJson('/api/v1/admin/workers').then(function (p) { setHtml('workers-content', table(asRows(p), [{key:'name', label:'Worker'}, {key:'running', label:'Running'}, {key:'pid', label:'PID'}])); }).catch(function (e) { setHtml('workers-content', '<p class="warn">Workers indisponibles : '+text(e.message)+'</p>'); });
    if (document.getElementById('logs-content')) fetchJson('/api/v1/admin/logs/executor?lines=120').then(function (p) { setHtml('logs-content', '<pre>' + text((p.lines || p.logs || []).join ? (p.lines || p.logs || []).join('\n') : JSON.stringify(p, null, 2)) + '</pre>'); }).catch(function (e) { setHtml('logs-content', '<p class="warn">Logs indisponibles : '+text(e.message)+'</p>'); });
  }

  function loadMarketData() {
    if (document.getElementById('ibkr-content')) fetchJson('/admin/market-data/ibkr-feed/status').then(function (p) { setHtml('ibkr-content', '<pre>'+text(JSON.stringify(p, null, 2))+'</pre>'); }).catch(function (e) { setHtml('ibkr-content', '<p class="warn">IBKR indisponible : '+text(e.message)+'</p>'); });
  }

  function loadAsset() {
    var input = document.getElementById('asset-symbol');
    var params = new URLSearchParams(window.location.search);
    var symbol = (input && input.value) || params.get('symbol') || '';
    if (input && symbol) input.value = symbol;
    if (!symbol) return;
    fetchJson('/api/v1/assets/' + encodeURIComponent(symbol)).then(function (p) { setHtml('asset-content', '<pre>'+text(JSON.stringify(p, null, 2))+'</pre>'); }).catch(function (e) { setHtml('asset-content', '<p class="warn">Actif indisponible : '+text(e.message)+'</p>'); });
  }

  function refresh() {
    var updatedAt = document.getElementById('updated-at');
    if (updatedAt) updatedAt.textContent = new Date().toLocaleString();
    loadOperatorToken();
    if (document.getElementById('health')) loadHealth();
    if (document.getElementById('positions-content')) loadPositions();
    if (document.getElementById('candidates-content')) loadCandidates();
    if (document.getElementById('momentum-content')) loadMomentum();
    if (document.getElementById('assets-content')) loadAssets();
    loadAdminSettings(); loadAdminWorkers();
    loadOps(); loadLogs(); loadMarketData(); loadAsset();
  }

  document.addEventListener('click', function (event) {
    var action = event.target && event.target.getAttribute('data-action');
    if (action === 'refresh') refresh();
    if (action === 'sync-momentum') fetchJson('/api/v1/executor/sync-momentum-candidates', { method: 'POST' }).then(function (p) { setHtml('sync-result', 'Sync OK : ' + text(JSON.stringify(p))); refresh(); }).catch(function (e) { setHtml('sync-result', 'Sync erreur : ' + text(e.message)); });
    if (action === 'test-ibkr') fetchJson('/admin/market-data/ibkr-feed/test-ingest', { method: 'POST' }).then(function (p) { setHtml('ibkr-result', 'Test OK : ' + text(JSON.stringify(p))); }).catch(function (e) { setHtml('ibkr-result', 'Test erreur : ' + text(e.message)); });
    if (action === 'load-asset') loadAsset();
    if (action === 'save-operator-token') saveOperatorToken();
    if (action === 'save-admin-settings') saveAdminSettings();
    if (action === 'reset-database' && window.confirm('Reset database runtime ?')) postAdminAction('/api/v1/admin/reset-database', 'admin-action-result');
    if (action === 'test-binance') postAdminAction('/api/v1/admin/test/binance', 'admin-action-result');
    if (action === 'test-notifications') postAdminAction('/api/v1/admin/test/notifications', 'admin-action-result');
  });
  refresh();
  setInterval(refresh, 30000);
}());
