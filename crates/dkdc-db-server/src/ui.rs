use std::sync::Arc;

use axum::Router;
use axum::extract::{Form, Path, State};
use axum::http::StatusCode;
use axum::http::header;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{delete, get, post};
use dkdc_db_core::DbManager;
use serde::Deserialize;

type AppState = Arc<DbManager>;

// ---------------------------------------------------------------------------
// Static assets (embedded in binary)
// ---------------------------------------------------------------------------

const HTMX_JS: &str = include_str!("../static/htmx.min.js");

const CSS: &str = r##"
:root {
  --bg: #fff; --bg2: #f5f5f5; --bg3: #e8e8e8;
  --fg: #1a1a1a; --fg2: #555; --fg3: #888;
  --accent: #2563eb; --accent-hover: #1d4ed8;
  --danger: #dc2626; --danger-hover: #b91c1c;
  --success: #16a34a;
  --border: #ddd; --radius: 8px;
  --shadow: 0 1px 3px rgba(0,0,0,0.08);
  --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  --mono: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
}
[data-theme="dark"] {
  --bg: #111; --bg2: #1a1a1a; --bg3: #252525;
  --fg: #e5e5e5; --fg2: #aaa; --fg3: #666;
  --accent: #3b82f6; --accent-hover: #60a5fa;
  --danger: #ef4444; --danger-hover: #f87171;
  --success: #22c55e;
  --border: #333; --shadow: 0 1px 3px rgba(0,0,0,0.3);
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { font-family: var(--font); background: var(--bg); color: var(--fg); }
body { max-width: 1100px; margin: 0 auto; padding: 0 16px 48px; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.header { display: flex; align-items: center; justify-content: space-between; padding: 16px 0; border-bottom: 1px solid var(--border); margin-bottom: 24px; }
.header h1 { font-size: 1.25rem; font-weight: 600; }
.header h1 a { color: var(--fg); }
.header-right { display: flex; align-items: center; gap: 12px; }
.health-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--success); display: inline-block; }
.nav { display: flex; gap: 16px; font-size: 0.9rem; }
.nav a { color: var(--fg2); }
.nav a:hover, .nav a.active { color: var(--accent); }
.theme-toggle { background: none; border: 1px solid var(--border); border-radius: var(--radius); padding: 4px 10px; cursor: pointer; color: var(--fg2); font-size: 0.85rem; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 16px; margin: 16px 0; }
.card { background: var(--bg2); border: 1px solid var(--border); border-radius: var(--radius); padding: 20px; box-shadow: var(--shadow); transition: border-color 0.15s; }
.card:hover { border-color: var(--accent); }
.card h3 { font-size: 1rem; margin-bottom: 4px; }
.card p { color: var(--fg2); font-size: 0.85rem; }
.card a { display: block; }
.btn { display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; border-radius: var(--radius); border: none; cursor: pointer; font-size: 0.875rem; font-weight: 500; transition: background 0.15s; }
.btn-primary { background: var(--accent); color: #fff; }
.btn-primary:hover { background: var(--accent-hover); }
.btn-danger { background: var(--danger); color: #fff; }
.btn-danger:hover { background: var(--danger-hover); }
.btn-secondary { background: var(--bg3); color: var(--fg); border: 1px solid var(--border); }
.btn-secondary:hover { background: var(--border); }
.btn-sm { padding: 4px 10px; font-size: 0.8rem; }
.form-group { margin-bottom: 12px; }
.form-group label { display: block; font-size: 0.85rem; color: var(--fg2); margin-bottom: 4px; font-weight: 500; }
input[type="text"], textarea, select {
  width: 100%; padding: 8px 12px; border: 1px solid var(--border); border-radius: var(--radius);
  background: var(--bg); color: var(--fg); font-size: 0.9rem; font-family: var(--font);
}
textarea { font-family: var(--mono); resize: vertical; }
input:focus, textarea:focus, select:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 2px rgba(37,99,235,0.15); }
.data-table { width: 100%; border-collapse: collapse; font-size: 0.85rem; margin: 12px 0; }
.data-table th, .data-table td { padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); }
.data-table th { background: var(--bg2); font-weight: 600; color: var(--fg2); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; position: sticky; top: 0; }
.data-table tr:hover td { background: var(--bg2); }
.data-table td { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: var(--radius); }
.query-area { width: 100%; min-height: 120px; font-family: var(--mono); font-size: 0.9rem; padding: 12px; }
.query-bar { display: flex; gap: 8px; align-items: center; margin: 8px 0; flex-wrap: wrap; }
.result-info { font-size: 0.85rem; color: var(--fg2); margin: 8px 0; }
.alert { padding: 12px 16px; border-radius: var(--radius); margin: 12px 0; font-size: 0.9rem; }
.alert-error { background: #fef2f2; color: #991b1b; border: 1px solid #fecaca; }
[data-theme="dark"] .alert-error { background: #2a1515; color: #fca5a5; border-color: #5a2020; }
.alert-success { background: #f0fdf4; color: #166534; border: 1px solid #bbf7d0; }
[data-theme="dark"] .alert-success { background: #152a15; color: #86efac; border-color: #205a20; }
.onboard { max-width: 500px; margin: 60px auto; text-align: center; }
.onboard h2 { font-size: 1.5rem; margin-bottom: 8px; }
.onboard p { color: var(--fg2); margin-bottom: 24px; }
.onboard form { text-align: left; }
.section { margin: 24px 0; }
.section-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
.section-header h2 { font-size: 1.1rem; }
.breadcrumb { font-size: 0.85rem; color: var(--fg3); margin-bottom: 16px; }
.breadcrumb a { color: var(--fg2); }
.mono { font-family: var(--mono); }
.badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.75rem; background: var(--bg3); color: var(--fg2); }
.empty { text-align: center; padding: 40px; color: var(--fg3); }
.tabs { display: flex; gap: 0; border-bottom: 1px solid var(--border); margin-bottom: 16px; }
.tab { padding: 8px 16px; cursor: pointer; font-size: 0.85rem; color: var(--fg2); border-bottom: 2px solid transparent; background: none; border-top: none; border-left: none; border-right: none; }
.tab.active { color: var(--accent); border-bottom-color: var(--accent); }
.tab-content { display: none; }
.tab-content.active { display: block; }
.inline-form { display: flex; gap: 8px; align-items: end; }
.inline-form .form-group { flex: 1; margin-bottom: 0; }
.columns-builder { margin: 8px 0; }
.col-row { display: flex; gap: 8px; margin-bottom: 6px; align-items: center; }
.col-row input, .col-row select { width: auto; flex: 1; }
.col-row .btn { flex-shrink: 0; }
.htmx-indicator { display: none; }
.htmx-request .htmx-indicator { display: inline; }
.spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid var(--border); border-top-color: var(--accent); border-radius: 50%; animation: spin 0.6s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
.query-history { margin-top: 16px; }
.query-history summary { cursor: pointer; font-size: 0.85rem; color: var(--fg2); }
.query-history-item { padding: 6px 8px; font-size: 0.8rem; font-family: var(--mono); cursor: pointer; border-bottom: 1px solid var(--border); color: var(--fg2); }
.query-history-item:hover { background: var(--bg2); color: var(--fg); }
@media (max-width: 600px) {
  body { padding: 0 8px 32px; }
  .cards { grid-template-columns: 1fr; }
  .header { flex-wrap: wrap; gap: 8px; }
  .inline-form { flex-direction: column; }
  .query-bar { flex-direction: column; align-items: stretch; }
}
"##;

const JS: &str = r##"
(function() {
  function getTheme() {
    return localStorage.getItem('theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  }
  function applyTheme(t) {
    document.documentElement.setAttribute('data-theme', t);
    localStorage.setItem('theme', t);
    var btn = document.getElementById('theme-btn');
    if (btn) btn.textContent = t === 'dark' ? 'Light' : 'Dark';
  }
  applyTheme(getTheme());
  window.toggleTheme = function() { applyTheme(getTheme() === 'dark' ? 'light' : 'dark'); };

  window.switchTab = function(group, name) {
    document.querySelectorAll('[data-tab-group="'+group+'"] .tab').forEach(function(t) {
      t.classList.toggle('active', t.dataset.tab === name);
    });
    document.querySelectorAll('[data-tab-content-group="'+group+'"]').forEach(function(c) {
      c.classList.toggle('active', c.dataset.tabContent === name);
    });
  };

  window.addColumn = function() {
    var container = document.getElementById('columns-list');
    if (!container) return;
    var div = document.createElement('div');
    div.className = 'col-row';
    div.innerHTML = '<input type="text" name="col_name" placeholder="column name" required>'
      + '<select name="col_type"><option>TEXT</option><option>INTEGER</option><option>REAL</option><option>BLOB</option></select>'
      + '<button type="button" class="btn btn-secondary btn-sm" onclick="this.parentElement.remove()">Remove</button>';
    container.appendChild(div);
  };

  window.buildCreateTableSql = function(form) {
    var tableName = form.querySelector('[name="table_name"]').value.trim();
    var names = form.querySelectorAll('[name="col_name"]');
    var types = form.querySelectorAll('[name="col_type"]');
    var cols = [];
    for (var i = 0; i < names.length; i++) {
      var n = names[i].value.trim();
      if (n) cols.push(n + ' ' + types[i].value);
    }
    if (!tableName || cols.length === 0) return '';
    return 'CREATE TABLE ' + tableName + ' (' + cols.join(', ') + ')';
  };

  document.addEventListener('keydown', function(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      var ta = document.querySelector('.query-area');
      if (ta && document.activeElement === ta) {
        var form = ta.closest('form');
        if (form) htmx.trigger(form, 'submit');
      }
    }
  });

  window.queryHistory = JSON.parse(localStorage.getItem('queryHistory') || '[]');
  window.addToHistory = function(sql) {
    if (!sql || !sql.trim()) return;
    sql = sql.trim();
    window.queryHistory = window.queryHistory.filter(function(q) { return q !== sql; });
    window.queryHistory.unshift(sql);
    if (window.queryHistory.length > 20) window.queryHistory.pop();
    localStorage.setItem('queryHistory', JSON.stringify(window.queryHistory));
    renderHistory();
  };
  window.loadQuery = function(sql) {
    var ta = document.querySelector('.query-area');
    if (ta) { ta.value = sql; ta.focus(); }
  };
  window.renderHistory = function() {
    var el = document.getElementById('history-list');
    if (!el) return;
    if (window.queryHistory.length === 0) { el.innerHTML = '<p style="font-size:0.8rem;color:var(--fg3)">No history yet</p>'; return; }
    el.innerHTML = window.queryHistory.map(function(q) {
      return '<div class="query-history-item" onclick="loadQuery(this.textContent)">' + q.replace(/</g,'&lt;') + '</div>';
    }).join('');
  };

  document.addEventListener('htmx:afterRequest', function(e) {
    var form = e.detail.elt;
    if (form && form.tagName === 'FORM') {
      var ta = form.querySelector('.query-area, textarea[name="sql"]');
      if (ta && ta.value) addToHistory(ta.value);
    }
  });

  document.addEventListener('DOMContentLoaded', function() { if (window.renderHistory) renderHistory(); });
})();
"##;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn ui_routes() -> Router<AppState> {
    Router::new()
        .route("/ui", get(dashboard))
        .route("/ui/db/{name}", get(database_view))
        .route("/ui/db/{name}/table/{table}", get(table_view))
        .route("/ui/query", get(query_editor))
        .route("/ui/api/create-db", post(api_create_db))
        .route("/ui/api/drop-db/{name}", delete(api_drop_db))
        .route("/ui/api/execute/{name}", post(api_execute))
        .route("/ui/api/query", post(api_query))
        .route(
            "/ui/onboarding/create-table/{name}",
            get(onboarding_create_table),
        )
        .route("/ui/onboarding/insert/{name}", get(onboarding_insert))
        .route("/ui/onboarding/done/{name}", get(onboarding_done))
        .route("/ui/static/htmx.min.js", get(serve_htmx))
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

fn layout(title: &str, content: &str, active_nav: &str) -> Html<String> {
    let dashboard_class = if active_nav == "dashboard" {
        "active"
    } else {
        ""
    };
    let query_class = if active_nav == "query" { "active" } else { "" };
    Html(format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} — dkdc-db</title>
<style>{css}</style>
<script>{htmx}</script>
<script>{js}</script>
</head>
<body>
<div class="header">
  <h1><a href="/ui">dkdc-db</a> <span class="health-dot" title="healthy"></span></h1>
  <div class="header-right">
    <nav class="nav">
      <a href="/ui" class="{dashboard_class}">Dashboard</a>
      <a href="/ui/query" class="{query_class}">Query</a>
    </nav>
    <button class="theme-toggle" id="theme-btn" onclick="toggleTheme()">Dark</button>
  </div>
</div>
{content}
</body>
</html>"##,
        title = escape_html(title),
        css = CSS,
        htmx = HTMX_JS,
        js = JS,
        content = content,
        dashboard_class = dashboard_class,
        query_class = query_class,
    ))
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

// ---------------------------------------------------------------------------
// Pages
// ---------------------------------------------------------------------------

async fn dashboard(State(mgr): State<AppState>) -> Response {
    let dbs = mgr.list_dbs().await;

    if dbs.is_empty() {
        return onboarding_welcome().into_response();
    }

    let mut cards = String::new();
    for db_name in &dbs {
        let table_count = mgr.list_tables(db_name).await.map(|t| t.len()).unwrap_or(0);
        let name_escaped = escape_html(db_name);
        let plural = if table_count == 1 { "" } else { "s" };
        cards.push_str(&format!(
            r##"<a href="/ui/db/{name}" class="card">
  <h3>{name}</h3>
  <p>{count} table{plural}</p>
</a>"##,
            name = name_escaped,
            count = table_count,
            plural = plural,
        ));
    }

    let content = format!(
        r##"<div class="section">
  <div class="section-header">
    <h2>Databases</h2>
  </div>
  <form hx-post="/ui/api/create-db" hx-target="#dashboard-content" hx-swap="innerHTML" class="inline-form" style="margin-bottom:16px">
    <div class="form-group">
      <input type="text" name="name" placeholder="New database name..." required>
    </div>
    <button type="submit" class="btn btn-primary">Create Database</button>
  </form>
  <div id="dashboard-content">
    <div class="cards">{cards}</div>
  </div>
</div>"##,
        cards = cards,
    );

    layout("Dashboard", &content, "dashboard").into_response()
}

fn onboarding_welcome() -> Html<String> {
    let content = r##"<div class="onboard">
  <h2>Welcome to dkdc-db</h2>
  <p>A lightweight HTAP database server with cross-database joins.<br>
  Let's create your first database to get started.</p>
  <form hx-post="/ui/api/create-db" hx-target="closest .onboard" hx-swap="innerHTML">
    <div class="form-group">
      <label>Database name</label>
      <input type="text" name="name" placeholder="mydb" required autofocus>
    </div>
    <button type="submit" class="btn btn-primary" style="width:100%;justify-content:center">Create Database</button>
  </form>
</div>"##;
    layout("Welcome", content, "")
}

async fn onboarding_create_table(
    State(mgr): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    // Validate database exists before showing the create-table form
    let dbs = mgr.list_dbs().await;
    if !dbs.contains(&name) {
        return Redirect::to("/ui").into_response();
    }

    let n = escape_html(&name);
    let content = format!(
        r##"<div class="onboard" style="max-width:600px">
  <h2>Create a Table</h2>
  <p>Add a table to <strong>{n}</strong>. Choose visual mode or write SQL directly.</p>

  <div class="tabs" data-tab-group="ct">
    <button class="tab active" data-tab="visual" onclick="switchTab('ct','visual')">Visual</button>
    <button class="tab" data-tab="sql" onclick="switchTab('ct','sql')">SQL</button>
  </div>

  <div class="tab-content active" data-tab-content-group="ct" data-tab-content="visual">
    <form hx-post="/ui/api/execute/{n}" hx-target="#ctresult" hx-swap="innerHTML"
          onsubmit="var s=buildCreateTableSql(this); if(!s){{event.preventDefault();return;}} this.querySelector('[name=sql]').value=s;">
      <input type="hidden" name="sql" value="">
      <div class="form-group">
        <label>Table name</label>
        <input type="text" name="table_name" placeholder="users" required>
      </div>
      <label style="font-size:0.85rem;color:var(--fg2);font-weight:500">Columns</label>
      <div id="columns-list" class="columns-builder">
        <div class="col-row">
          <input type="text" name="col_name" placeholder="id" required>
          <select name="col_type"><option>INTEGER</option><option>TEXT</option><option>REAL</option><option>BLOB</option></select>
          <button type="button" class="btn btn-secondary btn-sm" onclick="this.parentElement.remove()">Remove</button>
        </div>
        <div class="col-row">
          <input type="text" name="col_name" placeholder="name" required>
          <select name="col_type"><option>TEXT</option><option>INTEGER</option><option>REAL</option><option>BLOB</option></select>
          <button type="button" class="btn btn-secondary btn-sm" onclick="this.parentElement.remove()">Remove</button>
        </div>
      </div>
      <button type="button" class="btn btn-secondary btn-sm" onclick="addColumn()" style="margin-bottom:12px">+ Add Column</button>
      <br>
      <button type="submit" class="btn btn-primary" style="width:100%;justify-content:center">Create Table</button>
    </form>
  </div>

  <div class="tab-content" data-tab-content-group="ct" data-tab-content="sql">
    <form hx-post="/ui/api/execute/{n}" hx-target="#ctresult" hx-swap="innerHTML">
      <div class="form-group">
        <label>SQL</label>
        <textarea name="sql" class="query-area" rows="4" placeholder="CREATE TABLE users (id INTEGER, name TEXT)"></textarea>
      </div>
      <button type="submit" class="btn btn-primary" style="width:100%;justify-content:center">Create Table</button>
    </form>
  </div>

  <div id="ctresult"></div>
  <div style="margin-top:16px">
    <a href="/ui/onboarding/insert/{n}">Skip to insert data &rarr;</a> &nbsp;|&nbsp;
    <a href="/ui/db/{n}">Go to database &rarr;</a>
  </div>
</div>"##,
        n = n,
    );
    layout("Create Table", &content, "").into_response()
}

async fn onboarding_insert(State(mgr): State<AppState>, Path(name): Path<String>) -> Html<String> {
    let n = escape_html(&name);
    let tables = mgr.list_tables(&name).await.unwrap_or_default();

    let table_info = if tables.is_empty() {
        format!(
            r##"<p>No tables in <strong>{n}</strong> yet. <a href="/ui/onboarding/create-table/{n}">Create one first</a>.</p>"##,
            n = n,
        )
    } else {
        let plural = if tables.len() == 1 { "" } else { "s" };
        format!(
            r##"<p>Insert data into <strong>{n}</strong>. {count} table{plural} available.</p>"##,
            n = n,
            count = tables.len(),
            plural = plural,
        )
    };

    let content = format!(
        r##"<div class="onboard" style="max-width:600px">
  <h2>Insert Data</h2>
  {table_info}
  <form hx-post="/ui/api/execute/{n}" hx-target="#insertresult" hx-swap="innerHTML">
    <div class="form-group">
      <label>SQL</label>
      <textarea name="sql" class="query-area" rows="4" placeholder="INSERT INTO users VALUES (1, 'Alice')"></textarea>
    </div>
    <button type="submit" class="btn btn-primary" style="width:100%;justify-content:center">Insert Data</button>
  </form>
  <div id="insertresult"></div>
  <div style="margin-top:16px">
    <a href="/ui/onboarding/done/{n}">Finish setup &rarr;</a> &nbsp;|&nbsp;
    <a href="/ui/db/{n}">Go to database &rarr;</a>
  </div>
</div>"##,
        table_info = table_info,
        n = n,
    );
    layout("Insert Data", &content, "")
}

async fn onboarding_done(Path(name): Path<String>) -> Html<String> {
    let n = escape_html(&name);
    let content = format!(
        r##"<div class="onboard">
  <h2>Your database is ready!</h2>
  <p><strong>{n}</strong> is set up and ready to use.</p>
  <div style="display:flex;gap:12px;justify-content:center;margin-top:24px">
    <a href="/ui/db/{n}" class="btn btn-primary">Browse Database</a>
    <a href="/ui/query" class="btn btn-secondary">Query Editor</a>
  </div>
</div>"##,
        n = n,
    );
    layout("Ready", &content, "")
}

async fn database_view(State(mgr): State<AppState>, Path(name): Path<String>) -> Response {
    let tables = match mgr.list_tables(&name).await {
        Ok(t) => t,
        Err(e) => {
            let content = format!(
                r##"<div class="alert alert-error">Database not found: {err}</div>
<a href="/ui">&larr; Back to dashboard</a>"##,
                err = escape_html(&e.to_string()),
            );
            return layout("Error", &content, "dashboard").into_response();
        }
    };

    let n = escape_html(&name);
    let mut table_rows = String::new();
    for table_name in &tables {
        let row_count = mgr
            .query_oltp(&name, &format!("SELECT COUNT(*) FROM \"{}\"", table_name))
            .await
            .ok()
            .and_then(|batches| {
                batches.first().filter(|b| b.num_rows() > 0).map(|b| {
                    use arrow::array::Int64Array;
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|a| a.value(0))
                        .unwrap_or(0)
                })
            })
            .unwrap_or(0);

        let tn = escape_html(table_name);
        table_rows.push_str(&format!(
            "<tr><td><a href=\"/ui/db/{n}/table/{tn}\">{tn}</a></td><td>{row_count}</td></tr>",
            n = n,
            tn = tn,
            row_count = row_count,
        ));
    }

    let tables_html = if tables.is_empty() {
        String::from(r##"<div class="empty">No tables yet. Create one below.</div>"##)
    } else {
        format!(
            r##"<div class="table-wrap"><table class="data-table">
<thead><tr><th>Table</th><th>Rows</th></tr></thead>
<tbody>{table_rows}</tbody>
</table></div>"##,
            table_rows = table_rows,
        )
    };

    let content = format!(
        r##"<div class="breadcrumb"><a href="/ui">Databases</a> / {n}</div>

<div class="section">
  <div class="section-header">
    <h2>{n}</h2>
    <button class="btn btn-danger btn-sm"
            hx-delete="/ui/api/drop-db/{n}"
            hx-confirm="Drop database '{n}'? This cannot be undone."
            hx-target="body">Drop Database</button>
  </div>
  {tables_html}
</div>

<div class="section">
  <div class="section-header"><h2>Create Table</h2></div>
  <div class="tabs" data-tab-group="ct">
    <button class="tab active" data-tab="visual" onclick="switchTab('ct','visual')">Visual</button>
    <button class="tab" data-tab="sql" onclick="switchTab('ct','sql')">SQL</button>
  </div>
  <div class="tab-content active" data-tab-content-group="ct" data-tab-content="visual">
    <form hx-post="/ui/api/execute/{n}" hx-target="#ctresult" hx-swap="innerHTML"
          onsubmit="var s=buildCreateTableSql(this); if(!s){{event.preventDefault();return;}} this.querySelector('[name=sql]').value=s;">
      <input type="hidden" name="sql" value="">
      <div class="form-group"><label>Table name</label>
        <input type="text" name="table_name" placeholder="users" required></div>
      <label style="font-size:0.85rem;color:var(--fg2);font-weight:500">Columns</label>
      <div id="columns-list" class="columns-builder">
        <div class="col-row">
          <input type="text" name="col_name" placeholder="column name" required>
          <select name="col_type"><option>TEXT</option><option>INTEGER</option><option>REAL</option><option>BLOB</option></select>
          <button type="button" class="btn btn-secondary btn-sm" onclick="this.parentElement.remove()">Remove</button>
        </div>
      </div>
      <button type="button" class="btn btn-secondary btn-sm" onclick="addColumn()" style="margin-bottom:12px">+ Add Column</button><br>
      <button type="submit" class="btn btn-primary">Create Table</button>
    </form>
  </div>
  <div class="tab-content" data-tab-content-group="ct" data-tab-content="sql">
    <form hx-post="/ui/api/execute/{n}" hx-target="#ctresult" hx-swap="innerHTML">
      <div class="form-group"><label>SQL</label>
        <textarea name="sql" class="query-area" rows="3" placeholder="CREATE TABLE users (id INTEGER, name TEXT)"></textarea></div>
      <button type="submit" class="btn btn-primary">Create Table</button>
    </form>
  </div>
  <div id="ctresult"></div>
</div>

<div class="section">
  <div class="section-header"><h2>Quick Execute</h2></div>
  <form hx-post="/ui/api/execute/{n}" hx-target="#execresult" hx-swap="innerHTML">
    <div class="form-group">
      <textarea name="sql" class="query-area" rows="3" placeholder="INSERT INTO users VALUES (1, 'Alice')"></textarea>
    </div>
    <button type="submit" class="btn btn-primary">Execute</button>
  </form>
  <div id="execresult"></div>
</div>"##,
        n = n,
        tables_html = tables_html,
    );

    layout(&name, &content, "dashboard").into_response()
}

async fn table_view(
    State(mgr): State<AppState>,
    Path((db_name, table_name)): Path<(String, String)>,
) -> Response {
    let db = escape_html(&db_name);
    let tbl = escape_html(&table_name);

    // Validate table_name exists in this database before using it in SQL
    let valid_tables = mgr.list_tables(&db_name).await.unwrap_or_default();
    if !valid_tables.contains(&table_name) {
        let content = format!(
            r##"<div class="alert alert-error">Table not found: {tbl}</div>
<a href="/ui/db/{db}">&larr; Back to database</a>"##,
            tbl = tbl,
            db = db,
        );
        return layout("Error", &content, "dashboard").into_response();
    }

    // Schema
    let schema_html = match mgr.table_schema(&db_name, &table_name).await {
        Ok(batches) => {
            let resp = crate::api::batches_to_response(&batches);
            let headers: String = resp
                .columns
                .iter()
                .map(|c| format!("<th>{}</th>", escape_html(&c.name)))
                .collect();
            let mut rows = String::new();
            for row in &resp.rows {
                rows.push_str("<tr>");
                for val in row {
                    rows.push_str(&format!("<td>{}</td>", escape_html(&format_value(val))));
                }
                rows.push_str("</tr>");
            }
            format!(
                r##"<div class="table-wrap"><table class="data-table">
<thead><tr>{headers}</tr></thead><tbody>{rows}</tbody></table></div>"##,
                headers = headers,
                rows = rows,
            )
        }
        Err(e) => format!(
            r##"<div class="alert alert-error">Error loading schema: {err}</div>"##,
            err = escape_html(&e.to_string()),
        ),
    };

    // Data (first 100 rows)
    let data_html = match mgr
        .query_oltp(
            &db_name,
            &format!("SELECT * FROM \"{}\" LIMIT 100", table_name),
        )
        .await
    {
        Ok(batches) => {
            let resp = crate::api::batches_to_response(&batches);
            if resp.rows.is_empty() {
                String::from(r##"<div class="empty">No rows yet.</div>"##)
            } else {
                let headers: String = resp
                    .columns
                    .iter()
                    .map(|c| format!("<th>{}</th>", escape_html(&c.name)))
                    .collect();
                let mut rows = String::new();
                for row in &resp.rows {
                    rows.push_str("<tr>");
                    for val in row {
                        rows.push_str(&format!("<td>{}</td>", escape_html(&format_value(val))));
                    }
                    rows.push_str("</tr>");
                }
                let count = resp.rows.len();
                let plural = if count == 1 { "" } else { "s" };
                format!(
                    r##"<div class="result-info">{count} row{plural}</div>
<div class="table-wrap"><table class="data-table">
<thead><tr>{headers}</tr></thead><tbody>{rows}</tbody></table></div>"##,
                    count = count,
                    plural = plural,
                    headers = headers,
                    rows = rows,
                )
            }
        }
        Err(e) => format!(
            r##"<div class="alert alert-error">Error loading data: {err}</div>"##,
            err = escape_html(&e.to_string()),
        ),
    };

    let content = format!(
        r##"<div class="breadcrumb">
  <a href="/ui">Databases</a> / <a href="/ui/db/{db}">{db}</a> / {tbl}
</div>
<div class="section">
  <div class="section-header">
    <h2>{tbl}</h2>
    <a href="/ui/query?db={db}&amp;sql=SELECT+*+FROM+{tbl}+LIMIT+100" class="btn btn-secondary btn-sm">Open in Query Editor</a>
  </div>
</div>
<div class="section">
  <h3 style="font-size:0.95rem;margin-bottom:8px">Schema</h3>
  {schema_html}
</div>
<div class="section">
  <h3 style="font-size:0.95rem;margin-bottom:8px">Data</h3>
  {data_html}
</div>
<div class="section">
  <div class="section-header"><h2>Insert Row</h2></div>
  <form hx-post="/ui/api/execute/{db}" hx-target="#insertresult" hx-swap="innerHTML">
    <div class="form-group">
      <textarea name="sql" class="query-area" rows="2" placeholder="INSERT INTO {tbl} VALUES (...)"></textarea>
    </div>
    <button type="submit" class="btn btn-primary">Insert</button>
  </form>
  <div id="insertresult"></div>
</div>"##,
        db = db,
        tbl = tbl,
        schema_html = schema_html,
        data_html = data_html,
    );

    layout(
        &format!("{}.{}", db_name, table_name),
        &content,
        "dashboard",
    )
    .into_response()
}

#[derive(Deserialize)]
struct QueryParams {
    db: Option<String>,
    sql: Option<String>,
}

async fn query_editor(
    State(mgr): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<QueryParams>,
) -> Html<String> {
    let dbs = mgr.list_dbs().await;

    let db_options: String = std::iter::once(String::from(
        r##"<option value="">All (cross-database)</option>"##,
    ))
    .chain(dbs.iter().map(|d| {
        let selected = if params.db.as_deref() == Some(d.as_str()) {
            " selected"
        } else {
            ""
        };
        format!(
            r##"<option value="{name}"{selected}>{name}</option>"##,
            name = escape_html(d),
            selected = selected,
        )
    }))
    .collect();

    let initial_sql = escape_html(&params.sql.as_deref().unwrap_or("").replace('+', " "));

    let content = format!(
        r##"<div class="section">
  <h2>Query Editor</h2>
  <form hx-post="/ui/api/query" hx-target="#queryresult" hx-swap="innerHTML">
    <div class="query-bar">
      <div class="form-group" style="min-width:200px">
        <label>Database</label>
        <select name="db">{db_options}</select>
      </div>
    </div>
    <div class="form-group">
      <textarea name="sql" class="query-area" rows="6" placeholder="SELECT * FROM ...">{initial_sql}</textarea>
    </div>
    <div class="query-bar">
      <button type="submit" class="btn btn-primary">Run Query <span class="htmx-indicator"><span class="spinner"></span></span></button>
      <span style="font-size:0.8rem;color:var(--fg3)">Ctrl+Enter to run</span>
    </div>
  </form>
  <div id="queryresult"></div>
  <details class="query-history">
    <summary>Query History</summary>
    <div id="history-list"></div>
  </details>
</div>"##,
        db_options = db_options,
        initial_sql = initial_sql,
    );

    layout("Query Editor", &content, "query")
}

// ---------------------------------------------------------------------------
// HTMX API endpoints (return HTML fragments)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CreateDbForm {
    name: String,
}

#[derive(Deserialize)]
struct SqlForm {
    sql: String,
    db: Option<String>,
}

async fn api_create_db(State(mgr): State<AppState>, Form(form): Form<CreateDbForm>) -> Response {
    match mgr.create_db(&form.name).await {
        Ok(()) => {
            let dbs = mgr.list_dbs().await;
            if dbs.len() == 1 {
                let n = escape_html(&form.name);
                Html(format!(
                    r##"<h2>Database created!</h2>
<p><strong>{n}</strong> is ready. Let's add a table.</p>
<a href="/ui/onboarding/create-table/{n}" class="btn btn-primary" style="margin-top:16px">Create a Table &rarr;</a>
<br><br>
<a href="/ui" style="font-size:0.85rem;color:var(--fg2)">Skip to dashboard</a>"##,
                    n = n,
                ))
                .into_response()
            } else {
                Redirect::to("/ui").into_response()
            }
        }
        Err(e) => Html(format!(
            r##"<div class="alert alert-error">{err}</div>"##,
            err = escape_html(&e.to_string()),
        ))
        .into_response(),
    }
}

async fn api_drop_db(State(mgr): State<AppState>, Path(name): Path<String>) -> Response {
    match mgr.drop_db(&name).await {
        Ok(()) => {
            let mut response = Html(String::new()).into_response();
            response
                .headers_mut()
                .insert("HX-Redirect", "/ui".parse().unwrap());
            response
        }
        Err(e) => Html(format!(
            r##"<div class="alert alert-error">{err}</div>"##,
            err = escape_html(&e.to_string()),
        ))
        .into_response(),
    }
}

async fn api_execute(
    State(mgr): State<AppState>,
    Path(name): Path<String>,
    Form(form): Form<SqlForm>,
) -> Html<String> {
    match mgr.execute(&name, &form.sql).await {
        Ok(affected) => {
            let plural = if affected == 1 { "" } else { "s" };
            Html(format!(
                r##"<div class="alert alert-success">Success — {affected} row{plural} affected. <a href="" onclick="location.reload();return false;">Refresh page</a></div>"##,
                affected = affected,
                plural = plural,
            ))
        }
        Err(e) => Html(format!(
            r##"<div class="alert alert-error">{err}</div>"##,
            err = escape_html(&e.to_string()),
        )),
    }
}

async fn api_query(State(mgr): State<AppState>, Form(form): Form<SqlForm>) -> Html<String> {
    let db_name = form.db.as_deref().filter(|s| !s.is_empty());
    let sql = &form.sql;
    let is_write = dkdc_db_core::router::is_write(sql);

    if is_write {
        let Some(db) = db_name else {
            return Html(String::from(
                r##"<div class="alert alert-error">Write operations require a specific database selected.</div>"##,
            ));
        };
        match mgr.execute(db, sql).await {
            Ok(affected) => {
                let plural = if affected == 1 { "" } else { "s" };
                Html(format!(
                    r##"<div class="alert alert-success">Success — {affected} row{plural} affected.</div>"##,
                    affected = affected,
                    plural = plural,
                ))
            }
            Err(e) => Html(format!(
                r##"<div class="alert alert-error">{err}</div>"##,
                err = escape_html(&e.to_string()),
            )),
        }
    } else if let Some(db) = db_name {
        match mgr.query_oltp(db, sql).await {
            Ok(batches) => render_query_results(&batches),
            Err(e) => Html(format!(
                r##"<div class="alert alert-error">{err}</div>"##,
                err = escape_html(&e.to_string()),
            )),
        }
    } else {
        match mgr.query(sql).await {
            Ok(batches) => render_query_results(&batches),
            Err(e) => Html(format!(
                r##"<div class="alert alert-error">{err}</div>"##,
                err = escape_html(&e.to_string()),
            )),
        }
    }
}

fn render_query_results(batches: &[dkdc_db_core::RecordBatch]) -> Html<String> {
    let resp = crate::api::batches_to_response(batches);

    if resp.columns.is_empty() {
        return Html(String::from(
            r##"<div class="result-info">Query returned no results.</div>"##,
        ));
    }

    let headers: String = resp
        .columns
        .iter()
        .map(|c| {
            format!(
                r##"<th>{name} <span class="badge">{ty}</span></th>"##,
                name = escape_html(&c.name),
                ty = escape_html(&c.r#type),
            )
        })
        .collect();

    let mut rows = String::new();
    for row in &resp.rows {
        rows.push_str("<tr>");
        for val in row {
            rows.push_str(&format!("<td>{}</td>", escape_html(&format_value(val))));
        }
        rows.push_str("</tr>");
    }

    let count = resp.rows.len();
    let cols = resp.columns.len();
    let rs = if count == 1 { "" } else { "s" };
    let cs = if cols == 1 { "" } else { "s" };
    Html(format!(
        r##"<div class="result-info">{count} row{rs}, {cols} column{cs}</div>
<div class="table-wrap"><table class="data-table">
<thead><tr>{headers}</tr></thead><tbody>{rows}</tbody></table></div>"##,
        count = count,
        rs = rs,
        cols = cols,
        cs = cs,
        headers = headers,
        rows = rows,
    ))
}

fn format_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Static assets
// ---------------------------------------------------------------------------

async fn serve_htmx() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/javascript")],
        HTMX_JS,
    )
        .into_response()
}
