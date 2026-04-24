// path: gas_comment_count_patch.gs

/**
 * 月次サマリ更新時にチャンネル名自動補完をかける対象シート名です。
 * A列: channel_id
 * B列: channel_title（空欄なら補完対象）
 */
const CHANNEL_NAME_AUTOFILL_TARGET_SHEET = '検索対象';

/**
 * スプレッドシートを開いたときにメニューを追加します。
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('ログツール')
    .addItem('record 圧縮＋summary 更新', 'compressRecordAndUpdateSummary')
    .addItem('選択動画の履歴テーブル作成', 'createHistoryForSelectedVideo')
    .addItem('Status 重複削除（同一日付×同一チャンネル）', 'dedupeStatusSheet')
    .addSeparator()
    .addItem('月次サマリ更新（Status → 月単位）', 'buildMonthlySummaryFromStatus')
    .addItem('type 別集計更新（summary → type 別）', 'buildTypeAnalyticsFromSummary')
    .addItem('成長プロファイル更新', 'buildGrowthProfileFromSummary')
    .addItem('週間チャンネル概況更新', 'buildWeeklyChannelOverview')
    .addToUi();
}

/**
 * 同時実行を避けるためのロック実行ラッパーです。
 */
function runWithDocLock_(fn, waitMs, alwaysRefilterSheetNames) {
  const lock = LockService.getDocumentLock();
  const ms = (waitMs != null) ? waitMs : 30000;
  if (!lock.tryLock(ms)) {
    SpreadsheetApp.getUi().alert('他の処理が実行中のため中止しました。少し待ってから再実行してください。');
    return;
  }
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let filterSheetNames = [];

  try {
    if (ss) {
      filterSheetNames = captureAndClearAllSheetFilters_(ss);
    }
    return fn();
  } finally {
    if (ss) {
      const restoreTargets = mergeSheetNameLists_(filterSheetNames, alwaysRefilterSheetNames);
      if (restoreTargets.length > 0) {
        restoreAllSheetFilters_(ss, restoreTargets);
      }
    }
    try { lock.releaseLock(); } catch (e) {}
  }
}

/**
 * ブック内の全シートについて、現在フィルタがあるシート名を控えて解除します。
 */
function captureAndClearAllSheetFilters_(ss) {
  const sheetNames = [];
  const sheets = ss.getSheets();

  for (let i = 0; i < sheets.length; i++) {
    const sheet = sheets[i];
    const filter = sheet.getFilter();
    if (!filter) continue;

    sheetNames.push(sheet.getName());
    filter.remove();
  }

  return sheetNames;
}

/**
 * シート名リストを結合し、空要素除去＋重複排除して返します。
 */
function mergeSheetNameLists_(baseNames, extraNames) {
  const merged = [];
  const seen = {};

  function addAll_(arr) {
    if (!arr || !arr.length) return;
    for (let i = 0; i < arr.length; i++) {
      const name = arr[i] != null ? String(arr[i]).trim() : '';
      if (!name || seen[name]) continue;
      seen[name] = true;
      merged.push(name);
    }
  }

  addAll_(baseNames);
  addAll_(extraNames);

  return merged;
}

/**
 * 処理完了時に、控えておいたシートへフィルタを作り直します（失敗しても継続）。
 */
function restoreAllSheetFilters_(ss, sheetNames) {
  for (let i = 0; i < sheetNames.length; i++) {
    const sheet = ss.getSheetByName(sheetNames[i]);
    if (!sheet) continue;

    try {
      const existing = sheet.getFilter();
      if (existing) existing.remove();

      const lastRow = sheet.getLastRow();
      const lastCol = sheet.getLastColumn();
      if (lastRow < 1 || lastCol < 1) continue;

      sheet.getRange(1, 1, lastRow, lastCol).createFilter();
    } catch (e) {}
  }
}

/**
 * トースト表示（失敗しても落としません）。
 */
function safeToast_(ss, msg, title, seconds) {
  try {
    ss.toast(msg, title || 'ログツール', seconds != null ? seconds : 5);
  } catch (e) {}
}

/**
 * HYPERLINK("https://www.youtube.com/watch?v=XXXX", "タイトル")
 * などの文字列/式から video_id を抜き出します。
 * watch?v= / youtu.be / shorts にも対応します。
 */
function extractVideoIdFromText_(text) {
  if (!text) return '';
  const s = String(text);

  // 典型: watch?v=xxxxxxxxxxx
  let m = s.match(/[?&]v=([A-Za-z0-9_-]{11})/);
  if (m) return m[1];

  // youtu.be/xxxxxxxxxxx
  m = s.match(/youtu\.be\/([A-Za-z0-9_-]{11})/);
  if (m) return m[1];

  // shorts/xxxxxxxxxxx
  m = s.match(/\/shorts\/([A-Za-z0-9_-]{11})/);
  if (m) return m[1];

  return '';
}

/**
 * シート上で「実際にデータが入っている範囲」に罫線を引きます。
 * （1行目のヘッダーを含め、最終行・最終列まで）
 */
function setBordersForUsedRange_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow === 0 || lastCol === 0) return;

  const cells = lastRow * lastCol;
  const SOFT_LIMIT = 80000;

  const range = sheet.getRange(1, 1, lastRow, lastCol);

  if (cells > SOFT_LIMIT) {
    // 外枠のみ（内側罫線を付けない）で軽量化
    range.setBorder(true, true, true, true, false, false);
    return;
  }

  // 外枠＋内側すべて罫線
  range.setBorder(true, true, true, true, true, true);
}

/**
 * 値を Date オブジェクトに変換します（失敗した場合は null）。
 */
function toDate_(val) {
  if (val instanceof Date) return val;
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d;
}

/**
 * 入力値をチャンネルIDキーに正規化します。
 * - そのままID
 * - URL中の /channel/UC... を抽出
 * - 文字列中に UCxxxxxxxxxxxxxxxxxxxxxx があれば抽出
 */
function normalizeChannelId_(val) {
  if (val == null) return '';
  let s = String(val).trim();
  if (!s) return '';

  // 文字列中の UC... を優先抽出（YouTube channel id 24桁）
  let m = s.match(/(UC[0-9A-Za-z_-]{22})/);
  if (m) return m[1];

  // URL形式
  m = s.match(/youtube\.com\/channel\/([0-9A-Za-z_-]+)/i);
  if (m) return m[1];

  // 末尾スラッシュ削除
  s = s.replace(/\/+$/, '');

  return s;
}

/**
 * Status シートから channel_id -> channel_title のマップを作ります。
 * 同じchannel_idが複数ある場合、logged_at が最新のチャンネル名を採用します。
 */
function buildStatusChannelNameMap_(statusSheet) {
  const map = {};
  const lastRow = statusSheet.getLastRow();
  if (lastRow < 2) return map;

  const numRows = lastRow - 1;
  // A: logged_at, B: channel_id, C: channel_title
  const rows = statusSheet.getRange(2, 1, numRows, 3).getValues();

  for (let i = 0; i < rows.length; i++) {
    const loggedAt = toDate_(rows[i][0]);
    const channelId = normalizeChannelId_(rows[i][1]);
    const channelTitle = rows[i][2] != null ? String(rows[i][2]).trim() : '';

    if (!channelId || !channelTitle) continue;

    const ts = loggedAt ? loggedAt.getTime() : 0;
    const prev = map[channelId];
    if (!prev || ts >= prev.ts) {
      map[channelId] = { title: channelTitle, ts: ts };
    }
  }

  return map;
}

/**
 * 指定シートの A列(channel_id) が入力済み かつ B列(channel_title) が空欄の行を
 * Status から補完します。
 */
function autofillChannelNameFromStatus_(ss, statusSheet, targetSheetName) {
  const result = {
    sheetFound: false,
    targetSheetName: targetSheetName,
    scanned: 0,
    missing: 0,
    updated: 0
  };

  if (!targetSheetName) return result;

  const targetSheet = ss.getSheetByName(targetSheetName);
  if (!targetSheet) return result;

  result.sheetFound = true;

  const lastRow = targetSheet.getLastRow();
  if (lastRow < 2) return result;

  const numRows = lastRow - 1;
  const rangeAB = targetSheet.getRange(2, 1, numRows, 2);
  const values = rangeAB.getValues(); // [A=id, B=name]

  const map = buildStatusChannelNameMap_(statusSheet);

  // B列を書き戻す配列
  const outB = values.map(function(r) { return [r[1]]; });

  for (let i = 0; i < values.length; i++) {
    result.scanned++;

    const rawId = values[i][0];
    const rawName = values[i][1];

    const id = normalizeChannelId_(rawId);
    const name = rawName != null ? String(rawName).trim() : '';

    if (!id) continue;
    if (name) continue; // すでに入力済み

    result.missing++;

    const hit = map[id];
    if (hit && hit.title) {
      outB[i][0] = hit.title;
      result.updated++;
    }
  }

  if (result.updated > 0) {
    targetSheet.getRange(2, 2, numRows, 1).setValues(outB);
  }

  return result;
}

/**
 * Date -> epochDay (UTC基準) に変換します（タイムゾーンの日付だけを使います）。
 */
function toEpochDayByTZ_(dateObj, tz) {
  if (!dateObj) return null;
  const ymd = Utilities.formatDate(dateObj, tz, 'yyyy-MM-dd');
  const p = ymd.split('-').map(function(x) { return Number(x); });
  if (p.length !== 3) return null;
  const y = p[0], m = p[1], d = p[2];
  if (!y || !m || !d) return null;
  return Math.floor(Date.UTC(y, m - 1, d) / 86400000);
}

/**
 * 日数差を計算します（start→end。1日目を1としてカウント）。
 * カレンダー日ベースです。
 */
function diffDaysByCalendar_(startDate, endDate, tz) {
  if (!startDate || !endDate) return null;
  const s = toEpochDayByTZ_(startDate, tz);
  const e = toEpochDayByTZ_(endDate, tz);
  if (s == null || e == null) return null;
  return (e - s) + 1;
}

/**
 * 集計期間ラベルを作成します。
 * 形式: dd days(mm/dd～mm/dd)
 */
function buildPeriodLabel_(startDate, endDate, tz) {
  if (!startDate || !endDate) return '';
  const days = diffDaysByCalendar_(startDate, endDate, tz);
  if (days == null || days < 1) return '';
  const s = Utilities.formatDate(startDate, tz, 'MM/dd');
  const e = Utilities.formatDate(endDate, tz, 'MM/dd');
  return days + ' days(' + s + '～' + e + ')';
}

/**
 * Status シートを B列(チャンネルID)→A列(logged_at) で並べ替え、
 * フィルタを全範囲に再作成します。
 */
function sortAndRefilterStatus_(statusSheet) {
  const lastRow = statusSheet.getLastRow();
  const lastCol = statusSheet.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return;

  // データ行のみソート（ヘッダー除外）
  statusSheet.getRange(2, 1, lastRow - 1, lastCol).sort([
    { column: 2, ascending: true }, // B: channel_id
    { column: 1, ascending: true }  // A: logged_at
  ]);

  const oldFilter = statusSheet.getFilter();
  if (oldFilter) oldFilter.remove();

  statusSheet.getRange(1, 1, lastRow, lastCol).createFilter();
}

/**
 * 配列の中央値を返します。
 */
function calcMedian_(arr) {
  if (!arr || arr.length === 0) return 0;
  const sorted = arr.slice().sort(function(a, b) { return a - b; });
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) return (sorted[mid - 1] + sorted[mid]) / 2;
  return sorted[mid];
}

/**
 * summary の note 文字列を解析して [{date, views, likes, comments}, ...] にします。
 * 新形式: "yyyy/MM/dd:view/like/comment | ..."
 * 旧形式: "yyyy/MM/dd:view/like | ..."
 */
function parseNoteToLogs_(note) {
  const logs = [];
  if (!note) return logs;

  const parts = String(note).split('|');
  for (let i = 0; i < parts.length; i++) {
    const s = parts[i].trim();
    if (!s) continue;

    const m = s.match(/^(\d{4}\/\d{2}\/\d{2}):(\d+)\/(\d+)(?:\/(\d+))?$/);
    if (!m) continue;

    const dStr = m[1];
    const v = parseInt(m[2], 10);
    const l = parseInt(m[3], 10);
    const c = m[4] != null ? parseInt(m[4], 10) : 0;

    const dm = dStr.match(/^(\d{4})\/(\d{2})\/(\d{2})$/);
    if (!dm) continue;
    const y = Number(dm[1]);
    const mo = Number(dm[2]);
    const da = Number(dm[3]);
    const d = new Date(y, mo - 1, da);

    if (!isNaN(d.getTime())) logs.push({ date: d, views: v, likes: l, comments: c });
  }

  logs.sort(function(a, b) {
    return a.date.getTime() - b.date.getTime();
  });

  return logs;
}

/**
 * 成長パターン分類
 */

/**
 * ヘッダー行から、ヘッダー名 -> 0始まり列indexのマップを返します。
 */
function getHeaderIndexMap_(headerRow) {
  const map = {};
  if (!headerRow || !headerRow.length) return map;

  for (let i = 0; i < headerRow.length; i++) {
    const key = headerRow[i] != null ? String(headerRow[i]).trim() : '';
    if (!key) continue;
    if (map[key] == null) map[key] = i;
  }
  return map;
}

/**
 * 必須ヘッダー不足を検出し、不足があれば例外を投げます。
 */
function requireHeaders_(headerMap, requiredHeaders, sheetName) {
  const missing = [];
  for (let i = 0; i < requiredHeaders.length; i++) {
    const h = requiredHeaders[i];
    if (headerMap[h] == null) missing.push(h);
  }
  if (missing.length > 0) {
    throw new Error('シート「' + sheetName + '」の必須ヘッダー不足: ' + missing.join(', '));
  }
}

/**
 * 概況計算向けの安全な数値変換。
 */
function toNumberForOverview_(value) {
  if (value === 0) return 0;
  if (value == null || value === '') return null;
  if (typeof value === 'number') return isFinite(value) ? value : null;

  const s = String(value).replace(/,/g, '').trim();
  if (!s) return null;
  const n = Number(s);
  return isFinite(n) ? n : null;
}

/**
 * 片側がnullなら空欄、両方あれば差分を返します。
 */
function safeDiffForOverview_(latest, base) {
  if (latest == null || base == null) return '';
  return latest - base;
}

/**
 * 安全な率計算。計算不能なら空欄。
 */
function safeRateForOverview_(numerator, denominator) {
  if (numerator == null || numerator === '') return '';
  if (denominator == null || denominator === '' || denominator === 0) return '';
  return numerator / denominator;
}

/**
 * タイトル比較用に正規化します。
 */
function normalizeTitleForMatch_(title) {
  if (title == null) return '';
  let s = String(title);

  const hm = s.match(/HYPERLINK\(\s*"[^"]*"\s*,\s*"([^"]*)"\s*\)/i);
  if (hm && hm[1]) s = hm[1];

  s = s.replace(/\r\n|\r|\n/g, ' ');
  s = s.replace(/　/g, ' ');
  s = s.trim().replace(/\s+/g, ' ').toLowerCase();
  return s;
}

/**
 * summary行を正規化タイトルで探索（完全一致優先、次に包含一致）。
 */
function findSummaryRowByTitle_(summaryRows, summaryHeaderMap, title) {
  const titleIdx = summaryHeaderMap.title;
  const viewIdx = summaryHeaderMap.last_view_count;
  const target = normalizeTitleForMatch_(title);
  if (!target) return null;

  const exact = [];
  const loose = [];

  for (let i = 0; i < summaryRows.length; i++) {
    const row = summaryRows[i];
    const rowTitle = row[titleIdx];
    const norm = normalizeTitleForMatch_(rowTitle);
    if (!norm) continue;

    const item = {
      row: row,
      rowIndex: i,
      title: rowTitle != null ? String(rowTitle) : '',
      lastViewCount: toNumberForOverview_(row[viewIdx])
    };

    if (norm === target) {
      exact.push(item);
    } else if (norm.indexOf(target) >= 0 || target.indexOf(norm) >= 0) {
      loose.push(item);
    }
  }

  function pickMax_(arr) {
    if (!arr || arr.length === 0) return null;
    arr.sort(function(a, b) {
      const av = a.lastViewCount != null ? a.lastViewCount : -1;
      const bv = b.lastViewCount != null ? b.lastViewCount : -1;
      return bv - av;
    });
    return arr[0];
  }

  return pickMax_(exact) || pickMax_(loose);
}

/**
 * targetDate以前で最も近いログを返します。
 */
function getNearestLogAtOrBefore_(logs, targetDate, tz) {
  if (!logs || logs.length === 0 || !targetDate) return null;
  const t = targetDate.getTime();
  let nearest = null;

  for (let i = 0; i < logs.length; i++) {
    const log = logs[i];
    if (!log || !log.date) continue;
    const ts = log.date.getTime();
    if (isNaN(ts) || ts > t) continue;
    if (!nearest || ts > nearest.date.getTime()) nearest = log;
  }

  return nearest;
}

/**
 * summaryのタイトル一致動画について、7日再生増分を計算します。
 */
function calcVideoSevenDayViewDeltaByTitle_(summaryIndex, title, latestStatusDate, tz) {
  const result = {
    found: false,
    delta: '',
    matchedTitle: '',
    reason: ''
  };

  if (!summaryIndex || !summaryIndex.exists) {
    result.reason = 'summary_missing';
    return result;
  }

  if (!title) {
    result.reason = 'title_empty';
    return result;
  }

  const match = findSummaryRowByTitle_(summaryIndex.rows, summaryIndex.headerMap, title);
  if (!match) {
    result.reason = 'top_video_not_found';
    return result;
  }

  result.found = true;
  result.matchedTitle = match.title;

  const row = match.row;
  const hm = summaryIndex.headerMap;
  const note = row[hm.note];
  const logs = parseNoteToLogs_(note);

  const latestLogFromNote = getNearestLogAtOrBefore_(logs, latestStatusDate, tz);
  let latestVideoLog = latestLogFromNote;

  if (!latestVideoLog) {
    const fallbackDate = toDate_(row[hm.last_logged_at]);
    const fallbackViews = toNumberForOverview_(row[hm.last_view_count]);
    if (fallbackDate && fallbackViews != null) {
      latestVideoLog = { date: fallbackDate, views: fallbackViews, likes: 0, comments: 0 };
    }
  }

  const baseDate = new Date(latestStatusDate.getTime() - 7 * 24 * 60 * 60 * 1000);
  const baseVideoLog = getNearestLogAtOrBefore_(logs, baseDate, tz);

  if (!latestVideoLog) {
    result.reason = 'latest_video_log_missing';
    return result;
  }

  if (!baseVideoLog) {
    result.reason = 'base_video_log_missing';
    return result;
  }

  const diff = latestVideoLog.views - baseVideoLog.views;
  if (diff < 0) {
    result.reason = 'negative_delta';
    return result;
  }

  result.delta = diff;
  return result;
}

/**
 * 週間概況の勢いラベルを判定します。
 */
function classifyWeeklyOverview_(metrics) {
  if (!metrics.hasBase) return 'データ不足';

  const viewsDiff = metrics.viewsDiff != null && metrics.viewsDiff !== '' ? metrics.viewsDiff : null;
  const subsDiff = metrics.subsDiff != null && metrics.subsDiff !== '' ? metrics.subsDiff : null;

  if (viewsDiff != null && viewsDiff <= 0) return '横ばい';

  const rate = metrics.topContributionRate;
  if (rate != null && rate !== '' && viewsDiff != null && viewsDiff > 0) {
    if (rate >= 0.80) return '単発バズ型';
    if (rate >= 0.50) return 'ヒット依存型';
    if (rate >= 0.25) return '一部ヒットあり';
    return '分散安定型';
  }

  if (viewsDiff != null && viewsDiff > 0 && subsDiff != null && subsDiff > 0) return '好調';
  if (viewsDiff != null && viewsDiff > 0 && subsDiff != null && subsDiff <= 0) return '再生先行';
  return '通常';
}

/**
 * 週間概況メモを作ります。
 */
function buildWeeklyOverviewMemo_(metrics) {
  if (!metrics.hasBase) return '比較基準データ不足。';
  if (metrics.summaryMissing) return 'summary未作成のためトップ動画寄与は未判定。';
  if (metrics.topVideoNotFound) return 'トップ動画履歴が見つからないため寄与率は未判定。';

  const rate = metrics.topContributionRate;
  const viewsDiff = metrics.viewsDiff;
  const subsDiff = metrics.subsDiff;
  const uploadsDiff = metrics.uploadsDiff;

  if (rate !== '' && rate != null) {
    if (rate >= 0.80) return '単一動画の寄与が非常に高いです。短期バズ寄りです。';
    if (rate >= 0.50) return '特定動画が強く牽引しています。ヒット依存傾向です。';
    if (rate >= 0.25) return '伸びている動画はありますが、全体にも再生が分散しています。';
    if (rate < 0.25 && viewsDiff > 0) return '特定動画依存は低く、複数動画で堅実に伸びています。';
  }

  if (viewsDiff > 0 && subsDiff <= 0) return '再生は伸びていますが、登録増は弱めです。';
  if (uploadsDiff === 0 && viewsDiff > 0) return '投稿なしでも再生増あり。過去動画が動いている可能性があります。';
  if (uploadsDiff > 0 && viewsDiff <= 0) return '投稿ありですが、反応は弱めです。';
  return '通常推移です。';
}

/**
 * Status / summary から週間チャンネル概況を作成します。
 */
function buildWeeklyChannelOverview() {
  return runWithDocLock_(function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const ui = SpreadsheetApp.getUi();
    const tz = ss.getSpreadsheetTimeZone() || 'Asia/Tokyo';

    try {
      const statusSheet = ss.getSheetByName('Status');
      if (!statusSheet) {
        throw new Error('Status シートが見つかりません。');
      }

      const statusValues = statusSheet.getDataRange().getValues();
      if (!statusValues || statusValues.length === 0) {
        throw new Error('Status シートにデータがありません。');
      }

      const statusHeaderMap = getHeaderIndexMap_(statusValues[0]);
      const requiredStatusHeaders = [
        '取得日時',
        'チャンネルID',
        'チャンネル名',
        '登録者数',
        '動画本数',
        '総再生回数',
        '直近10日トップ動画タイトル'
      ];
      requireHeaders_(statusHeaderMap, requiredStatusHeaders, 'Status');

      const grouped = {};
      for (let i = 1; i < statusValues.length; i++) {
        const row = statusValues[i];
        const channelIdRaw = row[statusHeaderMap['チャンネルID']];
        const channelId = channelIdRaw != null ? String(channelIdRaw).trim() : '';
        if (!channelId) continue;

        const loggedAt = toDate_(row[statusHeaderMap['取得日時']]);
        if (!loggedAt) continue;

        if (!grouped[channelId]) grouped[channelId] = [];
        grouped[channelId].push({ row: row, date: loggedAt, channelId: channelId });
      }

      const summarySheet = ss.getSheetByName('summary');
      const summaryIndex = { exists: !!summarySheet, rows: [], headerMap: {} };
      if (summarySheet) {
        const summaryValues = summarySheet.getDataRange().getValues();
        if (summaryValues && summaryValues.length > 0) {
          const summaryHeaderMapRaw = getHeaderIndexMap_(summaryValues[0]);
          requireHeaders_(summaryHeaderMapRaw, ['title', 'last_view_count', 'last_logged_at', 'note'], 'summary');
          summaryIndex.headerMap = {
            title: summaryHeaderMapRaw.title,
            last_view_count: summaryHeaderMapRaw.last_view_count,
            last_logged_at: summaryHeaderMapRaw.last_logged_at,
            note: summaryHeaderMapRaw.note
          };
          summaryIndex.rows = summaryValues.slice(1);
        }
      }

      const headers = [
        '集計日',
        'チャンネル名',
        '登録者数',
        '7日登録者増',
        '7日再生増',
        '7日投稿増',
        '週間トップ動画',
        'トップ動画7日再生増',
        'トップ動画寄与率',
        '勢い',
        '投稿状況',
        '傾向メモ'
      ];

      const momentumPriority = {
        '単発バズ型': 1,
        'ヒット依存型': 2,
        '一部ヒットあり': 3,
        '分散安定型': 4,
        '好調': 5,
        '再生先行': 6,
        '通常': 7,
        '横ばい': 8,
        'データ不足': 9
      };

      const outputRows = [];
      let dataShortageCount = 0;
      let contributionComputedCount = 0;
      let topVideoNotFoundCount = 0;

      const channelIds = Object.keys(grouped);
      for (let ci = 0; ci < channelIds.length; ci++) {
        const channelId = channelIds[ci];
        const items = grouped[channelId];
        if (!items || items.length === 0) continue;

        items.sort(function(a, b) { return a.date.getTime() - b.date.getTime(); });

        const latest = items[items.length - 1];
        const latestRow = latest.row;
        const latestDate = latest.date;
        const baseDate = new Date(latestDate.getTime() - 7 * 24 * 60 * 60 * 1000);

        let baseItem = null;
        for (let bi = items.length - 1; bi >= 0; bi--) {
          if (items[bi].date.getTime() <= baseDate.getTime()) {
            baseItem = items[bi];
            break;
          }
        }

        const hasBase = !!baseItem;
        if (!hasBase) dataShortageCount++;

        const latestSubs = toNumberForOverview_(latestRow[statusHeaderMap['登録者数']]);
        const latestUploads = toNumberForOverview_(latestRow[statusHeaderMap['動画本数']]);
        const latestViews = toNumberForOverview_(latestRow[statusHeaderMap['総再生回数']]);

        const baseRow = hasBase ? baseItem.row : null;
        const baseSubs = hasBase ? toNumberForOverview_(baseRow[statusHeaderMap['登録者数']]) : null;
        const baseUploads = hasBase ? toNumberForOverview_(baseRow[statusHeaderMap['動画本数']]) : null;
        const baseViews = hasBase ? toNumberForOverview_(baseRow[statusHeaderMap['総再生回数']]) : null;

        const subsDiff = safeDiffForOverview_(latestSubs, baseSubs);
        const viewsDiff = safeDiffForOverview_(latestViews, baseViews);
        const uploadsDiff = safeDiffForOverview_(latestUploads, baseUploads);

        const topVideoTitleRaw = latestRow[statusHeaderMap['直近10日トップ動画タイトル']];
        const topVideoTitle = topVideoTitleRaw != null ? String(topVideoTitleRaw).trim() : '';

        let topVideoDelta = '';
        let topContributionRate = '';
        let topVideoNotFound = false;
        let summaryMissing = !summaryIndex.exists;

        if (summaryIndex.exists) {
          const calc = calcVideoSevenDayViewDeltaByTitle_(summaryIndex, topVideoTitle, latestDate, tz);
          if (calc.reason === 'top_video_not_found') {
            topVideoNotFound = true;
            topVideoNotFoundCount++;
          }
          if (calc.delta !== '' && calc.delta != null) {
            topVideoDelta = calc.delta;
          }
          if (viewsDiff !== '' && viewsDiff != null && viewsDiff > 0 && topVideoDelta !== '' && topVideoDelta != null) {
            topContributionRate = safeRateForOverview_(topVideoDelta, viewsDiff);
            if (topContributionRate !== '' && topContributionRate != null) {
              contributionComputedCount++;
            }
          }
        }

        const metrics = {
          hasBase: hasBase,
          summaryMissing: summaryMissing,
          topVideoNotFound: topVideoNotFound,
          subsDiff: subsDiff === '' ? null : subsDiff,
          viewsDiff: viewsDiff === '' ? null : viewsDiff,
          uploadsDiff: uploadsDiff === '' ? null : uploadsDiff,
          topContributionRate: topContributionRate
        };

        const momentum = classifyWeeklyOverview_(metrics);
        let postingStatus = '不明';
        if (!hasBase) {
          postingStatus = 'データ不足';
        } else if (uploadsDiff >= 3) {
          postingStatus = '投稿多め';
        } else if (uploadsDiff >= 1) {
          postingStatus = '投稿あり';
        } else if (uploadsDiff === 0) {
          postingStatus = '投稿なし';
        }

        const memo = buildWeeklyOverviewMemo_(metrics);

        const outRow = [
          Utilities.formatDate(latestDate, tz, 'yyyy/MM/dd'),
          latestRow[statusHeaderMap['チャンネル名']] != null ? String(latestRow[statusHeaderMap['チャンネル名']]) : '',
          latestSubs != null ? latestSubs : '',
          subsDiff,
          viewsDiff,
          uploadsDiff,
          topVideoTitle,
          topVideoDelta,
          topContributionRate,
          momentum,
          postingStatus,
          memo
        ];

        outRow.__momentumPriority = momentumPriority[momentum] != null ? momentumPriority[momentum] : 99;
        outRow.__viewsDiff = (viewsDiff !== '' && viewsDiff != null) ? viewsDiff : -Infinity;
        outputRows.push(outRow);
      }

      outputRows.sort(function(a, b) {
        if (a.__momentumPriority !== b.__momentumPriority) {
          return a.__momentumPriority - b.__momentumPriority;
        }
        return b.__viewsDiff - a.__viewsDiff;
      });

      for (let i = 0; i < outputRows.length; i++) {
        delete outputRows[i].__momentumPriority;
        delete outputRows[i].__viewsDiff;
      }

      const outSheetName = '週間チャンネル概況';
      let outSheet = ss.getSheetByName(outSheetName);
      if (!outSheet) {
        outSheet = ss.insertSheet(outSheetName);
      } else {
        outSheet.clear();
      }

      outSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      if (outputRows.length > 0) {
        outSheet.getRange(2, 1, outputRows.length, headers.length).setValues(outputRows);
      }

      outSheet.setFrozenRows(1);
      const oldFilter = outSheet.getFilter();
      if (oldFilter) oldFilter.remove();
      const usedRows = Math.max(1, outputRows.length + 1);
      outSheet.getRange(1, 1, usedRows, headers.length).createFilter();

      if (outputRows.length > 0) {
        outSheet.getRange(2, 3, outputRows.length, 1).setNumberFormat('#,##0');
        outSheet.getRange(2, 4, outputRows.length, 1).setNumberFormat('#,##0');
        outSheet.getRange(2, 5, outputRows.length, 1).setNumberFormat('#,##0');
        outSheet.getRange(2, 6, outputRows.length, 1).setNumberFormat('#,##0');
        outSheet.getRange(2, 8, outputRows.length, 1).setNumberFormat('#,##0');
        outSheet.getRange(2, 9, outputRows.length, 1).setNumberFormat('0.0%');
      }

      setBordersForUsedRange_(outSheet);

      ui.alert(
        '週間チャンネル概況を更新しました。\n' +
        '出力チャンネル数: ' + outputRows.length + '\n' +
        'データ不足件数: ' + dataShortageCount + '\n' +
        'トップ動画寄与率算出件数: ' + contributionComputedCount + '\n' +
        'トップ動画未一致件数: ' + topVideoNotFoundCount
      );
    } catch (err) {
      appendErrorLog_(ss, 'buildWeeklyChannelOverview', 'main', err, {});
      ui.alert('週間チャンネル概況更新でエラー: ' + (err && err.message ? err.message : err));
      throw err;
    }
  }, 30000, ['週間チャンネル概況']);
}

function classifyGrowthPattern_(typeKey, r3, r7, r30, daysSince, totalViews, medianTotal) {
  const t = (typeKey || '').toLowerCase();
  const rr3 = (r3 != null) ? r3 : 0;
  const rr7 = (r7 != null) ? r7 : 0;
  const rr30 = (r30 != null) ? r30 : 0;
  const dv = daysSince || 0;
  const tv = totalViews || 0;
  const med = medianTotal || 0;

  if (!tv || !dv) return 'データ不足';

  if (t === 'short') {
    if (rr3 >= 0.80) return 'ショート瞬間バズ型';
    if (rr3 >= 0.60 && rr7 >= 0.85) return 'ショート準瞬間型';
    if (rr7 < 0.60 && rr30 >= 0.80) return 'ショート後伸びレア型';
    if (rr7 >= 0.80) return 'ショート短期完結型';
    return 'ショート埋没型';
  } else if (t === 'video') {
    if (rr30 >= 0.80 && rr7 >= 0.60) return '動画フロントローデッド型';
    if (rr30 >= 0.80 && rr7 < 0.60) return '動画ロングテール完走型';
    if (rr30 < 0.80 && dv >= 60) return '動画長期ロングラン型';
    if (rr7 >= 0.70 && rr30 < 0.80) return '動画短期集中型';
    return '動画伸び悩み型';
  } else if (t === 'live') {
    const lowThreshold = med ? med * 0.5 : 0;
    if (rr3 >= 0.70 && rr7 >= 0.85) return 'ライブ一発完結型';
    if (rr3 >= 0.50 && rr30 >= 0.90) return 'ライブ即伸び＋アーカイブ強め型';
    if (rr3 < 0.50 && rr30 >= 0.80) return 'ライブアーカイブ強め型';
    if (rr7 < 0.50 && lowThreshold && tv < lowThreshold) return 'ライブ埋没型';
    return 'ライブ中庸型';
  } else {
    return '未分類';
  }
}

/**
 * スピードランク分類
 */
function classifySpeedRank_(avgPerDay, medianSpeed) {
  const a = avgPerDay || 0;
  const med = medianSpeed || 0;
  if (a <= 0 || med <= 0) return '不明';
  const idx = a / med;
  if (idx >= 2.0) return '爆速';
  if (idx >= 1.3) return '好調';
  if (idx >= 0.7) return 'ふつう';
  return '鈍足';
}


/**
 * エラーログシートを取得します（無ければ作成）。
 */
function ensureErrorLogSheet_(ss) {
  const name = 'error_log';
  let sheet = ss.getSheetByName(name);
  const header = ['timestamp', 'function_name', 'stage', 'message', 'stack', 'meta_json'];

  if (!sheet) {
    sheet = ss.insertSheet(name);
    sheet.getRange(1, 1, 1, header.length).setValues([header]);
    setBordersForUsedRange_(sheet);
    return sheet;
  }

  const firstRow = sheet.getRange(1, 1, 1, header.length).getValues()[0];
  let needsHeader = false;
  for (let i = 0; i < header.length; i++) {
    if (String(firstRow[i] || '') !== header[i]) {
      needsHeader = true;
      break;
    }
  }

  if (needsHeader) {
    sheet.insertRows(1, 1);
    sheet.getRange(1, 1, 1, header.length).setValues([header]);
  }

  return sheet;
}

/**
 * エラーログを1行追記します（失敗しても処理を落としません）。
 */
function appendErrorLog_(ss, functionName, stage, err, meta) {
  try {
    const sheet = ensureErrorLogSheet_(ss);
    const message = err && err.message ? err.message : String(err || 'unknown error');
    const stack = err && err.stack ? String(err.stack) : '';
    const metaJson = meta ? JSON.stringify(meta) : '';
    sheet.appendRow([new Date(), functionName || '', stage || '', message, stack, metaJson]);
  } catch (loggingErr) {}
}

/**
 * record 行データの様式を統一します。
 */
function normalizeRecordRows_(values, formulas) {
  const normalized = [];
  let filledTitle = 0;
  let filledType = 0;

  for (let i = 0; i < values.length; i++) {
    const row = values[i] || [];
    const titleFormula = formulas[i] && formulas[i][0] ? formulas[i][0] : '';

    const loggedAt = row[0] || '';

    let type = row[1] != null ? String(row[1]).trim().toLowerCase() : '';
    if (type !== 'video' && type !== 'live' && type !== 'short') {
      type = 'video';
      filledType++;
    }

    const titleValue = row[2] != null ? String(row[2]).trim() : '';
    const titleCell = titleFormula || titleValue;
    const safeTitle = titleCell || '（タイトル未設定）';
    if (!titleCell) filledTitle++;

    const publishedAt = row[3] || '';
    const durationSec = row[4] || '';
    const viewCount = Number(row[5]) || 0;
    const likeCount = Number(row[6]) || 0;
    const commentCount = Number(row[7]) || 0;

    normalized.push([loggedAt, type, safeTitle, publishedAt, durationSec, viewCount, likeCount, commentCount]);
  }

  return {
    rows: normalized,
    stats: { total: normalized.length, filledTitle: filledTitle, filledType: filledType }
  };
}

/**
 * record を圧縮しつつ summary を更新
 */
function compressRecordAndUpdateSummary() {
  return runWithDocLock_(function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    try {
      safeToast_(ss, 'record を読み込み中です…', 'ログツール', 10);

      const recordSheet = ss.getSheetByName('record');
      if (!recordSheet) {
        SpreadsheetApp.getUi().alert('record シートが見つかりません。');
        return;
      }

      const lastRow = recordSheet.getLastRow();
      const lastCol = recordSheet.getLastColumn();
      if (lastRow < 2) {
        SpreadsheetApp.getUi().alert('record シートにデータがありません（ヘッダーのみ）です。');
        return;
      }

      const numRows = lastRow - 1;
      const values = recordSheet.getRange(2, 1, numRows, lastCol).getValues();
      const formulas = recordSheet.getRange(2, 3, numRows, 1).getFormulas();

      // 圧縮前に全行の様式を統一
      const normalizedResult = normalizeRecordRows_(values, formulas);
      const normalizedRows = normalizedResult.rows;
      const normalizedStats = normalizedResult.stats;

      const byVideo = {};
      const passthroughRows = [];

      for (let i = 0; i < normalizedRows.length; i++) {
        const row = normalizedRows[i];
        const loggedAt = row[0];
        const type = row[1];
        const titleCell = row[2];
        const publishedAt = row[3];
        const durationSec = row[4];
        const viewCount = Number(row[5]) || 0;
        const likeCount = Number(row[6]) || 0;
        const commentCount = Number(row[7]) || 0;

        const videoId = extractVideoIdFromText_(titleCell);
        if (!videoId) {
          passthroughRows.push([
            loggedAt || '',
            type || '',
            titleCell || '',
            publishedAt || '',
            durationSec || '',
            viewCount,
            likeCount,
            commentCount
          ]);
          continue;
        }

        const loggedAtDate = loggedAt instanceof Date ? loggedAt : (loggedAt ? new Date(loggedAt) : null);
        const publishedAtDate = publishedAt instanceof Date ? publishedAt : (publishedAt ? new Date(publishedAt) : null);

        if (!byVideo[videoId]) byVideo[videoId] = [];
        byVideo[videoId].push({
          loggedAt: loggedAtDate,
          type: type,
          titleCell: titleCell,
          publishedAt: publishedAtDate,
          durationSec: durationSec,
          viewCount: viewCount,
          likeCount: likeCount,
          commentCount: commentCount
        });
      }

      safeToast_(ss, 'record を圧縮中です…', 'ログツール', 10);

      const compressedAllRows = [];
      const summaryRows = [];
      const timeZone = 'Asia/Tokyo';

      Object.keys(byVideo).forEach(function(videoId) {
        const logs = byVideo[videoId];
        logs.sort(function(a, b) {
          if (!a.loggedAt && !b.loggedAt) return 0;
          if (!a.loggedAt) return 1;
          if (!b.loggedAt) return -1;
          return a.loggedAt - b.loggedAt;
        });

        if (logs.length === 0) return;

        const kept = [];
        const base = logs[0];
        kept.push(base);

        let prevView = base.viewCount || 0;
        let prevLike = base.likeCount || 0;
        let prevComment = base.commentCount || 0;

        for (let i = 1; i < logs.length; i++) {
          const cur = logs[i];
          const curView = cur.viewCount || 0;
          const curLike = cur.likeCount || 0;
          const curComment = cur.commentCount || 0;

          if (curView === prevView && curLike === prevLike && curComment === prevComment) continue;

          kept.push(cur);
          prevView = curView;
          prevLike = curLike;
          prevComment = curComment;
        }

        kept.forEach(function(k) {
          compressedAllRows.push([
            k.loggedAt || '',
            k.type || '',
            k.titleCell || '',
            k.publishedAt || '',
            k.durationSec || '',
            k.viewCount || 0,
            k.likeCount || 0,
            k.commentCount || 0
          ]);
        });

        const noteParts = [];
        kept.forEach(function(k) {
          if (k.loggedAt) {
            const dateStr = Utilities.formatDate(k.loggedAt, timeZone, 'yyyy/MM/dd');
            const v = k.viewCount || 0;
            const l = k.likeCount || 0;
            const c = k.commentCount || 0;
            noteParts.push(dateStr + ':' + v + '/' + l + '/' + c);
          }
        });

        const first = kept[0];
        const last = kept[kept.length - 1];
        const latest = logs[logs.length - 1];

        summaryRows.push([
          videoId,
          latest.type || last.type || '',
          latest.titleCell || last.titleCell || '',
          first.publishedAt || '',
          first.durationSec || '',
          first.loggedAt || '',
          first.viewCount || 0,
          first.likeCount || 0,
          first.commentCount || 0,
          latest.loggedAt || last.loggedAt || '',
          latest.viewCount || last.viewCount || 0,
          latest.likeCount || last.likeCount || 0,
          latest.commentCount || last.commentCount || 0,
          noteParts.join(' | ')
        ]);
      });

      compressedAllRows.sort(function(a, b) {
        const da = a[0] instanceof Date ? a[0] : (a[0] ? new Date(a[0]) : null);
        const db = b[0] instanceof Date ? b[0] : (b[0] ? new Date(b[0]) : null);
        if (!da && !db) return 0;
        if (!da) return 1;
        if (!db) return -1;
        return da - db;
      });

      if (passthroughRows.length > 0) {
        compressedAllRows.push.apply(compressedAllRows, passthroughRows);
      }

      safeToast_(ss, 'record を書き戻し中です…', 'ログツール', 10);

      recordSheet.getRange(2, 1, numRows, lastCol).clearContent();
      if (compressedAllRows.length > 0) {
        recordSheet.getRange(2, 1, compressedAllRows.length, 8).setValues(compressedAllRows);
      }
      setBordersForUsedRange_(recordSheet);

      const summaryName = 'summary';
      let summarySheet = ss.getSheetByName(summaryName);
      if (!summarySheet) summarySheet = ss.insertSheet(summaryName);

      const sd = summarySheet.getDataRange();
      sd.clearContent();
      sd.clearFormat();

      const header = [
        'video_id', 'type', 'title', 'published_at', 'duration_sec',
        'first_logged_at', 'first_view_count', 'first_like_count', 'first_comment_count',
        'last_logged_at', 'last_view_count', 'last_like_count', 'last_comment_count', 'note'
      ];
      summarySheet.getRange(1, 1, 1, header.length).setValues([header]);

      summaryRows.sort(function(a, b) {
        return (b[10] || 0) - (a[10] || 0);
      });

      if (summaryRows.length > 0) {
        summarySheet.getRange(2, 1, summaryRows.length, header.length).setValues(summaryRows);
      }

      setBordersForUsedRange_(summarySheet);
      safeToast_(ss, '完了しました。', 'ログツール', 3);

      SpreadsheetApp.getUi().alert(
        'record の圧縮と summary の更新が完了しました。\n' +
        'record の行数（ヘッダー除く）: ' + compressedAllRows.length + '\n' +
        'summary の動画数: ' + summaryRows.length + '\n' +
        '様式統一: type補完 ' + normalizedStats.filledType + ' 件 / title補完 ' + normalizedStats.filledTitle + ' 件'
      );
    } catch (err) {
      appendErrorLog_(ss, 'compressRecordAndUpdateSummary', 'main', err, {
        spreadsheetId: ss.getId(),
        activeSheet: ss.getActiveSheet() ? ss.getActiveSheet().getName() : ''
      });
      SpreadsheetApp.getUi().alert(
        'record 圧縮＋summary 更新でエラーが発生しました。error_log シートを確認してください。\n' +
        (err && err.message ? err.message : String(err))
      );
      throw err;
    }
  }, null, ['record', 'summary']);
}


/**
 * summary シートで選択中の動画について、record から履歴テーブルを作成
 */
function createHistoryForSelectedVideo() {
  return runWithDocLock_(function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getActiveSheet();
    const sheetName = sheet.getName();

    if (sheetName !== 'summary') {
      SpreadsheetApp.getUi().alert('summary シートで動画の行（2行目以降）を選択して実行してください。');
      return;
    }

    const cell = sheet.getActiveCell();
    const row = cell.getRow();
    if (row < 2) {
      SpreadsheetApp.getUi().alert('ヘッダー以外の行（動画の行）を選択してください。');
      return;
    }

    const videoId = sheet.getRange(row, 1).getValue();
    if (!videoId) {
      SpreadsheetApp.getUi().alert('video_id を取得できませんでした。');
      return;
    }

    const title = sheet.getRange(row, 3).getValue();
    const publishedAt = sheet.getRange(row, 4).getValue();

    const recordSheet = ss.getSheetByName('record');
    if (!recordSheet) {
      SpreadsheetApp.getUi().alert('record シートが見つかりません。');
      return;
    }

    const lastRow = recordSheet.getLastRow();
    const lastCol = recordSheet.getLastColumn();
    if (lastRow < 2) {
      SpreadsheetApp.getUi().alert('record シートにデータがありません（ヘッダーのみ）です。');
      return;
    }

    const numRows = lastRow - 1;
    const valueRange = recordSheet.getRange(2, 1, numRows, lastCol);
    const values = valueRange.getValues();
    const formulaRange = recordSheet.getRange(2, 3, numRows, 1);
    const formulas = formulaRange.getFormulas();

    const logs = [];
    for (let i = 0; i < numRows; i++) {
      const rowVals = values[i];
      const loggedAt = rowVals[0];
      const viewCount = Number(rowVals[5]) || 0;
      const likeCount = Number(rowVals[6]) || 0;
      const commentCount = Number(rowVals[7]) || 0;

      const titleFormula = formulas[i][0];
      const titleValue = rowVals[2];
      const vid = extractVideoIdFromText_(titleFormula || titleValue);
      if (vid !== videoId) continue;

      logs.push({
        loggedAt: loggedAt,
        viewCount: viewCount,
        likeCount: likeCount,
        commentCount: commentCount
      });
    }

    if (logs.length === 0) {
      SpreadsheetApp.getUi().alert('指定された動画のログが record シートに見つかりませんでした。');
      return;
    }

    logs.sort(function(a, b) {
      const da = a.loggedAt instanceof Date ? a.loggedAt : (a.loggedAt ? new Date(a.loggedAt) : null);
      const db = b.loggedAt instanceof Date ? b.loggedAt : (b.loggedAt ? new Date(b.loggedAt) : null);
      if (!da && !db) return 0;
      if (!da) return 1;
      if (!db) return -1;
      return da - db;
    });

    const historyName = 'history';
    let historySheet = ss.getSheetByName(historyName);
    if (!historySheet) historySheet = ss.insertSheet(historyName);

    const hd = historySheet.getDataRange();
    hd.clearContent();
    hd.clearFormat();

    historySheet.getRange(1, 1).setValue('video_id');
    historySheet.getRange(1, 2).setValue(videoId);
    historySheet.getRange(2, 1).setValue('title');
    historySheet.getRange(2, 2).setValue(title);
    historySheet.getRange(3, 1).setValue('published_at');
    historySheet.getRange(3, 2).setValue(publishedAt);

    const header = ['logged_at', 'view_count', 'like_count', 'comment_count', 'diff_view', 'diff_like', 'diff_comment'];
    historySheet.getRange(5, 1, 1, header.length).setValues([header]);

    const rows = [];
    let prevView = null;
    let prevLike = null;
    let prevComment = null;

    logs.forEach(function(log) {
      const v = log.viewCount || 0;
      const l = log.likeCount || 0;
      const c = log.commentCount || 0;
      let dv = '';
      let dl = '';
      let dc = '';
      if (prevView !== null) {
        dv = v - prevView;
        dl = l - prevLike;
        dc = c - prevComment;
      }
      rows.push([log.loggedAt || '', v, l, c, dv, dl, dc]);
      prevView = v;
      prevLike = l;
      prevComment = c;
    });

    if (rows.length > 0) {
      historySheet.getRange(6, 1, rows.length, header.length).setValues(rows);
    }

    setBordersForUsedRange_(historySheet);

    SpreadsheetApp.getUi().alert(
      'history シートに履歴テーブルを作成しました。\n' +
      'ログ件数: ' + rows.length
    );
  });
}

/**
 * Status シートの重複行を削除
 */
function dedupeStatusSheet() {
  return runWithDocLock_(function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName('Status');
    if (!sheet) {
      SpreadsheetApp.getUi().alert('Status シートが見つかりません。');
      return;
    }

    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();
    if (lastRow < 2) {
      SpreadsheetApp.getUi().alert('Status シートにデータがありません（ヘッダーのみ）です。');
      return;
    }

    const numRows = lastRow - 1;
    const range = sheet.getRange(2, 1, numRows, lastCol);
    const values = range.getValues();

    const timeZone = ss.getSpreadsheetTimeZone() || 'Asia/Tokyo';

    function normalizeDate(d) {
      if (!d) return '';
      if (d instanceof Date) return Utilities.formatDate(d, timeZone, 'yyyy/MM/dd');
      const s = String(d);
      const parsed = new Date(s);
      if (!isNaN(parsed.getTime())) return Utilities.formatDate(parsed, timeZone, 'yyyy/MM/dd');
      return s.substring(0, 10);
    }

    const groupBest = {};
    const removeByError = {};
    const removeByDuplicate = {};

    let deletedByError = 0;
    let deletedByDuplicate = 0;

    function toNumber_(v) {
      if (v === null || v === '' || v === undefined) return NaN;
      if (typeof v === 'number') return v;
      const n = Number(String(v).replace(/,/g, '').trim());
      return isNaN(n) ? NaN : n;
    }

    function hasZeroInColsIToM_(row) {
      // row は A列始まりの配列。I〜M は index 8〜12。
      if (row.length <= 8) return false;
      const end = Math.min(12, row.length - 1);
      for (let col = 8; col <= end; col++) {
        const n = toNumber_(row[col]);
        if (!isNaN(n) && n === 0) return true;
      }
      return false;
    }

    for (let i = 0; i < values.length; i++) {
      const row = values[i];

      if (hasZeroInColsIToM_(row)) {
        removeByError[i] = true;
        deletedByError++;
        continue;
      }

      const loggedAt = row[0];
      const channelId = normalizeChannelId_(row[1]);
      const dateStr = normalizeDate(loggedAt);

      if (!dateStr || !channelId) continue;

      const key = dateStr + '||' + channelId;
      const t = toDate_(loggedAt);
      const ts = t ? t.getTime() : Number.MAX_SAFE_INTEGER;
      const prev = groupBest[key];

      if (!prev || ts < prev.ts || (ts === prev.ts && i < prev.index)) {
        if (prev) removeByDuplicate[prev.index] = true;
        groupBest[key] = { index: i, ts: ts };
        if (prev) {
          deletedByDuplicate++;
          delete removeByDuplicate[i];
        }
      } else {
        removeByDuplicate[i] = true;
        deletedByDuplicate++;
      }
    }

    const kept = [];
    for (let i = 0; i < values.length; i++) {
      if (removeByError[i] || removeByDuplicate[i]) continue;
      kept.push(values[i]);
    }

    const deleted = deletedByError + deletedByDuplicate;
    if (deleted === 0) {
      SpreadsheetApp.getUi().alert('Status シートに削除対象行はありませんでした。');
      return;
    }

    range.clearContent();
    if (kept.length > 0) {
      sheet.getRange(2, 1, kept.length, lastCol).setValues(kept);
    }

    SpreadsheetApp.getUi().alert(
      'Status シートの重複削除が完了しました。\n' +
      '削除行数(合計): ' + deleted + '\n' +
      '  ・同一日付×同一チャンネルの重複: ' + deletedByDuplicate + '\n' +
      '  ・I〜M列に0を含む行: ' + deletedByError + '\n' +
      '判定条件:\n' +
      '  ・同一日付（時刻は無視）×同一チャンネルIDで最古時刻を1行だけ残す\n' +
      '  ・I〜M列に0を含む行は削除\n' +
      '補足:\n' +
      '  ・速度優先のため deleteRow は使わず、一括書き戻しに変更しています。'
    );
  }, null, ['Status']);
}

/**
 * 月次サマリ（Status → 月ごと1行）を作成します。
 * 追記: 検索対象シート(A=id, B=name)のB空欄を Status の情報で補完します。
 */
function buildMonthlySummaryFromStatus() {
  return runWithDocLock_(function() {
    const SKIP_BLANK_DELTA_VIDEO_ROWS = true;
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const statusSheet = ss.getSheetByName('Status');
    if (!statusSheet) {
      SpreadsheetApp.getUi().alert('Status シートが見つかりません。');
      return;
    }

    const lastRow = statusSheet.getLastRow();
    const lastCol = statusSheet.getLastColumn();
    if (lastRow < 2) {
      SpreadsheetApp.getUi().alert('Status シートにデータがありません（ヘッダーのみ）です。');
      return;
    }

    const tz = ss.getSpreadsheetTimeZone() || 'Asia/Tokyo';
    const numRows = lastRow - 1;
    const values = statusSheet.getRange(2, 1, numRows, lastCol).getValues();

    const groupMap = {}; // channelId|yyyy/MM -> data

    for (let i = 0; i < numRows; i++) {
      const row = values[i];
      const loggedAt = toDate_(row[0]); // A
      const channelId = row[1];         // B
      const channelTitle = row[2];      // C
      if (!loggedAt || !channelId) continue;

      const y = loggedAt.getFullYear();
      const m = loggedAt.getMonth() + 1;
      const ym = y + '/' + (m < 10 ? '0' + m : m);

      const key = channelId + '|' + ym;
      const prev = groupMap[key];

      if (!prev) {
        groupMap[key] = {
          channelId: channelId,
          channelTitle: channelTitle,
          yearMonth: ym,
          latestLoggedAt: loggedAt,
          minLoggedAt: loggedAt,
          maxLoggedAt: loggedAt,
          row: row
        };
      } else {
        if (loggedAt.getTime() < prev.minLoggedAt.getTime()) prev.minLoggedAt = loggedAt;
        if (loggedAt.getTime() > prev.maxLoggedAt.getTime()) prev.maxLoggedAt = loggedAt;

        if (loggedAt.getTime() > prev.latestLoggedAt.getTime()) {
          prev.latestLoggedAt = loggedAt;
          prev.row = row;
          prev.channelTitle = channelTitle || prev.channelTitle;
        }
      }
    }

    const groups = Object.keys(groupMap).map(function(k) { return groupMap[k]; });
    if (groups.length === 0) {
      SpreadsheetApp.getUi().alert('月次サマリを作成できるデータがありません。');
      return;
    }

    groups.sort(function(a, b) {
      if (a.channelId < b.channelId) return -1;
      if (a.channelId > b.channelId) return 1;
      if (a.yearMonth < b.yearMonth) return -1;
      if (a.yearMonth > b.yearMonth) return 1;
      return 0;
    });

    const prevByChannel = {};
    const outRows = [];
    let skippedBlankDelta = 0;

    groups.forEach(function(g) {
      const row = g.row;
      const channelId = g.channelId;
      const channelTitle = g.channelTitle || '';

      const subs = Number(row[3]) || 0;          // D
      const videos = Number(row[4]) || 0;        // E
      const views = Number(row[5]) || 0;         // F
      const last30Views = Number(row[28]) || 0;  // AC

      const prev = prevByChannel[channelId];
      const deltaSubs = prev ? (subs - prev.subs) : '';
      const deltaViews = prev ? (views - prev.views) : '';
      const deltaVideos = prev ? (videos - prev.videos) : '';

      prevByChannel[channelId] = { subs: subs, views: views, videos: videos };

      if (SKIP_BLANK_DELTA_VIDEO_ROWS && deltaVideos === '') {
        skippedBlankDelta++;
        return;
      }

      const periodLabel = buildPeriodLabel_(g.minLoggedAt, g.maxLoggedAt, tz);

      outRows.push([
        g.yearMonth,    // A
        channelId,      // B
        channelTitle,   // C
        subs,           // D
        views,          // E
        videos,         // F
        deltaSubs,      // G
        deltaViews,     // H
        deltaVideos,    // I
        last30Views,    // J
        '',             // K
        periodLabel     // L
      ]);
    });

    const sheetName = '月次サマリ';
    let monthlySheet = ss.getSheetByName(sheetName);
    if (!monthlySheet) monthlySheet = ss.insertSheet(sheetName);

    const md = monthlySheet.getDataRange();
    md.clearContent();
    md.clearFormat();

    const header = [
      '年月',
      'チャンネルID',
      'チャンネル名',
      '月末登録者数',
      '月末総再生数',
      '月末動画本数',
      '月間登録者増加',
      '月間総再生数増加',
      '月間動画本数増加',
      '直近30日合計再生数（その月末時点）',
      'メモ',
      '集計期間'
    ];
    monthlySheet.getRange(1, 1, 1, header.length).setValues([header]);

    if (outRows.length > 0) {
      monthlySheet.getRange(2, 1, outRows.length, header.length).setValues(outRows);
    }

    setBordersForUsedRange_(monthlySheet);

    // Status の並び替え＋フィルタ再設定
    sortAndRefilterStatus_(statusSheet);

    // ★追加: 検索対象シートの A入力/B空欄 を Status の channel_title で補完
    const fillResult = autofillChannelNameFromStatus_(ss, statusSheet, CHANNEL_NAME_AUTOFILL_TARGET_SHEET);

    let fillMsg = '';
    if (!fillResult.sheetFound) {
      fillMsg =
        '\nチャンネル名自動補完:\n' +
        '  ・対象シート「' + fillResult.targetSheetName + '」が見つからないためスキップしました。';
    } else {
      fillMsg =
        '\nチャンネル名自動補完（' + fillResult.targetSheetName + '）:\n' +
        '  ・走査行数: ' + fillResult.scanned + '\n' +
        '  ・B空欄（A入力あり）: ' + fillResult.missing + '\n' +
        '  ・補完件数: ' + fillResult.updated;
    }

    SpreadsheetApp.getUi().alert(
      '月次サマリ（Status → 月ごと）が更新されました。\n' +
      '出力行数: ' + outRows.length + '\n' +
      '初回月として非表示にした行数: ' + skippedBlankDelta + '\n' +
      'Status を B列(チャンネルID)→A列(logged_at) で整列し、フィルタを再作成しました。' +
      fillMsg
    );
  });
}

/**
 * summary から type 別の基礎集計を作成
 */
function buildTypeAnalyticsFromSummary() {
  return runWithDocLock_(function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const summarySheet = ss.getSheetByName('summary');
    if (!summarySheet) {
      SpreadsheetApp.getUi().alert('summary シートが見つかりません。先に「record 圧縮＋summary 更新」を実行してください。');
      return;
    }

    const lastRow = summarySheet.getLastRow();
    const lastCol = summarySheet.getLastColumn();
    if (lastRow < 2) {
      SpreadsheetApp.getUi().alert('summary シートにデータがありません（ヘッダーのみ）です。');
      return;
    }

    const numRows = lastRow - 1;
    const values = summarySheet.getRange(2, 1, numRows, lastCol).getValues();

    const byType = {};

    for (let i = 0; i < numRows; i++) {
      const row = values[i];
      const type = row[1]; // B
      if (!type) continue;

      const typeKey = String(type);
      if (!byType[typeKey]) {
        byType[typeKey] = { count: 0, totalViews: 0, totalLikes: 0, totalDuration: 0 };
      }

      const lastViews = Number(row[10]) || 0;  // K
      const lastLikes = Number(row[11]) || 0;  // L
      const durationSec = Number(row[4]) || 0; // E

      byType[typeKey].count += 1;
      byType[typeKey].totalViews += lastViews;
      byType[typeKey].totalLikes += lastLikes;
      byType[typeKey].totalDuration += durationSec;
    }

    const outRows = [];
    Object.keys(byType).sort().forEach(function(typeKey) {
      const s = byType[typeKey];
      const c = s.count || 0;
      const avgViews = c ? Math.round(s.totalViews / c) : 0;
      const avgLikes = c ? Math.round(s.totalLikes / c) : 0;
      const avgDuration = c ? Math.round(s.totalDuration / c) : 0;

      outRows.push([
        typeKey,
        c,
        s.totalViews,
        avgViews,
        s.totalLikes,
        avgLikes,
        s.totalDuration,
        avgDuration
      ]);
    });

    const sheetName = 'type別集計';
    let typeSheet = ss.getSheetByName(sheetName);
    if (!typeSheet) typeSheet = ss.insertSheet(sheetName);

    const td = typeSheet.getDataRange();
    td.clearContent();
    td.clearFormat();

    const header = [
      '動画タイプ',
      '動画本数',
      '合計再生数（最新）',
      '平均再生数（最新）',
      '合計高評価数（最新）',
      '平均高評価数（最新）',
      '合計再生時間（秒）',
      '平均再生時間（秒）'
    ];
    typeSheet.getRange(1, 1, 1, header.length).setValues([header]);

    if (outRows.length > 0) {
      typeSheet.getRange(2, 1, outRows.length, header.length).setValues(outRows);
    }

    setBordersForUsedRange_(typeSheet);

    SpreadsheetApp.getUi().alert(
      'type別集計（summary → type別）が更新されました。\n' +
      '行数: ' + outRows.length
    );
  });
}

/**
 * summary から成長プロファイル作成
 */
function buildGrowthProfileFromSummary() {
  return runWithDocLock_(function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const summarySheet = ss.getSheetByName('summary');
    if (!summarySheet) {
      SpreadsheetApp.getUi().alert('summary シートが見つかりません。先に「record 圧縮＋summary 更新」を実行してください。');
      return;
    }

    const lastRow = summarySheet.getLastRow();
    const lastCol = summarySheet.getLastColumn();
    if (lastRow < 2) {
      SpreadsheetApp.getUi().alert('summary シートにデータがありません（ヘッダーのみ）です。');
      return;
    }

    const tz = ss.getSpreadsheetTimeZone() || 'Asia/Tokyo';

    const numRows = lastRow - 1;
    const values = summarySheet.getRange(2, 1, numRows, lastCol).getValues();

    const rowsInfo = [];
    const speedArrByType = {};
    const totalViewsArrByType = {};

    for (let i = 0; i < numRows; i++) {
      const row = values[i];

      const videoId = row[0];
      if (!videoId) continue;

      const type = row[1] || '';
      const typeKey = String(type).toLowerCase();
      const title = row[2];
      const publishedAt = toDate_(row[3]);
      const durationSec = Number(row[4]) || 0;
      const firstLoggedAt = toDate_(row[5]);
      const firstViews = Number(row[6]) || 0;
      const firstLikes = Number(row[7]) || 0;
      const lastLoggedAt = toDate_(row[9]);
      const lastViews = Number(row[10]) || 0;
      const lastLikes = Number(row[11]) || 0;
      const note = row[13];

      const logs = parseNoteToLogs_(note);
      let r3 = null;
      let r7 = null;
      let r30 = null;

      if (publishedAt && lastViews > 0 && logs.length > 0) {
        for (let j = 0; j < logs.length; j++) {
          const lg = logs[j];
          const diff = diffDaysByCalendar_(publishedAt, lg.date, tz);
          if (diff == null || diff < 1) continue;

          if (diff <= 3)  r3  = lg.views / lastViews;
          if (diff <= 7)  r7  = lg.views / lastViews;
          if (diff <= 30) r30 = lg.views / lastViews;
        }
      }

      let daysSince = null;
      if (publishedAt && lastLoggedAt) {
        daysSince = diffDaysByCalendar_(publishedAt, lastLoggedAt, tz);
      }

      let avgPerDay = null;
      if (daysSince && daysSince > 0 && lastViews > 0) {
        avgPerDay = lastViews / daysSince;
      }

      if (avgPerDay && avgPerDay > 0 && typeKey) {
        if (!speedArrByType[typeKey]) speedArrByType[typeKey] = [];
        speedArrByType[typeKey].push(avgPerDay);
      }
      if (lastViews && lastViews > 0 && typeKey) {
        if (!totalViewsArrByType[typeKey]) totalViewsArrByType[typeKey] = [];
        totalViewsArrByType[typeKey].push(lastViews);
      }

      rowsInfo.push({
        videoId: videoId,
        type: type,
        typeKey: typeKey,
        title: title,
        publishedAt: publishedAt,
        durationSec: durationSec,
        firstLoggedAt: firstLoggedAt,
        firstViews: firstViews,
        firstLikes: firstLikes,
        lastLoggedAt: lastLoggedAt,
        lastViews: lastViews,
        lastLikes: lastLikes,
        daysSince: daysSince,
        avgPerDay: avgPerDay,
        r3: r3,
        r7: r7,
        r30: r30,
        note: note
      });
    }

    const medianSpeedByType = {};
    const medianTotalViewsByType = {};

    Object.keys(speedArrByType).forEach(function(k) {
      medianSpeedByType[k] = calcMedian_(speedArrByType[k]);
    });
    Object.keys(totalViewsArrByType).forEach(function(k) {
      medianTotalViewsByType[k] = calcMedian_(totalViewsArrByType[k]);
    });

    const growthRows = [];

    rowsInfo.forEach(function(info) {
      const typeKey = info.typeKey;
      const medSpeed = medianSpeedByType[typeKey] || 0;
      const medTotal = medianTotalViewsByType[typeKey] || 0;

      const pattern = classifyGrowthPattern_(
        typeKey,
        info.r3,
        info.r7,
        info.r30,
        info.daysSince,
        info.lastViews,
        medTotal
      );
      const speedRank = classifySpeedRank_(info.avgPerDay, medSpeed);

      growthRows.push([
        info.videoId,
        info.type,
        info.title,
        info.publishedAt || '',
        info.lastLoggedAt || '',
        info.lastViews || 0,
        info.lastLikes || 0,
        info.daysSince || '',
        info.avgPerDay || '',
        info.r3 != null ? info.r3 : '',
        info.r7 != null ? info.r7 : '',
        info.r30 != null ? info.r30 : '',
        pattern,
        speedRank,
        info.firstLoggedAt || '',
        info.firstViews || 0,
        info.firstLikes || 0,
        info.note || ''
      ]);
    });

    const sheetName = '成長プロファイル';
    let gpSheet = ss.getSheetByName(sheetName);
    if (!gpSheet) gpSheet = ss.insertSheet(sheetName);

    const gd = gpSheet.getDataRange();
    gd.clearContent();
    gd.clearFormat();

    const header = [
      '動画ID',
      '動画タイプ',
      'タイトル',
      '投稿日',
      '最新ログ日時',
      '最新再生数',
      '最新高評価数',
      '経過日数',
      '平均再生/日',
      '3日以内再生割合',
      '7日以内再生割合',
      '30日以内再生割合',
      '成長パターン',
      'スピードランク',
      '初回ログ日時',
      '初回再生数',
      '初回高評価数',
      '成長メモ'
    ];
    gpSheet.getRange(1, 1, 1, header.length).setValues([header]);

    if (growthRows.length > 0) {
      gpSheet.getRange(2, 1, growthRows.length, header.length).setValues(growthRows);
    }

    setBordersForUsedRange_(gpSheet);

    SpreadsheetApp.getUi().alert(
      '成長プロファイル（summary → note 解析）が更新されました。\n' +
      '動画数: ' + growthRows.length
    );
  });
}
