/**
 * path: investigation/gas_comment_count_patch.gs
 *
 * 目的:
 * - 手動貼り付け等で C列(HYPERLINK) が崩れた行のリンクを、同一 video_id の既存式から自己修復する。
 * - compressRecordAndUpdateSummary 実行時に video_id 抽出失敗行を捨てず保持し、行消失を防ぐ。
 * - 書き戻し時に C列の式が落ちないよう values/formulas を分離して再設定する。
 *
 * 反映方法:
 * - 既存 code.gs の同名関数を下記内容で置き換える。
 */

/**
 * HYPERLINK 文字列を解析し、{url, label} を返す。
 * 解析失敗時は null。
 */
function parseHyperlinkFormula_(formulaText) {
  if (!formulaText) return null;
  const s = String(formulaText).trim();
  const m = s.match(/^\s*=?\s*HYPERLINK\(\s*"([^"]+)"\s*,\s*"([^"]*)"\s*\)\s*$/i);
  if (!m) return null;
  return { url: m[1], label: m[2] };
}

/**
 * C列の値/数式から video_id を抽出する。
 * 取れない場合は空文字を返す。
 */
function extractVideoIdFromText_(text) {
  if (text === null || text === undefined) return '';
  const s = String(text).trim();
  if (!s) return '';

  // watch?v=VIDEO_ID
  let m = s.match(/[?&]v=([A-Za-z0-9_-]{11})/i);
  if (m) return m[1];

  // youtu.be/VIDEO_ID
  m = s.match(/youtu\.be\/([A-Za-z0-9_-]{11})/i);
  if (m) return m[1];

  // shorts/VIDEO_ID
  m = s.match(/\/shorts\/([A-Za-z0-9_-]{11})/i);
  if (m) return m[1];

  // 生の11文字ID
  m = s.match(/^[A-Za-z0-9_-]{11}$/);
  if (m) return m[0];

  return '';
}

/**
 * 同一 video_id の既存 HYPERLINK 式を参照して、C列リンク崩れを自己修復する。
 *
 * @param {Array<Array<any>>} values 2行目以降の値
 * @param {Array<Array<string>>} formulas 2行目以降の数式
 * @returns {number} 修復件数
 */
function repairRecordHyperlinksFromSameVideo_(values, formulas) {
  const knownByVideoId = new Map();

  // 1st pass: 正常な HYPERLINK 式を video_id -> 式 として収集
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    const fRow = formulas[i];
    const f = fRow[2]; // C列
    const parsed = parseHyperlinkFormula_(f);
    if (!parsed) continue;

    const videoId = extractVideoIdFromText_(parsed.url);
    if (!videoId) continue;

    if (!knownByVideoId.has(videoId)) {
      knownByVideoId.set(videoId, f);
    }
  }

  // 2nd pass: C列式が壊れている行を復元
  let repaired = 0;
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    const fRow = formulas[i];

    const currentFormula = fRow[2] || '';
    if (parseHyperlinkFormula_(currentFormula)) continue; // 既に正常

    const videoId = extractVideoIdFromText_(currentFormula || row[2]);
    if (!videoId) continue;

    const backupFormula = knownByVideoId.get(videoId);
    if (!backupFormula) continue;

    fRow[2] = backupFormula;
    repaired++;
  }

  return repaired;
}

/**
 * record 圧縮 + summary 更新。
 *
 * ポイント:
 * - 先頭で C列リンク自己修復を実施。
 * - video_id 抽出不能行は破棄せず保持。
 * - 書き戻しは values + formulas を再設定し、C列式消失を防ぐ。
 */
function compressRecordAndUpdateSummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const recordSheet = ss.getSheetByName('record');
  if (!recordSheet) throw new Error('record シートが見つかりません');

  const lastRow = recordSheet.getLastRow();
  const lastCol = recordSheet.getLastColumn();
  if (lastRow < 2) return;

  const numRows = lastRow - 1;
  const values = recordSheet.getRange(2, 1, numRows, lastCol).getValues();
  const formulas = recordSheet.getRange(2, 1, numRows, lastCol).getFormulas();

  // 1) 圧縮前の自己修復
  const repairedCount = repairRecordHyperlinksFromSameVideo_(values, formulas);
  if (repairedCount > 0) {
    Logger.log('[INFO] C列リンク自己修復: ' + repairedCount + ' 件');
  }

  // 2) 動画ごとに集約（解析不能行は退避）
  const byVideo = new Map();
  const unparseableRows = [];

  for (let i = 0; i < numRows; i++) {
    const row = values[i];
    const fRow = formulas[i];

    const cFormula = fRow[2];
    const cValue = row[2];
    const videoId = extractVideoIdFromText_(cFormula || cValue);

    if (!videoId) {
      unparseableRows.push({ values: row.slice(), formulas: fRow.slice() });
      continue;
    }

    if (!byVideo.has(videoId)) byVideo.set(videoId, []);
    byVideo.get(videoId).push({ values: row, formulas: fRow });
  }

  // 3) 各動画内で view/like/comment が連続同値の行を圧縮
  const compressedRows = [];
  for (const [, rows] of byVideo) {
    let prevView = null;
    let prevLike = null;
    let prevComment = null;

    for (let i = 0; i < rows.length; i++) {
      const obj = rows[i];
      const r = obj.values;
      const curView = r[3];
      const curLike = r[4];
      const curComment = r[5];

      if (i > 0 && curView === prevView && curLike === prevLike && curComment === prevComment) {
        continue;
      }

      compressedRows.push({ values: obj.values.slice(), formulas: obj.formulas.slice() });
      prevView = curView;
      prevLike = curLike;
      prevComment = curComment;
    }
  }

  // 非解析行も必ず保持
  const outputRows = compressedRows.concat(unparseableRows);

  // 4) 書き戻し（値 + 数式）
  recordSheet.getRange(2, 1, numRows, lastCol).clearContent();

  if (outputRows.length > 0) {
    const outValues = outputRows.map(function(x) { return x.values; });
    const outFormulas = outputRows.map(function(x) { return x.formulas; });

    const writeRange = recordSheet.getRange(2, 1, outputRows.length, lastCol);
    writeRange.setValues(outValues);
    writeRange.setFormulas(outFormulas);
  }

  if (unparseableRows.length > 0) {
    Logger.log('[WARN] video_id 抽出失敗行: ' + unparseableRows.length + ' 件（行は削除せず保持）');
  }

  // NOTE:
  // 既存の summary 更新処理がこの下にある場合は、そのまま残してください。
}
