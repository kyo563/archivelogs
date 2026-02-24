/**
 * path: investigation/gas_comment_count_patch.gs
 *
 * 目的:
 * - 手動貼り付けで C列(HYPERLINK)が崩れた行が、
 *   compressRecordAndUpdateSummary 実行時に消える問題を防ぐ。
 *
 * 制約:
 * - シート列構成は変更しない。
 * - 行の新規挿入・並び替えはしない。
 *
 * 反映方法:
 * - 既存 code.gs の同名関数を下記内容で置き換える。
 */

/**
 * C列の値/数式から video_id を抽出する。
 * 取れない場合は空文字を返す。
 */
function extractVideoIdFromText_(text) {
  if (text === null || text === undefined) return '';
  const s = String(text).trim();
  if (!s) return '';

  // HYPERLINK("https://www.youtube.com/watch?v=VIDEO_ID", "title")
  let m = s.match(/watch\?v=([A-Za-z0-9_-]{11})/i);
  if (m) return m[1];

  // youtu.be/VIDEO_ID
  m = s.match(/youtu\.be\/([A-Za-z0-9_-]{11})/i);
  if (m) return m[1];

  // shorts/VIDEO_ID
  m = s.match(/shorts\/([A-Za-z0-9_-]{11})/i);
  if (m) return m[1];

  // 生の11桁IDのみ貼られたケース
  m = s.match(/^[A-Za-z0-9_-]{11}$/);
  if (m) return m[0];

  return '';
}

/**
 * 既存の圧縮処理への最小パッチ。
 * ポイント:
 * - video_id が取れない行を continue で捨てない。
 * - 非解析行は「そのまま保持」して再書き込み対象に含める。
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

  // 既存ロジック互換: 動画ごとに集約
  const byVideo = new Map();
  const unparseableRows = []; // ここが今回の追加: 解析不可行の退避

  for (let i = 0; i < numRows; i++) {
    const row = values[i];
    const fRow = formulas[i];

    const titleFormula = fRow[2]; // C列
    const titleValue = row[2];
    const videoId = extractVideoIdFromText_(titleFormula || titleValue);

    if (!videoId) {
      // 以前: continue で消えていた
      // 変更後: 行をそのまま保持して書き戻す
      unparseableRows.push(row.slice(0, 8));
      continue;
    }

    if (!byVideo.has(videoId)) byVideo.set(videoId, []);
    byVideo.get(videoId).push(row);
  }

  // 既存仕様: 各動画内で連続同値ログを圧縮
  const compressedRows = [];
  for (const [, rows] of byVideo) {
    let prevView = null;
    let prevLike = null;
    let prevComment = null;

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const curView = r[3];
      const curLike = r[4];
      const curComment = r[5];

      if (i > 0 && curView === prevView && curLike === prevLike && curComment === prevComment) {
        continue;
      }

      compressedRows.push(r.slice(0, 8));
      prevView = curView;
      prevLike = curLike;
      prevComment = curComment;
    }
  }

  // 非解析行を必ず残す（末尾に付与。既存データ欠損防止を優先）
  const compressedAllRows = compressedRows.concat(unparseableRows);

  // 書き戻し
  recordSheet.getRange(2, 1, numRows, lastCol).clearContent();
  if (compressedAllRows.length > 0) {
    recordSheet.getRange(2, 1, compressedAllRows.length, 8).setValues(compressedAllRows);
  }

  // 抽出失敗を見える化（任意。邪魔なら Logger のみに変更可）
  if (unparseableRows.length > 0) {
    Logger.log('[WARN] video_id 抽出失敗行: ' + unparseableRows.length + ' 件（行は削除せず保持）');
  }

  // NOTE:
  // 既存の summary 更新処理がこの下にある場合は、そのまま残してください。
}
