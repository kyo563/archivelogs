# record シートで手動補完分が消える原因調査

対象: `compressRecordAndUpdateSummary()`

## 結論（原因）

手動で貼り付けた行が次回実行で消える主因は、`video_id` 抽出に失敗した行を **集計対象から除外** しているためです。

```js
const videoId = extractVideoIdFromText_(titleFormula || titleValue);
if (!videoId) continue;
```

手動貼り付け時に、C列が `HYPERLINK("https://www.youtube.com/watch?v=...", "...")` ではなく、
単なるタイトル文字列やExcel由来のURLでない値になると `extractVideoIdFromText_` が空を返し、
その行は `byVideo` に入らず最終的に書き戻されません。

さらに同関数は一度 `record` の既存データを消してから再書き込みするため、
除外された行は実行後に消えます。

```js
recordSheet.getRange(2, 1, numRows, lastCol).clearContent();
...
recordSheet.getRange(2, 1, compressedAllRows.length, 8).setValues(compressedAllRows);
```

## なぜ「復元→手動補完」後に起きやすいか

- 復元やExcel経由で貼ると、数式が値に変換される。
- C列の `HYPERLINK` 式が崩れ、URL情報が失われる。
- 本スクリプトは C列からしか `video_id` を再構築していない。

## 補足（見かけ上の「消えた」ケース）

`video_id` が取れていても、同一動画でビュー/いいね/コメントが直前行と同じ場合、
圧縮ロジックにより重複ログとして削減されます（仕様）。

```js
if (curView === prevView && curLike === prevLike && curComment === prevComment) continue;
```

ただし今回の症状（手動補完分がまとめて残らない）は、上の `video_id` 抽出失敗の影響が最有力です。

## 再発防止の方向性（実装候補）

1. `record` に専用の `video_id` 列を追加し、C列依存をやめる。
2. `video_id` が取れない行をスキップせず、`_UNPARSEABLE_` などで退避して残す。
3. 圧縮前に「`video_id` 抽出失敗件数」をアラート表示し、0件でない場合は中断。
