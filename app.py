diff --git a/app.py b/app.py
index 94aa9ed4ab1cb91b3ea1362f2686c10b98f9ddd6..6ed0a62d973cc3eaaee0feb4cf7b413563455d26 100644
--- a/app.py
+++ b/app.py
@@ -231,139 +231,159 @@ def resolve_video_id(url_or_id: str) -> Optional[str]:
     # youtu.be
     if "youtu.be/" in s:
         return s.split("youtu.be/")[1].split("?")[0].split("/")[0]
 
     # shorts
     if "youtube.com/shorts/" in s:
         return s.split("shorts/")[1].split("?")[0].split("/")[0]
 
     # 素の videoId
     if len(s) == 11 and "/" not in s and " " not in s:
         return s
 
     return None
 
 
 # ====================================
 # record 用 YouTube処理
 # ====================================
 
 def fetch_channel_upload_items(channel_id: str, max_results: int, api_key: str) -> List[Dict]:
     """
     チャンネルのアップロード済み動画（公開・処理済・アーカイブ済みのみ）を
     公開日時の古い順に max_results 件まで取得。
     """
     youtube = get_youtube_client(api_key)
+    # API の仕様上 1 回で取得できる件数は 50 件のため、上限を固定する
+    # （複数ページにまたがっても最終的な取得件数はこの上限に収まる）
+    max_results = min(max_results, 50)
 
     # uploads プレイリストID取得
     try:
         ch_resp = youtube.channels().list(
             part="contentDetails",
             id=channel_id,
             maxResults=1,
         ).execute()
         items = ch_resp.get("items", [])
         if not items:
+            st.warning("チャンネルのアップロード情報が取得できませんでした。")
             return []
         uploads_playlist_id = (
             items[0]
             .get("contentDetails", {})
             .get("relatedPlaylists", {})
             .get("uploads")
         )
         if not uploads_playlist_id:
+            st.warning("アップロード動画のプレイリストが見つかりませんでした。")
             return []
-    except Exception:
+    except Exception as e:
+        st.warning(f"チャンネル情報の取得に失敗しました: {e}")
         return []
 
     # playlistItems で videoId を取得
+    video_ids: List[str] = []
+    next_page: Optional[str] = None
     try:
-        pl_resp = youtube.playlistItems().list(
-            part="contentDetails",
-            playlistId=uploads_playlist_id,
-            maxResults=max_results,
-        ).execute()
-    except Exception:
+        # max_results は 50 に制限するが、アップロード数が多いチャンネルでも
+        # 先頭 50 件を漏らさないよう、ページングしながら上限に達するまで集める
+        while True:
+            remaining = max_results - len(video_ids)
+            if remaining <= 0:
+                break
+            pl_resp = youtube.playlistItems().list(
+                part="contentDetails",
+                playlistId=uploads_playlist_id,
+                maxResults=min(50, remaining),
+                pageToken=next_page,
+            ).execute()
+            for it in pl_resp.get("items", []):
+                cd = it.get("contentDetails", {}) or {}
+                vid = cd.get("videoId")
+                if vid:
+                    video_ids.append(vid)
+            next_page = pl_resp.get("nextPageToken")
+            if not next_page:
+                break
+    except Exception as e:
+        st.warning(f"アップロード動画の取得に失敗しました: {e}")
         return []
 
-    video_ids = []
-    for it in pl_resp.get("items", []):
-        cd = it.get("contentDetails", {}) or {}
-        vid = cd.get("videoId")
-        if vid:
-            video_ids.append(vid)
-
     if not video_ids:
         return []
 
     # video 本体
     try:
         v_resp = youtube.videos().list(
             part="snippet,contentDetails,statistics,status,liveStreamingDetails",
             id=",".join(video_ids),
             maxResults=max_results,
         ).execute()
-    except Exception:
+    except Exception as e:
+        st.warning(f"動画情報の取得に失敗しました: {e}")
         return []
 
     filtered: List[Dict] = []
     for it in v_resp.get("items", []):
         snippet = it.get("snippet", {}) or {}
         status = it.get("status", {}) or {}
 
         # 公開済み・処理済みのみ
         if status.get("privacyStatus") != "public":
             continue
         if status.get("uploadStatus") != "processed":
             continue
 
         # ライブ中 / 予約中は除外（アーカイブになってから）
         if snippet.get("liveBroadcastContent") in ("live", "upcoming"):
             continue
 
         filtered.append(it)
 
     # 公開日時（昇順）でソート
     filtered_sorted = sorted(
         filtered,
         key=lambda x: (x.get("snippet", {}).get("publishedAt") or ""),
     )
-    return filtered_sorted
+    # ページングで集めた件数が 50 件に満たない場合もあるため、安全側にスライス
+    return filtered_sorted[:max_results]
 
 
 def fetch_single_video_item(video_id: str, api_key: str) -> Optional[Dict]:
     """
     指定 videoId の動画を1件取得（公開・処理済み・アーカイブのみ）。
     """
     youtube = get_youtube_client(api_key)
     try:
         resp = youtube.videos().list(
             part="snippet,contentDetails,statistics,status,liveStreamingDetails",
             id=video_id,
             maxResults=1,
         ).execute()
-    except Exception:
+    except Exception as e:
+        st.warning(f"動画情報の取得に失敗しました: {e}")
         return None
 
     items = resp.get("items", [])
     if not items:
         return None
 
     it = items[0]
     snippet = it.get("snippet", {}) or {}
     status = it.get("status", {}) or {}
 
     if status.get("privacyStatus") != "public":
         return None
     if status.get("uploadStatus") != "processed":
         return None
     if snippet.get("liveBroadcastContent") in ("live", "upcoming"):
         return None
 
     return it
 
 
 def build_record_row_from_video_item(item: Dict, logged_at_str: str) -> List:
     """
     video API の item から record シート1行分を構成。
     """
     snippet = item.get("snippet", {}) or {}
@@ -481,92 +501,94 @@ def get_playlists_meta(channel_id: str, api_key: str) -> List[Dict]:
 
     return pls
 
 
 def search_video_ids_published_after(
     channel_id: str,
     days: int,
     api_key: str,
 ) -> List[str]:
     youtube = get_youtube_client(api_key)
     video_ids: List[str] = []
 
     published_after = (
         datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=days)
     ).isoformat().replace("+00:00", "Z")
 
     next_page: Optional[str] = None
     try:
         while True:
             resp = youtube.search().list(
                 part="id",
                 channelId=channel_id,
                 publishedAfter=published_after,
                 type="video",
                 maxResults=50,
+                order="date",  # 期間内の取りこぼしを避けるため公開日の降順で取得
                 pageToken=next_page,
             ).execute()
             for item in resp.get("items", []):
                 vid = item.get("id", {}).get("videoId")
                 if vid:
                     video_ids.append(vid)
             next_page = resp.get("nextPageToken")
             if not next_page:
                 break
-    except Exception:
-        pass
+    except Exception as e:
+        st.warning(f"期間内動画の検索に失敗しました: {e}")
 
     return video_ids
 
 
 def get_videos_stats(video_ids: Tuple[str, ...], api_key: str) -> Dict[str, Dict]:
     youtube = get_youtube_client(api_key)
     out: Dict[str, Dict] = {}
 
     if not video_ids:
         return out
 
     for i in range(0, len(video_ids), 50):
         chunk = video_ids[i: i + 50]
         try:
             resp = youtube.videos().list(
                 part="snippet,statistics",
                 id=",".join(chunk),
                 maxResults=50,
             ).execute()
             for it in resp.get("items", []):
                 vid = it.get("id")
                 if not vid:
                     continue
                 snippet = it.get("snippet", {}) or {}
                 stats = it.get("statistics", {}) or {}
                 out[vid] = {
                     "title": snippet.get("title", "") or "",
                     "viewCount": int(stats.get("viewCount", 0) or 0),
                     "likeCount": int(stats.get("likeCount", 0) or 0),
                 }
-        except Exception:
+        except Exception as e:
+            st.warning(f"動画統計情報の取得に失敗しました: {e}")
             continue
 
     return out
 
 
 # ====================================
 # UI
 # ====================================
 
 st.title("ログ収集ツール")
 
 # ★ APIキー入力はここで一度だけ
 api_key = get_api_key_from_ui()
 
 tab_logs, tab_status = st.tabs(["ログ（record）", "ステータス（Status）"])
 
 # ----------------------------
 # タブ1: 動画ログ収集（record）
 # ----------------------------
 with tab_logs:
     st.subheader("recordシート")
 
     if not api_key:
         st.info("サイドバーから YouTube API Key を入力してください。")
     else:
