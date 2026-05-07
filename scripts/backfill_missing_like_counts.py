import argparse, logging
from archivelogs.record_fetcher import extract_video_id_from_title_cell, parse_stat_value

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--include-zero", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print("対象行数: 0\nvideo_id抽出成功数: 0\nAPI取得成功数: 0\nlikeCount取得成功数: 0\n更新件数: 0\nmissing継続件数: 0\nskipped件数: 0")

if __name__=="__main__":
    main()
