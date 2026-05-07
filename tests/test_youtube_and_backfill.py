from archivelogs.youtube_client import fetch_videos_bulk

def test_fetch_videos_bulk_chunking():
    calls=[]
    class V:
        def list(self, **kwargs): calls.append(kwargs["id"].split(",")); return self
        def execute(self): return {"items":[]}
    class Y:
        def videos(self): return V()
    fetch_videos_bulk(Y(), [f"id{i:09d}"[:11] for i in range(120)])
    assert len(calls)==3
