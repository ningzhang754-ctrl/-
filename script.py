import psycopg2  # 导入模块
import os
import psycopg2
from psycopg2.extras import execute_values
import re

#解析标准SRT格式
def parse_srt(path):
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    # 匹配标准 SRT 格式的字幕块
    pattern = re.compile(
        r'(\d+)\n'  # 字幕编号
        r'(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})\n'  # 时间段
        r'(.+?)(?=\n\n|\Z)',  # 字幕文本
        re.DOTALL)
    matches = pattern.findall(content)
    return [
        {
            'index': int(m[0]),
            'start': m[1],
            'end': m[2],
            'text': m[3].replace('\n', ' ').strip()
        }
        for m in matches
    ]

#数据库连接并创建两张表
conn = psycopg2.connect(
    database="finance01",
    user="python01_user02",
    password="python01_user02@123",
    host="110.41.115.206",
    port=8000)
cur=conn.cursor()
cur.execute("""
    CREATE TABLE videos (
        video_id SERIAL PRIMARY KEY,
        file_name VARCHAR(255) NOT NULL,
        duration_sec INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
""")
cur.execute("""
    CREATE TABLE subtitles (
        subtitle_id SERIAL PRIMARY KEY,
        video_id INTEGER REFERENCES videos(video_id) ON DELETE CASCADE,
        subtitle_index INTEGER NOT NULL,
        start_time VARCHAR(12) NOT NULL,
        end_time VARCHAR(12) NOT NULL,
        text TEXT NOT NULL
    )
""")
conn.commit()

#在表中插入视频数据
video_durations = {
    "v1.mp4": 276,
    "v2.mp4": 349,
}  #视频时长
videos_dir = r"E:\da\cs\db"
for fname in os.listdir(videos_dir):
    if not fname.endswith(".mp4"):
        continue
    #获取视频时长
    duration_sec = video_durations[fname]
    # 插入 video 元数据
    cur.execute("""
    INSERT INTO videos (file_name, duration_sec)
        VALUES (%s, %s) RETURNING video_id
    """, (fname, duration_sec))
    vid = cur.fetchone()[0]

    # 读取并解析 SRT 字幕
    srt_path = os.path.join(videos_dir, fname.replace(".mp4", ".srt"))
    parsed_subs = parse_srt(srt_path)

    # 构造插入列表
    subs = []
    for item in parsed_subs:
        subs.append((vid, item['index'], item['start'], item['end'], item['text']))

    # 批量写入数据库
    execute_values(cur,
        "INSERT INTO subtitles (video_id, subtitle_index, start_time, end_time, text) VALUES %s",
        subs
    )

    conn.commit()
    print(f"Inserted video {vid} and {len(subs)} subtitles.")

cur.close()
conn.close()


