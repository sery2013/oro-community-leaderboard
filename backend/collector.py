import os
import asyncio
import aiohttp
import requests
import re
import time
import sys
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

# === КОНФИГУРАЦИЯ (Берем из секретов / Environment Variables) ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
SOCIALDATA_API_KEY = os.getenv("SOCIALDATA_API_KEY")

# Ветки (можно тоже вынести в секреты, если хочешь)
GUILD_ID = "1349045850331938826"
CONTENT_THREAD_ID = "1351488160206426227"
XP_BOT_THREAD_ID = "1351492950768619552"
THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1351492950768619552", "1367864741548261416", "1371904712001065000", 
    "1465733325149835295", "1371110511919497226", "1366338962057396316"
]

# Лимиты времени
DISCORD_TARGET = datetime.now(timezone.utc) - timedelta(days=2)
CONTENT_TARGET = datetime.now(timezone.utc) - timedelta(days=30)

# === HEADERS (Эмуляция реального браузера) ===
HEADERS = {
    'Authorization': DISCORD_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    'Origin': 'https://discord.com',
    'Referer': 'https://discord.com/channels/@me',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin'
}

# Инициализация Supabase
if not SUPABASE_URL or not SUPABASE_KEY:
    log("ОШИБКА: SUPABASE_URL или SUPABASE_KEY не найдены в секретах!")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def get_discord_messages(session, thread_id, is_content=False):
    target_date = CONTENT_TARGET if is_content else DISCORD_TARGET
    messages = []
    last_id = None
    
    while True:
        url = f"https://discord.com/api/v9/channels/{thread_id}/messages?limit=100"
        if last_id: url += f"&before={last_id}"
        
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status != 200:
                log(f"Ошибка {resp.status} в ветке {thread_id}")
                break
            batch = await resp.json()
            if not batch: break
            
            for m in batch:
                dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                if dt < target_date: return messages
                messages.append(m)
            last_id = batch[-1]['id']
            await asyncio.sleep(0.5) # Пауза между страницами
    return messages

def parse_xp(text):
    match = re.search(r'(\d[\d\s,.]*)\s*XP', text)
    if match:
        return int(match.group(1).replace(' ', '').replace(',', '').replace('.', ''))
    return None

async def main():
    log("Запуск коллектора...")
    users = {}
    
    # 1. Загружаем старые данные для кэша ролей
    old_db = supabase.table("leaderboard_stats").select("*").execute()
    old_map = {item['user_id']: item for item in old_db.data} if old_db.data else {}

    async with aiohttp.ClientSession() as session:
        for tid in THREAD_IDS:
            is_cont = (tid == CONTENT_THREAD_ID)
            log(f"Парсинг ветки {tid}...")
            msgs = await get_discord_messages(session, tid, is_cont)
            
            for m in msgs:
                uid = m['author']['id']
                if uid not in users:
                    exist = old_map.get(uid, {})
                    users[uid] = {
                        "user_id": uid, "username": m['author']['username'],
                        "discord_messages": 0, "twitter_posts": 0, "total_score": 0,
                        "discord_roles": exist.get("discord_roles", []),
                        "discord_joined_at": exist.get("discord_joined_at")
                    }
                
                if is_cont:
                    if any(x in m['content'] for x in ["t.co", "x.com", "twitter.com"]):
                        users[uid]["twitter_posts"] += 1
                else:
                    users[uid]["discord_messages"] += 1

        # 2. Парсим XP от бота
        log("Сбор XP из логов бота...")
        bot_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, False)
        for bm in bot_msgs:
            if bm.get('mentions'):
                t_uid = bm['mentions'][0]['id']
                if t_uid in users:
                    val = parse_xp(bm['content'])
                    if val: users[t_uid]["total_score"] = val

    # 3. Сохранение результатов
    now = datetime.now(timezone.utc).isoformat()
    final_list = []
    for uid, data in users.items():
        if data["total_score"] == 0:
            data["total_score"] = data["discord_messages"] * 10
        data["updated_at"] = now
        final_list.append(data)

    if final_list:
        for i in range(0, len(final_list), 50):
            supabase.table("leaderboard_stats").upsert(final_list[i:i+50]).execute()
        log(f"Успешно синхронизировано {len(final_list)} пользователей.")

if __name__ == "__main__":
    asyncio.run(main())
