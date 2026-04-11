import os, asyncio, aiohttp, requests, re, time, sys, random
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

# === КОНФИГУРАЦИЯ (Берется из секретов) ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
SOCIALDATA_API_KEY = os.getenv("SOCIALDATA_API_KEY")

GUILD_ID = "1349045850331938826"
CONTENT_THREAD_ID = "1351488160206426227"  # Ветка с твитами (30 дней)
XP_BOT_THREAD_ID = "1351492950768619552"   # Ветка, где бот пишет XP
THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1351492950768619552", "1367864741548261416", "1371904712001065000", 
    "1465733325149835295", "1371110511919497226", "1366338962813222993", 
    "1371904910324404325", "1371413462982594620", "1372149550793490505", 
    "1372149324192153620", "1372149873188536330", "1372242189240897596", 
    "1351488556924932128", "1389273374748049439"
]

# === УЛУЧШЕННЫЕ HEADERS (ОТПЕЧАТКИ БРАУЗЕРА) ===
HEADERS = {
    'Authorization': DISCORD_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    'Origin': 'https://discord.com',
    'Referer': 'https://discord.com/channels/@me',
    'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'x-debug-options': 'bugReporterEnabled',
    'x-discord-locale': 'en-US'
}

if not SUPABASE_URL or not SUPABASE_KEY:
    log("Ошибка: Секреты Supabase не найдены!")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

async def get_discord_messages(session, thread_id, days=2):
    target_date = datetime.now(timezone.utc) - timedelta(days=days)
    messages = []
    last_id = None
    
    log(f"Начинаю сбор сообщений из {thread_id} (глубина {days} дн.)")
    
    while True:
        url = f"https://discord.com/api/v9/channels/{thread_id}/messages?limit=100"
        if last_id: url += f"&before={last_id}"
        
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status == 429:
                retry_after = (await resp.json()).get('retry_after', 1)
                log(f"Rate limit! Ждем {retry_after} сек.")
                await asyncio.sleep(retry_after)
                continue
            if resp.status != 200:
                log(f"Ошибка {resp.status} в ветке {thread_id}")
                break
            
            batch = await resp.json()
            if not batch: 
                log("Сообщений больше нет (конец ветки).")
                break
            
            current_batch_count = len(batch)
            last_m_date_str = batch[-1]['timestamp']
            
            for m in batch:
                m_date = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                if m_date < target_date:
                    log(f"Достигли даты {m_date.strftime('%Y-%m-%d')}. Сбор завершен. Итого: {len(messages)} шт.")
                    return messages
                messages.append(m)
            
            last_id = batch[-1]['id']
            log(f"Скачано пачкой: {current_batch_count}. Всего: {len(messages)}. Последняя дата: {last_m_date_str[:10]}")
            await asyncio.sleep(random.uniform(0.4, 0.8))
            
    return messages

async def main():
    log("Запуск обновления данных...")
    users = {}
    
    # Кэш старых данных для сохранения ролей
    old_res = supabase.table("leaderboard_stats").select("*").execute()
    old_data = {item['user_id']: item for item in old_res.data} if old_res.data else {}

    async with aiohttp.ClientSession() as session:
        for tid in THREAD_IDS:
            is_content = (tid == CONTENT_THREAD_ID)
            days = 30 if is_content else 2
            log(f"Обработка ветки: {tid}")
            
            msgs = await get_discord_messages(session, tid, days)
            for m in msgs:
                uid = m['author']['id']
                if uid not in users:
                    exist = old_data.get(uid, {})
                    users[uid] = {
                        "user_id": uid, "username": m['author']['username'],
                        "discord_messages": 0, "twitter_posts": 0, "total_score": 0,
                        "discord_roles": exist.get("discord_roles", []),
                        "discord_joined_at": exist.get("discord_joined_at")
                    }
                
                if is_content:
                    if any(x in m['content'].lower() for x in ["t.co", "x.com", "twitter.com"]):
                        users[uid]["twitter_posts"] += 1
                else:
                    users[uid]["discord_messages"] += 1

        # Парсинг XP из логов бота
        log("Обновление XP из логов бота...")
        xp_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, 2)
        for xm in xp_msgs:
            if xm.get('mentions'):
                t_uid = xm['mentions'][0]['id']
                if t_uid in users:
                    match = re.search(r'(\d[\d\s,.]*)\s*XP', xm['content'])
                    if match:
                        val = int(match.group(1).replace(' ', '').replace(',', '').replace('.', ''))
                        if val > users[t_uid]["total_score"]:
                            users[t_uid]["total_score"] = val

    # Сохранение результатов
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for uid, info in users.items():
        if info["total_score"] == 0:
            info["total_score"] = info["discord_messages"] * 10
        info["updated_at"] = now
        payload.append(info)

    if payload:
        for i in range(0, len(payload), 50):
            supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
        log(f"Синхронизация завершена. Обработано {len(payload)} пользователей.")

if __name__ == "__main__":
    asyncio.run(main())
