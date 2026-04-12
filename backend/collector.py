import os, asyncio, aiohttp, requests, re, time, sys, random
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

# === КОНФИГУРАЦИЯ ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
SOCIALDATA_API_KEY = os.getenv("SOCIALDATA_API_KEY")

CONTENT_THREAD_ID = "1389273374748049439"
XP_BOT_THREAD_ID = "1351492950768619552"

# Список каналов для сбора обычных сообщений
THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1367864741548261416", "1465733325149835295", "1371110511919497226", 
    "1366338962813222993", "1371904910324404325", "1371413462982594620", 
    "1372149550793490505", "1372149324192153620", "1372149873188536330", 
    "1372242189240897596", "1351488556924932128"
]

HEADERS = {
    'Authorization': DISCORD_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

async def get_twitter_stats(session, tweet_url):
    """Статистика твита (Лайки, Просмотры, Комменты)"""
    tweet_id_match = re.search(r"status/(\d+)", tweet_url)
    if not tweet_id_match: return None
    
    tweet_id = tweet_id_match.group(1)
    url = f"https://api.socialdata.tools/twitter/tweets/{tweet_id}"
    headers = {"Authorization": f"Bearer {SOCIALDATA_API_KEY}", "Accept": "application/json"}
    
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {
                    "views": data.get("views_count", 0) or 0,
                    "likes": data.get("favorite_count", 0) or 0,
                    "replies": data.get("reply_count", 0) or 0
                }
    except Exception as e:
        log(f"Ошибка SocialData для {tweet_id}: {e}")
    return None

async def get_discord_messages(session, thread_id, days):
    """Сбор сообщений с сохранением отпечатков"""
    target_date = datetime.now(timezone.utc) - timedelta(days=days)
    messages = []
    last_id = None
    log(f"Сканирование {thread_id} за {days} дн...")
    
    total_fetched = 0
    while True:
        url = f"https://discord.com/api/v9/channels/{thread_id}/messages?limit=100"
        if last_id: url += f"&before={last_id}"
        
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status == 429:
                wait = (await resp.json()).get('retry_after', 2)
                log(f"   [!] Лимит Discord. Ждем {wait} сек...")
                await asyncio.sleep(wait)
                continue
            if resp.status != 200: break
            batch = await resp.json()
            if not batch: break
            
            for m in batch:
                m_date = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                if m_date < target_date: 
                    log(f"   --- Готово. Собрано: {len(messages)}")
                    return messages
                messages.append(m)
            
            total_fetched += len(batch)
            last_id = batch[-1]['id']
            if total_fetched % 500 == 0:
                log(f"   [+] Пройдено {total_fetched}...")
            
            # ДВОЙНАЯ ЗАДЕРЖКА (БЕЗОПАСНОСТЬ)
            await asyncio.sleep(random.uniform(0.4, 0.8))
            
    return messages

async def main():
    log("Запуск коллектора (Приоритет: Twitter)...")
    users = {}
    user_tweets = {} 
    
    # Загружаем старые данные для сохранения ролей
    old_res = supabase.table("leaderboard_stats").select("*").execute()
    old_data = {item['user_id']: item for item in old_res.data} if old_res.data else {}

    async with aiohttp.ClientSession() as session:
        # ЭТАП 1: TWITTER (30 ДНЕЙ) - САМЫЙ ВАЖНЫЙ
        log(">>> ШАГ 1: Сбор контента (30 дней)")
        content_msgs = await get_discord_messages(session, CONTENT_THREAD_ID, 30)
        for m in content_msgs:
            uid = m['author']['id']
            if uid not in users:
                exist = old_data.get(uid, {})
                users[uid] = {
                    "user_id": uid, "username": m['author']['username'],
                    "discord_messages": 0, "twitter_posts": 0, "total_score": 0,
                    "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0,
                    "discord_roles": exist.get("discord_roles", []),
                    "discord_joined_at": exist.get("discord_joined_at")
                }
            
            links = re.findall(r'(https?://[^\s]+(?:x\.com|twitter\.com)[^\s]+)', m['content'].lower())
            if links:
                users[uid]["twitter_posts"] += len(links)
                if uid not in user_tweets: user_tweets[uid] = []
                user_tweets[uid].extend(links)

        # ЭТАП 2: SOCIAL DATA API
        if SOCIALDATA_API_KEY and user_tweets:
            log(f">>> ШАГ 2: Статистика Twitter ({len(user_tweets)} авторов)")
            count = 0
            for uid, links in user_tweets.items():
                for link in list(set(links))[:5]: 
                    stats = await get_twitter_stats(session, link)
                    if stats:
                        users[uid]["twitter_likes"] += stats["likes"]
                        users[uid]["twitter_views"] += stats["views"]
                        users[uid]["twitter_replies"] += stats["replies"]
                        users[uid]["total_score"] += (stats["likes"] * 2) + (stats["replies"] * 5)
                    count += 1
                    await asyncio.sleep(random.uniform(0.3, 0.6))
            log(f"   --- Обработано {count} ссылок")

        # ЭТАП 3: СООБЩЕНИЯ (7 ДНЕЙ)
        log(">>> ШАГ 3: Сбор сообщений в чатах (7 дней)")
        for tid in THREAD_IDS:
            msgs = await get_discord_messages(session, tid, 7)
            for m in msgs:
                uid = m['author']['id']
                if uid not in users:
                    exist = old_data.get(uid, {})
                    users[uid] = {
                        "user_id": uid, "username": m['author']['username'],
                        "discord_messages": 0, "twitter_posts": 0, "total_score": 0,
                        "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0,
                        "discord_roles": exist.get("discord_roles", []),
                        "discord_joined_at": exist.get("discord_joined_at")
                    }
                users[uid]["discord_messages"] += 1

        # ЭТАП 4: XP БОТ (7 ДНЕЙ)
        log(">>> ШАГ 4: Анализ XP бота (7 дней)")
        xp_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, 7)
        for xm in xp_msgs:
            if xm.get('mentions'):
                t_uid = xm['mentions'][0]['id']
                if t_uid in users:
                    match = re.search(r'(\d[\d\s,.]*)\s*XP', xm['content'])
                    if match:
                        val = int(match.group(1).replace(' ', '').replace(',', '').replace('.', ''))
                        if val > users[t_uid]["total_score"]:
                            users[t_uid]["total_score"] = val

    # Синхронизация
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for uid, info in users.items():
        if info["total_score"] == 0:
            info["total_score"] = info["discord_messages"] * 10
        info["updated_at"] = now
        payload.append(info)

    if payload:
        log(f">>> СИНХРОНИЗАЦИЯ: {len(payload)} пользователей")
        for i in range(0, len(payload), 50):
            supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
        log("Все данные успешно обновлены!")

if __name__ == "__main__":
    asyncio.run(main())
