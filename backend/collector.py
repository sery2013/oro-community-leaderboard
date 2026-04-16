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

api_stats = {"success": 0, "403_errors": 0, "404_errors": 0, "cached": 0, "total": 0}

async def get_twitter_stats(session, tweet_url, max_retries=1):
    """Умный запрос: мгновенный пропуск битых ссылок + ожидание при лимитах"""
    clean_url = tweet_url.replace('/x.com/i/status/', '/x.com/status/').replace('twitter.com/i/status/', 'twitter.com/status/')
    tweet_id_match = re.search(r"status/(\d+)", clean_url)
    if not tweet_id_match or not SOCIALDATA_API_KEY: return None
    
    tweet_id = tweet_id_match.group(1)
    url = f"https://api.socialdata.tools/twitter/tweets/{tweet_id}"
    headers = {"Authorization": f"Bearer {SOCIALDATA_API_KEY}", "Accept": "application/json"}
    
    for attempt in range(max_retries + 1):
        try:
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    api_stats["success"] += 1
                    return {
                        "views": data.get("views_count", 0) or 0,
                        "likes": data.get("favorite_count", 0) or 0,
                        "replies": data.get("reply_count", 0) or 0,
                        "author_handle": data.get("user", {}).get("screen_name", "unknown")
                    }
                elif resp.status == 403:
                    try:
                        err_msg = (await resp.json()).get('message', '').lower()
                    except: err_msg = ""
                    
                    # Если твит удален или аккаунт в бане — не ждем
                    if any(x in err_msg for x in ["not found", "forbidden", "suspended", "protected"]):
                        log(f"❌ Доступ закрыт ({err_msg}): {tweet_id}")
                        return None
                    
                    # Если это лимит ключа — ждем один раз
                    api_stats["403_errors"] += 1
                    if attempt < max_retries:
                        log(f"⏳ 403 (лимит ключа?), жду 10s...")
                        await asyncio.sleep(10)
                        continue
                elif resp.status == 404:
                    api_stats["404_errors"] += 1
                    return None
                elif resp.status == 429:
                    wait = int(resp.headers.get('Retry-After', 5))
                    await asyncio.sleep(wait)
                    continue
        except: return None
    return None

async def get_discord_messages(session, thread_id, days):
    target_date = datetime.now(timezone.utc) - timedelta(days=days)
    messages = []
    last_id = None
    log(f"Сканирование {thread_id}...")
    while True:
        url = f"https://discord.com/api/v9/channels/{thread_id}/messages?limit=100"
        if last_id: url += f"&before={last_id}"
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status == 429:
                wait = (await resp.json()).get('retry_after', 5)
                await asyncio.sleep(wait); continue
            if resp.status != 200: break
            batch = await resp.json()
            if not batch: break
            for m in batch:
                m_date = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                if m_date < target_date: return messages
                messages.append(m)
            last_id = batch[-1]['id']
            await asyncio.sleep(0.4)
    return messages

async def main():
    global api_stats
    log("Запуск коллектора (Версия: Оптимизированный Qwen + Фиксы)...")
    
    # Загрузка кеша и старых данных
    old_res = supabase.table("leaderboard_stats").select("*").execute()
    old_data = {item['user_id']: item for item in old_res.data} if old_res.data else {}
    cache_res = supabase.table("tweet_cache").select("*").execute()
    tweet_cache = {item['tweet_url']: item for item in cache_res.data} if cache_res.data else {}

    async with aiohttp.ClientSession() as session:
        # ШАГ 1: Сбор ссылок
        content_msgs = await get_discord_messages(session, CONTENT_THREAD_ID, 30)
        twitter_pattern = r'https?://(?:www\.|mobile\.)?(?:x\.com|twitter\.com)/[a-zA-Z0-9_]+/status/\d+'
        
        users = {}
        user_tweets = {}
        for m in content_msgs:
            uid = str(m['author']['id'])
            if uid not in users:
                exist = old_data.get(uid, {})
                users[uid] = {
                    "user_id": uid, "username": m['author']['username'],
                    "discord_messages": 0, "twitter_posts": 0, "total_score": 0,
                    "likes": 0, "views": 0, "replies": 0,
                    "twitter_handle": exist.get("twitter_handle", "@not_linked"),
                    "discord_roles": exist.get("discord_roles", []),
                    "discord_joined_at": exist.get("discord_joined_at")
                }
            found = re.findall(twitter_pattern, m['content'], re.IGNORECASE)
            if found:
                if uid not in user_tweets: user_tweets[uid] = []
                user_tweets[uid].extend([l.split('?')[0].lower() for l in found])

        # ШАГ 2: Статистика твитов (Лимит 20!)
        if user_tweets:
            log(f">>> ШАГ 2: Обработка твитов...")
            for uid, links in user_tweets.items():
                unique_links = list(set(links))[:20] # ✅ Увеличили до 20
                users[uid]["twitter_posts"] = len(unique_links)
                
                for link in unique_links:
                    api_stats["total"] += 1
                    stats = tweet_cache.get(link)
                    
                    if not stats:
                        api_result = await get_twitter_stats(session, link)
                        if api_result:
                            stats = {
                                'tweet_url': link, 'views': api_result['views'],
                                'likes': api_result['likes'], 'replies': api_result['replies'],
                                'author_handle': api_result['author_handle'],
                                'updated_at': datetime.now(timezone.utc).isoformat()
                            }
                            try:
                                supabase.table("tweet_cache").upsert(stats).execute()
                                tweet_cache[link] = stats
                                log(f"💾 Кеш: {link.split('/')[-1]}")
                            except: pass
                        await asyncio.sleep(0.05) # Максимальная скорость
                    else:
                        api_stats["cached"] += 1

                    if stats:
                        users[uid]["likes"] += stats.get("likes", 0)
                        users[uid]["views"] += stats.get("views", 0)
                        users[uid]["replies"] += stats.get("replies", 0)
                        users[uid]["total_score"] += (stats.get("likes", 0) * 2) + (stats.get("replies", 0) * 5)
                        if stats.get("author_handle") != "unknown":
                            users[uid]["twitter_handle"] = f"@{stats['author_handle']}"

        # ШАГ 3-4: Чаты и XP
        for tid in THREAD_IDS:
            msgs = await get_discord_messages(session, tid, 7)
            for m in msgs:
                uid = str(m['author']['id'])
                if uid in users: users[uid]["discord_messages"] += 1

        xp_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, 7)
        for xm in xp_msgs:
            if xm.get('mentions'):
                t_uid = str(xm['mentions'][0]['id'])
                if t_uid in users:
                    match = re.search(r'(\d[\d\s,.]*)\s*XP', xm['content'])
                    if match:
                        val = int(match.group(1).replace(' ', '').replace(',', '').replace('.', ''))
                        if val > users[t_uid]["total_score"]: users[t_uid]["total_score"] = val

    # Сохранение (payload)
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for uid, info in users.items():
        if info["total_score"] == 0: info["total_score"] = info["discord_messages"] * 10
        payload.append({
            "user_id": uid, "username": info["username"], "twitter_handle": info["twitter_handle"],
            "total_score": int(info["total_score"]), "twitter_likes": int(info["likes"]),
            "twitter_views": int(info["views"]), "twitter_replies": int(info["replies"]),
            "discord_messages": int(info["discord_messages"]), "updated_at": now
        })

    if payload:
        for i in range(0, len(payload), 50):
            supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
        log(f"🎉 Готово! Обработано {len(payload)} пользователей.")

if __name__ == "__main__":
    asyncio.run(main())
