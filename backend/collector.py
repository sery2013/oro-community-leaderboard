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

GUILD_ID = "1349045850331938826"
CONTENT_THREAD_ID = "1389273374748049439"
XP_BOT_THREAD_ID = "1351492950768619552"
THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1351492950768619552", "1367864741548261416", "1371904712001065000", 
    "1465733325149835295", "1371110511919497226", "1366338962813222993", 
    "1371904910324404325", "1371413462982594620", "1372149550793490505", 
    "1372149324192153620", "1372149873188536330", "1372242189240897596", 
    "1351488556924932128", "1389273374748049439"
]

HEADERS = {
    'Authorization': DISCORD_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

async def get_twitter_stats(session, tweet_url):
    """Получает статистику твита через SocialData"""
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
                    "retweets": data.get("retweet_count", 0) or 0
                }
    except Exception as e:
        log(f"Ошибка SocialData для {tweet_id}: {e}")
    return None

async def get_discord_messages(session, thread_id, days=2):
    target_date = datetime.now(timezone.utc) - timedelta(days=days)
    messages = []
    last_id = None
    log(f"Сбор из {thread_id} ({days} дн.)")
    
    while True:
        url = f"https://discord.com/api/v9/channels/{thread_id}/messages?limit=100"
        if last_id: url += f"&before={last_id}"
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status == 429:
                await asyncio.sleep((await resp.json()).get('retry_after', 1))
                continue
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
    log("Запуск...")
    users = {}
    user_tweets = {} # Храним ссылки на твиты для каждого юзера
    
    old_res = supabase.table("leaderboard_stats").select("*").execute()
    old_data = {item['user_id']: item for item in old_res.data} if old_res.data else {}

    async with aiohttp.ClientSession() as session:
        for tid in THREAD_IDS:
            is_content = (tid == CONTENT_THREAD_ID)
            days = 30 if is_content else (14 if tid == XP_BOT_THREAD_ID else 7)
            
            msgs = await get_discord_messages(session, tid, days)
            for m in msgs:
                uid = m['author']['id']
                if uid not in users:
                    exist = old_data.get(uid, {})
                    users[uid] = {
                        "user_id": uid, "username": m['author']['username'],
                        "discord_messages": 0, "twitter_posts": 0, "total_score": 0,
                        "twitter_likes": 0, "twitter_views": 0, "twitter_retweets": 0,
                        "discord_roles": exist.get("discord_roles", []),
                        "discord_joined_at": exist.get("discord_joined_at")
                    }
                
                if is_content:
                    links = re.findall(r'(https?://[^\s]+(?:x\.com|twitter\.com)[^\s]+)', m['content'].lower())
                    if links:
                        users[uid]["twitter_posts"] += len(links)
                        if uid not in user_tweets: user_tweets[uid] = []
                        user_tweets[uid].extend(links)
                else:
                    users[uid]["discord_messages"] += 1

        # Собираем XP Бота (14 дней)
        log("Обработка XP...")
        xp_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, 14)
        for xm in xp_msgs:
            if xm.get('mentions'):
                t_uid = xm['mentions'][0]['id']
                if t_uid in users:
                    match = re.search(r'(\d[\d\s,.]*)\s*XP', xm['content'])
                    if match:
                        val = int(match.group(1).replace(' ', '').replace(',', '').replace('.', ''))
                        if val > users[t_uid]["total_score"]:
                            users[t_uid]["total_score"] = val

        # НОВЫЙ БЛОК: Сбор статистики Twitter через SocialData
        if SOCIALDATA_API_KEY:
            log(f"Запрос статистики SocialData для {len(user_tweets)} активных авторов...")
            for uid, links in user_tweets.items():
                # Берем только последние 5 уникальных твитов, чтобы не тратить все лимиты SocialData
                for link in list(set(links))[:5]: 
                    stats = await get_twitter_stats(session, link)
                    if stats:
                        users[uid]["twitter_likes"] += stats["likes"]
                        users[uid]["twitter_views"] += stats["views"]
                        users[uid]["twitter_retweets"] += stats["retweets"]
                        # Добавляем к скору: 1 лайк = 2 XP, 1 ретвит = 5 XP (можно поменять)
                        users[uid]["total_score"] += (stats["likes"] * 2) + (stats["retweets"] * 5)
                    await asyncio.sleep(0.2) # Небольшая пауза для API

    # Финализация
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
        log(f"Готово. Обработано {len(payload)} юзеров.")

if __name__ == "__main__":
    asyncio.run(main())
