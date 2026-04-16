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

GUILD_ID = "1389273374748049439"
CONTENT_THREAD_ID = "1389273374748049439"
XP_BOT_THREAD_ID = "1351492950768619552"

THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1367864741548261416", "1465733325149835295", "1371110511919497226", 
    "1366338962813222993", "1371904910324404325", "1371413462982594620", 
    "1372149550793490505", "1372149324192153620", "1372149873188536330", 
    "1372242189240897596", "1351488556924932128"
]

DAYS_BACK_CONTENT = 30
DAYS_BACK_CHAT = 7
DAYS_BACK_XP = 7

PRIORITY_ROLES = {
    "1468552780238033009": "Bronze",
    "1468552336204103774": "Iron",
    "1468552865759891596": "Silver",
    "1468552932034351280": "Gold",
    "1468692622242484385": "Creator T1",
    "1468692668325302272": "Creator T2",
    "1468692694296563884": "Creator T3",
    "1468692722436149536": "Creator T4"
}

HEADERS = {
    'Authorization': DISCORD_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_discord_member_info(user_id, token):
    url = f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{user_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data.get('joined_at'), data.get('roles', [])
    except: pass
    return None, []

async def fetch_tweet_stats(session, tweet_url, api_key):
    id_match = re.search(r"status/(\d+)", tweet_url)
    if not id_match or not api_key: return None
    tweet_id = id_match.group(1)
    api_url = f"https://api.socialdata.tools/twitter/tweets/{tweet_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        async with session.get(api_url, headers=headers, timeout=15) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {
                    "likes": data.get('favorite_count', 0) or 0,
                    "views": data.get('views_count', 0) or 0,
                    "replies": data.get('reply_count', 0) or 0,
                    "twitter_handle": data.get('user', {}).get('screen_name')
                }
            elif resp.status == 429:
                wait = int(resp.headers.get('Retry-After', 10))
                await asyncio.sleep(wait)
    except: pass
    return None

async def get_discord_messages(session, thread_id, days, is_content_thread=False):
    target_date = datetime.now(timezone.utc) - timedelta(days=days)
    messages = []
    last_id = None
    while True:
        url = f"https://discord.com/api/v10/channels/{thread_id}/messages?limit=100"
        if last_id: url += f"&before={last_id}"
        try:
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
                await asyncio.sleep(random.uniform(0.1, 0.3))
        except: break
    return messages

async def main():
    log("🚀 Запуск с фиксами (Пагинация + Твиттер-кэш)...")
    
    # === ФИКС 1: Пагинация для загрузки всех пользователей (больше 1000) ===
    old_data = {}
    offset = 0
    while True:
        res = supabase.table("leaderboard_stats").select("*").range(offset, offset + 999).execute()
        if not res.data: break
        for item in res.data:
            old_data[item['user_id']] = item
        offset += 1000
    log(f"📥 Загружено {len(old_data)} пользователей из базы")

    try:
        cache_res = supabase.table("tweet_cache").select("*").execute()
        tweet_cache = {item['tweet_url']: item for item in cache_res.data} if cache_res.data else {}
    except: tweet_cache = {}
    
    users = {}
    tweet_list = []
    twitter_pattern = r'https?://(?:www\.|mobile\.)?(?:x\.com|twitter\.com)/[a-zA-Z0-9_]+/status/\d+'
    
    async with aiohttp.ClientSession() as session:
        log(">>> ШАГ 1: Сбор ссылок...")
        content_msgs = await get_discord_messages(session, CONTENT_THREAD_ID, DAYS_BACK_CONTENT, is_content_thread=True)
        for m in content_msgs:
            uid = str(m['author']['id'])
            if uid not in users:
                exist = old_data.get(uid, {})
                users[uid] = {
                    "user_id": uid, "username": m['author']['username'], "avatar_url": m['author'].get('avatar'),
                    "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0,
                    "twitter_handle": exist.get("twitter_handle", "@not_linked"), "discord_joined_at": exist.get("discord_joined_at"),
                    "discord_roles": exist.get("discord_roles", []), "total_score": 0, "channels": set()
                }
            links = re.findall(twitter_pattern, m['content'], re.IGNORECASE)
            for l in links: tweet_list.append((uid, l.split('?')[0].lower()))

        # === ФИКС 2: Логика Твиттера (применяем кэш к пользователю) ===
        log(">>> ШАГ 2: Twitter API + Кэш...")
        for uid, link in tweet_list:
            stats = None
            if link in tweet_cache:
                stats = tweet_cache[link]
            elif SOCIALDATA_API_KEY:
                stats = await fetch_tweet_stats(session, link, SOCIALDATA_API_KEY)
                if stats:
                    try:
                        supabase.table("tweet_cache").upsert({
                            'tweet_url': link, **stats, 'updated_at': datetime.now(timezone.utc).isoformat()
                        }).execute()
                        tweet_cache[link] = stats
                    except: pass
                await asyncio.sleep(random.uniform(0.5, 1.0))
            
            if stats and uid in users:
                users[uid]["twitter_posts"] += 1
                users[uid]["twitter_likes"] += stats.get("likes", 0)
                users[uid]["twitter_views"] += stats.get("views", 0)
                users[uid]["twitter_replies"] += stats.get("replies", 0)
                if stats.get("twitter_handle"): 
                    users[uid]["twitter_handle"] = f"@{stats['twitter_handle']}"

        log(">>> ШАГ 3: Сообщения...")
        for tid in THREAD_IDS:
            msgs = await get_discord_messages(session, tid, DAYS_BACK_CHAT)
            for m in msgs:
                uid = str(m['author']['id'])
                if uid not in users:
                    exist = old_data.get(uid, {})
                    users[uid] = {
                        "user_id": uid, "username": m['author']['username'], "avatar_url": m['author'].get('avatar'),
                        "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0,
                        "twitter_handle": exist.get("twitter_handle", "@not_linked"), "discord_joined_at": exist.get("discord_joined_at"),
                        "discord_roles": exist.get("discord_roles", []), "total_score": 0, "channels": set()
                    }
                users[uid]["discord_messages"] += 1
                users[uid]["channels"].add(tid)

        log(">>> ШАГ 4: XP...")
        xp_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, DAYS_BACK_XP)
        for xm in xp_msgs:
            if not xm['author'].get('bot', False): continue
            text = xm.get('content', '')
            for emb in xm.get('embeds', []):
                text += f"\n{emb.get('description', '')}\n" + "\n".join([f.get('value', '') for f in emb.get('fields', [])])
            msg_upper = text.upper()
            for uid, info in users.items():
                username = info.get('username', '').upper()
                if username and username in msg_upper:
                    match = re.search(r'\b' + re.escape(username) + r'\b.*?(\d[\d\s,]*[KM]?)\s*XP', msg_upper, re.DOTALL)
                    if match:
                        xp_str = match.group(1).replace(' ', '').replace(',', '').upper()
                        try:
                            val = int(float(xp_str.replace('K', ''))*1000) if 'K' in xp_str else int(float(xp_str.replace('M', ''))*1000000) if 'M' in xp_str else int(xp_str)
                            if val > info["total_score"]: info["total_score"] = val
                        except: pass

        log("🛡️ Роли...")
        for i, uid in enumerate(users):
            joined, roles = get_discord_member_info(uid, DISCORD_TOKEN)
            if joined: users[uid]["discord_joined_at"] = joined
            if roles:
                p_roles = [r for r in roles if r in PRIORITY_ROLES]
                if p_roles: users[uid]["discord_roles"] = p_roles
            await asyncio.sleep(0.05)

    log("\n📊 Сохранение...")
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for uid, info in users.items():
        old = old_data.get(uid, {})
        if info["total_score"] == 0: info["total_score"] = info["discord_messages"] * 10
        
        payload.append({
            "user_id": uid, "username": info["username"],
            "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{info['avatar_url']}.png" if info.get('avatar_url') else old.get("avatar_url"),
            "twitter_handle": info["twitter_handle"] if info["twitter_handle"] != "@not_linked" else old.get("twitter_handle", "@not_linked"),
            "total_score": int(info["total_score"]),
            # Сохраняем максимум, чтобы статы не падали
            "twitter_likes": max(int(info["twitter_likes"]), old.get("twitter_likes", 0)),
            "twitter_views": max(int(info["twitter_views"]), old.get("twitter_views", 0)),
            "twitter_replies": max(int(info["twitter_replies"]), old.get("twitter_replies", 0)),
            "twitter_posts": max(int(info["twitter_posts"]), old.get("twitter_posts", 0)),
            "discord_messages": int(info["discord_messages"]),
            # ФИКС 3: Счетчик каналов теперь накопительный
            "channels_count": max(len(info["channels"]), old.get("channels_count", 0)),
            "discord_roles": info["discord_roles"] if info["discord_roles"] else old.get("discord_roles", []),
            "discord_joined_at": info["discord_joined_at"] if info["discord_joined_at"] else old.get("discord_joined_at"),
            "prev_total_score": old.get("total_score", 0),
            "prev_discord_messages": old.get("discord_messages", 0),
            "updated_at": now
        })

    for i in range(0, len(payload), 50):
        try: supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
        except Exception as e: log(f"❌ {e}")
    log("🎉 ГОТОВО!")

if __name__ == "__main__":
    asyncio.run(main())
