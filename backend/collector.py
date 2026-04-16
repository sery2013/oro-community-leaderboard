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

HEADERS = {
    'Authorization': DISCORD_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_discord_member_info(user_id, token):
    url = f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{user_id}"
    headers = HEADERS
    try:
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data.get('joined_at'), data.get('roles', [])
    except:
        pass
    return None, []

async def fetch_tweet_stats(session, tweet_url, api_key, max_retries=2):
    """Запрос с повторными попытками при 403"""
    id_match = re.search(r"status/(\d+)", tweet_url)
    if not id_match or not api_key:
        return None
    
    tweet_id = id_match.group(1)
    api_url = f"https://api.socialdata.tools/twitter/tweets/{tweet_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    for attempt in range(max_retries):
        try:
            async with session.get(api_url, headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    twitter_handle = None
                    if 'user' in data and 'screen_name' in data['user']:
                        twitter_handle = data['user']['screen_name']
                    
                    return {
                        "likes": data.get('favorite_count', 0) or 0,
                        "views": data.get('views_count', 0) or 0,
                        "replies": data.get('reply_count', 0) or 0,
                        "twitter_handle": twitter_handle
                    }
                elif resp.status == 403:
                    if attempt < max_retries - 1:
                        wait_time = 5 * (attempt + 1) # Ждем 5 или 10 сек
                        log(f"⚠️ 403 для {tweet_id}, пробую снова через {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        log(f"❌ 403 после попыток: {tweet_id}")
                        return None
                elif resp.status == 404:
                    return None
                elif resp.status == 429:
                    wait = int(resp.headers.get('Retry-After', 10))
                    await asyncio.sleep(wait)
                    continue
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                return None
    return None

async def get_discord_messages(session, thread_id, days, is_content_thread=False):
    target_date = datetime.now(timezone.utc) - timedelta(days=days)
    messages = []
    last_id = None
    
    log(f"📡 Сканирование {thread_id}...")
    
    while True:
        url = f"https://discord.com/api/v10/channels/{thread_id}/messages?limit=100"
        if last_id: url += f"&before={last_id}"
        
        try:
            async with session.get(url, headers=HEADERS) as resp:
                if resp.status == 429:
                    wait = (await resp.json()).get('retry_after', 5)
                    await asyncio.sleep(wait); continue
                if resp.status == 403:
                    log(f"⛔ НЕТ ДОСТУПА к каналу {thread_id}")
                    return messages
                if resp.status != 200: break
                
                batch = await resp.json()
                if not batch: break
                
                for m in batch:
                    m_date = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                    if m_date < target_date: return messages
                    messages.append(m)
                last_id = batch[-1]['id']
                
                if is_content_thread: await asyncio.sleep(random.uniform(0.1, 0.2))
                else: await asyncio.sleep(random.uniform(0.4, 0.7))
        except Exception as e:
            break
    return messages

async def main():
    log("🚀 Запуск...")
    
    # 1. Загрузка старых данных
    old_res = supabase.table("leaderboard_stats").select("*").execute()
    old_data = {item['user_id']: item for item in old_res.data} if old_res.data else {}
    
    # 2. 🔥 ЗАГРУЗКА КЭША (Чтобы не долбить API зря)
    try:
        cache_res = supabase.table("tweet_cache").select("*").execute()
        tweet_cache = {item['tweet_url']: item for item in cache_res.data} if cache_res.data else {}
        log(f"📦 Кэш загружен: {len(tweet_cache)} твитов")
    except Exception as e:
        log(f"⚠️ Нет таблицы кэша? Создай её в Supabase. Ошибка: {e}")
        tweet_cache = {}
    
    users = {}
    tweet_list = []
    twitter_pattern = r'https?://(?:www\.|mobile\.)?(?:x\.com|twitter\.com)/[a-zA-Z0-9_]+/status/\d+'
    
    async with aiohttp.ClientSession() as session:
        # ШАГ 1: Сбор ссылок
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
            for l in links:
                tweet_list.append((uid, l.split('?')[0].lower()))
        
        log(f"✅ Найдено {len(tweet_list)} ссылок")
        
        # ШАГ 2: Twitter API (УМНЫЙ РЕЖИМ)
        if tweet_list and SOCIALDATA_API_KEY:
            log(">>> ШАГ 2: Обновление Twitter данных...")
            
            # Группируем по уникальным ссылкам
            unique_tweets = {}
            for uid, link in tweet_list:
                if link not in unique_tweets: unique_tweets[link] = []
                unique_tweets[link].append(uid)
            
            # 🔥 ФИЛЬТР: Берем только те ссылки, которых НЕТ в кэше
            links_to_fetch = [link for link in unique_tweets.keys() if link not in tweet_cache]
            log(f"🔍 В кэше есть {len(unique_tweets) - len(links_to_fetch)}, нужно запросить: {len(links_to_fetch)}")
            
            fetched_count = 0
            for link in links_to_fetch:
                uids = unique_tweets[link]
                
                # Запрос к API
                stats = await fetch_tweet_stats(session, link, SOCIALDATA_API_KEY)
                
                if stats:
                    # Сохраняем в кэш
                    try:
                        supabase.table("tweet_cache").upsert({
                            'tweet_url': link, 'likes': stats['likes'], 'views': stats['views'],
                            'replies': stats['replies'], 'twitter_handle': stats.get('twitter_handle'),
                            'updated_at': datetime.now(timezone.utc).isoformat()
                        }).execute()
                        tweet_cache[link] = stats # Обновляем локальный кэш
                    except Exception as e:
                        log(f"⚠️ Ошибка сохранения в кэш: {e}")
                    
                    # Начисляем баллы всем, кто репостил этот твит
                    for uid in uids:
                        if uid in users:
                            users[uid]["twitter_posts"] += 1
                            users[uid]["twitter_likes"] += stats.get("likes", 0)
                            users[uid]["twitter_views"] += stats.get("views", 0)
                            users[uid]["twitter_replies"] += stats.get("replies", 0)
                            if stats.get("twitter_handle"): users[uid]["twitter_handle"] = f"@{stats['twitter_handle']}"
                    
                    fetched_count += 1
                    if fetched_count % 10 == 0: log(f"⏳ Обработано {fetched_count}/{len(links_to_fetch)} новых твитов")
                
                # ⏱️ ЗАДЕРЖКА 0.5 - 1.0 сек (Оптимально)
                await asyncio.sleep(random.uniform(0.5, 1.0))

        # ШАГ 3: Сообщения Discord
        log(">>> ШАГ 3: Подсчет сообщений...")
        for tid in THREAD_IDS:
            msgs = await get_discord_messages(session, tid, DAYS_BACK_CHAT, is_content_thread=False)
            for m in msgs:
                uid = str(m['author']['id'])
                if uid in users:
                    users[uid]["discord_messages"] += 1
                    users[uid]["channels"].add(tid)
                else:
                    exist = old_data.get(uid, {})
                    users[uid] = {
                        "user_id": uid, "username": m['author']['username'], "avatar_url": m['author'].get('avatar'),
                        "discord_messages": 1, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0,
                        "twitter_handle": "@not_linked", "discord_joined_at": exist.get("discord_joined_at"),
                        "discord_roles": exist.get("discord_roles", []), "total_score": 0, "channels": {tid}
                    }

        # ШАГ 4: XP
        log(">>> ШАГ 4: XP...")
        xp_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, DAYS_BACK_XP, is_content_thread=False)
        for xm in xp_msgs:
            if xm.get('mentions'):
                t_uid = str(xm['mentions'][0]['id'])
                if t_uid in users:
                    match = re.search(r'(\d[\d\s,.KM]*)\s*XP', xm['content'].upper())
                    if match:
                        xp_str = match.group(1).replace(' ', '').replace(',', '')
                        try:
                            if 'K' in xp_str: val = int(float(xp_str.replace('K', '')) * 1000)
                            elif 'M' in xp_str: val = int(float(xp_str.replace('M', '')) * 1000000)
                            else: val = int(xp_str)
                            if val > users[t_uid]["total_score"]: users[t_uid]["total_score"] = val
                        except: pass

        # Обогащение (Роли)
        log("🛡️ Обогащение (Роли)...")
        for i, uid in enumerate(users):
            joined, roles = get_discord_member_info(uid, DISCORD_TOKEN)
            if joined: users[uid]["discord_joined_at"] = joined
            if roles: users[uid]["discord_roles"] = roles
            if i % 50 == 0: log(f"📋 Роли: {i}/{len(users)}")
            time.sleep(0.1)

    # СОХРАНЕНИЕ
    log("\n📊 Сохранение...")
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for uid, info in users.items():
        if info["total_score"] == 0: info["total_score"] = info["discord_messages"] * 10
        old_entry = old_data.get(uid, {})
        payload.append({
            "user_id": uid, "username": info["username"],
            "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{info['avatar_url']}.png" if info.get('avatar_url') else None,
            "twitter_handle": info["twitter_handle"], "total_score": int(info["total_score"]),
            "twitter_likes": int(info["twitter_likes"]), "twitter_views": int(info["twitter_views"]),
            "twitter_replies": int(info["twitter_replies"]), "discord_messages": int(info["discord_messages"]),
            "channels_count": len(info["channels"]), "discord_roles": info["discord_roles"],
            "discord_joined_at": info["discord_joined_at"],
            "prev_total_score": old_entry.get("total_score", 0), "prev_discord_messages": old_entry.get("discord_messages", 0),
            "updated_at": now
        })

    if payload:
        for i in range(0, len(payload), 50):
            try: supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
            except Exception as e: log(f"❌ Ошибка: {e}")
        log("🎉 ГОТОВО!")

if __name__ == "__main__":
    asyncio.run(main())
