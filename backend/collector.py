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

# ✅ ПОЛНЫЙ НАБОР ЗАГОЛОВКОВ (маскировка под Chrome)
HEADERS = {
    'Authorization': DISCORD_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Origin': 'https://discord.com',
    'Referer': 'https://discord.com/channels/@me',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'DNT': '1',
    'Connection': 'keep-alive'
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_discord_member_info(user_id, token):
    """Получает дату вступления и роли — с рабочими заголовками и логами"""
    url = f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{user_id}"
    
    # ✅ Берём проверенные браузерные заголовки и подставляем личный токен
    headers = HEADERS.copy()
    headers['Authorization'] = token
    
    try:
        r = requests.get(url, headers=headers, timeout=5)
        
        if r.status_code == 200:
            data = r.json()
            joined = data.get('joined_at')
            roles = data.get('roles', [])
            
            if joined:
                return joined, roles
            else:
                # ⚠️ Discord вернул 200, но без joined_at — это ограничение API для пользовательских токенов
                log(f"⚠️ {user_id}: 200 OK, но joined_at отсутствует (ограничение API)")
                return None, roles  # Возвращаем роли, если они есть
        
        elif r.status_code == 404:
            log(f"❓ {user_id}: не найден на сервере (возможно, вышел)")
        elif r.status_code == 403:
            log(f"🚫 {user_id}: 403 — токен не имеет прав на чтение участников")
        elif r.status_code == 401:
            log(f"🔑 {user_id}: 401 — невалидный токен")
        else:
            log(f"❌ {user_id}: API {r.status_code} — {r.text[:200]}")
            
    except Exception as e:
        log(f"💥 {user_id}: Ошибка сети — {e}")
        
    return None, []

async def fetch_tweet_stats(session, tweet_url, api_key):
    """⚡ БЫСТРАЯ ВЕРСИЯ: Мгновенно пропускает 403, ждёт только 429"""
    id_match = re.search(r"status/(\d+)", tweet_url)
    if not id_match or not api_key: 
        return None
    
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
            elif resp.status == 403:
                log(f"⛔ 403 (приватный/удалён): {tweet_id}")
                return None
            elif resp.status == 404:
                return None
            elif resp.status == 429:
                wait = int(resp.headers.get('Retry-After', 10))
                log(f"⏳ Лимит API, жду {wait}s...")
                await asyncio.sleep(wait)
    except Exception as e:
        log(f"❌ Ошибка сети {tweet_id}: {e}")
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
                    log(f"⏳ Discord rate limit, жду {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                if resp.status == 403:
                    log(f"⛔ НЕТ ДОСТУПА к каналу {thread_id}")
                    return messages
                if resp.status != 200: 
                    break
                
                batch = await resp.json()
                if not batch: 
                    break
                
                for m in batch:
                    m_date = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                    if m_date < target_date: 
                        return messages
                    messages.append(m)
                
                last_id = batch[-1]['id']
                
                # Рандомная задержка
                if is_content_thread: 
                    await asyncio.sleep(random.uniform(0.1, 0.2))
                else: 
                    await asyncio.sleep(random.uniform(0.4, 0.7))
                    
        except Exception as e:
            log(f"❌ Ошибка при сканировании {thread_id}: {e}")
            break
    return messages

async def main():
    log("🚀 Запуск (Финальная версия + Улучшенный get_discord_member_info)...")
    
    # === ФИКС 1: Пагинация для загрузки всех пользователей (больше 1000) ===
    old_data = {}
    offset = 0
    while True:
        res = supabase.table("leaderboard_stats").select("*").range(offset, offset + 999).execute()
        if not res.data: 
            break
        for item in res.data:
            old_data[item['user_id']] = item
        offset += 1000
    log(f"📥 Загружено {len(old_data)} пользователей из базы")

    try:
        cache_res = supabase.table("tweet_cache").select("*").execute()
        tweet_cache = {item['tweet_url']: item for item in cache_res.data} if cache_res.data else {}
        log(f"📦 Кэш твитов: {len(tweet_cache)} записей")
    except Exception as e:
        log(f"⚠️ Ошибка загрузки кэша: {e}")
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

        # === ФИКС 2: Группировка твитов для избежания дубликатов ===
        log(">>> ШАГ 2: Twitter API + Кэш...")
        
        # Группируем: { user_id: set(links) }
        unique_user_tweets = {}
        for uid, link in tweet_list:
            if uid not in unique_user_tweets:
                unique_user_tweets[uid] = set()
            unique_user_tweets[uid].add(link)
        
        log(f"🔍 Уникальных пользователей с твитами: {len(unique_user_tweets)}")
        
        # Обрабатываем только уникальные твиты на пользователя
        total_fetched = 0
        for uid, links in unique_user_tweets.items():
            for link in links:
                stats = None
                
                # Проверяем кэш
                if link in tweet_cache:
                    stats = tweet_cache[link]
                elif SOCIALDATA_API_KEY:
                    stats = await fetch_tweet_stats(session, link, SOCIALDATA_API_KEY)
                    if stats:
                        try:
                            supabase.table("tweet_cache").upsert({
                                'tweet_url': link, 
                                'likes': stats['likes'], 
                                'views': stats['views'],
                                'replies': stats['replies'], 
                                'twitter_handle': stats.get('twitter_handle'),
                                'updated_at': datetime.now(timezone.utc).isoformat()
                            }).execute()
                            tweet_cache[link] = stats
                        except Exception as e:
                            log(f"⚠️ Ошибка сохранения в кэш: {e}")
                    
                    # Задержка между запросами к API
                    await asyncio.sleep(random.uniform(0.5, 1.0))
                
                # Применяем статистику к пользователю
                if stats and uid in users:
                    users[uid]["twitter_posts"] += 1
                    users[uid]["twitter_likes"] += stats.get("likes", 0)
                    users[uid]["twitter_views"] += stats.get("views", 0)
                    users[uid]["twitter_replies"] += stats.get("replies", 0)
                    if stats.get("twitter_handle"): 
                        users[uid]["twitter_handle"] = f"@{stats['twitter_handle']}"
                    
                    total_fetched += 1
        
        log(f"✅ Обработано {total_fetched} твитов")

        # ШАГ 3: Сообщения
        log(">>> ШАГ 3: Сообщения...")
        for tid in THREAD_IDS:
            msgs = await get_discord_messages(session, tid, DAYS_BACK_CHAT, is_content_thread=False)
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

        # ШАГ 4: XP
        log(">>> ШАГ 4: XP...")
        xp_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, DAYS_BACK_XP, is_content_thread=False)
        xp_found_count = 0
        
        for xm in xp_msgs:
            if not xm['author'].get('bot', False): 
                continue
            
            # Собираем весь текст из сообщения и embed'ов
            text = xm.get('content', '')
            for emb in xm.get('embeds', []):
                if emb.get('description'): 
                    text += "\n" + emb['description']
                if emb.get('fields'):
                    for f in emb['fields']:
                        if f.get('value'): 
                            text += "\n" + f['value']
            
            msg_upper = text.upper()

            # Ищем каждого пользователя в сообщении
            for uid, info in users.items():
                username = info.get('username', '').upper()
                if not username or len(username) < 3: 
                    continue
                
                if username in msg_upper:
                    # \b гарантирует точное совпадение ника
                    pattern = r'\b' + re.escape(username) + r'\b.*?(\d[\d\s,]*[KM]?)\s*XP'
                    match = re.search(pattern, msg_upper, re.IGNORECASE | re.DOTALL)
                    
                    if match:
                        xp_str = match.group(1).replace(' ', '').replace(',', '').upper()
                        try:
                            if 'K' in xp_str: 
                                val = int(float(xp_str.replace('K', '')) * 1000)
                            elif 'M' in xp_str: 
                                val = int(float(xp_str.replace('M', '')) * 1000000)
                            else: 
                                val = int(xp_str)
                            
                            if val > info["total_score"]:
                                info["total_score"] = val
                                xp_found_count += 1
                        except: 
                            pass
        
        log(f"✅ Обновлен XP для {xp_found_count} пользователей")

        # Обогащение (Роли + Дата)
        log("🛡️ Обогащение данными (Роли + Дата)...")
        success_count = 0
        for i, uid in enumerate(users):
            joined, roles = get_discord_member_info(uid, DISCORD_TOKEN)
            if joined: 
                users[uid]["discord_joined_at"] = joined
                success_count += 1
            if roles:
                # Фильтруем только приоритетные роли
                p_roles = [r for r in roles if r in PRIORITY_ROLES]
                if p_roles: 
                    users[uid]["discord_roles"] = p_roles
            
            if i % 50 == 0: 
                log(f"📋 Обработано {i}/{len(users)} пользователей")
            
            await asyncio.sleep(0.05)
        
        log(f"✅ Даты вступления получены для {success_count}/{len(users)} пользователей")

    # 🔐 СОХРАНЕНИЕ (БЕЗОПАСНОЕ: не затирает старые данные)
    log("\n📊 Сохранение...")
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    
    for uid, info in users.items():
        old = old_data.get(uid, {})
        
        # Если total_score 0, берём из сообщений
        if info["total_score"] == 0:
            info["total_score"] = info["discord_messages"] * 10
        
        payload.append({
            "user_id": uid,
            "username": info["username"],
            # Аватар: новый или старый
            "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{info['avatar_url']}.png" if info.get('avatar_url') else old.get("avatar_url"),
            
            # Твиттер-хендл: обновляем только если нашли реальный
            "twitter_handle": info["twitter_handle"] if info["twitter_handle"] != "@not_linked" else old.get("twitter_handle", "@not_linked"),
            
            "total_score": int(info["total_score"]),
            
            # 🛡️ ВАЖНО: Сохраняем МАКСИМУМ из нового и старого
            "twitter_likes": max(int(info["twitter_likes"]), old.get("twitter_likes", 0)),
            "twitter_views": max(int(info["twitter_views"]), old.get("twitter_views", 0)),
            "twitter_replies": max(int(info["twitter_replies"]), old.get("twitter_replies", 0)),
            "twitter_posts": max(int(info["twitter_posts"]), old.get("twitter_posts", 0)),
            
            "discord_messages": int(info["discord_messages"]),
            
            # ✅ ФИКС 3: Просто берём актуальное число каналов
            "channels_count": len(info["channels"]),
            
            # 🛡️ Сохраняем старые роли и дату, если новые не пришли
            "discord_roles": info["discord_roles"] if info["discord_roles"] else old.get("discord_roles", []),
            "discord_joined_at": info["discord_joined_at"] if info["discord_joined_at"] else old.get("discord_joined_at"),
            
            "prev_total_score": old.get("total_score", 0),
            "prev_discord_messages": old.get("discord_messages", 0),
            "updated_at": now
        })

    if payload:
        log(f">>> СИНХРОНИЗАЦИЯ: {len(payload)} строк")
        for i in range(0, len(payload), 50):
            try: 
                supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
            except Exception as e: 
                log(f"❌ Ошибка пачки {i}: {e}")
        log("🎉 ГОТОВО!")

if __name__ == "__main__":
    asyncio.run(main())
