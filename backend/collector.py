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

# 🔧 Глобальная статистика для логирования
api_stats = {
    "success": 0,
    "403_errors": 0,
    "404_errors": 0,
    "cached": 0,
    "total": 0
}

async def get_twitter_stats(session, tweet_url, max_retries=2):
    """Запрос статистики из API SocialData с обработкой ошибок и ретраями"""
    
    # 🔧 Нормализация URL — убираем /i/ если есть
    clean_url = tweet_url.replace('/x.com/i/status/', '/x.com/status/') \
                         .replace('twitter.com/i/status/', 'twitter.com/status/')
    
    tweet_id_match = re.search(r"status/(\d+)", clean_url)
    if not tweet_id_match or not SOCIALDATA_API_KEY: 
        log(f"⚠️ Пропущено: нет tweet_id или API ключа для {clean_url}")
        return None
    
    tweet_id = tweet_id_match.group(1)
    url = f"https://api.socialdata.tools/twitter/tweets/{tweet_id}"
    headers = {"Authorization": f"Bearer {SOCIALDATA_API_KEY}", "Accept": "application/json"}
    
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = {
                        "views": data.get("views_count", 0) or 0,
                        "likes": data.get("favorite_count", 0) or 0,
                        "replies": data.get("reply_count", 0) or 0,
                        "author_handle": data.get("user", {}).get("screen_name", "unknown")
                    }
                    api_stats["success"] += 1
                    log(f"✅ API ответ: {result['likes']} likes, {result['views']} views")
                    return result
                    
                elif resp.status == 403:
                    api_stats["403_errors"] += 1
                    if attempt < max_retries - 1:
                        wait_time = 10 * (attempt + 1)  # 10s, 20s
                        log(f"⚠️ 403 Forbidden (попытка {attempt+1}/{max_retries}). Жду {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        log(f"❌ 403 после {max_retries} попыток для {clean_url}. Пропускаем.")
                        return None
                        
                elif resp.status == 404:
                    api_stats["404_errors"] += 1
                    log(f"⚠️ 404 Not Found для {clean_url} (удалён или приватный)")
                    return None
                    
                elif resp.status == 429:  # Rate limit
                    retry_after = int(resp.headers.get('Retry-After', 30))
                    log(f"⏳ Rate limit (429). Жду {retry_after}s...")
                    await asyncio.sleep(retry_after)
                    continue
                    
                else:
                    log(f"⚠️ API ответ {resp.status} для {clean_url}")
                    return None
                    
        except asyncio.TimeoutError:
            log(f"⏱️ Timeout для {clean_url}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                return None
        except Exception as e:
            log(f"❌ Ошибка запроса {clean_url}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                return None
    
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
                log(f"⏳ Rate limit, жду {wait}s...")
                await asyncio.sleep(wait); continue
            if resp.status != 200: 
                log(f"⚠️ Discord API ответ {resp.status}")
                break
            batch = await resp.json()
            if not batch: break
            
            for m in batch:
                m_date = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                if m_date < target_date: return messages
                messages.append(m)
            
            last_id = batch[-1]['id']
            await asyncio.sleep(0.1)
    return messages

async def main():
    global api_stats
    
    log("Запуск коллектора (Версия: Full User-ID Sync)...")
    
    # 🔧 Тест API ключа
    if SOCIALDATA_API_KEY:
        async with aiohttp.ClientSession() as test_session:
            test_url = "https://api.socialdata.tools/twitter/tweets/999999999999999999"
            headers = {"Authorization": f"Bearer {SOCIALDATA_API_KEY}"}
            try:
                async with test_session.get(test_url, headers=headers, timeout=5) as resp:
                    if resp.status == 401:
                        log("❌ SOCIALDATA_API_KEY недействителен!")
                    elif resp.status in [404, 200]:
                        log("✅ SOCIALDATA_API_KEY работает")
                    else:
                        log(f"🤔 SOCIALDATA API ответ: {resp.status}")
            except Exception as e:
                log(f"⚠️ Не удалось протестировать API ключ: {e}")
    
    users = {}
    user_tweets = {} 
    
    # Загружаем старые данные
    old_res = supabase.table("leaderboard_stats").select("*").execute()
    old_data = {item['user_id']: item for item in old_res.data} if old_res.data else {}

    # Загружаем текущий кеш твитов
    cache_res = supabase.table("tweet_cache").select("*").execute()
    tweet_cache = {item['tweet_url']: item for item in cache_res.data} if cache_res.data else {}
    log(f"📦 Загружено {len(tweet_cache)} записей из tweet_cache")

    async with aiohttp.ClientSession() as session:
        # ШАГ 1: Сбор ссылок
        log(">>> ШАГ 1: Сбор ссылок из Discord...")
        content_msgs = await get_discord_messages(session, CONTENT_THREAD_ID, 30)
        twitter_pattern = r'https?://(?:www\.|mobile\.)?(?:x\.com|twitter\.com)/[a-zA-Z0-9_]+/status/\d+'
        
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
                clean_links = [link.split('?')[0].lower() for link in found]
                if uid not in user_tweets: user_tweets[uid] = []
                user_tweets[uid].extend(clean_links)

        # 🔧 ШАГ 2: Статистика твитов (ИСПРАВЛЕННЫЙ)
        if user_tweets:
            total_links = sum(len(v) for v in user_tweets.values())
            log(f">>> ШАГ 2: Статистика твитов ({len(user_tweets)} авторов, {total_links} ссылок)...")
            
            for uid, links in user_tweets.items():
                unique_links = list(set(links))[:10] 
                users[uid]["twitter_posts"] = len(unique_links)
                
                for link in unique_links:
                    api_stats["total"] += 1
                    stats = tweet_cache.get(link)
                    
                    # Если данных нет — обновляем через API
                    if not stats:
                        # ✅ ИСПРАВЛЕНО: используем api_result вместо api_stats
                        api_result = await get_twitter_stats(session, link)
                        if api_result:
                            stats = {
                                'tweet_url': link,
                                'views': api_result.get('views', 0),
                                'likes': api_result.get('likes', 0), 
                                'replies': api_result.get('replies', 0),
                                'author_handle': api_result.get('author_handle', 'unknown'),
                                'updated_at': datetime.now(timezone.utc).isoformat()
                            }
                            try:
                                supabase.table("tweet_cache").upsert(stats).execute()
                                tweet_cache[link] = stats
                                log(f"💾 Сохранено в кеш: {link}")
                            except Exception as e:
                                log(f"⚠️ Не удалось сохранить в кеш: {e}")
                        await asyncio.sleep(0.2)  # Немного дольше между запросами
                    else:
                        api_stats["cached"] += 1

                    # ✅ Начисляем баллы ВСЕГДА, если есть статистика
                    if stats:
                        users[uid]["likes"] += stats.get("likes", 0)
                        users[uid]["views"] += stats.get("views", 0)
                        users[uid]["replies"] += stats.get("replies", 0)
                        users[uid]["total_score"] += (stats.get("likes", 0) * 2) + (stats.get("replies", 0) * 5)
                        
                        if stats.get("author_handle") and stats["author_handle"] != "unknown":
                            users[uid]["twitter_handle"] = f"@{stats['author_handle']}"
                        
                        log(f"📊 +{stats.get('likes',0)} likes, +{stats.get('views',0)} views для {uid}")

        # ШАГ 3: Discord чаты
        log(">>> ШАГ 3: Подсчет сообщений в чатах...")
        for tid in THREAD_IDS:
            msgs = await get_discord_messages(session, tid, 7)
            for m in msgs:
                uid = str(m['author']['id'])
                if uid not in users:
                    exist = old_data.get(uid, {})
                    users[uid] = {
                        "user_id": uid, "username": m['author']['username'], 
                        "discord_messages": 0, "twitter_posts": 0, "total_score": 0, 
                        "likes": 0, "views": 0, "replies": 0, 
                        "twitter_handle": "@not_linked", 
                        "discord_roles": exist.get("discord_roles", []), 
                        "discord_joined_at": exist.get("discord_joined_at")
                    }
                users[uid]["discord_messages"] += 1

        # ШАГ 4: XP Бот
        log(">>> ШАГ 4: Синхронизация XP...")
        xp_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, 7)
        for xm in xp_msgs:
            if xm.get('mentions'):
                t_uid = str(xm['mentions'][0]['id'])
                if t_uid in users:
                    match = re.search(r'(\d[\d\s,.]*)\s*XP', xm['content'])
                    if match:
                        val = int(match.group(1).replace(' ', '').replace(',', '').replace('.', ''))
                        if val > users[t_uid]["total_score"]: 
                            users[t_uid]["total_score"] = val
                            log(f"⭐ Обновлён XP для {t_uid}: {val}")

    # 📊 Логируем статистику API
    log(f"\n📊 СТАТИСТИКА TWITTER API:")
    log(f"   ✅ Успешно: {api_stats['success']}")
    log(f"   📦 Из кеша: {api_stats['cached']}")
    log(f"   ⚠️ 403 ошибки: {api_stats['403_errors']}")
    log(f"   ❌ 404 ошибки: {api_stats['404_errors']}")
    log(f"   🔢 Всего запросов: {api_stats['total']}")

    # СОХРАНЕНИЕ
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for uid, info in users.items():
        if info["total_score"] == 0: 
            info["total_score"] = info["discord_messages"] * 10
            
        payload.append({
            "user_id": uid, "username": info["username"], "twitter_handle": info["twitter_handle"],
            "total_score": int(info["total_score"]), "twitter_likes": int(info.get("likes", 0)),
            "twitter_views": int(info.get("views", 0)), "twitter_replies": int(info.get("replies", 0)),
            "discord_messages": int(info.get("discord_messages", 0)), "discord_roles": info.get("discord_roles", []),
            "discord_joined_at": info.get("discord_joined_at"), "updated_at": now
        })

    if payload:
        log(f">>> СИНХРОНИЗАЦИЯ: {len(payload)} строк")
        for i in range(0, len(payload), 50):
            try:
                result = supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
                log(f"💾 Пачка {i//50 + 1}/{(len(payload)+49)//50} сохранена")
            except Exception as e:
                log(f"❌ Ошибка пачки {i//50 + 1}: {e}")
        log("🎉 ГОТОВО! Лидерборд обновлен.")
    else:
        log("⚠️ Нет данных для сохранения")

if __name__ == "__main__":
    asyncio.run(main())
