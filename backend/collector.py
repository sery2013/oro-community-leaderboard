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

async def get_twitter_stats(session, tweet_url):
    """Запрос статистики из API SocialData"""
    tweet_id_match = re.search(r"status/(\d+)", tweet_url)
    if not tweet_id_match: return None
    
    tweet_id = tweet_id_match.group(1)
    url = f"https://api.socialdata.tools/twitter/tweets/{tweet_id}"
    headers = {"Authorization": f"Bearer {SOCIALDATA_API_KEY}", "Accept": "application/json"}
    
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                # Извлекаем данные и ник автора для кеша
                return {
                    "views": data.get("views_count", 0) or 0,
                    "likes": data.get("favorite_count", 0) or 0,
                    "replies": data.get("reply_count", 0) or 0,
                    "author_handle": data.get("user", {}).get("screen_name", "unknown")
                }
            elif resp.status == 429:
                log(f"   [!] Лимит SocialData API. Пропуск твита {tweet_id}")
    except Exception as e:
        log(f"Ошибка SocialData для {tweet_id}: {e}")
    return None

async def get_discord_messages(session, thread_id, days):
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
                wait = (await resp.json()).get('retry_after', 5)
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
            if total_fetched % 500 == 0: log(f"   [+] Пройдено {total_fetched}...")
            await asyncio.sleep(random.uniform(0.5, 1.0))
            
    return messages

async def main():
    log("Запуск коллектора (Версия: Full Stats + Fixed Cache)...")
    users = {}
    user_tweets = {} 
    
    # 1. Загружаем старые данные и КЕШ твитов
    old_res = supabase.table("leaderboard_stats").select("*").execute()
    old_data = {item['user_id']: item for item in old_res.data} if old_res.data else {}

    cache_res = supabase.table("tweet_cache").select("*").execute()
    tweet_cache = {item['tweet_url']: item for item in cache_res.data} if cache_res.data else {}

    async with aiohttp.ClientSession() as session:
        # ШАГ 1: TWITTER (30 ДНЕЙ)
        log(">>> ШАГ 1: Сбор контента (Твиттер-ссылки)...")
        content_msgs = await get_discord_messages(session, CONTENT_THREAD_ID, 30)
        
        twitter_pattern = r'https?://(?:www\.|mobile\.)?(?:x\.com|twitter\.com)/[a-zA-Z0-9_]+/status/\d+'
        
        for m in content_msgs:
            uid = m['author']['id']
            if uid not in users:
                exist = old_data.get(uid, {})
                users[uid] = {
                    "user_id": uid, "username": m['author']['username'],
                    "discord_messages": 0, "twitter_posts": 0, "total_score": 0,
                    "likes": 0, "views": 0, "replies": 0, # Колонки для сайта
                    "twitter_handle": exist.get("twitter_handle", "@not_linked")
                }
            
            found = re.findall(twitter_pattern, m['content'], re.IGNORECASE)
            if found:
                clean_links = [link.split('?')[0].lower() for link in found]
                if uid not in user_tweets: user_tweets[uid] = []
                user_tweets[uid].extend(clean_links)

        # ШАГ 2: ОБРАБОТКА СТАТИСТИКИ (API + КЕШ)
        if user_tweets:
            log(f">>> ШАГ 2: Статистика твитов ({len(user_tweets)} авторов)...")
            for uid, links in user_tweets.items():
                unique_links = list(set(links))[:10] 
                users[uid]["twitter_posts"] = len(unique_links)
                
                for link in unique_links:
                    stats = None
                    if link in tweet_cache:
                        stats = tweet_cache[link]
                    else:
                        log(f"   [API] Запрос: {link}")
                        stats = await get_twitter_stats(session, link)
                        if stats:
                            stats['tweet_url'] = link
                            stats['updated_at'] = datetime.now(timezone.utc).isoformat()
                            # Записываем в кеш с правильным author_handle
                            supabase.table("tweet_cache").upsert(stats).execute()
                            tweet_cache[link] = stats
                            await asyncio.sleep(random.uniform(0.7, 1.5))

                    if stats:
                        # Суммируем для основной таблицы
                        users[uid]["likes"] += stats.get("likes", 0)
                        users[uid]["views"] += stats.get("views", 0)
                        users[uid]["replies"] += stats.get("replies", 0)
                        # Расчет очков
                        users[uid]["total_score"] += (stats.get("likes", 0) * 2) + (stats.get("replies", 0) * 5)
                        # Обновляем ник из твита
                        if stats.get("author_handle"):
                            users[uid]["twitter_handle"] = f"@{stats['author_handle']}"

        # ШАГ 3: СООБЩЕНИЯ В ЧАТАХ (7 ДНЕЙ)
        log(">>> ШАГ 3: Сбор сообщений в чатах...")
        for tid in THREAD_IDS:
            msgs = await get_discord_messages(session, tid, 7)
            for m in msgs:
                uid = m['author']['id']
                if uid not in users:
                    exist = old_data.get(uid, {})
                    users[uid] = {
                        "user_id": uid, "username": m['author']['username'],
                        "discord_messages": 0, "twitter_posts": 0, "total_score": 0,
                        "likes": 0, "views": 0, "replies": 0,
                        "twitter_handle": "@not_linked"
                    }
                users[uid]["discord_messages"] += 1

        # ШАГ 4: АНАЛИЗ XP БОТА
        log(">>> ШАГ 4: Синхронизация XP бота...")
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

    # === ФИНАЛЬНАЯ ПАКОВКА ===
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    
    for uid, info in users.items():
        # Базовые очки за сообщения если нет твиттера и бота
        if info["total_score"] == 0:
            info["total_score"] = info["discord_messages"] * 10
            
        info["updated_at"] = now
        payload.append(info)

    if payload:
        log(f">>> СИНХРОНИЗАЦИЯ: {len(payload)} пользователей")
        # Пакетная загрузка по 50 записей
        for i in range(0, len(payload), 50):
            try:
                supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
            except Exception as e:
                log(f"Ошибка при сохранении пачки: {e}")
        log("Все данные успешно обновлены! База чиста.")

if __name__ == "__main__":
    asyncio.run(main())
