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

GUILD_ID = "1389273374748049439"  # Твой сервер
CONTENT_THREAD_ID = "1389273374748049439"
XP_BOT_THREAD_ID = "1351492950768619552"

# Все чаты для сбора сообщений
THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1367864741548261416", "1465733325149835295", "1371110511919497226", 
    "1366338962813222993", "1371904910324404325", "1371413462982594620", 
    "1372149550793490505", "1372149324192153620", "1372149873188536330", 
    "1372242189240897596", "1351488556924932128"
]

DAYS_BACK_CONTENT = 30  # Сколько дней собирать ссылки на твиты
DAYS_BACK_CHAT = 7      # Сколько дней считать сообщения
DAYS_BACK_XP = 7        # Сколько дней собирать XP

# ✅ ПОЛНЫЙ User-Agent (как в старом коде)
HEADERS = {
    'Authorization': DISCORD_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_discord_member_info(user_id, token):
    """Получает дату вступления и роли (как в старом коде)"""
    url = f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{user_id}"
    headers = {"Authorization": token}
    try:
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data.get('joined_at'), data.get('roles', [])
    except:
        pass
    return None, []

async def fetch_tweet_stats(session, tweet_url, api_key):
    """Запрос статистики твита с умной обработкой ошибок"""
    id_match = re.search(r"status/(\d+)", tweet_url)
    if not id_match or not api_key:
        return None
    
    tweet_id = id_match.group(1)
    api_url = f"https://api.socialdata.tools/twitter/tweets/{tweet_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        async with session.get(api_url, headers=headers, timeout=10) as resp:
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
                log(f"⚠️ 403 (нет доступа): {tweet_id}")
                return None
            elif resp.status == 404:
                log(f"⚠️ 404 (удалён): {tweet_id}")
                return None
            elif resp.status == 429:
                wait = int(resp.headers.get('Retry-After', 10))
                log(f"⏳ Rate limit, жду {wait}s...")
                await asyncio.sleep(wait)
                return await fetch_tweet_stats(session, tweet_url, api_key)  # Повтор
    except Exception as e:
        log(f"❌ Ошибка запроса {tweet_id}: {e}")
        return None
    
    return None

async def get_discord_messages(session, thread_id, days, is_content_thread=False):
    """Сбор сообщений с рандомной задержкой"""
    target_date = datetime.now(timezone.utc) - timedelta(days=days)
    messages = []
    last_id = None
    
    log(f"📡 Сканирование {thread_id}...")
    
    while True:
        url = f"https://discord.com/api/v10/channels/{thread_id}/messages?limit=100"
        if last_id:
            url += f"&before={last_id}"
        
        try:
            async with session.get(url, headers=HEADERS) as resp:
                if resp.status == 429:
                    wait = (await resp.json()).get('retry_after', 5)
                    log(f"⏳ Discord rate limit, жду {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                
                if resp.status != 200:
                    log(f"⚠️ Discord API ответ {resp.status}")
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
                
                # 🎲 РАНДОМНАЯ ЗАДЕРЖКА (как ты просил)
                if is_content_thread:
                    await asyncio.sleep(random.uniform(0.5, 1.0))  # Медленнее для контента
                else:
                    await asyncio.sleep(random.uniform(0.4, 0.7))  # Быстрее для чатов
                    
        except Exception as e:
            log(f"❌ Ошибка {thread_id}: {e}")
            break
    
    return messages

async def main():
    log("🚀 Запуск коллектора (Гибридная версия: Старый + Новый)...")
    
    # 🔍 Загрузка старых данных
    old_res = supabase.table("leaderboard_stats").select("*").execute()
    old_data = {item['user_id']: item for item in old_res.data} if old_res.data else {}
    
    users = {}
    tweet_list = []
    twitter_pattern = r'https?://(?:www\.|mobile\.)?(?:x\.com|twitter\.com)/[a-zA-Z0-9_]+/status/\d+'
    
    async with aiohttp.ClientSession() as session:
        # ШАГ 1: Сбор ссылок из Discord (30 дней)
        log(">>> ШАГ 1: Сбор ссылок из Discord...")
        content_msgs = await get_discord_messages(session, CONTENT_THREAD_ID, DAYS_BACK_CONTENT, is_content_thread=True)
        
        for m in content_msgs:
            uid = str(m['author']['id'])
            if uid not in users:
                exist = old_data.get(uid, {})
                users[uid] = {
                    "user_id": uid,
                    "username": m['author']['username'],
                    "avatar_url": m['author'].get('avatar'),
                    "discord_messages": 0,
                    "twitter_posts": 0,
                    "twitter_likes": 0,
                    "twitter_views": 0,
                    "twitter_replies": 0,
                    "twitter_handle": exist.get("twitter_handle", "@not_linked"),
                    "discord_joined_at": exist.get("discord_joined_at"),
                    "discord_roles": exist.get("discord_roles", []),
                    "total_score": 0,
                    "channels": set()
                }
            
            # Сбор ссылок на твиты
            links = re.findall(twitter_pattern, m['content'], re.IGNORECASE)
            for l in links:
                clean_link = l.split('?')[0].lower()
                tweet_list.append((uid, clean_link))
        
        log(f"✅ Найдено {len(tweet_list)} ссылок на твиты")
        
        # ШАГ 2: Запрос Twitter API (БЕЗ КЕША!)
        if tweet_list and SOCIALDATA_API_KEY:
            log(">>> ШАГ 2: Запрос Twitter API...")
            
            # Убираем дубликаты
            unique_tweets = {}
            for uid, link in tweet_list:
                if link not in unique_tweets:
                    unique_tweets[link] = []
                unique_tweets[link].append(uid)
            
            processed = 0
            for link, uids in unique_tweets.items():
                stats = await fetch_tweet_stats(session, link, SOCIALDATA_API_KEY)
                
                if stats:
                    for uid in uids:
                        if uid in users:
                            users[uid]["twitter_posts"] += 1
                            users[uid]["twitter_likes"] += stats["likes"]
                            users[uid]["twitter_views"] += stats["views"]
                            users[uid]["twitter_replies"] += stats["replies"]
                            if stats["twitter_handle"]:
                                users[uid]["twitter_handle"] = f"@{stats['twitter_handle']}"
                
                processed += 1
                if processed % 10 == 0:
                    log(f"⏳ Прогресс: {processed}/{len(unique_tweets)}")
                
                # Задержка между запросами
                await asyncio.sleep(0.15)
        
        # ШАГ 3: Подсчет сообщений в чатах (7 дней)
        log(">>> ШАГ 3: Подсчет сообщений...")
        for tid in THREAD_IDS:
            msgs = await get_discord_messages(session, tid, DAYS_BACK_CHAT, is_content_thread=False)
            for m in msgs:
                uid = str(m['author']['id'])
                if uid in users:
                    users[uid]["discord_messages"] += 1
                    users[uid]["channels"].add(tid)
                else:
                    # Новый пользователь только в чатах
                    exist = old_data.get(uid, {})
                    users[uid] = {
                        "user_id": uid,
                        "username": m['author']['username'],
                        "avatar_url": m['author'].get('avatar'),
                        "discord_messages": 1,
                        "twitter_posts": 0,
                        "twitter_likes": 0,
                        "twitter_views": 0,
                        "twitter_replies": 0,
                        "twitter_handle": "@not_linked",
                        "discord_joined_at": exist.get("discord_joined_at"),
                        "discord_roles": exist.get("discord_roles", []),
                        "total_score": 0,
                        "channels": {tid}
                    }
        
        # ШАГ 4: XP Бот (7 дней)
        log(">>> ШАГ 4: Синхронизация XP...")
        xp_msgs = await get_discord_messages(session, XP_BOT_THREAD_ID, DAYS_BACK_XP, is_content_thread=False)
        for xm in xp_msgs:
            if xm.get('mentions'):
                t_uid = str(xm['mentions'][0]['id'])
                if t_uid in users:
                    match = re.search(r'(\d[\d\s,.KM]*)\s*XP', xm['content'].upper())
                    if match:
                        xp_str = match.group(1).replace(' ', '').replace(',', '')
                        try:
                            if 'K' in xp_str:
                                val = int(float(xp_str.replace('K', '')) * 1000)
                            elif 'M' in xp_str:
                                val = int(float(xp_str.replace('M', '')) * 1000000)
                            else:
                                val = int(xp_str)
                            
                            if val > users[t_uid]["total_score"]:
                                users[t_uid]["total_score"] = val
                                log(f"⭐ XP для {t_uid}: {val}")
                        except:
                            pass
        
        # 🔧 Обогащение данными (Роли + Дата) - КАК В СТАРОМ КОДЕ
        log("🛡️ Обогащение данными (Роли + Дата)...")
        for i, uid in enumerate(users):
            joined, roles = get_discord_member_info(uid, DISCORD_TOKEN)
            if joined:
                users[uid]["discord_joined_at"] = joined
            if roles:
                users[uid]["discord_roles"] = roles
            
            if i % 50 == 0:
                log(f"📋 Обработано {i}/{len(users)} пользователей")
            
            time.sleep(0.1)  # Чтобы не словить 429 от Discord API

    # СОХРАНЕНИЕ
    log("\n📊 Подготовка данных...")
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    
    for uid, info in users.items():
        # Расчет XP
        if info["total_score"] == 0:
            info["total_score"] = info["discord_messages"] * 10
        
        # prev_ поля для отслеживания дельты
        old_entry = old_data.get(uid, {})
        
        payload.append({
            "user_id": uid,
            "username": info["username"],
            "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{info['avatar_url']}.png" if info.get('avatar_url') else None,
            "twitter_handle": info["twitter_handle"],
            "total_score": int(info["total_score"]),
            "twitter_likes": int(info["twitter_likes"]),
            "twitter_views": int(info["twitter_views"]),
            "twitter_replies": int(info["twitter_replies"]),
            "discord_messages": int(info["discord_messages"]),
            "channels_count": len(info["channels"]),
            "discord_roles": info["discord_roles"],
            "discord_joined_at": info["discord_joined_at"],
            "prev_total_score": old_entry.get("total_score", 0),
            "prev_discord_messages": old_entry.get("discord_messages", 0),
            "updated_at": now
        })
    
    if payload:
        log(f">>> СИНХРОНИЗАЦИЯ: {len(payload)} строк")
        for i in range(0, len(payload), 50):
            try:
                supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
                log(f"💾 Пачка {i//50 + 1}/{(len(payload)+49)//50} сохранена")
            except Exception as e:
                log(f"❌ Ошибка пачки: {e}")
        log("🎉 ГОТОВО! Лидерборд обновлен.")
    else:
        log("⚠️ Нет данных для сохранения")

if __name__ == "__main__":
    asyncio.run(main())
