import os, asyncio, aiohttp, requests, re, time, sys
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(msg)
    sys.stdout.flush()

# ✅ НАСТРОЙКИ СЕРВЕРА ORO
GUILD_ID = "1349045850331938826"
CONTENT_THREAD = "1389273374748049439"   # Ветка для твитов
XP_SOURCE_THREAD = "1351492950768619552" # Ветка для парсинга XP

# Все ветки для учета сообщений (добавлена новая 1351487907042431027)
THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1351492950768619552", "1367864741548261416", "1371904712001065000", 
    "1465733325149835295", "1371110511919497226", "1366338962813222993", 
    "1371904910324404325", "1371413462982594620", "1372149550793490505", 
    "1372149324192153620", "1372149873188536330", "1372242189240897596", 
    "1351488556924932128", "1389273374748049439"
]

DAYS_BACK = 2
TARGET_DATE = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

def parse_xp_value(xp_str):
    try:
        xp_str = xp_str.upper().replace(' ', '').replace(',', '')
        multiplier = 1000 if 'K' in xp_str else 1000000 if 'M' in xp_str else 1
        num = xp_str.replace('K', '').replace('M', '')
        return int(float(num) * multiplier)
    except: return 0

async def fetch_tweet(session, tweet_info, api_key):
    uid, url = tweet_info
    # Улучшенная регулярка для захвата ID твита без лишних параметров
    t_id = re.search(r"status/(\d+)", url)
    if not t_id: return uid, 0, 0, 0, "Unknown", None
    
    api_url = f"https://api.socialdata.tools/twitter/tweets/{t_id.group(1)}"
    try:
        async with session.get(api_url, headers={"Authorization": f"Bearer {api_key}"}, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                # Обработка разных форматов ответа API SocialData
                u = data.get('user') or data.get('author') or {}
                return (uid, 
                        data.get('favorite_count', 0), 
                        data.get('views_count', 0), 
                        data.get('reply_count', 0), 
                        "Found", 
                        u.get('screen_name') or u.get('username'))
    except: pass
    return uid, 0, 0, 0, "Error", None

def get_discord_member_info(user_id, token):
    """Запрос ролей и даты вступления для корректного отображения на сайте"""
    url = f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{user_id}"
    headers = {"Authorization": token}
    try:
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data.get('joined_at'), data.get('roles', [])
    except: pass
    return None, []

def get_discord_data():
    token = os.getenv('DISCORD_TOKEN')
    headers = {"Authorization": token}
    user_stats, tweet_list = {}, []
    
    for tid in set(THREAD_IDS):
        log(f"📡 Сбор данных из ветки: {tid}")
        last_id = None
        count = 0
        while True:
            url = f"https://discord.com/api/v10/channels/{tid}/messages?limit=100"
            if last_id: url += f"&before={last_id}"
            
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200: break
            msgs = r.json()
            if not msgs: break
            
            for m in msgs:
                dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                if dt < TARGET_DATE: { last_id := "STOP" }; break
                
                uid = m['author']['id']
                if uid not in user_stats:
                    avatar = m['author'].get('avatar')
                    user_stats[uid] = {
                        "user_id": uid, 
                        "username": m['author']['username'],
                        "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{avatar}.png" if avatar else None,
                        "discord_messages": 0, 
                        "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0,
                        "twitter_handle": "not_linked",
                        "channels": set(), 
                        "total_score": 0,
                        "discord_joined_at": None,
                        "discord_roles": [],
                        "prev_total_score": 0,
                        "prev_discord_messages": 0
                    }
                
                user_stats[uid]["discord_messages"] += 1
                user_stats[uid]["channels"].add(tid)

                # 1. Сбор твитов ТОЛЬКО из ветки контента
                if tid == CONTENT_THREAD:
                    full_text = m.get('content', '')
                    for e in m.get('embeds', []):
                        full_text += f" {e.get('url', '')} {e.get('description', '')}"
                    
                    links = re.findall(r'https?://(?:twitter\.com|x\.com|vxtwitter\.com|fxtwitter\.com)/[\w\d_]+/status/(\d+)', full_text)
                    for tweet_id_only in links:
                        tweet_list.append((uid, f"https://twitter.com/i/status/{tweet_id_only}"))

                # 2. Сбор XP ТОЛЬКО из ветки XP (парсинг эмбедов бота)
                if tid == XP_SOURCE_THREAD and m.get('embeds'):
                    for emb in m['embeds']:
                        txt = (emb.get('description', '') + " ".join([f.get('value', '') for f in emb.get('fields', [])]))
                        xp_m = re.search(r'([\d\.,]+[KM]?)\s?/\s?[\d\.,]+[KM]?\s?XP', txt)
                        u_m = re.search(r'<@!?(\d+)>', txt)
                        if xp_m and u_m:
                            target_id = u_m.group(1)
                            val = parse_xp_value(xp_m.group(1))
                            if target_id in user_stats:
                                user_stats[target_id]["total_score"] = max(user_stats[target_id]["total_score"], val)
                
                last_id = m['id']
                count += 1
            if last_id == "STOP": break
        log(f"✅ Готово: {tid}. Сообщений: {count}")
        time.sleep(2) # Защита от Rate Limit

    # Обогащение данными (Роли + Дата)
    log("🛡️ Обогащение данными из Discord API...")
    for uid in user_stats:
        joined, roles = get_discord_member_info(uid, token)
        user_stats[uid]["discord_joined_at"] = joined
        user_stats[uid]["discord_roles"] = roles
    
    return user_stats, tweet_list

async def main():
    log("🚀 Старт ORO Collector...")
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    tw_key = os.getenv('SOCIALDATA_KEY')

    # Загружаем текущие данные для истории (prev_*)
    try:
        old_res = supabase.table("leaderboard_stats").select("user_id, total_score, discord_messages").execute()
        old_data = {r['user_id']: r for r in old_res.data}
    except: old_data = {}

    users, tweets = get_discord_data()

    if tweets:
        log(f"🐦 Найдено {len(tweets)} твитов. Обработка через Twitter API...")
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(tweets), 10):
                results = await asyncio.gather(*[fetch_tweet(session, t, tw_key) for t in tweets[i:i+10]])
                for uid, l, v, r, status, handle in results:
                    if status == "Found" and uid in users:
                        # ✅ Исправлено: защита от NoneType через (val or 0)
                        users[uid]["twitter_posts"] += 1
                        users[uid]["twitter_likes"] += (l or 0)
                        users[uid]["twitter_views"] += (v or 0)
                        users[uid]["twitter_replies"] += (r or 0)
                        if handle: users[uid]["twitter_handle"] = handle
                log(f"⏳ Прогресс твитов: {min(i+10, len(tweets))}/{len(tweets)}")

    payload = []
    for uid, info in users.items():
        # Базовый расчет XP, если бот ничего не выдал (10 XP за сообщение)
        calc_xp = info["discord_messages"] * 10
        if info["total_score"] < calc_xp:
            info["total_score"] = calc_xp
        
        # Запись исторических данных для фронтенда
        old_entry = old_data.get(uid, {})
        info["prev_total_score"] = old_entry.get("total_score", 0)
        info["prev_discord_messages"] = old_entry.get("discord_messages", 0)
        info["channels_count"] = len(info.pop("channels"))
        
        payload.append(info)

    if payload:
        log(f"📤 Сохранение {len(payload)} пользователей в Supabase...")
        try:
            supabase.table("leaderboard_stats").upsert(payload, on_conflict="user_id").execute()
            log("✅ База данных успешно обновлена!")
        except Exception as e:
            log(f"❌ ОШИБКА SUPABASE: {e}")

    log("🏁 ЗАВЕРШЕНО!")

if __name__ == "__main__":
    asyncio.run(main())
