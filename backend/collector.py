import os, asyncio, aiohttp, requests, re, time, sys
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(msg)
    sys.stdout.flush()

# === КОНФИГУРАЦИЯ ===
MY_DISCORD_ID = "829735798173728789" 
GUILD_ID = "1349045850331938826"
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
        multiplier = 1
        if 'K' in xp_str:
            multiplier = 1000
            xp_str = xp_str.replace('K', '')
        elif 'M' in xp_str:
            multiplier = 1000000
            xp_str = xp_str.replace('M', '')
        return int(float(xp_str) * multiplier)
    except:
        return 0

async def fetch_tweet(session, tweet_info, api_key):
    uid, url = tweet_info
    id_match = re.search(r"status/(\d+)", url)
    tweet_id = id_match.group(1) if id_match else None
    if not tweet_id: return uid, 0, 0, 0, "Unknown", None
    
    api_url = f"https://api.socialdata.tools/twitter/tweets/{tweet_id}"
    try:
        async with session.get(api_url, headers={"Authorization": f"Bearer {api_key}"}, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                twitter_handle = data.get('user', {}).get('screen_name') or data.get('author', {}).get('username')
                return (uid, data.get('favorite_count', 0), data.get('views_count', 0), 
                        data.get('reply_count', 0), "Found", twitter_handle)
            return uid, 0, 0, 0, "Error", None
    except:
        return uid, 0, 0, 0, "Timeout", None

def get_discord_member_info(user_id, token):
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
    
    for tid in THREAD_IDS:
        log(f"📡 Сбор из ветки: {tid}")
        last_id, count = None, 0
        while True:
            try:
                url = f"https://discord.com/api/v10/channels/{tid}/messages?limit=100"
                if last_id: url += f"&before={last_id}"
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code != 200: break
                msgs = r.json()
                if not msgs: break
                
                for m in msgs:
                    dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                    if dt < TARGET_DATE:
                        last_id = "STOP"
                        break
                    
                    uid = m['author']['id']
                    content = m.get('content', '')
                    
                    # Парсинг XP из эмбедов ботов
                    if m.get('embeds'):
                        for embed in m['embeds']:
                            search_text = f"{embed.get('description', '')} " + " ".join([f.get('value', '') for f in embed.get('fields', [])])
                            xp_match = re.search(r'([\d\.,]+[KM]?)\s?/\s?[\d\.,]+[KM]?\s?XP', search_text)
                            if xp_match:
                                user_mention = re.search(r'<@!?(\d+)>', search_text)
                                if user_mention:
                                    target_uid = user_mention.group(1)
                                    xp_val = parse_xp_value(xp_match.group(1))
                                    if target_uid not in user_stats: user_stats[target_uid] = {"user_id": target_uid, "username": "Unknown", "avatar_url": None, "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0, "twitter_handle": "not_linked", "channels": set(), "total_score": 0, "discord_joined_at": None, "discord_roles": [], "prev_total_score": 0, "prev_discord_messages": 0}
                                    if xp_val > user_stats[target_uid].get("total_score", 0): user_stats[target_uid]["total_score"] = xp_val
                    
                    if uid not in user_stats:
                        avatar = m['author'].get('avatar')
                        user_stats[uid] = {"user_id": uid, "username": m['author']['username'], "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{avatar}.png" if avatar else None, "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0, "twitter_handle": "not_linked", "channels": set(), "total_score": 0, "discord_joined_at": None, "discord_roles": [], "prev_total_score": 0, "prev_discord_messages": 0}
                    
                    user_stats[uid]["discord_messages"] += 1
                    user_stats[uid]["channels"].add(tid)
                    links = re.findall(r'https?://(?:twitter\.com|x\.com|vxtwitter\.com|fxtwitter\.com)/\w+/status/\d+', content)
                    for l in links: tweet_list.append((uid, l))
                    last_id = m['id']
                    count += 1
                if last_id == "STOP": break
            except Exception as e:
                log(f"❌ Ошибка: {e}")
                break
        
        log(f"✅ Ветка {tid} готова. Сообщений: {count}")
        time.sleep(12) # Пауза между ветками (10-15 сек)
    
    log("🛡️ Обогащение данными (Роли)...")
    for uid in user_stats:
        joined, roles = get_discord_member_info(uid, token)
        user_stats[uid]["discord_joined_at"] = joined
        user_stats[uid]["discord_roles"] = roles
        time.sleep(0.5) # Защита от бана токена
    
    return user_stats, tweet_list

async def main():
    log("🚀 Запуск Collector 2.0...")
    s_url, s_key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    tw_key, token = os.getenv('SOCIALDATA_KEY'), os.getenv('DISCORD_TOKEN')
    
    if not s_url or not s_key: return log("❌ Ключи Supabase не найдены!")
    supabase = create_client(s_url, s_key)
    
    log("🔍 Бэкап старых данных...")
    try:
        res = supabase.table("leaderboard_stats").select("user_id, total_score, discord_messages").execute()
        old_data = {row['user_id']: {'total_score': row['total_score'], 'discord_messages': row['discord_messages']} for row in res.data}
    except: old_data = {}
    
    users, tweets = get_discord_data()
    
    if tweets:
        log(f"🐦 Проверка {len(tweets)} твитов...")
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(tweets), 10):
                batch = tweets[i:i+10]
                results = await asyncio.gather(*[fetch_tweet(session, t, tw_key) for t in batch])
                for uid, likes, views, replies, status, handle in results:
                    if status == "Found":
                        users[uid]["twitter_posts"] += 1
                        users[uid]["twitter_likes"] += likes
                        users[uid]["twitter_views"] += views
                        users[uid]["twitter_replies"] += replies
                        if handle: users[uid]["twitter_handle"] = handle
                log(f"⏳ Twitter прогресс: {min(i + 10, len(tweets))}/{len(tweets)}")
                time.sleep(1)

    payload = []
    for uid, info in users.items():
        # Если XP из ботов нет, считаем по сообщениям
        calc_xp = info["discord_messages"] * 10
        if info.get("total_score", 0) < calc_xp: info["total_score"] = calc_xp
        
        old_entry = old_data.get(uid, {})
        info["prev_total_score"] = old_entry.get("total_score", 0)
        info["prev_discord_messages"] = old_entry.get("discord_messages", 0)
        info["channels_count"] = len(info.get("channels", []))
        
        clean_info = info.copy()
        if "channels" in clean_info: del clean_info["channels"]
        payload.append(clean_info)
    
    if payload:
        log(f"📤 Сохранение {len(payload)} юзеров в Supabase...")
        supabase.table("leaderboard_stats").upsert(payload, on_conflict="user_id").execute()
        log("✅ База данных успешно обновлена (UPSERT)!")
    
    log("🏁 ЗАВЕРШЕНО!")

if __name__ == "__main__":
    asyncio.run(main())
