import os, asyncio, aiohttp, re, sys, discord
from datetime import datetime, timezone
from discord.ext import commands, tasks
from supabase import create_client

def log(msg):
    print(msg)
    sys.stdout.flush()

# --- НАСТРОЙКИ ---
GUILD_ID = 1349045850331938826
# Список каналов/веток для прослушивания
THREAD_IDS = [
    1351487907042431027, 1351488160206426227, 1351488253332557867, 
    1351492950768619552, 1367864741548261416, 1371904712001065000, 
    1465733325149835295, 1371110511919497226, 1366338962813222993, 
    1371904910324404325, 1371413462982594620, 1372149550793490505, 
    1372149324192153620, 1372149873188536330, 1372242189240897596, 
    1351488556924932128, 1389273374748049439
]

# Инициализация Supabase
sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Инициализация Discord Bot
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Временное хранилище твитов для фоновой обработки
pending_tweets = [] # Формат: (user_id, tweet_url)

def parse_xp_value(xp_str):
    try:
        xp_str = xp_str.upper().replace(' ', '').replace(',', '')
        mult = 1
        if 'K' in xp_str: mult = 1000; xp_str = xp_str.replace('K', '')
        elif 'M' in xp_str: mult = 1000000; xp_str = xp_str.replace('M', '')
        return int(float(xp_str) * mult)
    except: return 0

async def update_supabase(user_data):
    """Метод для быстрой записи одного юзера в базу"""
    try:
        # Получаем текущие данные для prev_ полей
        res = sb.table("leaderboard_stats").select("total_score, discord_messages").eq("user_id", user_data["user_id"]).execute()
        if res.data:
            user_data["prev_total_score"] = res.data[0].get("total_score", 0)
            user_data["prev_discord_messages"] = res.data[0].get("discord_messages", 0)
        
        sb.table("leaderboard_stats").upsert(user_data, on_conflict="user_id").execute()
    except Exception as e:
        log(f"❌ Ошибка Supabase: {e}")

@bot.event
async def on_ready():
    log(f"🚀 Бот запущен как {bot.user}. Слушаю сообщения...")
    twitter_sync_task.start()

@bot.event
async def on_message(message):
    if message.author.bot and not message.embeds: return
    if message.channel.id not in THREAD_IDS: return

    uid = str(message.author.id)
    content = message.content
    log(f"📩 Новое сообщение от {message.author.name} в {message.channel.id}")

    # 1. Инициализация/Получение данных юзера из базы
    try:
        res = sb.table("leaderboard_stats").select("*").eq("user_id", uid).execute()
        if res.data:
            user_stats = res.data[0]
        else:
            user_stats = {
                "user_id": uid, "username": message.author.name,
                "avatar_url": str(message.author.display_avatar.url),
                "discord_messages": 0, "total_score": 0, "twitter_posts": 0,
                "twitter_handle": "not_linked", "discord_roles": [r.name for r in message.author.roles[1:]] if hasattr(message.author, 'roles') else []
            }
    except: return

    # 2. Обработка XP из Эмбедов (если пишет бот)
    if message.embeds:
        for embed in message.embeds:
            search_text = f"{embed.description} " + " ".join([f.value for f in embed.fields])
            xp_match = re.search(r'([\d\.,]+[KM]?)\s?/\s?[\d\.,]+[KM]?\s?XP', search_text)
            if xp_match:
                mention = re.search(r'<@!?(\d+)>', search_text)
                target_uid = mention.group(1) if mention else uid
                xp_val = parse_xp_value(xp_match.group(1))
                if xp_val > user_stats.get("total_score", 0):
                    user_stats["total_score"] = xp_val

    # 3. Обычный счетчик сообщений
    if not message.author.bot:
        user_stats["discord_messages"] += 1
        # Минимум XP по формуле сообщений
        user_stats["total_score"] = max(user_stats["discord_messages"] * 10, user_stats.get("total_score", 0))

    # 4. Поиск ссылок на твиты (складываем в очередь)
    links = re.findall(r'https?://(?:twitter\.com|x\.com|vxtwitter\.com|fxtwitter\.com)/\w+/status/(\d+)', content)
    for t_id in links:
        pending_tweets.append((uid, f"https://x.com/i/status/{t_id}"))

    # Обновляем в Supabase
    await update_supabase(user_stats)

@tasks.loop(minutes=30)
async def twitter_sync_task():
    """Фоновая задача: раз в 30 минут обновляет метрики для всех новых ссылок"""
    global pending_tweets
    if not pending_tweets: return
    
    log(f"🐦 Синхронизация Twitter для {len(pending_tweets)} ссылок...")
    tw_key = os.getenv('SOCIALDATA_KEY')
    
    async with aiohttp.ClientSession() as session:
        # Копируем и очищаем очередь
        current_batch = pending_tweets[:]
        pending_tweets = []
        
        for uid, url in current_batch:
            t_id = re.search(r"status/(\d+)", url)
            if not t_id: continue
            
            api_url = f"https://api.socialdata.tools/twitter/tweets/{t_id.group(1)}"
            try:
                async with session.get(api_url, headers={"Authorization": f"Bearer {tw_key}"}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        u = data.get('user') or data.get('author') or {}
                        
                        # Обновляем статы в базе
                        res = sb.table("leaderboard_stats").select("*").eq("user_id", uid).execute()
                        if res.data:
                            stats = res.data[0]
                            stats["twitter_posts"] += 1
                            stats["twitter_likes"] = stats.get("twitter_likes", 0) + data.get('favorite_count', 0)
                            stats["twitter_views"] = stats.get("twitter_views", 0) + data.get('views_count', 0)
                            stats["twitter_handle"] = u.get('screen_name') or u.get('username') or stats["twitter_handle"]
                            sb.table("leaderboard_stats").upsert(stats, on_conflict="user_id").execute()
            except: pass
            await asyncio.sleep(1) # Защита от лимитов

if __name__ == "__main__":
    bot.run(os.getenv('DISCORD_TOKEN'))
