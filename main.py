import aiohttp
import asyncio
import logging
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, types
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Integer, select
from conf import API_TOKEN, WEBHOOK_URL, WEBHOOK_PATH, WEBAPP_HOST, WEBAPP_PORT, TELEGRAM_CHAT_ID, DATABASE_URL

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

class Measure(Base):
    __tablename__ = 'measures'
    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True)
    title = Column(String)

# Создание таблиц
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# Инициализация бота
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# Функция отправки сообщения в Telegram
async def notify_telegram(url, title):
    message = f"Добавлена новая страница: {url}\nНазвание: {title}"
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
    logger.info(f"Отправлено сообщение в Telegram: {message}")

# Асинхронная функция для парсинга страницы и добавления новых данных в БД
async def parse_measures():
    url = 'https://it.nso.ru/measures/finance/'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Ошибка доступа к {url}: {response.status}")
                    return

                page_content = await response.text()

        soup = BeautifulSoup(page_content, 'html.parser')
        links = soup.find_all('a', class_='main-page__content-information-link')

        async with AsyncSessionLocal() as db_session:
            for link in links:
                page_url = link['href']
                title = link.find('h4').get_text(strip=True)

                # Проверяем, есть ли этот идентификатор страницы в базе данных
                existing_measure = await db_session.execute(
                    select(Measure).filter_by(url=page_url)
                )
                existing_measure = existing_measure.scalar_one_or_none()

                if existing_measure is None:
                    # Добавляем новый идентификатор страницы в базу
                    new_measure = Measure(url=page_url, title=title)
                    db_session.add(new_measure)
                    await db_session.commit()

                    # Логируем и отправляем уведомление в Telegram
                    logger.info(f"Новая запись добавлена: {page_url} - {title}")
                    await notify_telegram(page_url, title)
                else:
                    logger.debug(f"Страница уже существует в базе: {page_url}")
    except Exception as e:
        logger.exception(f"Ошибка при парсинге страницы: {e}")

@dp.message()
async def handle_message(message: types.Message):
    await message.answer("Бот активен!")

async def on_startup(dp):
    await bot.set_webhook(WEBHOOK_URL)
    await init_db()

async def on_shutdown(dp):
    await bot.delete_webhook()

# Асинхронный планировщик
async def schedule_parsing(interval_seconds):
    while True:
        await parse_measures()
        await asyncio.sleep(interval_seconds)  # Интервал ожидания между парсингами

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(schedule_parsing(3600))  # Интервал проверки — 1 час
    dp.start_webhook(
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host=WEBAPP_HOST,
        port=WEBAPP_PORT,
    )
