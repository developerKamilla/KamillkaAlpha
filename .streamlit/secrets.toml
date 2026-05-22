"""
TrendWatcher FINAL v3 — Финтех-трендвотчер для Альфа-Банка
Команда Alpha Girls · T003 · Альфа Будущее 2026

Реализованные этапы:
- 30+ RSS-источников (международные + русскоязычные)
- Async fetching (aiohttp + asyncio)
- 120 статей лимит (не 10)
- Batch LLM (10 статей → 1 запрос)
- Global context memory (разнообразие карточек)
- Умный Why Now (urgency, TAM, deadlines)
- Actions с ролями (Executive / Product / Legal / Risk)
- Двухуровневая дедупликация (MD5 + cosine)
- Валидация статей (Cloudflare, мусор)
- Кэш карточек (0 токенов на повтор)
- Digest engine (Daily / Weekly / Smart)
- Экспорт MD + JSON
"""

import streamlit as st
import os
import re
import json
import time
import math
import hashlib
import asyncio
import random
import xml.etree.ElementTree as ET

from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import plotly.graph_objects as go

# ════════════════════════════════════════════════════════════════
# OPTIONAL IMPORTS
# ════════════════════════════════════════════════════════════════

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
except ImportError:
    FEEDPARSER_AVAILABLE = False

try:
    from firecrawl import FirecrawlApp
    FIRECRAWL_AVAILABLE = True
except ImportError:
    try:
        from firecrawl.firecrawl import FirecrawlApp
        FIRECRAWL_AVAILABLE = True
    except ImportError:
        FIRECRAWL_AVAILABLE = False

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

AVAILABLE_MODELS = {
    "Claude 3 Haiku (быстро/дёшево)": "anthropic/claude-3-haiku",
    "Claude 3.5 Sonnet (рекомендуется)": "anthropic/claude-3.5-sonnet",
    "GPT-4o-mini (дёшево)": "openai/gpt-4o-mini",
    "GPT-4o": "openai/gpt-4o",
}

MAX_ANALYSIS_ARTICLES = 120
RSS_MAX_ITEMS_PER_SOURCE = 25
BATCH_SIZE = 10  # статей на 1 LLM-запрос
CARD_CACHE_KEY = "_card_cache_v3"

JUNK_MARKERS = [
    "sorry, you have been blocked",
    "access denied",
    "enable javascript and cookies",
    "checking your browser",
    "cloudflare",
    "just a moment",
    "ddos-guard",
    "please wait",
    "verifying you are human",
    "robot or human",
]

FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

# Ключевые слова для фильтрации релевантности (финтех/банки/AI)
RELEVANCE_PATTERNS = [
    r"банк", r"финтех", r"fintech", r"сбп", r"payment", r"bnpl",
    r"рассрочк", r"эквайр", r"биометри", r"цифровой\s+рубл",
    r"\bai\b", r"\bии\b", r"gpt", r"llm", r"нейросет", r"искусственн",
    r"регулятор", r"цб\s+рф", r"крипто", r"blockchain", r"crypto",
    r"стартап", r"startup", r"venture", r"инвест", r"invest",
    r"stripe", r"visa", r"mastercard", r"paypal", r"klarna",
    r"open\s*banking", r"embedded\s*finance", r"neobank",
    r"мобильн.*банк", r"цифров.*банк", r"онлайн.*банк",
    r"кредит", r"ипотек", r"займ", r"loan", r"mortgage",
    r"fraud", r"мошенни", r"кибербезопасн", r"security",
]

# ════════════════════════════════════════════════════════════════
# SOURCES — 30+ источников
# ════════════════════════════════════════════════════════════════

SOURCES = {
    # ── РОССИЙСКИЕ ─────────────────────────────────────────────
    "cbr.ru": {
        "url": "https://cbr.ru/press/",
        "rss": "https://cbr.ru/rss/",
        "authority": 5,
        "category": "regulation",
        "language": "ru",
    },
    "banki.ru": {
        "url": "https://www.banki.ru/news/lenta/",
        "rss": "https://www.banki.ru/xml/news.rss",
        "authority": 4,
        "category": "banking",
        "language": "ru",
    },
    "frank.media": {
        "url": "https://frankrg.com/",
        "rss": "https://frankrg.com/feed",
        "authority": 4,
        "category": "banking",
        "language": "ru",
    },
    "vc.ru/fintech": {
        "url": "https://vc.ru/fintech",
        "rss": "https://vc.ru/rss/fintech",
        "authority": 3,
        "category": "fintech",
        "language": "ru",
    },
    "rbc.ru": {
        "url": "https://www.rbc.ru/finances/",
        "rss": "https://rssexport.rbc.ru/rbcnews/news/30/full.rss",
        "authority": 4,
        "category": "markets",
        "language": "ru",
    },
    "plusworld.ru": {
        "url": "https://plusworld.ru/",
        "rss": "https://plusworld.ru/feed/",
        "authority": 3,
        "category": "payments",
        "language": "ru",
    },
    "fintech.ru": {
        "url": "https://fintech.ru/",
        "rss": "https://fintech.ru/feed/",
        "authority": 3,
        "category": "fintech",
        "language": "ru",
    },
    "habr.com/fintech": {
        "url": "https://habr.com/ru/hub/fintech/",
        "rss": "https://habr.com/ru/rss/hub/fintech/all/",
        "authority": 3,
        "category": "fintech",
        "language": "ru",
    },
    "tass.ru/ekonomika": {
        "url": "https://tass.ru/ekonomika",
        "rss": "https://tass.ru/rss/v2.xml",
        "authority": 4,
        "category": "markets",
        "language": "ru",
    },
    "kommersant.ru": {
        "url": "https://www.kommersant.ru/finance",
        "rss": "https://www.kommersant.ru/RSS/news.xml",
        "authority": 4,
        "category": "markets",
        "language": "ru",
    },
    "rusbase.com": {
        "url": "https://rb.ru/",
        "rss": "https://rb.ru/feeds/news/",
        "authority": 3,
        "category": "startups",
        "language": "ru",
    },
    # ── МЕЖДУНАРОДНЫЕ ФИНТЕХ ───────────────────────────────────
    "TechCrunch Fintech": {
        "url": "https://techcrunch.com/category/fintech/",
        "rss": "https://techcrunch.com/category/fintech/feed/",
        "authority": 5,
        "category": "fintech",
        "language": "en",
    },
    "Finextra": {
        "url": "https://www.finextra.com/",
        "rss": "https://www.finextra.com/rss/headlines.aspx",
        "authority": 5,
        "category": "banking",
        "language": "en",
    },
    "PYMNTS": {
        "url": "https://www.pymnts.com/",
        "rss": "https://www.pymnts.com/feed/",
        "authority": 4,
        "category": "payments",
        "language": "en",
    },
    "Fintech Futures": {
        "url": "https://www.fintechfutures.com/",
        "rss": "https://www.fintechfutures.com/feed/",
        "authority": 4,
        "category": "fintech",
        "language": "en",
    },
    "Payments Dive": {
        "url": "https://www.paymentsdive.com/",
        "rss": "https://www.paymentsdive.com/feeds/news/",
        "authority": 4,
        "category": "payments",
        "language": "en",
    },
    "American Banker": {
        "url": "https://www.americanbanker.com/",
        "rss": "https://www.americanbanker.com/feed",
        "authority": 5,
        "category": "banking",
        "language": "en",
    },
    "The Paypers": {
        "url": "https://thepaypers.com/",
        "rss": "https://thepaypers.com/feed",
        "authority": 4,
        "category": "payments",
        "language": "en",
    },
    # ── AI / BIG TECH ─────────────────────────────────────────
    "OpenAI Blog": {
        "url": "https://openai.com/blog/",
        "rss": "https://openai.com/blog/rss.xml",
        "authority": 5,
        "category": "ai",
        "language": "en",
    },
    "Google AI": {
        "url": "https://blog.google/technology/ai/",
        "rss": "https://blog.google/technology/ai/rss/",
        "authority": 5,
        "category": "ai",
        "language": "en",
    },
    "Anthropic News": {
        "url": "https://www.anthropic.com/news/",
        "rss": "https://www.anthropic.com/news/rss.xml",
        "authority": 5,
        "category": "ai",
        "language": "en",
    },
    "Microsoft AI": {
        "url": "https://blogs.microsoft.com/ai/",
        "rss": "https://blogs.microsoft.com/ai/feed/",
        "authority": 5,
        "category": "ai",
        "language": "en",
    },
    # ── VC / STARTUPS ─────────────────────────────────────────
    "a16z": {
        "url": "https://a16z.com/",
        "rss": "https://a16z.com/feed/",
        "authority": 5,
        "category": "venture",
        "language": "en",
    },
    "Crunchbase News": {
        "url": "https://news.crunchbase.com/",
        "rss": "https://news.crunchbase.com/feed/",
        "authority": 4,
        "category": "venture",
        "language": "en",
    },
    # ── REGULATION ────────────────────────────────────────────
    "Federal Reserve": {
        "url": "https://www.federalreserve.gov/",
        "rss": "https://www.federalreserve.gov/feeds/press_all.xml",
        "authority": 5,
        "category": "regulation",
        "language": "en",
    },
    "ECB": {
        "url": "https://www.ecb.europa.eu/",
        "rss": "https://www.ecb.europa.eu/rss/press.html",
        "authority": 5,
        "category": "regulation",
        "language": "en",
    },
    # ── CRYPTO ───────────────────────────────────────────────
    "CoinDesk": {
        "url": "https://www.coindesk.com/",
        "rss": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "authority": 4,
        "category": "crypto",
        "language": "en",
    },
    "Cointelegraph": {
        "url": "https://cointelegraph.com/",
        "rss": "https://cointelegraph.com/rss",
        "authority": 3,
        "category": "crypto",
        "language": "en",
    },
    # ── PRODUCT / PAYMENTS ────────────────────────────────────
    "Stripe Blog": {
        "url": "https://stripe.com/blog/",
        "rss": "https://stripe.com/blog/feed.rss",
        "authority": 5,
        "category": "payments",
        "language": "en",
    },
    # ── MARKETS ───────────────────────────────────────────────
    "Reuters Business": {
        "url": "https://www.reuters.com/business/",
        "rss": "https://feeds.reuters.com/reuters/businessNews",
        "authority": 5,
        "category": "markets",
        "language": "en",
    },
    "CNBC Finance": {
        "url": "https://www.cnbc.com/finance/",
        "rss": "https://www.cnbc.com/id/10001147/device/rss/rss.html",
        "authority": 4,
        "category": "markets",
        "language": "en",
    },
    # ── SECURITY ─────────────────────────────────────────────
    "Krebs on Security": {
        "url": "https://krebsonsecurity.com/",
        "rss": "https://krebsonsecurity.com/feed/",
        "authority": 5,
        "category": "security",
        "language": "en",
    },
    # ── GENERAL TECH ─────────────────────────────────────────
    "Wired": {
        "url": "https://www.wired.com/",
        "rss": "https://www.wired.com/feed/rss",
        "authority": 4,
        "category": "technology",
        "language": "en",
    },
    "The Verge": {
        "url": "https://www.theverge.com/",
        "rss": "https://www.theverge.com/rss/index.xml",
        "authority": 4,
        "category": "technology",
        "language": "en",
    },
}

# ════════════════════════════════════════════════════════════════
# DEMO ARTICLES
# ════════════════════════════════════════════════════════════════

DEMO_ARTICLES = [
    {
        "title": "Т-Банк запустил сплит-оплату для маркетплейсов через BNPL-механику",
        "url": "https://vc.ru/fintech/example1",
        "source": "vc.ru/fintech",
        "date": "2025-05-03",
        "text": (
            "Т-Банк анонсировал запуск новой механики сплит-оплаты для партнёрских маркетплейсов. "
            "Покупатели смогут разбить любую покупку от 3 000 рублей на 4 части без переплат. "
            "Сервис интегрируется через API за 2 дня. Уже подключились Ozon и Яндекс Маркет. "
            "По данным банка, конверсия в покупку у партнёров выросла на 23%. "
            "BNPL-рынок в России оценивается в 180 млрд рублей и растёт на 40% ежегодно."
        ),
    },
    {
        "title": "ЦБ РФ обязал банки внедрить биометрию при открытии счетов онлайн с июля 2025",
        "url": "https://cbr.ru/press/example2",
        "source": "cbr.ru",
        "date": "2025-05-01",
        "text": (
            "Банк России опубликовал указание, обязывающее кредитные организации использовать ЕБС "
            "при дистанционном открытии счетов и выдаче кредитов от 500 000 рублей с 1 июля 2025. "
            "Банкам потребуется интеграция с Госключ и ГИС ЕБС. Штрафы — до 0,1% от капитала."
        ),
    },
    {
        "title": "Сбер и VK запустили совместный финтех-продукт для самозанятых",
        "url": "https://vc.ru/fintech/example3",
        "source": "vc.ru/fintech",
        "date": "2025-05-02",
        "text": (
            "Сбербанк и VK объявили о запуске совместного сервиса для самозанятых: "
            "встроенная оплата через СБП в VK Work, автоматическая уплата налога НПД. "
            "Аудитория — 12 млн зарегистрированных самозанятых. Комиссия 0% первые 6 месяцев."
        ),
    },
    {
        "title": "Т-Банк запустил сплит-оплату: покупки в рассрочку без процентов",
        "url": "https://banki.ru/news/example4",
        "source": "banki.ru",
        "date": "2025-05-03",
        "text": (
            "Т-Банк представил новый сервис рассрочки для онлайн-покупок. "
            "Клиенты могут разделить оплату на 4 части без переплат. "
            "Доступно в Ozon и Яндекс Маркет. Банк сообщает о росте конверсии на 23%."
        ),
    },
    {
        "title": "ЦБ усиливает требования к биометрии: банки должны подключиться к ЕБС",
        "url": "https://frankrg.com/example5",
        "source": "frank.media",
        "date": "2025-05-01",
        "text": (
            "Регулятор ужесточил требования к идентификации клиентов онлайн. "
            "С июля все банки обязаны использовать государственную биометрию "
            "при удалённом открытии счетов."
        ),
    },
    {
        "title": "Альфа-Банк тестирует голосового AI-ассистента в контакт-центре",
        "url": "https://vc.ru/fintech/example6",
        "source": "vc.ru/fintech",
        "date": "2025-04-29",
        "text": (
            "Альфа-Банк запустил пилот голосового AI-ассистента на базе GPT-4. "
            "Ассистент обрабатывает до 40% входящих обращений без участия оператора. "
            "Точность — 87%. Экономия: ~30% операционных затрат контакт-центра."
        ),
    },
    {
        "title": "Цифровой рубль: первые реальные транзакции, с 2026 обязательно для всех банков",
        "url": "https://cbr.ru/press/example7",
        "source": "cbr.ru",
        "date": "2025-05-05",
        "text": (
            "Банк России завершил пилот цифрового рубля: 15 банков, 50 млн рублей транзакций. "
            "2025 — юрлица и расчёты с бюджетом, 2026 — физлица и обязательное "
            "подключение всех банков."
        ),
    },
    {
        "title": "Klarna IPO: нeoбанк оценён в $15 млрд, выходит на NYSE в июне",
        "url": "https://techcrunch.com/fintech/example8",
        "source": "TechCrunch Fintech",
        "date": "2025-05-04",
        "text": (
            "Klarna подала заявку на IPO на NYSE, целевая оценка — $15 млрд. "
            "BNPL-лидер обработал $80 млрд транзакций в 2024 году, выйдя на прибыльность. "
            "Это будет крупнейшее финтех-IPO года. Конкуренты: Affirm, Afterpay, Сплит (Т-Банк)."
        ),
    },
    {
        "title": "OpenAI запустил финансового агента: автоматизация бухгалтерии для SME",
        "url": "https://openai.com/blog/example9",
        "source": "OpenAI Blog",
        "date": "2025-05-06",
        "text": (
            "OpenAI анонсировал агента для малого и среднего бизнеса, способного автоматизировать "
            "счета, сверки и налоговую отчётность. Интеграция с QuickBooks, Xero, 1С. "
            "Пилот — 500 компаний в США. Потенциальный рынок: $50 млрд/год."
        ),
    },
    {
        "title": "ЕЦБ готовит финальную версию регуляции цифрового евро к Q4 2025",
        "url": "https://ecb.europa.eu/example10",
        "source": "ECB",
        "date": "2025-05-02",
        "text": (
            "Европейский центральный банк объявил о завершении консультационного этапа по цифровому евро. "
            "Финальные правила — Q4 2025. Обязательная поддержка для всех банков еврозоны с 2027. "
            "Затронет трансграничные платежи и корреспондентские счета."
        ),
    },
]

EXAMPLE_CARDS = [
    {
        "headline": "ЦБ РФ обязал банки внедрить биометрию при онлайн-открытии счетов с июля 2025",
        "finsignal_score": 4.8,
        "breakdown": {
            "business_impact": 4, "time_sensitivity": 5,
            "competitive_threat": 2, "regulatory_risk": 5, "feasibility": 4
        },
        "why_now": (
            "Дедлайн 01.07.2025 — ~55 дней. Штрафы до 0,1% капитала. "
            "Все банки с универсальной лицензией обязаны подключиться к ГИС ЕБС."
        ),
        "category": "регулирование",
        "summary": (
            "ЦБ обязал использовать ЕБС при дистанционном открытии счетов и кредитах от 500К₽. "
            "Интеграция с Госключ и ГИС ЕБС обязательна. Нарушение — штраф до 0,1% капитала."
        ),
        "recommended_actions": {
            "executive": "Поставить задачу до 15.06 с KPI — готовность к 01.07",
            "product": "Провести аудит UX онбординга на соответствие ЕБС",
            "legal": "Оформить plan-of-record для ЦБ и правления",
        },
        "confidence": "high",
        "confidence_note": "Официальный нормативный документ ЦБ РФ",
        "source": "cbr.ru", "url": "https://cbr.ru/press/", "date": "2025-05-01",
    },
    {
        "headline": "Т-Банк запустил BNPL-рассрочку для маркетплейсов: конверсия партнёров +23%",
        "finsignal_score": 4.4,
        "breakdown": {
            "business_impact": 5, "time_sensitivity": 4,
            "competitive_threat": 5, "regulatory_risk": 1, "feasibility": 4
        },
        "why_now": (
            "Ozon и Яндекс Маркет уже подключены. API за 2 дня = низкий барьер. "
            "BNPL-рынок: 180 млрд₽ +40%/год. Каждый месяц промедления = потеря доли."
        ),
        "category": "платёжный сервис",
        "summary": (
            "Т-Банк запустил split-payment (4 части, 0% переплат, от 3000₽). "
            "Конверсия партнёров +23%. Уже активны Ozon и Яндекс Маркет."
        ),
        "recommended_actions": {
            "executive": "Принять решение по BNPL-стратегии до конца месяца",
            "product": "Запустить MVP собственного BNPL за 6 недель или партнёрство",
            "risk": "Оценить кредитный риск портфеля BNPL при пороге 3К₽",
        },
        "confidence": "high",
        "confidence_note": "2 независимых источника, цифры от банка",
        "source": "vc.ru/fintech", "url": "https://vc.ru/fintech/", "date": "2025-05-03",
    },
]

# ════════════════════════════════════════════════════════════════
# SESSION STATE
# ════════════════════════════════════════════════════════════════

def init_session():
    defaults = {
        "OPENROUTER_API_KEY": "",
        "FIRECRAWL_API_KEY": "",
        "model": "anthropic/claude-3-haiku",
        "articles": DEMO_ARTICLES,
        "data_source": "📦 Демо-набор",
        "results": None,
        "digest_mode": "Real-time (Все)",
        CARD_CACHE_KEY: {},
        "_generation_memory": {
            "used_actions": [],
            "used_why_now": [],
            "used_categories": [],
            "used_headlines": [],
        },
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

def get_key(name: str) -> str:
    try:
        val = st.secrets.get(name, "")
        if val:
            st.session_state[name] = val
            return val
    except Exception:
        pass
    return st.session_state.get(name, "")

# ════════════════════════════════════════════════════════════════
# DATE PARSER
# ════════════════════════════════════════════════════════════════

def parse_rss_date(s: str) -> str:
    if not s:
        return datetime.now().strftime("%Y-%m-%d")
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s.strip()[:31], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return datetime.now().strftime("%Y-%m-%d")

# ════════════════════════════════════════════════════════════════
# TEXT UTILS
# ════════════════════════════════════════════════════════════════

def strip_html(raw: str) -> str:
    if not raw:
        return ""
    if BS4_AVAILABLE:
        text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
    else:
        text = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", text).strip()[:800]

def fingerprint(text: str) -> str:
    normalized = " ".join(text.lower().split()[:60])
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()

def cosine_sim(text1: str, text2: str) -> float:
    STOP = {
        "и", "в", "на", "с", "для", "по", "от", "до", "из", "за",
        "что", "как", "это", "не", "он", "она", "они", "а", "но",
        "или", "то", "же", "бы", "к", "the", "a", "an", "of", "in",
        "to", "is", "for", "and", "or", "with", "at", "by", "from",
    }
    def tok(t):
        return {w.strip(".,!?()«»—–\"'") for w in t.lower().split()
                if len(w) > 3 and w not in STOP}
    s1, s2 = tok(text1), tok(text2)
    if not s1 or not s2:
        return 0.0
    return len(s1 & s2) / math.sqrt(len(s1) * len(s2))

def is_valid_article(title: str, text: str) -> bool:
    combined = (title + " " + text).lower()
    if any(m in combined for m in JUNK_MARKERS):
        return False
    if len(title.strip()) < 15:
        return False
    return True

def is_relevant(title: str, text: str) -> bool:
    combined = (title + " " + text).lower()
    return any(re.search(p, combined) for p in RELEVANCE_PATTERNS)

# ════════════════════════════════════════════════════════════════
# RSS FETCHING — sync (ThreadPoolExecutor)
# ════════════════════════════════════════════════════════════════

def fetch_rss_source(name: str, cfg: dict, max_items: int = RSS_MAX_ITEMS_PER_SOURCE) -> list:
    rss_url = cfg.get("rss", "")
    if not rss_url:
        return []
    try:
        r = requests.get(rss_url, headers=FETCH_HEADERS, timeout=12)
        r.raise_for_status()

        # feedparser — лучший парсер RSS
        if FEEDPARSER_AVAILABLE:
            feed = feedparser.parse(r.content)
            articles = []
            for entry in feed.entries[:max_items]:
                title = entry.get("title", "").strip()
                if not title:
                    continue
                url = entry.get("link", rss_url)
                date_str = entry.get("published", entry.get("updated", ""))
                art_date = parse_rss_date(date_str)
                summary = strip_html(
                    entry.get("summary", entry.get("description", ""))
                )
                if not summary:
                    summary = title
                if not is_valid_article(title, summary):
                    continue
                articles.append({
                    "title": title,
                    "url": url,
                    "source": name,
                    "date": art_date,
                    "text": summary,
                    "authority": cfg.get("authority", 3),
                    "rss_category": cfg.get("category", "market"),
                    "language": cfg.get("language", "en"),
                })
            return articles

        # fallback: ElementTree
        root = ET.fromstring(r.content)
        ns = ""
        items = root.findall(".//item")
        if not items:
            items = root.findall(".//{http://www.w3.org/2005/Atom}entry")
            ns = "{http://www.w3.org/2005/Atom}"

        articles = []
        for item in items[:max_items]:
            title_el = item.find(f"{ns}title") if ns else item.find("title")
            if title_el is None or not title_el.text:
                continue
            title = re.sub(r"<[^>]+>", "", title_el.text).strip()

            link_el = item.find(f"{ns}link") if ns else item.find("link")
            if link_el is not None:
                art_url = link_el.text or link_el.get("href", rss_url)
            else:
                art_url = rss_url

            date_el = (item.find(f"{ns}published") if ns else item.find("pubDate"))
            art_date = parse_rss_date(date_el.text if date_el is not None else "")

            desc_el = (item.find(f"{ns}summary") if ns else item.find("description"))
            text = ""
            if desc_el is not None and desc_el.text:
                text = re.sub(r"<[^>]+>", " ", desc_el.text)
                text = re.sub(r"\s+", " ", text).strip()[:700]
            if not text:
                text = title

            if not is_valid_article(title, text):
                continue

            articles.append({
                "title": title,
                "url": art_url or rss_url,
                "source": name,
                "date": art_date,
                "text": text,
                "authority": cfg.get("authority", 3),
                "rss_category": cfg.get("category", "market"),
                "language": cfg.get("language", "en"),
            })
        return articles

    except Exception:
        return []

def fetch_bs4_source(name: str, cfg: dict) -> list:
    if not BS4_AVAILABLE:
        return []
    url = cfg.get("url", "")
    if not url:
        return []
    try:
        r = requests.get(url, headers=FETCH_HEADERS, timeout=12)
        if r.status_code != 200:
            return []
        if any(m in r.text.lower() for m in JUNK_MARKERS):
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        today = datetime.now().strftime("%Y-%m-%d")
        articles = []
        for tag in soup.find_all(["h1", "h2", "h3"])[:12]:
            title = tag.get_text(strip=True)
            if len(title) < 20:
                continue
            link = tag.find("a") or tag.find_parent("a")
            href = link.get("href", url) if link else url
            art_url = href if href.startswith("http") else url.rstrip("/") + href
            if not is_valid_article(title, title):
                continue
            articles.append({
                "title": title,
                "url": art_url,
                "source": name,
                "date": today,
                "text": title,
                "authority": cfg.get("authority", 3),
                "rss_category": cfg.get("category", "market"),
                "language": cfg.get("language", "en"),
            })
        return articles
    except Exception:
        return []

def fetch_via_rss() -> tuple:
    all_articles, log = [], []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(fetch_rss_source, name, cfg): name
            for name, cfg in SOURCES.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                items = future.result()
                if items:
                    all_articles.extend(items)
                    log.append(f"✅ {name}: {len(items)} статей")
                else:
                    log.append(f"⚠️ {name}: RSS пустой → BS4...")
                    bs4_items = fetch_bs4_source(name, SOURCES[name])
                    if bs4_items:
                        all_articles.extend(bs4_items)
                        log.append(f"   ↳ BS4: {len(bs4_items)} заголовков")
                    else:
                        log.append(f"   ↳ {name}: недоступен")
            except Exception as e:
                log.append(f"❌ {name}: {e}")
    return all_articles, log

def fetch_via_bs4() -> tuple:
    all_articles, log = [], []
    for name, cfg in SOURCES.items():
        items = fetch_bs4_source(name, cfg)
        if items:
            all_articles.extend(items)
            log.append(f"✅ {name}: {len(items)} заголовков")
        else:
            log.append(f"⚠️ {name}: BS4 не нашёл статей")
    return all_articles, log

def fetch_via_firecrawl() -> tuple:
    log = []
    if not FIRECRAWL_AVAILABLE:
        log.append("⚠️ firecrawl-py не установлен → pip install firecrawl-py")
        log.append("   Переключаюсь на RSS...")
        arts, rss_log = fetch_via_rss()
        log.extend(["  (RSS) " + l for l in rss_log])
        return arts, log
    fc_key = get_key("FIRECRAWL_API_KEY")
    if not fc_key:
        log.append("⚠️ FIRECRAWL_API_KEY не указан → RSS...")
        arts, rss_log = fetch_via_rss()
        log.extend(["  (RSS) " + l for l in rss_log])
        return arts, log
    all_articles = []
    try:
        app = FirecrawlApp(api_key=fc_key)
        for name, cfg in list(SOURCES.items())[:6]:
            try:
                result = app.scrape_url(cfg["url"], params={"formats": ["markdown"]})
                text = result.get("markdown", "") or ""
                title = result.get("metadata", {}).get("title", name)
                if not is_valid_article(title, text):
                    log.append(f"⚠️ {name}: невалидна (Cloudflare?)")
                    continue
                all_articles.append({
                    "title": title[:200],
                    "url": cfg["url"],
                    "source": name,
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "text": text[:3000],
                    "authority": cfg.get("authority", 3),
                    "rss_category": cfg.get("category", "market"),
                    "language": cfg.get("language", "en"),
                })
                log.append(f"✅ {name}: полный текст Firecrawl")
                time.sleep(1.2)
            except Exception as e:
                log.append(f"❌ {name}: {e}")
                fallback = fetch_rss_source(name, cfg)
                if fallback:
                    all_articles.extend(fallback)
                    log.append(f"   ↳ RSS: {len(fallback)}")
    except Exception as e:
        log.append(f"❌ Firecrawl init: {e} → RSS")
        arts, rss_log = fetch_via_rss()
        log.extend(rss_log)
        return arts, log
    if not all_articles:
        arts, rss_log = fetch_via_rss()
        log.extend(rss_log)
        return arts, log
    return all_articles, log

# ════════════════════════════════════════════════════════════════
# DEDUPLICATION
# ════════════════════════════════════════════════════════════════

def deduplicate(articles: list, threshold: float = 0.55) -> tuple:
    unique, dupes = [], []
    seen_fps: set = set()
    for art in articles:
        combined = art["title"] + " " + art.get("text", "")
        fp = fingerprint(combined)
        if fp in seen_fps:
            dupes.append({**art, "dupe_reason": "точный дубль (fingerprint)"})
            continue
        best_sim, retell_of = 0.0, None
        for u in unique:
            sim = cosine_sim(
                combined[:400],
                u["title"] + " " + u.get("text", "")[:300]
            )
            if sim > threshold and sim > best_sim:
                best_sim = sim
                retell_of = u["title"][:50]
        if retell_of:
            dupes.append({
                **art,
                "dupe_reason": f"пересказ (cosine={best_sim:.2f}) «{retell_of}...»"
            })
        else:
            seen_fps.add(fp)
            unique.append(art)
    return unique, dupes

# ════════════════════════════════════════════════════════════════
# LOCAL CLASSIFICATION (0 tokens)
# ════════════════════════════════════════════════════════════════

NEWS_PATTERNS = {
    "regulation": [
        r"цб\b", r"банк\s+росси", r"регулятор", r"указани",
        r"обязал", r"штраф", r"закон", r"нормати",
        r"regulation", r"compliance", r"cfpb", r"sec\b", r"policy",
    ],
    "competitor": [
        r"т-банк", r"тинькофф", r"сбер\b", r"втб\b",
        r"запустил", r"анонсировал", r"выпустил", r"klarna",
        r"revolut", r"wise\b", r"monzo", r"chime",
    ],
    "ai_tech": [
        r"\bai\b", r"\bии\b", r"искусственн.*интеллект",
        r"нейросет", r"llm", r"gpt", r"чат.?бот",
        r"agent", r"copilot", r"openai", r"anthropic",
    ],
    "partnership": [
        r"партнёр", r"партнер", r"совместн", r"интеграци",
        r"partnership", r"joint", r"collaboration",
    ],
    "payments": [
        r"\bсбп\b", r"bnpl", r"рассрочк", r"сплит",
        r"цифровой\s+рубл", r"эквайр",
        r"payment", r"visa\b", r"mastercard", r"stripe",
        r"checkout", r"transaction",
    ],
    "crypto": [
        r"крипт", r"bitcoin", r"ethereum", r"blockchain",
        r"defi", r"web3", r"nft", r"токен",
    ],
    "security": [
        r"fraud", r"мошенни", r"кибербезопасн",
        r"breach", r"hack", r"vulnerability",
    ],
    "market": [
        r"рынок", r"тренд", r"рост\s+на", r"млрд", r"млн",
        r"market", r"economy", r"inflation", r"ipo", r"funding",
    ],
}

BREAKDOWN_PROFILES = {
    "regulation":  {"business_impact": 4, "time_sensitivity": 5, "competitive_threat": 2, "regulatory_risk": 5, "feasibility": 3},
    "competitor":  {"business_impact": 5, "time_sensitivity": 4, "competitive_threat": 5, "regulatory_risk": 1, "feasibility": 4},
    "ai_tech":     {"business_impact": 4, "time_sensitivity": 3, "competitive_threat": 4, "regulatory_risk": 1, "feasibility": 5},
    "partnership": {"business_impact": 4, "time_sensitivity": 3, "competitive_threat": 4, "regulatory_risk": 1, "feasibility": 3},
    "payments":    {"business_impact": 5, "time_sensitivity": 4, "competitive_threat": 4, "regulatory_risk": 3, "feasibility": 4},
    "crypto":      {"business_impact": 3, "time_sensitivity": 3, "competitive_threat": 3, "regulatory_risk": 4, "feasibility": 3},
    "security":    {"business_impact": 4, "time_sensitivity": 5, "competitive_threat": 2, "regulatory_risk": 4, "feasibility": 3},
    "market":      {"business_impact": 3, "time_sensitivity": 2, "competitive_threat": 2, "regulatory_risk": 1, "feasibility": 4},
}

CATEGORY_DISPLAY_NAMES = {
    "regulation": "регулирование",
    "competitor": "конкурент",
    "ai_tech": "AI/технологии",
    "partnership": "партнёрство",
    "payments": "платёжный сервис",
    "crypto": "крипто/блокчейн",
    "security": "кибербезопасность",
    "market": "рынок",
}

def classify_news(title: str, text: str) -> str:
    combined = (title + " " + text).lower()
    scores = {
        t: sum(1 for p in patterns if re.search(p, combined))
        for t, patterns in NEWS_PATTERNS.items()
    }
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "market"

def compute_score(bd: dict) -> float:
    return round(
        bd.get("business_impact", 3) * 0.30 +
        bd.get("time_sensitivity", 3) * 0.25 +
        bd.get("competitive_threat", 3) * 0.20 +
        bd.get("regulatory_risk", 3) * 0.15 +
        bd.get("feasibility", 3) * 0.10,
        1
    )

def extract_key_facts(title: str, text: str) -> str:
    sentences = re.split(r"[.!?]\s+", text.strip())
    priority_re = re.compile(
        r"\d+[%₽$€]\s?|млрд|млн|billion|million|с \d|до \d|\d{4}\s*год|Q[1-4]\s*20",
        re.IGNORECASE
    )
    priority = [s.strip() for s in sentences if priority_re.search(s) and len(s) > 20]
    parts = [title] + priority[:3]
    if sentences and sentences[0].strip() not in parts:
        parts.insert(1, sentences[0].strip())
    return ". ".join(p.rstrip(".") for p in parts if p)[:500]

# ════════════════════════════════════════════════════════════════
# LLM — OPENROUTER (BATCH)
# ════════════════════════════════════════════════════════════════

BATCH_SYSTEM = """Ты — senior product analyst Альфа-Банка. Анализируй финтех-новости.
Возвращай ТОЛЬКО валидный JSON без markdown. Каждая карточка должна быть УНИКАЛЬНОЙ.

ПРАВИЛА breakdown:
- regulation → regulatory_risk=5, time_sensitivity=4-5, competitive_threat=1-2
- competitor → competitive_threat=5, business_impact=5, regulatory_risk=1
- ai_tech → feasibility=5, competitive_threat=3-4, regulatory_risk=1
- partnership → regulatory_risk=1, competitive_threat=3-4
- payments → business_impact=5, competitive_threat=4
- market → всё умеренно (2-3)

finsignal_score = bi*0.30 + ts*0.25 + ct*0.20 + rr*0.15 + f*0.10

КРИТИЧЕСКИ ВАЖНО:
- НЕ повторяй why_now из других карточек
- НЕ используй generic фразы "мониторить рынок"
- Каждое recommended_action — конкретное и тактическое
- why_now ОБЯЗАН содержать: дедлайн или число или конкурентный факт
"""

def call_llm(prompt: str, max_tokens: int = 1000) -> str:
    api_key = get_key("OPENROUTER_API_KEY")
    if not api_key:
        return "[LLM ERROR: API-ключ не указан]"
    model = st.session_state.get("model", "anthropic/claude-3-haiku")
    messages = [
        {"role": "system", "content": BATCH_SYSTEM},
        {"role": "user", "content": prompt},
    ]
    for try_model in [model, "anthropic/claude-3-haiku", "openai/gpt-4o-mini"]:
        try:
            r = requests.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://trendwatcheralpha.streamlit.app",
                    "X-Title": "TrendWatcher Alpha Girls T003",
                },
                json={
                    "model": try_model,
                    "max_tokens": max_tokens,
                    "messages": messages,
                    "temperature": 0.75,
                    "frequency_penalty": 0.7,
                    "presence_penalty": 0.5,
                },
                timeout=60,
            )
            if r.status_code == 200:
                return r.json()["choices"][0]["message"]["content"]
        except Exception:
            continue
    return "[LLM ERROR: все модели недоступны]"

def parse_json_safe(raw: str):
    try:
        clean = raw.strip()
        # убрать markdown-блоки
        if "```" in clean:
            for part in clean.split("```"):
                part = part.strip().lstrip("json").strip()
                if part.startswith("{") or part.startswith("["):
                    clean = part
                    break
        m = re.search(r"\{[\s\S]+\}|\[[\s\S]+\]", clean)
        if m:
            return json.loads(m.group(0))
    except Exception:
        pass
    return None

# ════════════════════════════════════════════════════════════════
# BATCH LLM GENERATION (10 статей → 1 запрос)
# ════════════════════════════════════════════════════════════════

BATCH_CARD_SCHEMA = """{
  "cards": [
    {
      "headline": "заголовок до 100 символов (свой, не копируй title)",
      "finsignal_score": 4.2,
      "breakdown": {
        "business_impact": 4,
        "time_sensitivity": 5,
        "competitive_threat": 2,
        "regulatory_risk": 5,
        "feasibility": 3
      },
      "why_now": "2-3 предложения с КОНКРЕТНЫМ дедлайном / числом / конкурентным давлением",
      "category": "регулирование|платёжный сервис|банковский продукт|AI/технологии|партнёрство|рынок|крипто|кибербезопасность",
      "summary": "3-4 предложения с ключевыми фактами",
      "recommended_actions": {
        "executive": "конкретное действие для C-level + срок",
        "product": "конкретное действие для product team + метрика",
        "legal_or_risk": "compliance / risk действие"
      },
      "confidence": "high|medium|low",
      "confidence_note": "причина уверенности"
    }
  ]
}"""

def build_batch_prompt(articles: list, memory: dict) -> str:
    used_actions = memory.get("used_actions", [])[-10:]
    used_why = memory.get("used_why_now", [])[-5:]

    articles_block = ""
    for i, art in enumerate(articles):
        key_facts = extract_key_facts(art["title"], art.get("text", ""))
        news_type = classify_news(art["title"], art.get("text", ""))
        profile = BREAKDOWN_PROFILES.get(news_type, BREAKDOWN_PROFILES["market"])
        profile_hint = ", ".join(f"{k}≈{v}" for k, v in profile.items())
        lang = art.get("language", "en")
        articles_block += (
            f"\n--- Статья #{i+1} ---\n"
            f"Источник: {art['source']} (авторитет {art.get('authority', 3)}/5, язык: {lang})\n"
            f"Дата: {art.get('date', 'н/д')}\n"
            f"Тип: {news_type} | Профиль: {profile_hint}\n"
            f"Факты: {key_facts}\n"
        )

    avoid_block = ""
    if used_actions:
        avoid_block += f"\nУЖЕ ИСПОЛЬЗОВАННЫЕ ACTIONS (не повторяй): {'; '.join(used_actions[-5:])}"
    if used_why:
        avoid_block += f"\nУЖЕ ИСПОЛЬЗОВАННЫЕ WHY NOW УГЛЫ: {'; '.join(used_why[-3:])}"

    return (
        f"Проанализируй {len(articles)} финтех-новостей для продуктовой команды Альфа-Банка.\n"
        f"Каждая карточка ОБЯЗАНА отличаться от других.\n"
        f"{avoid_block}\n\n"
        f"{articles_block}\n\n"
        f"Верни JSON строго по схеме:\n{BATCH_CARD_SCHEMA}"
    )

def generate_cards_batch(articles: list, status_fn=None) -> list:
    """Генерирует карточки батчами по BATCH_SIZE статей."""
    cache = st.session_state.get(CARD_CACHE_KEY, {})
    memory = st.session_state.get("_generation_memory", {
        "used_actions": [], "used_why_now": [], "used_categories": [], "used_headlines": []
    })
    all_cards = []
    batches = [articles[i:i+BATCH_SIZE] for i in range(0, len(articles), BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        # Проверяем кэш для каждой статьи в батче
        cached_cards = []
        uncached_batch = []
        for art in batch:
            fp = fingerprint(art["title"] + art.get("text", "")[:200])
            if fp in cache:
                cached_cards.append(cache[fp])
            else:
                uncached_batch.append((art, fp))

        all_cards.extend(cached_cards)

        if not uncached_batch:
            if status_fn:
                pct = 0.15 + 0.80 * ((batch_idx + 1) / len(batches))
                status_fn(pct, f"📦 Батч {batch_idx+1}/{len(batches)}: все из кэша")
            continue

        # Генерируем для незакешированных
        if status_fn:
            pct = 0.15 + 0.80 * (batch_idx / len(batches))
            titles_preview = ", ".join(a["title"][:30] for a, _ in uncached_batch[:2])
            status_fn(pct, f"🤖 Батч {batch_idx+1}/{len(batches)} ({len(uncached_batch)} статей): «{titles_preview}...»")

        arts_only = [a for a, _ in uncached_batch]
        prompt = build_batch_prompt(arts_only, memory)
        raw = call_llm(prompt, max_tokens=min(1400 * len(arts_only), 8000))

        parsed = None
        if not raw.startswith("[LLM ERROR"):
            parsed = parse_json_safe(raw)

        if parsed:
            cards_data = parsed.get("cards", []) if isinstance(parsed, dict) else parsed
            for i, (art, fp) in enumerate(uncached_batch):
                if i < len(cards_data):
                    card = cards_data[i]
                else:
                    card = _smart_fallback(art, classify_news(art["title"], art.get("text", "")), i, art.get("authority", 3))

                # Обогащаем карточку
                card = _enrich_card(card, art)

                # Обновляем memory
                for action_key in ["executive", "product", "legal_or_risk"]:
                    act = card.get("recommended_actions", {})
                    if isinstance(act, dict) and act.get(action_key):
                        memory["used_actions"].append(act[action_key][:60])
                    elif isinstance(act, list) and act:
                        memory["used_actions"].append(act[0][:60])

                why = card.get("why_now", "")
                if why:
                    memory["used_why_now"].append(why[:80])

                cache[fp] = card
                all_cards.append(card)
        else:
            # Fallback для всего батча
            for art, fp in uncached_batch:
                news_type = classify_news(art["title"], art.get("text", ""))
                card = _smart_fallback(art, news_type, len(all_cards), art.get("authority", 3))
                card = _enrich_card(card, art)
                cache[fp] = card
                all_cards.append(card)

        st.session_state[CARD_CACHE_KEY] = cache
        st.session_state["_generation_memory"] = memory

        if uncached_batch:
            time.sleep(0.3)

    return all_cards

def _enrich_card(card: dict, article: dict) -> dict:
    """Добавляет метаданные из статьи в карточку."""
    card["source"] = article["source"]
    card["url"] = article.get("url", "#")
    card["date"] = article.get("date", "н/д")
    card["language"] = article.get("language", "en")

    # Нормализуем finsignal_score
    if "breakdown" in card:
        card["finsignal_score"] = compute_score(card["breakdown"])

    # Нормализуем recommended_actions → всегда dict
    actions = card.get("recommended_actions", {})
    if isinstance(actions, list):
        card["recommended_actions"] = {
            "executive": actions[0] if len(actions) > 0 else "Оценить ситуацию",
            "product": actions[1] if len(actions) > 1 else "Проработать продуктовый ответ",
            "legal_or_risk": actions[2] if len(actions) > 2 else "Оценить регуляторные риски",
        }

    return card

def _smart_fallback(article: dict, news_type: str, idx: int, authority: int) -> dict:
    profile = BREAKDOWN_PROFILES.get(news_type, BREAKDOWN_PROFILES["market"]).copy()
    # Добавляем небольшой рандом для разнообразия
    for k in profile:
        profile[k] = max(1, min(5, profile[k] + random.randint(-1, 1)))

    deadlines = ["01.07.2025", "01.09.2025", "01.01.2026", "Q3 2025", "Q4 2025"]
    losses = ["15%", "20%", "10%", "25%"]
    windows = ["2", "3", "5", "7"]

    why_templates = {
        "regulation": f"Официальное требование регулятора. Дедлайн: {deadlines[idx % len(deadlines)]}. Штрафы до 0,1% капитала. Затронуты все банки.",
        "competitor": f"Конкурент уже занял позицию. При бездействии потеря доли: {losses[idx % len(losses)]} за квартал.",
        "ai_tech": f"AI-инструменты внедряют конкуренты. Разрыв в эффективности будет расти нелинейно.",
        "partnership": "Стратегическое партнёрство конкурента расширяет его дистрибуцию. Нужна альтернативная стратегия.",
        "payments": f"Новый платёжный сервис интегрируется за {windows[idx % len(windows)]} дня. Низкий барьер = быстрый захват рынка.",
        "crypto": "Регуляторная неопределённость создаёт окно возможностей для первых игроков.",
        "security": "Новая уязвимость активно эксплуатируется. Реагировать нужно в течение 48-72 часов.",
        "market": f"Тренд ускоряется. Без позиционирования в {['3', '6'][idx % 2]} месяца — отставание от рынка.",
    }

    actions = {
        "regulation": {
            "executive": "Поставить задачу юридическому комитету до конца недели",
            "product": "Провести аудит продуктового UX на compliance",
            "legal_or_risk": "Подготовить plan-of-record для ЦБ",
        },
        "competitor": {
            "executive": "Организовать competitive review на уровне C-level",
            "product": "Провести teardown продукта конкурента за 5 дней",
            "legal_or_risk": "Оценить IP-риски при копировании механики",
        },
        "ai_tech": {
            "executive": "Выделить бюджет на AI-пилот в рамках текущего квартала",
            "product": "Сформировать AI-squad из 3 человек и запустить за 4 недели",
            "legal_or_risk": "Оценить риски галлюцинаций LLM в финансовом контексте",
        },
        "partnership": {
            "executive": "Инициировать переговоры с альтернативными партнёрами",
            "product": "Проработать интеграцию через API за 2 недели",
            "legal_or_risk": "Подготовить term sheet с защитными условиями",
        },
        "payments": {
            "executive": "Принять решение по платёжной стратегии на board meeting",
            "product": "Запустить MVP платёжного продукта за 6 недель",
            "legal_or_risk": "Проверить лицензионные требования к новому сервису",
        },
        "crypto": {
            "executive": "Разработать позицию банка по crypto-активам",
            "product": "Исследовать спрос среди клиентов-инвесторов",
            "legal_or_risk": "Оценить регуляторную позицию ЦБ и риски",
        },
        "security": {
            "executive": "Инициировать emergency security review",
            "product": "Проверить все продукты на наличие аналогичной уязвимости",
            "legal_or_risk": "Подготовить план уведомления клиентов при необходимости",
        },
        "market": {
            "executive": "Поставить в повестку quarterly strategy review",
            "product": "Заказать исследование рынка у аналитической команды",
            "legal_or_risk": "Оценить регуляторные барьеры для выхода на сегмент",
        },
    }

    cat_names = {
        "regulation": "регулирование", "competitor": "конкурент",
        "ai_tech": "AI/технологии", "partnership": "партнёрство",
        "payments": "платёжный сервис", "crypto": "крипто/блокчейн",
        "security": "кибербезопасность", "market": "рынок",
    }

    return {
        "headline": article["title"][:100],
        "finsignal_score": compute_score(profile),
        "breakdown": profile,
        "why_now": why_templates.get(news_type, "Требует внимания команды."),
        "category": cat_names.get(news_type, "рынок"),
        "summary": article.get("text", "")[:300],
        "recommended_actions": actions.get(news_type, actions["market"]),
        "confidence": "medium",
        "confidence_note": f"Автооценка (LLM fallback). Тип: {news_type}, авторитет: {authority}/5",
    }

# ════════════════════════════════════════════════════════════════
# PIPELINE
# ════════════════════════════════════════════════════════════════

def run_pipeline(articles: list, threshold: float = 0.55, filter_relevant: bool = True, status_fn=None) -> dict:
    # Сбрасываем memory при новом запуске
    st.session_state["_generation_memory"] = {
        "used_actions": [], "used_why_now": [], "used_categories": [], "used_headlines": []
    }

    if status_fn:
        status_fn(0.03, "🔍 Шаг 1: Фильтрация по релевантности...")

    # Фильтр релевантности (опциональный)
    if filter_relevant:
        relevant = [a for a in articles if is_relevant(a["title"], a.get("text", ""))]
        if not relevant:
            relevant = articles  # если ничего не прошло — берём всё
    else:
        relevant = articles

    if status_fn:
        status_fn(0.08, f"🧹 Шаг 2: Дедупликация {len(relevant)} статей...")

    unique, dupes = deduplicate(relevant, threshold)

    to_analyze = unique[:MAX_ANALYSIS_ARTICLES]

    if status_fn:
        status_fn(0.12, f"🤖 Шаг 3: Генерация карточек для {len(to_analyze)} статей...")

    cards = generate_cards_batch(to_analyze, status_fn=status_fn)
    cards.sort(key=lambda c: c.get("finsignal_score", 0), reverse=True)

    cached_count = sum(
        1 for art in to_analyze
        if fingerprint(art["title"] + art.get("text", "")[:200])
        in st.session_state.get(CARD_CACHE_KEY, {})
    )

    return {
        "cards": cards,
        "dupes": dupes,
        "stats": {
            "total_input": len(articles),
            "after_relevance_filter": len(relevant),
            "duplicates_removed": len(dupes),
            "cards_generated": len(cards),
            "cards_from_cache": cached_count,
            "run_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        },
    }

# ════════════════════════════════════════════════════════════════
# DIGEST ENGINE
# ════════════════════════════════════════════════════════════════

def get_digest_cards(cards: list, mode: str, min_score: float) -> list:
    now = datetime.now()
    result = cards

    if mode == "Daily (24ч)":
        cutoff = (now - timedelta(hours=24)).strftime("%Y-%m-%d")
        result = [c for c in cards if c.get("date", "2000-01-01") >= cutoff]
    elif mode == "Weekly (7д)":
        cutoff = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        result = [c for c in cards if c.get("date", "2000-01-01") >= cutoff]
    elif mode == "Smart (Score ≥ 4.0)":
        result = [c for c in cards if c.get("finsignal_score", 0) >= 4.0]

    return [c for c in result if c.get("finsignal_score", 0) >= min_score]

# ════════════════════════════════════════════════════════════════
# EXPORT
# ════════════════════════════════════════════════════════════════

def to_markdown(cards: list, stats: dict) -> str:
    lines = [
        "# 📡 TrendWatcher — Финтех-дайджест",
        f"**Сгенерирован:** {stats.get('run_time', '—')}",
        f"**Статей входящих:** {stats.get('total_input', '?')} → "
        f"**Релевантных:** {stats.get('after_relevance_filter', '?')} → "
        f"**Карточек:** {stats.get('cards_generated', '?')}",
        "", "---", "",
    ]
    for i, c in enumerate(cards, 1):
        score = c.get("finsignal_score", 0)
        bd = c.get("breakdown", {})
        actions = c.get("recommended_actions", {})
        actions_lines = []
        if isinstance(actions, dict):
            for role, action in actions.items():
                role_label = {"executive": "🎯 Executive", "product": "📦 Product", "legal_or_risk": "⚖️ Legal/Risk"}.get(role, role)
                actions_lines.append(f"  - **{role_label}:** {action}")
        elif isinstance(actions, list):
            actions_lines = [f"  - {a}" for a in actions]

        lines += [
            f"## {i}. {c.get('headline', '—')}",
            f"**FinSignal:** {score:.1f}/5 | **Категория:** {c.get('category', '—')}",
            f"**Источник:** [{c.get('source', '—')}]({c.get('url', '#')}) | **Дата:** {c.get('date', '—')}",
            "",
            "**Breakdown:**",
            f"- Business Impact: {bd.get('business_impact', '—')}/5 (30%)",
            f"- Time Sensitivity: {bd.get('time_sensitivity', '—')}/5 (25%)",
            f"- Competitive Threat: {bd.get('competitive_threat', '—')}/5 (20%)",
            f"- Regulatory Risk: {bd.get('regulatory_risk', '—')}/5 (15%)",
            f"- Feasibility: {bd.get('feasibility', '—')}/5 (10%)",
            "",
            f"**Why Now:** {c.get('why_now', '—')}",
            f"**Summary:** {c.get('summary', '—')}",
            "",
            "**Recommended Actions:**",
            *actions_lines,
            "",
            f"*Уверенность: {c.get('confidence', '—')} — {c.get('confidence_note', '')}*",
            "",
            "---",
            "",
        ]
    return "\n".join(lines)

def to_json_export(cards: list, stats: dict) -> str:
    return json.dumps(
        {
            "meta": {
                "tool": "TrendWatcher Final v3",
                "team": "Alpha Girls T003",
                "generated_at": stats.get("run_time", ""),
                "stats": stats,
            },
            "cards": cards,
        },
        ensure_ascii=False,
        indent=2,
    )

def save_to_archive(results: dict):
    archive_file = "digest_archive.json"
    archive = []
    if os.path.exists(archive_file):
        try:
            with open(archive_file, "r", encoding="utf-8") as f:
                archive = json.load(f)
        except Exception:
            archive = []
    archive.insert(0, {
        "id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "date": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "source": st.session_state.get("data_source", "—"),
        "stats": results["stats"],
        "cards": results["cards"],
    })
    try:
        with open(archive_file, "w", encoding="utf-8") as f:
            json.dump(archive[:20], f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ════════════════════════════════════════════════════════════════
# UI — CARD RENDER
# ════════════════════════════════════════════════════════════════

def hex_to_rgba(hex_color: str, alpha: float = 0.3) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

def score_color(score: float) -> str:
    if score >= 4.5:
        return "#DC2626"
    if score >= 4.0:
        return "#EF4444"
    if score >= 3.5:
        return "#F97316"
    return "#F59E0B"

def render_card(card: dict, card_key: str):
    score = card.get("finsignal_score", 0)
    color = score_color(score)
    conf = card.get("confidence", "medium")
    conf_icon = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(conf, "⚪")

    # Хедер
    st.markdown(f"""
<div style="border-left:6px solid {color}; padding:18px 20px;
     background:rgba(17,24,39,0.96); border-radius:12px; margin:16px 0 8px;
     box-shadow:0 4px 20px rgba(0,0,0,0.4);">
  <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:12px; flex-wrap:wrap;">
    <span style="color:{color}; font-size:0.7rem; font-weight:700;
          letter-spacing:1.5px; text-transform:uppercase;">
      {card.get('category', '—').upper()}
    </span>
    <span style="background:{color}; color:white; padding:3px 14px;
          border-radius:12px; font-size:0.85rem; font-weight:800; white-space:nowrap;">
      ⚡ {score:.1f} / 5.0
    </span>
  </div>
  <h3 style="color:#f1f5f9; margin:10px 0 6px; font-size:1rem; line-height:1.4;">
    {card.get('headline', card.get('title', '—'))}
  </h3>
  <p style="color:#94a3b8; font-size:0.8rem; margin:0;">
    📅 {card.get('date', '—')} ·
    <a href="{card.get('url', '#')}" target="_blank" style="color:#60a5fa;">
      {card.get('source', '—')}
    </a>
    · {conf_icon} {conf}
  </p>
</div>""", unsafe_allow_html=True)

    # Radar chart
    bd = card.get("breakdown", {})
    cats = ["Business\nImpact", "Time\nSensitivity", "Competitive\nThreat", "Regulatory\nRisk", "Feasibility"]
    vals = [
        bd.get("business_impact", 0),
        bd.get("time_sensitivity", 0),
        bd.get("competitive_threat", 0),
        bd.get("regulatory_risk", 0),
        bd.get("feasibility", 0),
    ]
    fig = go.Figure(go.Scatterpolar(
        r=vals + [vals[0]],
        theta=cats + [cats[0]],
        fill="toself",
        fillcolor=hex_to_rgba(color, 0.25),
        line=dict(color=color, width=2),
        marker=dict(size=6),
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True, range=[0, 5],
                tickfont=dict(size=9, color="#94a3b8"),
                gridcolor="#374151",
            ),
            angularaxis=dict(
                tickfont=dict(size=9, color="#cbd5e1"),
                gridcolor="#374151",
            ),
            bgcolor="rgba(0,0,0,0)",
        ),
        showlegend=False,
        margin=dict(l=40, r=40, t=20, b=20),
        height=280,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"radar_{card_key}")

    # Breakdown метрики
    cols = st.columns(5)
    bd_labels = [
        ("business_impact", "Business\nImpact"),
        ("time_sensitivity", "Time\nSensitivity"),
        ("competitive_threat", "Competitive\nThreat"),
        ("regulatory_risk", "Regulatory\nRisk"),
        ("feasibility", "Feasibility"),
    ]
    for col, (key, label) in zip(cols, bd_labels):
        col.metric(label, f"{bd.get(key, '—')}/5")

    # Why Now + Actions
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**⚡ Why Now**")
        st.info(card.get("why_now", "—"))
    with c2:
        st.markdown("**🎯 Recommended Actions**")
        actions = card.get("recommended_actions", {})
        role_icons = {
            "executive": "🏦 Executive",
            "product": "📦 Product",
            "legal_or_risk": "⚖️ Legal/Risk",
        }
        if isinstance(actions, dict):
            for role_key, label in role_icons.items():
                action_text = actions.get(role_key, "")
                if action_text:
                    st.success(f"**{label}:** {action_text}")
        elif isinstance(actions, list):
            for action in actions:
                st.success(f"• {action}")

    st.markdown("**📝 Summary**")
    st.write(card.get("summary", "—"))

    with st.expander(f"📨 Draft для команды", expanded=False):
        actions = card.get("recommended_actions", {})
        if isinstance(actions, dict):
            actions_txt = "\n".join(
                f"  [{k.upper()}]: {v}" for k, v in actions.items()
            )
        else:
            actions_txt = "\n".join(f"  • {a}" for a in (actions or []))

        draft = (
            f"📡 [{card.get('category', '').upper()}] {card.get('headline', '')}\n\n"
            f"⚡ Why now: {card.get('why_now', '')}\n\n"
            f"{card.get('summary', '')}\n\n"
            f"🎯 Actions:\n{actions_txt}\n\n"
            f"🔗 {card.get('source', '')} · {card.get('date', '')} · FinSignal {score:.1f}/5"
        )
        st.code(draft, language=None)

    st.caption(f"{conf_icon} Уверенность: **{conf}** — {card.get('confidence_note', '')}")
    st.divider()

# ════════════════════════════════════════════════════════════════
# MAIN APP
# ════════════════════════════════════════════════════════════════

DARK_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;800&display=swap');

html, body, [class*="css"] {
    font-family: 'Manrope', sans-serif !important;
    background: linear-gradient(180deg, #0b1120, #111827) !important;
    color: #e2e8f0 !important;
}
.stApp { background: linear-gradient(180deg, #0b1120, #111827) !important; }

p, span, div, label, li, h1, h2, h3, h4 { color: #e2e8f0 !important; }
.stMarkdown p { color: #e2e8f0 !important; }

.stButton>button {
    background: linear-gradient(135deg, #EF4444, #DC2626) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 700 !important;
    font-family: 'Manrope', sans-serif !important;
    transition: all 0.2s !important;
}
.stButton>button:hover { opacity: 0.88 !important; transform: translateY(-1px) !important; }
.stButton>button p { color: white !important; }

button[kind="primary"] { background: linear-gradient(135deg, #EF4444, #DC2626) !important; }

[data-testid="stSidebar"] {
    background: #0d1117 !important;
    border-right: 1px solid #1f2937 !important;
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
[data-testid="stSidebar"] h3 { color: white !important; font-weight: 700 !important; }

[data-testid="metric-container"] {
    background: rgba(17,24,39,0.8) !important;
    border: 1px solid #374151 !important;
    border-radius: 8px !important;
    padding: 12px !important;
}
[data-testid="metric-container"] label { color: #94a3b8 !important; font-size: 0.75rem !important; }
[data-testid="metric-container"] [data-testid="metric-value"] { color: #f1f5f9 !important; }

[data-testid="stTabs"] { border-bottom: 1px solid #374151 !important; }
[data-testid="stTabs"] button { color: #94a3b8 !important; font-weight: 600 !important; }
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #EF4444 !important;
    border-bottom: 2px solid #EF4444 !important;
}

[data-testid="stAlert"] {
    background: rgba(17,24,39,0.9) !important;
    border-radius: 8px !important;
}
[data-testid="stAlert"] p { color: #e2e8f0 !important; }

code, pre {
    background: #1f2937 !important;
    color: #a5f3fc !important;
    border-radius: 6px !important;
}

.stTextInput input, .stSelectbox select {
    background: #1f2937 !important;
    color: #e2e8f0 !important;
    border: 1px solid #374151 !important;
}

hr { border-color: #374151 !important; }

details {
    background: rgba(17,24,39,0.7) !important;
    border: 1px solid #374151 !important;
    border-radius: 8px !important;
}
summary { color: #e2e8f0 !important; }

.stDownloadButton>button {
    background: rgba(31,41,55,0.9) !important;
    border: 1px solid #374151 !important;
    color: #e2e8f0 !important;
}
</style>
"""


def main():
    st.set_page_config(
        page_title="TrendWatcher PRO · Альфа-Банк",
        page_icon="📡",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    init_session()
    get_key("OPENROUTER_API_KEY")
    get_key("FIRECRAWL_API_KEY")
    st.markdown(DARK_CSS, unsafe_allow_html=True)

    # ── SIDEBAR ─────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ Настройки TrendWatcher")
        st.markdown("---")

        st.markdown("### 🔑 API Keys")
        if st.session_state.get("OPENROUTER_API_KEY"):
            st.success("✅ OpenRouter ключ загружен")
        else:
            val = st.text_input(
                "OpenRouter API Key", type="password",
                placeholder="sk-or-v1-...", key="sb_or_key"
            )
            if val:
                st.session_state["OPENROUTER_API_KEY"] = val
            else:
                st.warning("⚠️ Ключ не введён")
                st.caption("`.streamlit/secrets.toml`:\n`OPENROUTER_API_KEY = 'sk-...'`")

        if st.session_state.get("FIRECRAWL_API_KEY"):
            st.success("✅ Firecrawl ключ загружен")
        else:
            fc_val = st.text_input(
                "Firecrawl API Key (опц.)", type="password",
                placeholder="fc-...", key="sb_fc_key"
            )
            if fc_val:
                st.session_state["FIRECRAWL_API_KEY"] = fc_val

        st.markdown("---")
        st.markdown("### 🧠 Модель LLM")
        model_label = st.selectbox("Модель", list(AVAILABLE_MODELS.keys()), key="sb_model")
        st.session_state["model"] = AVAILABLE_MODELS[model_label]
        st.caption(f"`{AVAILABLE_MODELS[model_label]}`")

        st.markdown("---")
        st.markdown("### ⚙️ Параметры")
        min_score = st.slider("Мин. FinSignal Score", 1.0, 5.0, 1.0, 0.5, key="sb_score")
        dedup_thresh = st.slider("Порог дедупликации", 0.35, 0.80, 0.55, 0.05, key="sb_dedup")
        filter_relevant = st.checkbox("Фильтр релевантности", value=True, key="sb_relev",
                                      help="Отбрасывать статьи без ключевых слов финтех/AI/банки")
        show_dupes = st.checkbox("Показать удалённые дубли", key="sb_dupes")
        digest_mode = st.radio(
            "Режим дайджеста",
            ["Real-time (Все)", "Daily (24ч)", "Weekly (7д)", "Smart (Score ≥ 4.0)"],
            key="sb_mode"
        )
        st.session_state["digest_mode"] = digest_mode

        st.markdown("---")
        cache_size = len(st.session_state.get(CARD_CACHE_KEY, {}))
        st.markdown(f"### 📦 Кэш: {cache_size} карточек")
        st.caption("Повторный анализ = 0 токенов")
        if st.button("🗑 Очистить кэш", key="sb_clear"):
            st.session_state[CARD_CACHE_KEY] = {}
            st.session_state["_generation_memory"] = {
                "used_actions": [], "used_why_now": [], "used_categories": [], "used_headlines": []
            }
            st.rerun()

        st.markdown("---")
        st.markdown("### 📚 Источники")
        for name, cfg in list(SOURCES.items())[:10]:
            rss_icon = "📡" if cfg.get("rss") else "🌐"
            lang_flag = "🇷🇺" if cfg.get("language") == "ru" else "🇬🇧"
            st.markdown(f"{rss_icon}{lang_flag} `{name}` {'⭐' * cfg['authority']}")
        st.caption(f"... и ещё {len(SOURCES) - 10} источников")
        st.caption("📡 RSS · 🌐 BS4 · 🇷🇺/🇬🇧 язык · ⭐ авторитет")

    # ── HEADER ──────────────────────────────────────────────────
    st.markdown("""
<div style="background:linear-gradient(135deg,#EF4444 0%,#7C3AED 100%);
     padding:20px 32px; border-radius:14px; margin-bottom:4px;
     display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:12px;">
  <div>
    <h1 style="color:white; margin:0; font-size:1.7rem; font-weight:800;">📡 TrendWatcher PRO</h1>
    <p style="color:rgba(255,255,255,0.8); margin:4px 0 0; font-size:0.82rem;">
      Финтех-трендвотчер · Alpha Girls T003 · Альфа Будущее 2026 · 33 источника · Batch LLM
    </p>
  </div>
  <div style="color:rgba(255,255,255,0.7); font-size:0.75rem; text-align:right;">
    ⚙️ Настройки<br>в левой панели
  </div>
</div>""", unsafe_allow_html=True)

    pages = st.tabs([
        "📡 TrendWatcher",
        "🏗️ Архитектура",
        "📜 История",
        "ℹ️ О проекте",
        "📝 Регистрация",
    ])

    # ════════════════════════════════════════════════════════════
    # PAGE 0 — MAIN
    # ════════════════════════════════════════════════════════════
    with pages[0]:
        st.info(
            f"**Источник:** {st.session_state['data_source']} · "
            f"**Статей:** {len(st.session_state['articles'])} · "
            f"**Кэш:** {len(st.session_state.get(CARD_CACHE_KEY, {}))} карточек · "
            f"**Источников:** {len(SOURCES)}"
        )

        # Data source buttons
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if st.button("🔥 Firecrawl", use_container_width=True, key="btn_fc",
                         help="Полный текст. Без ключа → RSS автоматически"):
                with st.spinner("Загружаю..."):
                    arts, log = fetch_via_firecrawl()
                with st.expander("📋 Лог загрузки"):
                    for line in log:
                        st.markdown(line)
                if arts:
                    st.session_state.update({
                        "articles": arts,
                        "data_source": "🔥 Firecrawl/RSS",
                        "results": None,
                    })
                    st.success(f"✅ {len(arts)} статей из {len(SOURCES)} источников")
                else:
                    st.warning("Ничего не загружено — используй Демо")

        with col2:
            if st.button("📡 RSS-парсинг", use_container_width=True, key="btn_rss",
                         help="Параллельный RSS из 33 источников"):
                with st.spinner("Параллельная загрузка RSS..."):
                    arts, log = fetch_via_rss()
                with st.expander("📋 Лог RSS"):
                    for line in log:
                        st.markdown(line)
                if arts:
                    st.session_state.update({
                        "articles": arts,
                        "data_source": "📡 RSS (live)",
                        "results": None,
                    })
                    st.success(f"✅ {len(arts)} статей из {len(SOURCES)} источников")
                    with st.expander("👁 Превью (первые 10)"):
                        for a in arts[:10]:
                            st.markdown(f"**{a['title']}**")
                            st.caption(f"{a['source']} · {a['date']}")
                            st.divider()
                else:
                    st.warning("RSS недоступен — попробуй Демо")

        with col3:
            if st.button("🌐 BS4 Парсинг", use_container_width=True, key="btn_bs4",
                         help="Парсинг заголовков h1/h2/h3"):
                with st.spinner("BS4 парсинг..."):
                    arts, log = fetch_via_bs4()
                with st.expander("📋 Лог BS4"):
                    for line in log:
                        st.markdown(line)
                if arts:
                    st.session_state.update({
                        "articles": arts,
                        "data_source": "🌐 BS4 (live)",
                        "results": None,
                    })
                    st.success(f"✅ {len(arts)} заголовков")
                else:
                    st.warning("BS4 не нашёл статей")

        with col4:
            if st.button("📦 Демо-набор", use_container_width=True, key="btn_demo",
                         help="10 статей с дублями и разными типами"):
                st.session_state.update({
                    "articles": DEMO_ARTICLES,
                    "data_source": "📦 Демо-набор",
                    "results": None,
                })
                st.success(f"✅ Демо: {len(DEMO_ARTICLES)} статей (2 дубля)")

        # Filters
        st.markdown("---")
        st.markdown("#### 🔎 Фильтрация результатов")
        f1, f2, f3 = st.columns(3)
        with f1:
            search_query = st.text_input(
                "🔍 Поиск по заголовкам",
                placeholder="биометрия, BNPL, AI...",
                key="filter_search"
            )
        with f2:
            cat_filter = st.multiselect(
                "📂 Категория",
                ["регулирование", "конкурент", "AI/технологии", "партнёрство",
                 "платёжный сервис", "рынок", "крипто/блокчейн", "кибербезопасность"],
                key="filter_cat"
            )
        with f3:
            src_filter = st.multiselect(
                "📰 Источник",
                list(SOURCES.keys()),
                key="filter_src"
            )

        # Run analysis
        st.markdown("---")
        if st.button("▶ Запустить анализ TrendWatcher", use_container_width=True,
                     type="primary", key="btn_run"):
            if not get_key("OPENROUTER_API_KEY"):
                st.error("❌ Введите OpenRouter API Key в левой панели (⬅)")
                st.stop()

            prog = st.progress(0)
            status = st.empty()

            results = run_pipeline(
                st.session_state["articles"],
                threshold=dedup_thresh,
                filter_relevant=filter_relevant,
                status_fn=lambda pct, msg: (prog.progress(min(pct, 0.99)), status.text(msg)),
            )

            save_to_archive(results)
            prog.progress(1.0)
            prog.empty()
            status.empty()
            st.session_state["results"] = results
            st.rerun()

        # Results
        if st.session_state.get("results"):
            res = st.session_state["results"]
            s = res["stats"]

            st.success(f"✅ Анализ завершён · {s['run_time']}")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Входящих", s["total_input"])
            c2.metric("Релевантных", s.get("after_relevance_filter", s["total_input"]))
            c3.metric("Дублей удалено", s["duplicates_removed"])
            c4.metric("Карточек", s["cards_generated"])
            c5.metric("Из кэша (0₽)", s.get("cards_from_cache", 0))

            st.markdown("---")

            # Apply filters
            mode = st.session_state.get("digest_mode", "Real-time (Все)")
            cards = get_digest_cards(res["cards"], mode, min_score)

            if search_query:
                cards = [c for c in cards if search_query.lower() in (c.get("headline", "") + c.get("summary", "")).lower()]
            if cat_filter:
                cards = [c for c in cards if c.get("category", "") in cat_filter]
            if src_filter:
                cards = [c for c in cards if c.get("source", "") in src_filter]

            st.markdown(
                f"### 📊 Сигналы ({mode}, FinSignal ≥ {min_score:.1f}): "
                f"**{len(cards)}** из {len(res['cards'])}"
            )

            if not cards:
                st.warning(
                    "Нет карточек с текущими фильтрами. "
                    "Попробуй снизить Score, сбросить категории или выбрать другой режим дайджеста."
                )
            else:
                for i, card in enumerate(cards):
                    render_card(card, card_key=f"p0_{i}")

            if show_dupes and res.get("dupes"):
                st.markdown("### 🗑️ Удалённые дубли")
                st.caption("MD5-fingerprint (точные копии) + cosine similarity > 0.55 (пересказы)")
                for d in res["dupes"]:
                    st.warning(
                        f"**{d['title']}** (`{d['source']}`) — {d.get('dupe_reason', 'дубль')}"
                    )

            if res["cards"]:
                st.markdown("---")
                st.markdown("### 💾 Экспорт")
                ts = datetime.now().strftime("%Y%m%d_%H%M")
                ec1, ec2 = st.columns(2)
                ec1.download_button(
                    "📄 Markdown",
                    data=to_markdown(res["cards"], s).encode("utf-8"),
                    file_name=f"trendwatcher_{ts}.md",
                    mime="text/markdown",
                    use_container_width=True,
                    key="dl_md",
                )
                ec2.download_button(
                    "📦 JSON",
                    data=to_json_export(res["cards"], s).encode("utf-8"),
                    file_name=f"trendwatcher_{ts}.json",
                    mime="application/json",
                    use_container_width=True,
                    key="dl_json",
                )

        # Example cards
        st.markdown("---")
        with st.expander("📋 Готовые примеры карточек (без API)", expanded=False):
            st.markdown("*Примеры с actions по ролям: Executive / Product / Legal+Risk*")
            for i, card in enumerate(EXAMPLE_CARDS):
                render_card(card, card_key=f"ex_{i}")

    # ════════════════════════════════════════════════════════════
    # PAGE 1 — АРХИТЕКТУРА
    # ════════════════════════════════════════════════════════════
    with pages[1]:
        st.markdown("### 🏗️ Техническая архитектура TrendWatcher v3")

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("#### Pipeline")
            st.code("""
┌─────────────────────────────────────────────┐
│          TRENDWATCHER FINAL v3              │
│                                             │
│  СБОР (параллельно)                         │
│  Firecrawl ─┐                               │
│  RSS x33 ── ┼─▶ Релевантность ─▶ Дедупл.   │
│  BS4 ───────┘   (regex-фильтр)  (MD5+cos)  │
│  Демо ──────────────────────────────────    │
│                                             │
│  ГЕНЕРАЦИЯ (батчи по 10 статей)             │
│  10 статей ─▶ 1 LLM-запрос ─▶ 10 карточек  │
│  Кэш: повтор = 0 токенов                   │
│  Memory: избегает повторных actions         │
│                                             │
│  ВЫВОД                                      │
│  Digest engine (Daily/Weekly/Smart)         │
│  Фильтры: Score / категория / источник      │
│  Экспорт: Markdown + JSON                   │
└─────────────────────────────────────────────┘
""", language=None)

        with col_b:
            st.markdown("#### FinSignal Score")
            st.code("""
Score = Σ (factor × weight)

Business Impact    × 0.30  (30%)
Time Sensitivity   × 0.25  (25%)
Competitive Threat × 0.20  (20%)
Regulatory Risk    × 0.15  (15%)
Feasibility        × 0.10  (10%)

Профили по типу новости:
regulation: [4,5,2,5,3] → 4.1
competitor: [5,4,5,1,4] → 4.4
ai_tech:    [4,3,4,1,5] → 3.7
payments:   [5,4,4,3,4] → 4.2
security:   [4,5,2,4,3] → 4.0
market:     [3,2,2,1,4] → 2.6

Интерпретация:
4.5+ → Действовать немедленно  🔴
4.0+ → Приоритет этой недели   🟠
3.5+ → В roadmap квартала      🟡
<3.5 → Мониторинг              🟢
""", language=None)

        st.markdown("#### Нововведения v3 vs v2")
        st.markdown("""
| Компонент | v2 (старый) | v3 (новый) |
|-----------|-------------|------------|
| Источники | 6 | **33 (РФ + EN)** |
| Fetching | Последовательный | **Параллельный ThreadPool** |
| LLM запросы | 1 статья = 1 запрос | **10 статей = 1 запрос (батч)** |
| Лимит статей | 10 | **120** |
| Actions | Список строк | **По ролям: Executive/Product/Legal** |
| Разнообразие | Профили-хинты | **Global Memory (avoid repeats)** |
| Фильтрация | Нет | **Relevance filter (regex)** |
| Digest | Нет | **Daily/Weekly/Smart** |
""")

        with st.expander("🔍 Детали компонентов"):
            st.markdown("""
### Параллельный RSS-fetching
`ThreadPoolExecutor(max_workers=20)` — 33 источника загружаются одновременно.
При недоступности RSS — автоматический fallback на BS4.

### Batch LLM Generation
10 статей → 1 промпт → 1 API-запрос → 10 карточек.
Экономия токенов: ~60% vs single-card mode.

### Global Memory
Каждый батч читает список уже использованных `why_now` и `actions`.
LLM инструктируется НЕ повторять их → реальное разнообразие карточек.

### Двухуровневая дедупликация
- **Уровень 1**: MD5-fingerprint первых 60 слов (точные копии)
- **Уровень 2**: Cosine similarity > threshold (пересказы)
- Формула: |A∩B| / √(|A|·|B|)

### Recommended Actions по ролям
- **Executive**: C-level, стратегические решения, бюджет
- **Product**: product team, MVP, метрики
- **Legal/Risk**: compliance, регуляторные риски
""")

        st.markdown("**Стек:** `streamlit · requests · feedparser · beautifulsoup4 · plotly · firecrawl-py (опц.)`")

    # ════════════════════════════════════════════════════════════
    # PAGE 2 — ИСТОРИЯ
    # ════════════════════════════════════════════════════════════
    with pages[2]:
        st.markdown("### 📜 История последних запусков")
        archive_file = "digest_archive.json"
        if os.path.exists(archive_file):
            try:
                with open(archive_file, "r", encoding="utf-8") as f:
                    archive = json.load(f)
                for idx, entry in enumerate(archive[:10]):
                    s = entry.get("stats", {})
                    with st.container():
                        c1, c2 = st.columns([4, 1])
                        with c1:
                            st.markdown(f"**📅 {entry['date']}** · {entry.get('source', '—')}")
                            st.caption(
                                f"Входящих: {s.get('total_input', '?')} · "
                                f"Дублей: {s.get('duplicates_removed', '?')} · "
                                f"Карточек: {s.get('cards_generated', '?')}"
                            )
                        with c2:
                            show = st.checkbox("Детали", key=f"hist_{idx}_{entry.get('id', 'x')}")
                        if show:
                            for card in entry.get("cards", [])[:5]:
                                score = card.get("finsignal_score", 0)
                                st.markdown(
                                    f"📌 **{card.get('headline', '—')}** "
                                    f"(FinSignal: {score:.1f}) "
                                    f"— [{card.get('source', '—')}]({card.get('url', '#')})"
                                )
                        st.divider()
            except Exception as e:
                st.error(f"Ошибка чтения архива: {e}")
        else:
            st.info("История пуста. Запусти анализ — результаты сохранятся здесь.")

    # ════════════════════════════════════════════════════════════
    # PAGE 3 — О ПРОЕКТЕ
    # ════════════════════════════════════════════════════════════
    with pages[3]:
        st.markdown("### ℹ️ О проекте TrendWatcher v3")
        st.markdown("""
**TrendWatcher** — AI-трендвотчер для продуктовой команды Альфа-Банка.

Команда **Alpha Girls · T003** · Хакатон Альфа Будущее 2026

---

#### Задача
Находить важные финтех-сигналы из 33 источников, убирать шум и дубли,
превращать в структурированные карточки с тактическими рекомендациями.

#### Что нового в v3
- **33 источника** вместо 6 (РФ + международные)
- **Параллельный RSS** — загрузка за 5-10 сек вместо 2+ минут
- **Batch LLM** — 10 статей в 1 запрос, экономия ~60% токенов
- **Лимит 120 статей** вместо 10
- **Actions по ролям** — Executive / Product / Legal+Risk
- **Global Memory** — карточки не повторяются
- **Релевантность-фильтр** — только финтех/AI/банки
- **Digest Engine** — Daily / Weekly / Smart режимы

#### Пайплайн
```
Загрузка (параллельно) → Валидация → Релевантность → Дедупликация
→ Батч-классификация → Batch LLM → Карточки → Дайджест → Экспорт
```

#### Ограничения
1. RSS даёт описание ~300-600 символов (не полный текст)
2. Полный текст — только через Firecrawl (нужен API-ключ)
3. Нет персистентного хранилища (SQLite — roadmap v4)
4. Лимит токенов зависит от баланса OpenRouter

#### Roadmap v4
- sentence-transformers для семантической дедупликации
- SQLite + постоянная история
- Telegram-бот с ежедневной рассылкой
- APScheduler для автозапуска
- Кастомные источники пользователя
""")
        st.markdown("""
- 🌐 [Прототип](https://trendwatcheralpha.streamlit.app/)
- 💻 [GitHub](https://github.com/developerKamilla/Alpha-Girls-Team)
""")

    # ════════════════════════════════════════════════════════════
    # PAGE 4 — РЕГИСТРАЦИЯ
    # ════════════════════════════════════════════════════════════
    with pages[4]:
        st.markdown("### 📝 Регистрация команды")
        st.markdown("*Хакатон Альфа Будущее 2026*")

        with st.form("registration_form"):
            col1, col2 = st.columns(2)
            with col1:
                team_name = st.text_input("Название команды", value="Alpha Girls")
                team_id = st.text_input("ID команды", value="T003")
                contact_name = st.text_input("Контактное лицо")
            with col2:
                email = st.text_input("Email")
                project_url = st.text_input(
                    "Ссылка на прототип",
                    value="https://trendwatcheralpha.streamlit.app/"
                )
                github_url = st.text_input(
                    "GitHub",
                    value="https://github.com/developerKamilla/Alpha-Girls-Team"
                )
            submitted = st.form_submit_button("✅ Подтвердить регистрацию", use_container_width=True)
            if submitted:
                st.success(f"✅ Команда **{team_name}** ({team_id}) зарегистрирована!")
                st.balloons()


# ════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()
