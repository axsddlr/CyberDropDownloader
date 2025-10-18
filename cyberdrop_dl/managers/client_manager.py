from __future__ import annotations

import asyncio
import contextlib
import ssl
import time
import weakref
from base64 import b64encode
from collections import defaultdict
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Literal, Self, overload

import aiohttp
import certifi
import truststore
from aiohttp import ClientResponse, ClientSession
from aiohttp_client_cache.response import CachedResponse
from aiohttp_client_cache.session import CachedSession
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from videoprops import get_audio_properties, get_video_properties

from cyberdrop_dl import constants, env
from cyberdrop_dl.clients.download_client import DownloadClient
from cyberdrop_dl.clients.flaresolverr import FlareSolverr
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.clients.scraper_client import ScraperClient
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
from cyberdrop_dl.exceptions import (
    DDOSGuardError,
    DownloadError,
    ScrapeError,
    TooManyCrawlerErrors,
)
from cyberdrop_dl.ui.prompts.user_prompts import get_cookies_from_browsers
from cyberdrop_dl.utils.cookie_management import read_netscape_files
from cyberdrop_dl.utils.logger import log, log_debug, log_spacer

_VALID_EXTENSIONS = (
    constants.FILE_FORMATS["Images"] | constants.FILE_FORMATS["Videos"] | constants.FILE_FORMATS["Audio"]
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator, Iterable, Mapping
    from http.cookies import BaseCookie

    from aiohttp_client_cache.response import CachedResponse
    from curl_cffi.requests import AsyncSession
    from curl_cffi.requests.models import Response as CurlResponse

    from cyberdrop_dl.managers.manager import Manager

_curl_import_error = None
try:
    from curl_cffi.requests import AsyncSession  # noqa: TC002
except ImportError as e:
    _curl_import_error = e

_DOWNLOAD_ERROR_ETAGS = {
    "d835884373f4d6c8f24742ceabe74946": "Imgur image has been removed",
    "65b7753c-528a": "SC Scrape Image",
    "5c4fb843-ece": "PixHost Removed Image",
    "637be5da-11d2b": "eFukt Video removed",
    "63a05f27-11d2b": "eFukt Video removed",
    "5a56b09d-1485eb": "eFukt Video removed",
}

_crawler_errors: dict[str, int] = defaultdict(int)


if TYPE_CHECKING:
    from cyberdrop_dl.managers.manager import Manager

_null_context = contextlib.nullcontext()


class DownloadSpeedLimiter(AsyncLimiter):
    __slots__ = (*AsyncLimiter.__slots__, "chunk_size")

    max_rate: int

    def __init__(self, speed_limit: int) -> None:
        self.chunk_size: int = 1024 * 1024 * 10  # 10MB
        if speed_limit:
            self.chunk_size = min(self.chunk_size, speed_limit)
        super().__init__(speed_limit, 1)

    async def acquire(self, amount: float | None = None) -> None:
        if self.max_rate <= 0:
            return
        if not amount:
            amount = self.chunk_size
        await super().acquire(amount)

    def __repr__(self):
        return f"{self.__class__.__name__}(speed_limit={self.max_rate}, chunk_size={self.chunk_size})"


class DDosGuard:
    TITLES = ("Just a moment...", "DDoS-Guard")
    SELECTORS = (
        "#cf-challenge-running",
        ".ray_id",
        ".attack-box",
        "#cf-please-wait",
        "#challenge-spinner",
        "#trk_jschal_js",
        "#turnstile-wrapper",
        ".lds-ring",
    )
    ALL_SELECTORS = ", ".join(SELECTORS)


class CloudflareTurnstile:
    TITLES = ("Simpcity Cuck Detection", "Attention Required! | Cloudflare", "Sentinel CAPTCHA")
    SELECTORS = (
        "captchawrapper",
        "cf-turnstile",
        "script[src*='challenges.cloudflare.com/turnstile']",
        "script:-soup-contains('Dont open Developer Tools')",
    )
    ALL_SELECTORS = ", ".join(SELECTORS)


class FileLocksVault:
    """File locks vault with automatic cleanup of unused locks to prevent memory leaks."""

    def __init__(self) -> None:
        self._locked_files: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # Cleanup every 5 minutes
        self._main_lock = asyncio.Lock()

    @contextlib.asynccontextmanager
    async def get_lock(self, filename: str) -> AsyncGenerator:
        """Get filelock for the provided filename. Creates one if none exists"""
        log_debug(f"Checking lock for '{filename}'", 20)

        async with self._main_lock:
            if filename not in self._locked_files:
                log_debug(f"Lock for '{filename}' does not exist, creating new one", 20)
                lock = asyncio.Lock()
                self._locked_files[filename] = lock
            else:
                log_debug(f"Lock for '{filename}' already exists", 20)

            file_lock = self._locked_files[filename]

        async with file_lock:
            log_debug(f"Lock for '{filename}' acquired", 20)
            yield
            log_debug(f"Lock for '{filename}' released", 20)

    def force_cleanup(self) -> int:
        """Force cleanup of unused locks and return number of locks removed.

        Note: WeakValueDictionary automatically removes entries when locks are garbage collected,
        so this method primarily serves to update the cleanup timestamp and report stats."""
        initial_count = len(self._locked_files)
        # WeakValueDictionary automatically cleans up, but we can trigger a check
        # by iterating (this will remove any dead weakrefs)
        _ = list(self._locked_files.keys())
        final_count = len(self._locked_files)
        removed = initial_count - final_count

        self._last_cleanup = time.time()
        if removed > 0:
            log_debug(f"Force cleanup removed {removed} unused locks", 10)
        return removed

    def get_stats(self) -> dict[str, int]:
        """Get statistics about the locks vault."""
        return {
            "total_locks": len(self._locked_files),
            "seconds_since_last_cleanup": int(time.time() - self._last_cleanup),
        }


class ClientManager:
    """Creates a 'client' that can be referenced by scraping or download sessions."""

    def __init__(self, manager: Manager) -> None:
        self.manager = manager
        ssl_context = self.manager.global_config.general.ssl_context
        if not ssl_context:
            self.ssl_context = False
        elif ssl_context == "certifi":
            self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        elif ssl_context == "truststore":
            self.ssl_context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        elif ssl_context == "truststore+certifi":
            self.ssl_context = ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.load_verify_locations(cafile=certifi.where())

        self.cookies = aiohttp.CookieJar(quote_cookie=False)
        self.rate_limits: dict[str, AsyncLimiter] = {}
        self.download_slots: dict[str, int] = {}
        self.global_rate_limiter = AsyncLimiter(self.rate_limiting_options.rate_limit, 1)

        # Session pooling for memory optimization
        self._shared_download_sessions: dict[str, aiohttp.ClientSession] = {}
        self._session_usage_count: dict[str, int] = defaultdict(int)
        self._max_sessions = 3  # Maximum number of pooled sessions
        self._session_reuse_limit = 1000  # Max requests per session before refresh
        self._session_lock = asyncio.Lock()

        self.global_download_slots = asyncio.Semaphore(self.rate_limiting_options.max_simultaneous_downloads)
        self.scraper_client = ScraperClient(self)
        self.speed_limiter = DownloadSpeedLimiter(self.rate_limiting_options.download_speed_limit)
        self.download_client = DownloadClient(manager, self)
        self.flaresolverr = FlareSolverr(manager)
        self.file_locks = FileLocksVault()
        self._default_headers = {"user-agent": self.manager.global_config.general.user_agent}
        self.reddit_session: CachedSession
        self._session: CachedSession
        self._download_session: aiohttp.ClientSession
        self._curl_session: AsyncSession[CurlResponse]
        self._json_response_checks: dict[str, Callable[[Any], None]] = {}

    def _startup(self) -> None:
        self._session = self.new_scrape_session()
        self.reddit_session = self.new_scrape_session()
        self._download_session = self.new_download_session()
        if _curl_import_error is not None:
            return

        self._curl_session = self.new_curl_cffi_session()

    async def __aenter__(self) -> Self:
        self._startup()
        return self

    async def __aexit__(self, *args) -> None:
        await self._session.close()
        await self.reddit_session.close()
        await self._download_session.close()
        if _curl_import_error is not None:
            return
        try:
            await self._curl_session.close()
        except Exception:
            pass

    @property
    def rate_limiting_options(self):
        return self.manager.global_config.rate_limiting_options

    def get_download_slots(self, domain: str) -> int:
        """Returns the download limit for a domain."""

        instances = self.download_slots.get(domain, self.rate_limiting_options.max_simultaneous_downloads_per_domain)

        return min(instances, self.rate_limiting_options.max_simultaneous_downloads_per_domain)

    @staticmethod
    def cache_control(session: CachedSession, disabled: bool = False):
        if constants.DISABLE_CACHE or disabled:
            return session.disabled()
        return _null_context

    @staticmethod
    def check_curl_cffi_is_available() -> None:
        if _curl_import_error is None:
            return

        system = "Android" if env.RUNNING_IN_TERMUX else "the system"
        msg = (
            f"curl_cffi is required to scrape this URL but a dependency it's not available on {system}.\n"
            f"See: https://github.com/lexiforest/curl_cffi/issues/74#issuecomment-1849365636\n{_curl_import_error!r}"
        )
        raise ScrapeError("Missing Dependency", msg)

    @staticmethod
    def basic_auth(username: str, password: str) -> str:
        """Returns a basic auth token."""
        token = b64encode(f"{username}:{password}".encode()).decode("ascii")
        return f"Basic {token}"

    def check_allowed_filetype(self, media_item: MediaItem) -> bool:
        """Checks if the file type is allowed to download."""
        ignore_options = self.manager.config_manager.settings_data.ignore_options

        if media_item.ext.lower() in constants.FILE_FORMATS["Images"] and ignore_options.exclude_images:
            return False
        if media_item.ext.lower() in constants.FILE_FORMATS["Videos"] and ignore_options.exclude_videos:
            return False
        if media_item.ext.lower() in constants.FILE_FORMATS["Audio"] and ignore_options.exclude_audio:
            return False
        return not (ignore_options.exclude_other and media_item.ext.lower() not in _VALID_EXTENSIONS)

    def pre_check_duration(self, media_item: MediaItem) -> bool:
        """Checks if the download is above the maximum runtime."""
        if not media_item.duration:
            return True

        return self.check_file_duration(media_item)

    def filter_cookies_by_word_in_domain(self, word: str) -> Iterable[tuple[str, BaseCookie[str]]]:
        """Yields pairs of `[domain, BaseCookie]` for every cookie with a domain that has `word` in it"""
        if not self.cookies:
            return
        self.cookies._do_expiration()
        for domain, _ in self.cookies._cookies:
            if word in domain:
                yield domain, self.cookies.filter_cookies(AbsoluteHttpURL(f"https://{domain}"))

    async def startup(self) -> None:
        await _set_dns_resolver()

    def new_curl_cffi_session(self) -> AsyncSession:
        # Calling code should have validated if curl is actually available
        from curl_cffi.requests import AsyncSession

        proxy_or_none = str(proxy) if (proxy := self.manager.global_config.general.proxy) else None
        return AsyncSession(
            headers=self._default_headers,
            impersonate="chrome",
            verify=bool(self.ssl_context),
            proxy=proxy_or_none,
            timeout=self.rate_limiting_options._curl_timeout,
            cookies={cookie.key: cookie.value for cookie in self.cookies},
        )

    def new_scrape_session(self) -> CachedSession:
        trace_configs = _create_request_log_hooks("scrape")
        return self._new_session(cached=True, trace_configs=trace_configs)

    def new_download_session(self) -> ClientSession:
        trace_configs = _create_request_log_hooks("download")
        return self._new_session(cached=False, trace_configs=trace_configs)

    async def _get_or_create_pooled_session(self, session_key: str) -> aiohttp.ClientSession:
        """Get or create a pooled download session for reuse."""
        async with self._session_lock:
            # Check if we have a usable session
            if session_key in self._shared_download_sessions:
                session = self._shared_download_sessions[session_key]
                usage_count = self._session_usage_count[session_key]

                # Check if session is still usable
                if not session.closed and usage_count < self._session_reuse_limit:
                    self._session_usage_count[session_key] += 1
                    return session
                # Session exceeded limit or closed, remove it
                if not session.closed:
                    await session.close()
                del self._shared_download_sessions[session_key]
                del self._session_usage_count[session_key]

            # Clean up old sessions if we're at the limit
            if len(self._shared_download_sessions) >= self._max_sessions:
                # Remove the session with the highest usage count
                oldest_key = max(self._session_usage_count.keys(), key=lambda k: self._session_usage_count[k])
                old_session = self._shared_download_sessions[oldest_key]
                if not old_session.closed:
                    await old_session.close()
                del self._shared_download_sessions[oldest_key]
                del self._session_usage_count[oldest_key]

            # Create new session with optimized settings
            trace_configs = _create_request_log_hooks("download")
            session = self._new_optimized_session(trace_configs)

            self._shared_download_sessions[session_key] = session
            self._session_usage_count[session_key] = 1

            return session

    def _new_optimized_session(
        self, trace_configs: list[aiohttp.TraceConfig] | None = None
    ) -> aiohttp.ClientSession:
        """Create a new session with optimized connection pooling settings."""
        timeout = self.rate_limiting_options._aiohttp_timeout

        return aiohttp.ClientSession(
            headers=self._default_headers,
            raise_for_status=False,
            cookie_jar=self.cookies,
            timeout=timeout,
            trace_configs=trace_configs,
            proxy=self.manager.global_config.general.proxy,
            connector=self._new_optimized_tcp_connector(),
        )

    def _new_optimized_tcp_connector(self) -> aiohttp.TCPConnector:
        """Create an optimized TCP connector with connection pooling."""
        assert constants.DNS_RESOLVER is not None

        # Optimized connector settings for memory efficiency
        conn = aiohttp.TCPConnector(
            ssl=self.ssl_context,
            resolver=constants.DNS_RESOLVER(),
            limit=100,  # Total connection limit
            limit_per_host=20,  # Per-host connection limit
            ttl_dns_cache=300,  # DNS cache TTL (5 minutes)
            use_dns_cache=True,
            keepalive_timeout=30,  # Keep connections alive for 30 seconds
            enable_cleanup_closed=True,  # Clean up closed connections
        )
        conn._resolver_owner = True
        return conn

    @asynccontextmanager
    async def get_download_session(self, domain: str = "default"):
        """Get a reusable download session with connection pooling (context manager)."""
        session_key = f"download_{domain}" if domain != "default" else "download_default"
        session = await self._get_or_create_pooled_session(session_key)
        try:
            yield session
        finally:
            # Session remains in pool for reuse
            pass

    async def close_all_sessions(self) -> None:
        """Close all pooled sessions (call during cleanup)."""
        async with self._session_lock:
            for session in self._shared_download_sessions.values():
                if not session.closed:
                    await session.close()
            self._shared_download_sessions.clear()
            self._session_usage_count.clear()

    def get_session_pool_stats(self) -> dict[str, int]:
        """Get statistics about session pool usage."""
        return {
            "active_sessions": len(self._shared_download_sessions),
            "max_sessions": self._max_sessions,
            "total_requests": sum(self._session_usage_count.values()),
            "reuse_limit": self._session_reuse_limit,
        }

    @overload
    def _new_session(
        self, cached: Literal[True], trace_configs: list[aiohttp.TraceConfig] | None = None
    ) -> CachedSession: ...

    @overload
    def _new_session(
        self, cached: Literal[False] = False, trace_configs: list[aiohttp.TraceConfig] | None = None
    ) -> ClientSession: ...

    def _new_session(
        self, cached: bool = False, trace_configs: list[aiohttp.TraceConfig] | None = None
    ) -> CachedSession | ClientSession:
        if cached:
            timeout = self.rate_limiting_options._aiohttp_timeout
            session_cls = CachedSession
            kwargs: dict[str, Any] = {"cache": self.manager.cache_manager.request_cache}
        else:
            timeout = self.rate_limiting_options._aiohttp_timeout
            session_cls = ClientSession
            kwargs = {}
        return session_cls(
            headers=self._default_headers,
            raise_for_status=False,
            cookie_jar=self.cookies,
            timeout=timeout,
            trace_configs=trace_configs,
            proxy=self.manager.global_config.general.proxy,
            connector=self._new_tcp_connector(),
            **kwargs,
        )

    def _new_tcp_connector(self) -> aiohttp.TCPConnector:
        assert constants.DNS_RESOLVER is not None
        conn = aiohttp.TCPConnector(ssl=self.ssl_context, resolver=constants.DNS_RESOLVER())
        conn._resolver_owner = True
        return conn

    def check_domain_errors(self, domain: str) -> None:
        if _crawler_errors[domain] >= env.MAX_CRAWLER_ERRORS:
            if crawler := self.manager.scrape_mapper.disable_crawler(domain):
                msg = (
                    f"{crawler.__class__.__name__} has been disabled after too many errors. "
                    f"URLs from the following domains will be ignored: {crawler.SCRAPE_MAPPER_KEYS}"
                )
                log(msg, 40)
            raise TooManyCrawlerErrors

    @contextlib.contextmanager
    def request_context(self, domain: str) -> Generator[None]:
        self.check_domain_errors(domain)
        try:
            yield
        except DDOSGuardError:
            _crawler_errors[domain] += 1
            raise
        else:
            # we could potentially reset the counter here
            # _crawler_errors[domain] = 0
            pass
        finally:
            pass

    async def load_cookie_files(self) -> None:
        if self.manager.config_manager.settings_data.browser_cookies.auto_import:
            assert self.manager.config_manager.settings_data.browser_cookies.browser
            get_cookies_from_browsers(
                self.manager, browser=self.manager.config_manager.settings_data.browser_cookies.browser
            )
        cookie_files = sorted(self.manager.path_manager.cookies_dir.glob("*.txt"))
        if not cookie_files:
            return
        async for domain, cookie in read_netscape_files(cookie_files):
            self.cookies.update_cookies(cookie, response_url=AbsoluteHttpURL(f"https://{domain}"))

        log_spacer(20, log_to_console=False)

    def get_rate_limiter(self, domain: str) -> AsyncLimiter:
        """Get a rate limiter for a domain."""
        if domain in self.rate_limits:
            return self.rate_limits[domain]
        return self.rate_limits["other"]

    async def check_http_status(
        self,
        response: ClientResponse | CachedResponse | CurlResponse | AbstractResponse,
        download: bool = False,
    ) -> BeautifulSoup | None:
        """Checks the HTTP status code and raises an exception if it's not acceptable.

        If the response is successful and has valid html, returns soup
        """
        if not isinstance(response, AbstractResponse):
            response = AbstractResponse.from_resp(response)

        message = None

        def check_etag() -> None:
            if download and (e_tag := response.headers.get("ETag")) in _DOWNLOAD_ERROR_ETAGS:
                message = _DOWNLOAD_ERROR_ETAGS[e_tag]
                raise DownloadError(HTTPStatus.NOT_FOUND, message=message)

        async def check_ddos_guard() -> BeautifulSoup | None:
            if "html" not in response.content_type:
                return
            try:
                soup = BeautifulSoup(await response.text(), "html.parser")
            except UnicodeDecodeError:
                return
            else:
                if self.check_ddos_guard(soup) or self.check_cloudflare(soup):
                    raise DDOSGuardError
                return soup

        check_etag()
        if HTTPStatus.OK <= response.status < HTTPStatus.BAD_REQUEST:
            # Check DDosGuard even on successful pages
            # await check_ddos_guard()
            return

        await self._check_json(response)
        await check_ddos_guard()
        raise DownloadError(status=response.status, message=message)

    async def _check_json(self, response: AbstractResponse) -> None:
        if "json" not in response.content_type:
            return

        if check := self._json_response_checks.get(response.url.host):
            check(await response.json())
            return

        for domain, check in self._json_response_checks.items():
            if domain in response.url.host:
                self._json_response_checks[response.url.host] = check
                check(await response.json())
                return

    @staticmethod
    def check_content_length(headers: Mapping[str, Any]) -> None:
        content_length, content_type = headers.get("Content-Length"), headers.get("Content-Type")
        if content_length is None or content_type is None:
            return
        if content_length == "322509" and content_type == "video/mp4":
            raise DownloadError(status="Bunkr Maintenance", message="Bunkr under maintenance")
        if content_length == "73003" and content_type == "video/mp4":
            raise DownloadError(410)  # Placeholder video with text "Video removed" (efukt)

    @staticmethod
    def check_ddos_guard(soup: BeautifulSoup) -> bool:
        if (title := soup.select_one("title")) and (title_str := title.string):
            if any(title.casefold() == title_str.casefold() for title in DDosGuard.TITLES):
                return True

        return bool(soup.select_one(DDosGuard.ALL_SELECTORS))

    @staticmethod
    def check_cloudflare(soup: BeautifulSoup) -> bool:
        if (title := soup.select_one("title")) and (title_str := title.string):
            if any(title.casefold() == title_str.casefold() for title in CloudflareTurnstile.TITLES):
                return True

        return bool(soup.select_one(CloudflareTurnstile.ALL_SELECTORS))

    def check_file_duration(self, media_item: MediaItem) -> bool:
        """Checks the file runtime against the config runtime limits."""
        if media_item.is_segment:
            return True

        is_video = media_item.ext.lower() in constants.FILE_FORMATS["Videos"]
        is_audio = media_item.ext.lower() in constants.FILE_FORMATS["Audio"]
        if not (is_video or is_audio):
            return True

        def get_duration() -> float | None:
            if media_item.duration:
                return media_item.duration
            props: dict = {}
            if is_video:
                props: dict = get_video_properties(str(media_item.complete_file))
            elif is_audio:
                props: dict = get_audio_properties(str(media_item.complete_file))
            return float(props.get("duration", 0)) or None

        duration_limits = self.manager.config.media_duration_limits
        min_video_duration: float = duration_limits.minimum_video_duration.total_seconds()
        max_video_duration: float = duration_limits.maximum_video_duration.total_seconds()
        min_audio_duration: float = duration_limits.minimum_audio_duration.total_seconds()
        max_audio_duration: float = duration_limits.maximum_audio_duration.total_seconds()
        video_duration_limits = min_video_duration, max_video_duration
        audio_duration_limits = min_audio_duration, max_audio_duration
        if is_video and not any(video_duration_limits):
            return True
        if is_audio and not any(audio_duration_limits):
            return True

        duration: float = get_duration()  # type: ignore
        media_item.duration = duration
        if duration is None:
            return True

        max_video_duration = max_video_duration or float("inf")
        max_audio_duration = max_audio_duration or float("inf")
        if is_video:
            return min_video_duration <= media_item.duration <= max_video_duration
        return min_audio_duration <= media_item.duration <= max_audio_duration

    async def close(self) -> None:
        await self.close_all_sessions()  # Clean up session pools
        await self.flaresolverr.close()


async def _set_dns_resolver(loop: asyncio.AbstractEventLoop | None = None) -> None:
    if constants.DNS_RESOLVER is not None:
        return
    try:
        await _test_async_resolver(loop)
        constants.DNS_RESOLVER = aiohttp.AsyncResolver
    except Exception as e:
        constants.DNS_RESOLVER = aiohttp.ThreadedResolver
        log(f"Unable to setup asynchronous DNS resolver. Falling back to thread based resolver: {e}", 30)


async def _test_async_resolver(loop: asyncio.AbstractEventLoop | None = None) -> None:
    """Test aiodns with a DNS lookup."""

    # pycares (the underlying C extension library that aiodns uses) installs successfully in most cases,
    # but it fails to actually connect to DNS servers on some platforms (e.g., Android).
    import aiodns

    async with aiodns.DNSResolver(loop=loop, timeout=5.0) as resolver:
        _ = await resolver.query("github.com", "A")


def _create_request_log_hooks(client_type: Literal["scrape", "download"]) -> list[aiohttp.TraceConfig]:
    async def on_request_start(*args) -> None:
        params: aiohttp.TraceRequestStartParams = args[2]
        log_debug(f"Starting {client_type} {params.method} request to {params.url}", 10)

    async def on_request_end(*args) -> None:
        params: aiohttp.TraceRequestEndParams = args[2]
        msg = f"Finishing {client_type} {params.method} request to {params.url}"
        msg += f" -> response status: {params.response.status}"
        log_debug(msg, 10)

    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    trace_config.on_request_end.append(on_request_end)
    return [trace_config]
