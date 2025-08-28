from __future__ import annotations

import asyncio
import calendar
import itertools
import time
from collections import defaultdict
from functools import partial, wraps
from http import HTTPStatus
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import aiofiles
import aiohttp
from dateutil import parser
from videoprops import get_audio_properties, get_video_properties

from cyberdrop_dl.constants import FILE_FORMATS
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.exceptions import DDOSGuardError, DownloadError, InvalidContentTypeError, SlowDownloadError, ErrorLogMessage
from cyberdrop_dl.utils.logger import log
from cyberdrop_dl.utils.utilities import get_size_or_none

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Generator
    from pathlib import Path
    from typing import Any

    import aiohttp
    from aiohttp import ClientSession
    from multidict import CIMultiDictProxy

    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.managers.client_manager import ClientManager
    from cyberdrop_dl.managers.manager import Manager


P = ParamSpec("P")
R = TypeVar("R")

CONTENT_TYPES_OVERRIDES = {"text/vnd.trolltech.linguist": "video/MP2T"}


def preserve_error_context(operation_name: str):
    """Decorator to preserve comprehensive error context for debugging and monitoring."""
    def decorator(func: Callable[P, Coroutine[None, None, R]]) -> Callable[P, Coroutine[None, None, R]]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> R:
            media_item = None
            domain = None
            
            # Extract context from arguments
            if args:
                self = args[0]
                if len(args) > 1:
                    if isinstance(args[1], str):
                        domain = args[1]
                    elif hasattr(args[1], 'url'):
                        media_item = args[1]
                if len(args) > 2 and hasattr(args[2], 'url'):
                    media_item = args[2]
                
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                # Log successful operations for monitoring
                if media_item and hasattr(media_item, 'url'):
                    duration = time.time() - start_time
                    log(f"{operation_name} succeeded for {media_item.url} in {duration:.2f}s", 10)
                return result
                
            except Exception as e:
                # Preserve comprehensive error context
                duration = time.time() - start_time
                error_context = {
                    'operation': operation_name,
                    'duration': f"{duration:.2f}s",
                    'domain': domain,
                    'url': str(media_item.url) if media_item and hasattr(media_item, 'url') else None,
                    'filename': getattr(media_item, 'filename', None) if media_item else None,
                    'filesize': getattr(media_item, 'filesize', None) if media_item else None,
                    'referer': str(media_item.referer) if media_item and hasattr(media_item, 'referer') else None,
                    'complete_file': str(media_item.complete_file) if media_item and hasattr(media_item, 'complete_file') else None,
                    'partial_file': str(media_item.partial_file) if media_item and hasattr(media_item, 'partial_file') else None,
                }
                
                # Create enhanced error message with context
                if hasattr(e, 'ui_failure') and hasattr(e, 'message'):
                    # This is already a CDLBaseError, enhance it
                    enhanced_msg = f"{e.message} | Context: {error_context}"
                    e.message = enhanced_msg
                else:
                    # Create new error log message with full context
                    error_log = ErrorLogMessage.from_unknown_exc(e)
                    log(f"{operation_name} failed: {error_log.main_log_msg} | Context: {error_context}", 40, exc_info=e)
                
                raise
                
        return wrapper
    return decorator


class IntelligentRetryManager:
    """Advanced retry mechanism with exponential backoff and adaptive strategies."""
    
    def __init__(self, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retry_stats: dict[str, dict] = defaultdict(lambda: {
            'attempts': 0, 
            'successes': 0, 
            'last_retry_time': 0.0,
            'error_types': defaultdict(int)
        })
    
    def should_retry(self, error: Exception, attempt: int, domain: str) -> bool:
        """Determine if an error should be retried based on type and context."""
        if attempt >= self.max_retries:
            return False
        
        # Errors that should be retried
        retryable_errors = (
            DownloadError, 
            SlowDownloadError,
            aiohttp.ClientError,
            aiohttp.ServerTimeoutError,
            aiohttp.ClientPayloadError,
            asyncio.TimeoutError,
        )
        
        # DDOSGuard and certain status codes need special handling
        if isinstance(error, DDOSGuardError):
            return attempt < 2  # Only retry DDOSGuard once
            
        if isinstance(error, DownloadError):
            # Don't retry client errors (4xx), but retry server errors (5xx)
            if hasattr(error, 'status'):
                status_code = error.status
                if isinstance(status_code, int):
                    return status_code >= 500 or status_code == 429  # Retry server errors and rate limits
        
        return isinstance(error, retryable_errors)
    
    def calculate_delay(self, attempt: int, domain: str, error: Exception) -> float:
        """Calculate intelligent backoff delay based on error type and domain history."""
        base_delay = self.base_delay
        
        # Adjust base delay based on error type
        if isinstance(error, DDOSGuardError):
            base_delay = 30.0  # Longer delay for DDoS protection
        elif isinstance(error, (aiohttp.ServerTimeoutError, asyncio.TimeoutError)):
            base_delay = 5.0   # Medium delay for timeouts
        elif isinstance(error, DownloadError) and hasattr(error, 'status'):
            if error.status == 429:  # Rate limiting
                base_delay = 10.0
            elif error.status >= 500:  # Server errors
                base_delay = 2.0
        
        # Exponential backoff with jitter
        import random
        exponential_delay = base_delay * (2 ** attempt)
        jitter = random.uniform(0.1, 0.3) * exponential_delay
        total_delay = min(exponential_delay + jitter, self.max_delay)
        
        # Domain-specific adjustment based on recent success rate
        domain_stats = self.retry_stats[domain]
        if domain_stats['attempts'] > 0:
            success_rate = domain_stats['successes'] / domain_stats['attempts']
            if success_rate < 0.5:  # Poor success rate, increase delays
                total_delay *= 1.5
        
        return total_delay
    
    async def execute_with_retry(self, func, *args, domain: str = "default", **kwargs):
        """Execute function with intelligent retry logic."""
        last_error = None
        
        for attempt in range(self.max_retries + 1):
            try:
                # Track attempt
                self.retry_stats[domain]['attempts'] += 1
                
                result = await func(*args, **kwargs)
                
                # Track success
                self.retry_stats[domain]['successes'] += 1
                
                if attempt > 0:
                    log(f"Retry succeeded on attempt {attempt + 1} for {domain}", 20)
                
                return result
                
            except Exception as e:
                last_error = e
                error_type = type(e).__name__
                self.retry_stats[domain]['error_types'][error_type] += 1
                
                if not self.should_retry(e, attempt, domain):
                    if attempt > 0:
                        log(f"Retry failed after {attempt + 1} attempts for {domain}: {e}", 40)
                    raise
                
                delay = self.calculate_delay(attempt, domain, e)
                log(f"Retry attempt {attempt + 1}/{self.max_retries} for {domain} after {delay:.1f}s delay: {error_type}", 30)
                
                await asyncio.sleep(delay)
        
        # Should not reach here, but just in case
        if last_error:
            raise last_error
    
    def get_stats(self) -> dict:
        """Get retry statistics for monitoring."""
        return dict(self.retry_stats)


# Global retry manager instance
_retry_manager = IntelligentRetryManager()


class AdaptiveContentStreamer:
    """Memory-efficient content streaming with adaptive chunk sizing."""
    
    def __init__(self, media_item: MediaItem, content: aiohttp.StreamReader, initial_chunk_size: int = 65536):
        self.media_item = media_item
        self.content = content
        self.initial_chunk_size = initial_chunk_size
        self.current_chunk_size = initial_chunk_size
        self.min_chunk_size = 8192    # 8KB minimum
        self.max_chunk_size = 1048576 # 1MB maximum
        
        # Performance tracking
        self.bytes_read = 0
        self.chunks_processed = 0
        self.start_time = time.time()
        self.last_speed_check = time.time()
        
    async def stream_chunks(self):
        """Stream content with adaptive chunk sizing based on performance."""
        async for chunk in self.content.iter_chunked(self.current_chunk_size):
            if not chunk:
                break
                
            self.bytes_read += len(chunk)
            self.chunks_processed += 1
            
            # Adapt chunk size every 10 chunks
            if self.chunks_processed % 10 == 0:
                await self._adapt_chunk_size()
            
            yield chunk
    
    async def _adapt_chunk_size(self):
        """Dynamically adjust chunk size based on download performance."""
        current_time = time.time()
        
        # Calculate current speed
        time_elapsed = current_time - self.last_speed_check
        if time_elapsed > 0:
            current_speed = self.bytes_read / time_elapsed  # bytes per second
            
            # Adjust chunk size based on speed
            if current_speed > 1_000_000:  # > 1MB/s, increase chunk size
                self.current_chunk_size = min(self.max_chunk_size, int(self.current_chunk_size * 1.2))
            elif current_speed < 100_000:  # < 100KB/s, decrease chunk size
                self.current_chunk_size = max(self.min_chunk_size, int(self.current_chunk_size * 0.8))
            
            # Reset for next measurement
            self.last_speed_check = current_time
            self.bytes_read = 0


def with_intelligent_retry(domain: str = "default"):
    """Decorator to add intelligent retry logic to functions."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await _retry_manager.execute_with_retry(func, *args, domain=domain, **kwargs)
        return wrapper
    return decorator


def limiter(func: Callable[P, Coroutine[None, None, R]]) -> Callable[P, Coroutine[None, None, R]]:
    """Wrapper handles limits for download session."""

    @wraps(func)
    async def wrapper(*args, **kwargs) -> R:
        self: DownloadClient = args[0]
        domain: str = args[1]
        async with self.client_manager.request_context(domain):
            domain_limiter = await self.client_manager.get_rate_limiter(domain)
            await asyncio.sleep(await self.client_manager.get_downloader_spacer(domain))
            await self.client_manager.global_rate_limiter.acquire()
            await domain_limiter.acquire()

            # Use pooled session for memory efficiency and connection reuse
            async with self.client_manager.get_download_session(domain) as client:
                kwargs["client_session"] = client
                return await func(*args, **kwargs)

    return wrapper


def check_file_duration(media_item: MediaItem, manager: Manager) -> bool:
    """Checks the file runtime against the config runtime limits."""
    if media_item.is_segment:
        return True

    is_video = media_item.ext.lower() in FILE_FORMATS["Videos"]
    is_audio = media_item.ext.lower() in FILE_FORMATS["Audio"]
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

    duration_limits = manager.config_manager.settings_data.media_duration_limits
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


class DownloadClient:
    """AIOHTTP operations for downloading."""

    def __init__(self, manager: Manager, client_manager: ClientManager) -> None:
        self.manager = manager
        self.client_manager = client_manager
        self.slow_download_period = 10  # seconds
        self.download_speed_threshold = self.manager.config_manager.settings_data.runtime_options.slow_download_speed
        self.chunk_size = client_manager.speed_limiter.chunk_size

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    def add_api_key_headers(self, domain: str, referer: AbsoluteHttpURL) -> dict:
        download_headers = self.client_manager._headers | {"Referer": str(referer)}
        auth_data = self.manager.config_manager.authentication_data
        if domain == "pixeldrain" and auth_data.pixeldrain.api_key:
            download_headers["Authorization"] = self.manager.download_manager.basic_auth(
                "Cyberdrop-DL", auth_data.pixeldrain.api_key
            )
        elif domain == "gofile":
            gofile_cookies = self.client_manager.cookies.filter_cookies(AbsoluteHttpURL("https://gofile.io"))
            api_key = gofile_cookies.get("accountToken", "")
            if api_key:
                download_headers["Authorization"] = f"Bearer {api_key.value}"  # type: ignore
        elif domain == "odnoklassniki":
            # TODO: Add "headers" attribute to MediaItem to use custom headers for downloads
            download_headers |= {
                "Accept-Language": "en-gb, en;q=0.8",
                "User-Agent": "Mozilla/5.0 (Linux; Android 16) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.7204.180 Mobile Safari/537.36",
                "Referer": "https://m.ok.ru/",
                "Origin": "https://m.ok.ru",
            }
        return download_headers

    @limiter
    @preserve_error_context("file_download")
    @with_intelligent_retry()
    async def _download(
        self,
        domain: str,
        manager: Manager,
        media_item: MediaItem,
        save_content: Callable[[aiohttp.StreamReader], Coroutine[Any, Any, None]],
        client_session: ClientSession,
    ) -> bool:
        """Downloads a file."""
        download_headers = self.add_api_key_headers(domain, media_item.referer)

        downloaded_filename = await self.manager.db_manager.history_table.get_downloaded_filename(domain, media_item)
        download_dir = self.get_download_dir(media_item)
        if media_item.is_segment:
            media_item.partial_file = media_item.complete_file = download_dir / media_item.filename
        else:
            media_item.partial_file = download_dir / f"{downloaded_filename}.part"

        resume_point = 0
        if media_item.partial_file and (size := await asyncio.to_thread(get_size_or_none, media_item.partial_file)):
            download_headers["Range"] = f"bytes={size}-"

        await asyncio.sleep(self.manager.config_manager.global_settings_data.rate_limiting_options.total_delay)

        download_url = media_item.debrid_link or media_item.url
        gen: Callable[..., AbsoluteHttpURL] | list[AbsoluteHttpURL] | None = media_item.fallbacks
        fallback_urls = fallback_call = None
        if gen is not None:
            if isinstance(gen, list):
                fallback_urls: list[AbsoluteHttpURL] | None = gen
            else:
                fallback_call: Callable[[aiohttp.ClientResponse, int], AbsoluteHttpURL] | None = gen

        def gen_fallback() -> Generator[AbsoluteHttpURL | None, aiohttp.ClientResponse, None]:
            response = yield
            if fallback_urls is not None:
                yield from fallback_urls

            elif fallback_call is not None:
                for retry in itertools.count(1):
                    if not response:
                        break
                    url = fallback_call(response, retry)
                    if not url:
                        break
                    response = yield url

        async def process_response(resp: aiohttp.ClientResponse) -> bool:
            nonlocal resume_point
            if resp.status == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
                await asyncio.to_thread(media_item.partial_file.unlink)

            await self.client_manager.check_http_status(resp, download=True)

            _ = get_content_type(media_item.ext, resp.headers)

            media_item.filesize = int(resp.headers.get("Content-Length", "0")) or None
            if not media_item.complete_file and not media_item.is_segment:
                proceed, skip = await self.get_final_file_info(media_item, domain)
                self.client_manager.check_content_length(resp.headers)
                if skip:
                    self.manager.progress_manager.download_progress.add_skipped()
                    return False
                if not proceed:
                    log(f"Skipping {media_item.url} as it has already been downloaded", 10)
                    self.manager.progress_manager.download_progress.add_previously_completed(False)
                    await self.process_completed(media_item, domain)
                    await self.handle_media_item_completion(media_item, downloaded=False)

                    return False

            if resp.status != HTTPStatus.PARTIAL_CONTENT:
                await asyncio.to_thread(media_item.partial_file.unlink, missing_ok=True)

            if not media_item.datetime and (last_modified := get_last_modified(resp.headers)):
                msg = f"Unable to parse upload date for {media_item.url}, using `Last-Modified` header as file datetime"
                log(msg, 30)
                media_item.datetime = last_modified

            task_id = media_item.task_id
            if task_id is None:
                size = (media_item.filesize + resume_point) if media_item.filesize is not None else None
                task_id = self.manager.progress_manager.file_progress.add_task(
                    domain=domain, filename=media_item.filename, expected_size=size
                )
                media_item.set_task_id(task_id)

            self.manager.progress_manager.file_progress.advance_file(task_id, resume_point)

            await save_content(resp.content)
            return True

        fallback_url_generator = gen_fallback()
        next(fallback_url_generator)  # Prime the generator, waiting for response
        await self.manager.states.RUNNING.wait()
        fallback_count = 0
        while True:
            resp = None
            try:
                async with client_session.get(download_url, headers=download_headers) as resp:
                    return await process_response(resp)
            except (DownloadError, DDOSGuardError):
                if resp is None:
                    raise
                try:
                    next_download_url = fallback_url_generator.send(resp)
                except StopIteration:
                    pass
                else:
                    if not next_download_url:
                        raise
                    if media_item.debrid_link and media_item.debrid_link == download_url:
                        msg = f" with debrid URL {download_url} failed, retrying with fallback URL: "
                    elif media_item.url == download_url:
                        msg = " failed, retrying with fallback URL: "
                    else:
                        fallback_count += 1
                        msg = f" with fallback URL #{fallback_count} {download_url} failed, retrying with new fallback URL: "
                    log(f"Download of {media_item.url}{msg}{next_download_url}", 40)
                    download_url = next_download_url
                    continue
                raise

    @preserve_error_context("content_append")
    async def _append_content(
        self,
        media_item: MediaItem,
        content: aiohttp.StreamReader,
        update_progress: partial,
    ) -> None:
        """Memory-efficient content streaming with adaptive chunk sizing and buffered I/O."""

        check_free_space = self.make_free_space_checker(media_item)
        check_download_speed = self.make_speed_checker(media_item)
        await check_free_space()
        await self._pre_download_check(media_item)

        # Create adaptive streamer for intelligent chunk sizing
        adaptive_streamer = AdaptiveContentStreamer(media_item, content, self.chunk_size)
        
        # Use memory-efficient streaming with write buffering
        write_buffer = bytearray()
        buffer_size_limit = 512 * 1024  # 512KB buffer for optimal I/O
        total_written = 0

        async with aiofiles.open(media_item.partial_file, mode="ab") as f:  # type: ignore
            async for chunk in adaptive_streamer.stream_chunks():
                await self.manager.states.RUNNING.wait()
                await check_free_space()
                
                chunk_size = len(chunk)
                await self.client_manager.speed_limiter.acquire(chunk_size)
                
                # Buffer chunks for more efficient disk I/O
                write_buffer.extend(chunk)
                
                # Flush buffer when it's full or for small files (< 10MB)
                should_flush = (
                    len(write_buffer) >= buffer_size_limit or
                    (media_item.filesize and media_item.filesize < 10_000_000 and len(write_buffer) >= chunk_size * 5)
                )
                
                if should_flush:
                    await f.write(write_buffer)
                    await f.fsync()  # Force write to disk
                    written_size = len(write_buffer)
                    total_written += written_size
                    update_progress(written_size)
                    write_buffer.clear()
                
                check_download_speed()
            
            # Flush any remaining buffered data
            if write_buffer:
                await f.write(write_buffer)
                await f.fsync()
                update_progress(len(write_buffer))
                total_written += len(write_buffer)

        self._post_download_check(media_item, content)

    def _pre_download_check(self, media_item: MediaItem) -> Coroutine[Any, Any, None]:
        def prepare() -> None:
            media_item.partial_file.parent.mkdir(parents=True, exist_ok=True)
            if not media_item.partial_file.is_file():
                media_item.partial_file.touch()

        return asyncio.to_thread(prepare)

    def _post_download_check(self, media_item: MediaItem, content: aiohttp.StreamReader) -> None:
        if not content.total_bytes and not media_item.partial_file.stat().st_size:
            media_item.partial_file.unlink()
            raise DownloadError(status=HTTPStatus.INTERNAL_SERVER_ERROR, message="File is empty")

    def make_free_space_checker(self, media_item: MediaItem) -> Callable[[], Coroutine[Any, Any, None]]:
        current_chunk = 0

        async def check_free_space() -> None:
            nonlocal current_chunk
            current_chunk += 1
            if current_chunk % 5 == 0:
                return await self.manager.storage_manager.check_free_space(media_item)

        return check_free_space

    def make_speed_checker(self, media_item: MediaItem) -> Callable[[], None]:
        last_slow_speed_read = None

        def check_download_speed() -> None:
            nonlocal last_slow_speed_read
            if not self.download_speed_threshold:
                return
            assert media_item.task_id is not None
            speed = self.manager.progress_manager.file_progress.get_speed(media_item.task_id)
            if speed > self.download_speed_threshold:
                last_slow_speed_read = None
            elif not last_slow_speed_read:
                last_slow_speed_read = time.perf_counter()
            elif time.perf_counter() - last_slow_speed_read > self.slow_download_period:
                raise SlowDownloadError(origin=media_item)

        return check_download_speed

    @preserve_error_context("download_file_orchestration")
    async def download_file(self, manager: Manager, domain: str, media_item: MediaItem) -> bool:
        """Starts a file."""
        if (
            self.manager.config_manager.settings_data.download_options.skip_download_mark_completed
            and not media_item.is_segment
        ):
            log(f"Download Removed {media_item.url} due to mark completed option", 10)
            self.manager.progress_manager.download_progress.add_skipped()
            # set completed path
            await self.process_completed(media_item, domain)
            return False

        async def save_content(content: aiohttp.StreamReader) -> None:
            assert media_item.task_id is not None
            await self._append_content(
                media_item,
                content,
                partial(manager.progress_manager.file_progress.advance_file, media_item.task_id),
            )

        downloaded = await self._download(domain, manager, media_item, save_content)  # type: ignore
        if downloaded:
            await asyncio.to_thread(media_item.partial_file.rename, media_item.complete_file)
            if not media_item.is_segment:
                proceed = check_file_duration(media_item, self.manager)
                await self.manager.db_manager.history_table.add_duration(domain, media_item)
                if not proceed:
                    log(f"Download Skip {media_item.url} due to runtime restrictions", 10)
                    await asyncio.to_thread(media_item.complete_file.unlink)
                    await self.mark_incomplete(media_item, domain)
                    self.manager.progress_manager.download_progress.add_skipped()
                    return False
                await self.process_completed(media_item, domain)
                await self.handle_media_item_completion(media_item, downloaded=True)
        return downloaded

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    @preserve_error_context("mark_incomplete")
    async def mark_incomplete(self, media_item: MediaItem, domain: str) -> None:
        """Marks the media item as incomplete in the database."""
        if media_item.is_segment:
            return
        await self.manager.db_manager.history_table.insert_incompleted(domain, media_item)

    @preserve_error_context("process_completed")
    async def process_completed(self, media_item: MediaItem, domain: str) -> None:
        """Marks the media item as completed in the database and adds to the completed list."""
        await self.mark_completed(domain, media_item)
        await self.add_file_size(domain, media_item)

    async def mark_completed(self, domain: str, media_item: MediaItem) -> None:
        await self.manager.db_manager.history_table.mark_complete(domain, media_item)

    @preserve_error_context("add_file_size")
    async def add_file_size(self, domain: str, media_item: MediaItem) -> None:
        if not media_item.complete_file:
            media_item.complete_file = self.get_file_location(media_item)
        if await asyncio.to_thread(media_item.complete_file.is_file):
            await self.manager.db_manager.history_table.add_filesize(domain, media_item)

    @preserve_error_context("handle_media_completion")
    async def handle_media_item_completion(self, media_item: MediaItem, downloaded: bool = False) -> None:
        """Sends to hash client to handle hashing and marks as completed/current download."""
        try:
            media_item.downloaded = downloaded
            await self.manager.hash_manager.hash_client.hash_item_during_download(media_item)
            self.manager.path_manager.add_completed(media_item)
        except Exception:
            log(f"Error handling media item completion of: {media_item.complete_file}", 10, exc_info=True)

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    def get_download_dir(self, media_item: MediaItem) -> Path:
        """Returns the download directory for the media item."""
        download_folder = media_item.download_folder
        if self.manager.parsed_args.cli_only_args.retry_any:
            return download_folder

        if self.manager.config_manager.settings_data.download_options.block_download_sub_folders:
            while download_folder.parent != self.manager.path_manager.download_folder:
                download_folder = download_folder.parent
            media_item.download_folder = download_folder
        return download_folder

    def get_file_location(self, media_item: MediaItem) -> Path:
        download_dir = self.get_download_dir(media_item)
        return download_dir / media_item.filename

    async def get_final_file_info(self, media_item: MediaItem, domain: str) -> tuple[bool, bool]:
        """Complicated checker for if a file already exists, and was already downloaded."""
        media_item.complete_file = self.get_file_location(media_item)
        media_item.partial_file = media_item.complete_file.with_suffix(media_item.complete_file.suffix + ".part")

        expected_size = media_item.filesize
        proceed = True
        skip = False

        while True:
            if expected_size:
                file_size_check = self.check_filesize_limits(media_item)
                if not file_size_check:
                    log(f"Download Skip {media_item.url} due to filesize restrictions", 10)
                    proceed = False
                    skip = True
                    return proceed, skip

            if not media_item.complete_file.exists() and not media_item.partial_file.exists():
                break

            if media_item.complete_file.exists() and media_item.complete_file.stat().st_size == media_item.filesize:
                log(f"Found {media_item.complete_file.name} locally, skipping download")
                proceed = False
                break

            downloaded_filename = await self.manager.db_manager.history_table.get_downloaded_filename(
                domain,
                media_item,
            )
            if not downloaded_filename:
                media_item.complete_file, media_item.partial_file = await self.iterate_filename(
                    media_item.complete_file,
                    media_item,
                )
                break

            if media_item.filename == downloaded_filename:
                if media_item.partial_file.exists():
                    log(f"Found {downloaded_filename} locally, trying to resume")
                    assert media_item.filesize
                    if media_item.partial_file.stat().st_size >= media_item.filesize != 0:
                        log(f"Deleting partial file {media_item.partial_file}")
                        media_item.partial_file.unlink()
                    if media_item.partial_file.stat().st_size == media_item.filesize:
                        if media_item.complete_file.exists():
                            log(
                                f"Found conflicting complete file '{media_item.complete_file}' locally, iterating filename",
                                30,
                            )
                            new_complete_filename, new_partial_file = await self.iterate_filename(
                                media_item.complete_file,
                                media_item,
                            )
                            media_item.partial_file.rename(new_complete_filename)
                            proceed = False

                            media_item.complete_file = new_complete_filename
                            media_item.partial_file = new_partial_file
                        else:
                            proceed = False
                            media_item.partial_file.rename(media_item.complete_file)
                        log(
                            f"Renaming found partial file '{media_item.partial_file}' to complete file {media_item.complete_file}"
                        )
                elif media_item.complete_file.exists():
                    if media_item.complete_file.stat().st_size == media_item.filesize:
                        log(f"Found complete file '{media_item.complete_file}' locally, skipping download")
                        proceed = False
                    else:
                        log(
                            f"Found conflicting complete file '{media_item.complete_file}' locally, iterating filename",
                            30,
                        )
                        media_item.complete_file, media_item.partial_file = await self.iterate_filename(
                            media_item.complete_file,
                            media_item,
                        )
                break

            media_item.filename = downloaded_filename
        media_item.download_filename = media_item.complete_file.name
        await self.manager.db_manager.history_table.add_download_filename(domain, media_item)
        return proceed, skip

    async def iterate_filename(self, complete_file: Path, media_item: MediaItem) -> tuple[Path, Path]:
        """Iterates the filename until it is unique."""
        partial_file = complete_file.with_suffix(complete_file.suffix + ".part")
        for iteration in itertools.count(1):
            filename = f"{complete_file.stem} ({iteration}){complete_file.suffix}"
            temp_complete_file = media_item.download_folder / filename
            if (
                not temp_complete_file.exists()
                and not await self.manager.db_manager.history_table.check_filename_exists(filename)
            ):
                media_item.filename = filename
                complete_file = media_item.download_folder / media_item.filename
                partial_file = complete_file.with_suffix(complete_file.suffix + ".part")
                break
        return complete_file, partial_file

    def check_filesize_limits(self, media: MediaItem) -> bool:
        """Checks if the file size is within the limits."""
        file_size_limits = self.manager.config_manager.settings_data.file_size_limits
        max_video_filesize = file_size_limits.maximum_video_size or float("inf")
        min_video_filesize = file_size_limits.minimum_video_size
        max_image_filesize = file_size_limits.maximum_image_size or float("inf")
        min_image_filesize = file_size_limits.minimum_image_size
        max_other_filesize = file_size_limits.maximum_other_size or float("inf")
        min_other_filesize = file_size_limits.minimum_other_size

        assert media.filesize is not None
        if media.ext in FILE_FORMATS["Images"]:
            proceed = min_image_filesize < media.filesize < max_image_filesize
        elif media.ext in FILE_FORMATS["Videos"]:
            proceed = min_video_filesize < media.filesize < max_video_filesize
        else:
            proceed = min_other_filesize < media.filesize < max_other_filesize

        return proceed


def get_content_type(ext: str, headers: CIMultiDictProxy) -> str | None:
    content_type: str = headers.get("Content-Type", "")
    content_length = headers.get("Content-Length")
    if not content_type and not content_length:
        msg = "No content type in response headers"
        raise InvalidContentTypeError(message=msg)

    if not content_type:
        return None

    override_key = next((name for name in CONTENT_TYPES_OVERRIDES if name in content_type), "<NO_OVERRIDE>")
    override: str | None = CONTENT_TYPES_OVERRIDES.get(override_key)
    content_type = override or content_type
    content_type = content_type.lower()

    if is_html_or_text(content_type) and ext.lower() not in FILE_FORMATS["Text"]:
        msg = f"Received '{content_type}', was expecting other"
        raise InvalidContentTypeError(message=msg)

    return content_type


def get_last_modified(headers: CIMultiDictProxy) -> int | None:
    if date_str := headers.get("Last-Modified"):
        parsed_date = parser.parse(date_str)
        return calendar.timegm(parsed_date.timetuple())


def is_html_or_text(content_type: str) -> bool:
    return any(s in content_type for s in ("html", "text"))
