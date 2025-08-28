from __future__ import annotations

import asyncio
import pathlib
from sqlite3 import IntegrityError, Row
from typing import TYPE_CHECKING
from collections import defaultdict

from cyberdrop_dl.utils.database.table_definitions import create_fixed_history, create_history
from cyberdrop_dl.utils.utilities import log

if TYPE_CHECKING:
    import datetime
    from collections.abc import Iterable

    import aiosqlite
    from yarl import URL

    from cyberdrop_dl.crawlers import Crawler
    from cyberdrop_dl.data_structures.url_objects import MediaItem

DB_UPDATES = (
    "UPDATE OR REPLACE media SET domain = 'jpg5.su' WHERE domain = 'sharex'",
    "UPDATE OR REPLACE media SET domain = 'nudostar.tv' WHERE domain = 'nudostartv'",
    "UPDATE OR REPLACE media SET referer = FIX_REDGIFS_REFERER(referer) WHERE domain = 'redgifs';",
    "UPDATE OR REPLACE media SET referer = FIX_JPG5_REFERER(referer) WHERE domain = 'jpg5.su';",
)


def get_db_path(url: URL, domain: str = "") -> str:
    """Gets the URL path to be put into the DB and checked from the DB."""

    # TODO: domain SHOULD be mandatory, not optional. Make it mandatory and update any place that uses it

    if domain:
        if "e-hentai" in domain:
            return url.path.split("keystamp")[0][:-1]

        if "mediafire" in domain:
            return url.name

        if "mega.nz" in domain:
            return url.path_qs if not (frag := url.fragment) else f"{url.path_qs}#{frag}"

    return url.path


class DatabaseBatch:
    """Batches database operations for improved performance."""
    
    def __init__(self, db_conn: aiosqlite.Connection, batch_size: int = 100):
        self.db_conn = db_conn
        self.batch_size = batch_size
        self.operations = defaultdict(list)
        self._lock = asyncio.Lock()
    
    def add_operation(self, query: str, params: tuple):
        """Add an operation to the batch."""
        self.operations[query].append(params)
    
    async def flush(self) -> None:
        """Execute all batched operations."""
        async with self._lock:
            if not self.operations:
                return
                
            cursor = await self.db_conn.cursor()
            try:
                for query, param_list in self.operations.items():
                    if len(param_list) == 1:
                        await cursor.execute(query, param_list[0])
                    else:
                        await cursor.executemany(query, param_list)
                await self.db_conn.commit()
                self.operations.clear()
            except Exception as e:
                await self.db_conn.rollback()
                log(f"Database batch operation failed: {e}", 40, exc_info=e)
                raise
    
    async def auto_flush(self) -> None:
        """Automatically flush if batch size is reached."""
        total_operations = sum(len(params) for params in self.operations.values())
        if total_operations >= self.batch_size:
            await self.flush()


class HistoryTable:
    def __init__(self, db_conn: aiosqlite.Connection) -> None:
        self.db_conn: aiosqlite.Connection = db_conn
        self.ignore_history: bool = False
        self.batch = DatabaseBatch(db_conn, batch_size=50)
        self._batch_timer_task: asyncio.Task | None = None

    async def startup(self) -> None:
        """Startup process for the HistoryTable."""
        from cyberdrop_dl.crawlers import jpg5, redgifs

        await self.db_conn.create_function("FIX_REDGIFS_REFERER", 1, redgifs.fix_db_referer, deterministic=True)
        await self.db_conn.create_function("FIX_JPG5_REFERER", 1, jpg5.fix_db_referer, deterministic=True)
        await self.db_conn.execute(create_history)
        await self.db_conn.commit()
        await self.fix_primary_keys()
        await self.add_columns_media()
        await self.run_updates()
        
        # Start periodic batch flush timer
        self._start_batch_timer()
    
    async def shutdown(self) -> None:
        """Shutdown process for the HistoryTable."""
        if self._batch_timer_task and not self._batch_timer_task.done():
            self._batch_timer_task.cancel()
            try:
                await self._batch_timer_task
            except asyncio.CancelledError:
                pass
        
        # Flush any remaining batched operations
        await self.batch.flush()
    
    def _start_batch_timer(self) -> None:
        """Start periodic timer to flush batched operations."""
        async def timer():
            while True:
                try:
                    await asyncio.sleep(2.0)  # Flush every 2 seconds
                    await self.batch.flush()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log(f"Batch timer error: {e}", 40, exc_info=e)
        
        self._batch_timer_task = asyncio.create_task(timer())

    async def update_previously_unsupported(self, crawlers: dict[str, Crawler]) -> None:
        """Update old `no_crawler` entries that are now supported."""
        domains_to_update = [
            (c.DOMAIN, f"http%{c.PRIMARY_URL.host}%") for c in crawlers.values() if c.UPDATE_UNSUPPORTED
        ]
        if not domains_to_update:
            return
        referers = [(d[1],) for d in domains_to_update]
        cursor = await self.db_conn.cursor()
        query = "UPDATE OR IGNORE media SET domain = ? WHERE domain = 'no_crawler' AND referer LIKE ?"
        await cursor.executemany(query, domains_to_update)
        query = "DELETE FROM media WHERE domain = 'no_crawler' AND referer LIKE ?"
        await cursor.executemany(query, referers)
        await self.db_conn.commit()

    async def run_updates(self) -> None:
        cursor = await self.db_conn.cursor()
        for query in DB_UPDATES:
            await cursor.execute(query)
        await self.db_conn.commit()

    async def delete_invalid_rows(self) -> None:
        query = """DELETE FROM media WHERE download_filename = '' """
        cursor = await self.db_conn.cursor()
        await cursor.execute(query)
        await self.db_conn.commit()

    async def check_complete(self, domain: str, url: URL, referer: URL) -> bool:
        """Checks whether an individual file has completed given its domain and url path."""
        if self.ignore_history:
            return False

        url_path = get_db_path(url, domain)
        cursor = await self.db_conn.cursor()
        query = """SELECT referer, completed FROM media WHERE domain = ? and url_path = ?"""
        result = await cursor.execute(query, (domain, url_path))
        sql_file_check = await result.fetchone()
        if sql_file_check and sql_file_check[1] != 0:
            # Update the referer if it has changed so that check_complete_by_referer can work
            if str(referer) != sql_file_check[0] and url != referer:
                log(f"Updating referer of {url} from {sql_file_check[0]} to {referer}")
                self.batch.add_operation(
                    """UPDATE media SET referer = ? WHERE domain = ? and url_path = ?""",
                    (str(referer), domain, url_path)
                )
                await self.batch.auto_flush()

            return True
        return False

    async def check_album(self, domain: str, album_id: str) -> dict[str, int]:
        """Checks whether an album has completed given its domain and album id."""
        if self.ignore_history:
            return {}

        cursor = await self.db_conn.cursor()
        result = await cursor.execute(
            """SELECT url_path, completed FROM media WHERE domain = ? and album_id = ?""",
            (domain, album_id),
        )
        result = await result.fetchall()
        return {row[0]: row[1] for row in result}

    async def set_album_id(self, domain: str, media_item: MediaItem) -> None:
        """Sets an album_id in the database."""

        url_path = get_db_path(media_item.url, str(media_item.referer))
        self.batch.add_operation(
            """UPDATE media SET album_id = ? WHERE domain = ? and url_path = ?""",
            (media_item.album_id, domain, url_path)
        )
        await self.batch.auto_flush()

    async def check_complete_by_referer(self, domain: str | None, referer: URL) -> bool:
        """Checks whether an individual file has completed given its domain and url path."""
        if self.ignore_history:
            return False
        if domain is None:
            query, *params = "SELECT completed FROM media WHERE referer = ?", str(referer)
        else:
            query, *params = "SELECT completed FROM media WHERE referer = ? and domain = ?", str(referer), domain

        cursor = await self.db_conn.cursor()
        result = await cursor.execute(query, params)
        if domain is None:
            results = await result.fetchall()
        else:
            row = await result.fetchone()
            results = [row] if row is not None else []
        return bool(results and any(row[0] != 0 for row in results))

    async def insert_incompleted(self, domain: str, media_item: MediaItem) -> None:
        """Inserts an uncompleted file into the database."""

        url_path = get_db_path(media_item.url, str(media_item.referer))
        download_filename = media_item.download_filename or ""
        
        # Add operations to batch instead of executing immediately
        try:
            self.batch.add_operation(
                """UPDATE media SET domain = ?, album_id = ? WHERE domain = 'no_crawler' and url_path = ? and referer = ?""",
                (domain, media_item.album_id, url_path, str(media_item.referer))
            )
        except IntegrityError:
            self.batch.add_operation(
                """DELETE FROM media WHERE domain = 'no_crawler' and url_path = ?""",
                (url_path,)
            )
        
        self.batch.add_operation(
            """INSERT OR IGNORE INTO media (domain, url_path, referer, album_id, download_path, download_filename, original_filename, completed, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (
                domain,
                url_path,
                str(media_item.referer),
                media_item.album_id,
                str(media_item.download_folder),
                download_filename,
                media_item.original_filename,
                0,
            )
        )
        
        if download_filename:
            self.batch.add_operation(
                """UPDATE media SET download_filename = ? WHERE domain = ? and url_path = ?""",
                (download_filename, domain, url_path)
            )
        
        await self.batch.auto_flush()

    async def mark_complete(self, domain: str, media_item: MediaItem) -> None:
        """Mark a download as completed in the database."""

        url_path = get_db_path(media_item.url, str(media_item.referer))
        self.batch.add_operation(
            """UPDATE media SET completed = 1, completed_at = CURRENT_TIMESTAMP WHERE domain = ? and url_path = ?""",
            (domain, url_path)
        )
        await self.batch.auto_flush()

    async def add_filesize(self, domain: str, media_item: MediaItem) -> None:
        """Add the file size to the db."""

        url_path = get_db_path(media_item.url, str(media_item.referer))
        file_size = pathlib.Path(media_item.complete_file).stat().st_size
        self.batch.add_operation(
            """UPDATE media SET file_size=? WHERE domain = ? and url_path = ?""",
            (file_size, domain, url_path)
        )
        await self.batch.auto_flush()

    async def add_duration(self, domain: str, media_item: MediaItem) -> None:
        """Add the file duration to the db."""

        url_path = get_db_path(media_item.url, str(media_item.referer))
        duration = media_item.duration
        self.batch.add_operation(
            """UPDATE media SET duration=? WHERE domain = ? and url_path = ?""",
            (duration, domain, url_path)
        )
        await self.batch.auto_flush()

    async def get_duration(self, domain: str, media_item: MediaItem) -> float | None:
        """Returns the duration from the database."""
        if media_item.is_segment:
            return
        url_path = get_db_path(media_item.url, str(media_item.referer))
        cursor = await self.db_conn.cursor()
        result = await cursor.execute(
            """SELECT duration FROM media WHERE domain = ? and url_path = ?""",
            (domain, url_path),
        )
        sql_duration = await result.fetchone()
        return sql_duration[0] if sql_duration else None

    async def add_download_filename(self, domain: str, media_item: MediaItem) -> None:
        """Add the download_filename to the db."""
        url_path = get_db_path(media_item.url, str(media_item.referer))
        self.batch.add_operation(
            """UPDATE media SET download_filename=? WHERE domain = ? and url_path = ? and download_filename = '' """,
            (media_item.download_filename, domain, url_path)
        )
        await self.batch.auto_flush()

    async def check_filename_exists(self, filename: str) -> bool:
        """Checks whether a downloaded filename exists in the database."""
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""SELECT EXISTS(SELECT 1 FROM media WHERE download_filename = ?)""", (filename,))
        sql_file_check = await result.fetchone()
        return sql_file_check == 1

    async def get_downloaded_filename(self, domain: str, media_item: MediaItem) -> str | None:
        """Returns the downloaded filename from the database."""

        if media_item.is_segment:
            return media_item.filename
        url_path = get_db_path(media_item.url, str(media_item.referer))
        cursor = await self.db_conn.cursor()
        result = await cursor.execute(
            """SELECT download_filename FROM media WHERE domain = ? and url_path = ?""",
            (domain, url_path),
        )
        sql_file_check = await result.fetchone()
        return sql_file_check[0] if sql_file_check else None

    async def get_failed_items(self) -> Iterable[Row]:
        """Returns a list of failed items."""
        cursor = await self.db_conn.cursor()
        result = await cursor.execute(
            """SELECT referer, download_path,completed_at,created_at FROM media WHERE completed = 0""",
        )
        return await result.fetchall()

    async def get_all_items(self, after: datetime.date, before: datetime.date) -> Iterable[Row]:
        """Returns a list of all items."""
        cursor = await self.db_conn.cursor()
        result = await cursor.execute(
            """
        SELECT referer, download_path,completed_at,created_at
        FROM media
        WHERE COALESCE(completed_at, '1970-01-01') BETWEEN ? AND ?
        ORDER BY completed_at DESC;""",
            (after.strftime("%Y-%m-%d"), before.strftime("%Y-%m-%d")),
        )
        return await result.fetchall()

    async def get_unique_download_paths(self) -> Iterable[Row]:
        """Returns a list of unique download paths."""
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""SELECT DISTINCT download_path FROM media""")
        return await result.fetchall()

    async def get_all_bunkr_failed(self) -> list:
        hash_list = await self.get_all_bunkr_failed_via_hash()
        size_list = await self.get_all_bunkr_failed_via_size()
        return hash_list + size_list

    async def get_all_bunkr_failed_via_size(self) -> list:
        try:
            """Returns a list of all items"""
            cursor = await self.db_conn.cursor()
            result = await cursor.execute("""
            SELECT referer,download_path,completed_at,created_at
            from media
            where file_size=322509
    ;
            """)
            all_files = await result.fetchall()
            return list(all_files)
        except Exception as e:
            log(f"Error getting bunkr failed via size: {e}", 40, exc_info=e)
            return []

    async def get_all_bunkr_failed_via_hash(self) -> list:
        try:
            """Returns a list of all items"""
            cursor = await self.db_conn.cursor()
            result = await cursor.execute("""
    SELECT m.referer,download_path,completed_at,created_at
    FROM hash h
    INNER JOIN media m ON h.download_filename= m.download_filename
    WHERE h.hash = 'eb669b6362e031fa2b0f1215480c4e30';
            """)
            all_files = await result.fetchall()
            return list(all_files)
        except Exception as e:
            log(f"Error getting bunkr failed via hash: {e}", 40, exc_info=e)
            return []

    async def fix_primary_keys(self) -> None:
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""pragma table_info(media)""")
        result = await result.fetchall()
        if result[0][5] == 0:  # type: ignore
            await self.db_conn.execute(create_fixed_history)
            await self.db_conn.commit()

            await self.db_conn.execute(
                """INSERT INTO media_copy (domain, url_path, referer, download_path, download_filename, original_filename, completed) SELECT * FROM media GROUP BY domain, url_path, original_filename;""",
            )
            await self.db_conn.commit()

            await self.db_conn.execute("""DROP TABLE media""")
            await self.db_conn.commit()

            await self.db_conn.execute("""ALTER TABLE media_copy RENAME TO media""")
            await self.db_conn.commit()

    async def add_columns_media(self) -> None:
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""pragma table_info(media)""")
        result = await result.fetchall()
        current_cols = [col[1] for col in result]

        async def add_column(name: str, type_: str) -> None:
            if name not in current_cols:
                await self.db_conn.execute(f"ALTER TABLE media ADD COLUMN {name} {type_}")
                await self.db_conn.commit()

        await add_column("album_id", "TEXT")
        await add_column("created_at", "TIMESTAMP")
        await add_column("completed_at", "TIMESTAMP")
        await add_column("file_size", "INT")
        await add_column("duration", "FLOAT")
