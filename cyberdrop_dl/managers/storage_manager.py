from __future__ import annotations

import asyncio
import itertools
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import psutil
from pydantic import ByteSize

from cyberdrop_dl.exceptions import InsufficientFreeSpaceError
from cyberdrop_dl.utils.logger import log, log_debug

if TYPE_CHECKING:
    from psutil._common import sdiskpart

    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.managers.manager import Manager


@dataclass(frozen=True, slots=True, order=True)
class DiskPartition:
    mountpoint: Path
    device: Path = field(compare=False, hash=False)
    fstype: str = field(compare=False, hash=False)
    opts: str = field(compare=False, hash=False)

    @classmethod
    def from_psutil(cls, diskpart: sdiskpart) -> DiskPartition:
        def resolve(path: str):
            # Resolve converts any mapped drive to UNC paths (windows)
            return Path(path).resolve()

        device, mount = tuple(map(resolve, diskpart[:2]))
        return DiskPartition(mount, device, *diskpart[2:4])


class MountStats(NamedTuple):
    partition: DiskPartition
    free_space: ByteSize


class StorageManager:
    """Runs an infinite loop to keep an updated value of the available space on all storage devices."""

    def __init__(self, manager: Manager):
        self.manager = manager
        self.total_data_written: int = 0
        self._paused_datetime = None
        self._used_mounts: set[Path] = set()
        self._free_space: dict[Path, int] = {}
        self._mount_addition_locks: dict[Path, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._updated = asyncio.Event()
        
        # Adaptive polling configuration
        self._base_period: int = 2  # minimum polling interval (seconds)
        self._max_period: int = 30  # maximum polling interval (seconds)
        self._current_period: int = self._base_period
        self._backoff_multiplier: float = 1.5
        self._activity_reset_threshold: int = 5  # reset backoff after this many active periods
        
        self._log_period: int = 10  # log storage details every <x> loops
        self._timedelta_period = timedelta(seconds=self._base_period)
        self._last_activity_time: float = time.time()
        self._consecutive_idle_periods: int = 0
        self._consecutive_active_periods: int = 0
        self._space_check_event = asyncio.Event()  # Event to trigger immediate space check
        
        self._partitions = []
        for p in psutil.disk_partitions(all=True):
            try:
                part = DiskPartition.from_psutil(p)
            except OSError as e:
                msg = f"Unable to get information about {p.mountpoint}. All files with that mountpoint as target will be skipped: {e!r}"
                log(msg, 40)
            else:
                self._partitions.append(part)

        self._loop = None  # Will be created when needed
        self._unavailable_mounts: set[Path] = set()

    def _ensure_loop_started(self) -> None:
        """Ensure the polling loop is started (lazy initialization)."""
        if self._loop is None:
            try:
                self._loop = asyncio.create_task(self._check_free_space_loop())
            except RuntimeError:
                # No event loop running, will try again later
                pass

    @property
    def mounts(self) -> tuple[Path, ...]:
        return tuple(p.mountpoint for p in self._partitions)

    @property
    def _simplified_stats(self) -> str:
        def stringify(mount_stats: MountStats) -> str:
            free_space = mount_stats.free_space.human_readable(decimal=True)
            stats_as_dict = asdict(mount_stats.partition) | {"free_space": free_space}
            return ", ".join(f"'{k}': '{v}'" for k, v in stats_as_dict.items())

        stats_as_str = "\n".join(f"    {stringify(mount_stats)}" for mount_stats in self.get_used_mounts_stats())
        return f"Storage status:\n {stats_as_str}"

    def get_used_mounts_stats(self) -> list[MountStats]:
        """Returns information of every used mount + its free space."""
        mounts_stats: list[MountStats] = []
        for mount in self._used_mounts:
            free_space = ByteSize(self._free_space[mount])
            partition = next(p for p in self._partitions if p.mountpoint == mount)
            mounts_stats.append(MountStats(partition, free_space))

        return mounts_stats

    async def check_free_space(self, media_item: MediaItem) -> None:
        """Checks if there is enough free space to download this item."""

        # Ensure polling loop is started
        self._ensure_loop_started()
        
        await self.manager.states.RUNNING.wait()
        
        # Record activity to optimize polling frequency
        self._record_activity()
        
        if not await self._has_sufficient_space(media_item.download_folder):
            raise InsufficientFreeSpaceError(origin=media_item)

    async def reset(self) -> None:
        # This is causing lockups
        # await self._updated.wait()  # Make sure a query is not running right now
        self.total_data_written = 0
        self._used_mounts = set()
        self._free_space = {}
        
        # Reset adaptive polling state
        self._current_period = self._base_period
        self._last_activity_time = time.time()
        self._consecutive_idle_periods = 0
        self._consecutive_active_periods = 0
        self._space_check_event.clear()

    async def close(self) -> None:
        await self.reset()
        if self._loop is not None:
            self._loop.cancel()
            try:
                await self._loop
            except asyncio.CancelledError:
                pass

    async def _has_sufficient_space(self, folder: Path) -> bool:
        """Checks if there is enough free space to download to this folder.

        `folder` must be an absolute path"""

        async def check_nt_network_drive():
            """Checks is the drive of this folder is a Windows network drive (UNC or unknown mapped drive) and exists."""
            # See: https://github.com/jbsparrow/CyberDropDownloader/issues/860
            if not psutil.WINDOWS:
                return

            # We can discard mapped drives because they would have been converted to UNC path at startup
            # calling resolve on a mapped network drive returns its UNC path
            # it would only still be a mapped drive is the network address is not available
            is_mapped_drive = ":" in folder.drive and len(folder.drive) == 2
            is_unc_path = folder.drive.startswith("\\\\")
            if is_mapped_drive or not is_unc_path:
                return

            folder_drive = drive_as_path(folder.drive)
            async with self._mount_addition_locks[folder_drive]:
                if folder_drive in itertools.chain(self._unavailable_mounts, self.mounts):
                    return
                msg = f"Checking new possible network_drive: '{folder_drive}' for folder '{folder}'"
                log_debug(msg)

                try:
                    is_dir = await asyncio.to_thread(folder_drive.is_dir)
                except OSError:
                    is_dir = False
                if is_dir:
                    net_drive = DiskPartition(folder_drive, folder_drive, "network_drive", "")
                    self._partitions.append(net_drive)
                else:
                    self._unavailable_mounts.add(folder_drive)

        await check_nt_network_drive()
        mount = get_mount_point(folder, self.mounts)
        if not mount:
            return False

        async with self._mount_addition_locks[mount]:
            if mount not in self._free_space:
                # Manually query this mount now. Next time it will be part of the loop
                result = await asyncio.to_thread(psutil.disk_usage, str(mount))
                self._free_space[mount] = result.free
                self._used_mounts.add(mount)
                log(f"A new mountpoint ('{mount!s}') will be used for '{folder}'")
                log(self._simplified_stats)

        return self._free_space[mount] > self.manager.config_manager.global_settings_data.general.required_free_space

    def _record_activity(self) -> None:
        """Record storage activity to optimize polling frequency."""
        current_time = time.time()
        self._last_activity_time = current_time
        self._consecutive_active_periods += 1
        
        # Reset polling period to base when activity is detected
        if self._consecutive_active_periods >= self._activity_reset_threshold:
            self._current_period = self._base_period
            self._consecutive_idle_periods = 0
            log_debug(f"Storage polling reset to {self._base_period}s due to activity", 20)
        
        # Trigger immediate space check if polling is slow
        if self._current_period > self._base_period:
            self._space_check_event.set()

    def _adjust_polling_frequency(self) -> None:
        """Adjust polling frequency based on recent activity."""
        current_time = time.time()
        time_since_activity = current_time - self._last_activity_time
        
        # If no activity for a while, increase polling interval (exponential backoff)
        if time_since_activity >= self._current_period * 2:
            self._consecutive_idle_periods += 1
            self._consecutive_active_periods = 0
            
            # Apply exponential backoff
            if self._consecutive_idle_periods > 2:  # Start backoff after 2 idle periods
                old_period = self._current_period
                self._current_period = min(
                    int(self._current_period * self._backoff_multiplier),
                    self._max_period
                )
                if old_period != self._current_period:
                    log_debug(f"Storage polling slowed to {self._current_period}s due to inactivity", 15)
        else:
            # Reset idle counter if there was recent activity
            self._consecutive_idle_periods = 0

    def trigger_immediate_check(self) -> None:
        """Trigger an immediate storage check (useful for external events)."""
        self._record_activity()
        self._space_check_event.set()

    def get_polling_stats(self) -> dict[str, float | int]:
        """Get statistics about the adaptive polling behavior."""
        current_time = time.time()
        return {
            "current_polling_interval": self._current_period,
            "base_polling_interval": self._base_period,
            "max_polling_interval": self._max_period,
            "seconds_since_last_activity": current_time - self._last_activity_time,
            "consecutive_idle_periods": self._consecutive_idle_periods,
            "consecutive_active_periods": self._consecutive_active_periods,
            "efficiency_ratio": self._base_period / self._current_period,  # 1.0 = most efficient, <1.0 = power saving
        }

    async def _check_free_space_loop(self) -> None:
        """Adaptive loop to get free space of all used mounts with exponential backoff optimization"""

        last_check = -1
        while True:
            await self.manager.states.RUNNING.wait()
            self._updated.clear()
            last_check += 1
            
            # Perform space check if there are used mounts
            if self._used_mounts:
                used_mounts = sorted(self._used_mounts)
                tasks = [asyncio.to_thread(psutil.disk_usage, str(mount)) for mount in used_mounts]
                results = await asyncio.gather(*tasks)
                for mount, result in zip(used_mounts, results, strict=True):
                    self._free_space[mount] = result.free
                if last_check % self._log_period == 0:
                    log_debug(self._simplified_stats)
                    log_debug(f"Storage polling interval: {self._current_period}s (base: {self._base_period}s, max: {self._max_period}s)", 20)
            
            self._updated.set()
            
            # Adjust polling frequency based on activity
            self._adjust_polling_frequency()
            
            # Wait for either the polling interval or an immediate check event
            try:
                await asyncio.wait_for(self._space_check_event.wait(), timeout=self._current_period)
                # If event was triggered, clear it and continue immediately
                self._space_check_event.clear()
                log_debug("Storage check triggered by activity event", 20)
            except asyncio.TimeoutError:
                # Normal timeout - continue with regular polling
                pass


@lru_cache
def get_mount_point(folder: Path, all_mounts: tuple[Path, ...]) -> Path | None:
    # Cached for performance.
    # It's not an expensive operation nor IO blocking, but it's very common for multiple files to share the same download folder
    # ex: HLS downloads could have over a thousand segments. All of them will go to the same folder
    assert folder.is_absolute()
    possible_mountpoints = [mount for mount in all_mounts if mount in folder.parents or mount == folder]
    if possible_mountpoints:
        # Get the closest mountpoint to `folder`
        # mount_a = /home/user/  -> points to an internal SSD
        # mount_b = /home/user/USB -> points to an external USB drive
        # If `folder`` is `/home/user/USB/videos`, the correct mountpoint is mount_b
        return max(possible_mountpoints, key=lambda path: len(path.parts))

    # Mount point for this path does not exists
    # This will only happen on Windows, ex: an USB drive (`D:`) that is not currently available (AKA disconnected)
    # On Unix there's always at least 1 mountpoint, root (`/`)
    msg = f"No available mountpoint found for '{folder}'"
    msg += f"\n -> drive = '{drive_as_path(folder.drive)}' , last_parent = '{folder.parents[-1]}'"
    log(msg, 40)


def drive_as_path(drive: str) -> Path:
    is_mapped_drive = ":" in drive and len(drive) == 2
    return Path(f"{drive}/" if is_mapped_drive else drive)
