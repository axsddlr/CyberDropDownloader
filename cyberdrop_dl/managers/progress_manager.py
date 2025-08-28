from __future__ import annotations

import time
import psutil
from contextlib import asynccontextmanager
from dataclasses import field
from datetime import timedelta
from functools import partial
from typing import TYPE_CHECKING, Any
from collections import defaultdict

from pydantic import ByteSize
from rich.columns import Columns
from rich.console import Group
from rich.layout import Layout
from rich.progress import Progress, SpinnerColumn, TaskID
from rich.text import Text
from yarl import URL

from cyberdrop_dl import __version__
from cyberdrop_dl.ui.progress.downloads_progress import DownloadsProgress
from cyberdrop_dl.ui.progress.file_progress import FileProgress
from cyberdrop_dl.ui.progress.hash_progress import HashProgress
from cyberdrop_dl.ui.progress.scraping_progress import ScrapingProgress
from cyberdrop_dl.ui.progress.sort_progress import SortProgress
from cyberdrop_dl.ui.progress.statistic_progress import DownloadStatsProgress, ScrapeStatsProgress
from cyberdrop_dl.utils.logger import log, log_spacer, log_with_color

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from rich.console import RenderableType

    from cyberdrop_dl.managers.manager import Manager
    from cyberdrop_dl.ui.progress.statistic_progress import UiFailureTotal

log_cyan = partial(log_with_color, style="cyan", level=20)
log_yellow = partial(log_with_color, style="yellow", level=20)
log_green = partial(log_with_color, style="green", level=20)
log_red = partial(log_with_color, style="red", level=20)


class ComprehensivePerformanceMonitor:
    """Advanced performance monitoring with system resource tracking and bottleneck detection."""
    
    def __init__(self):
        self.start_time = time.time()
        self.metrics = {
            'system_stats': defaultdict(list),
            'download_performance': defaultdict(list),
            'database_performance': defaultdict(list),
            'memory_usage': defaultdict(list),
            'bottlenecks': defaultdict(int),
            'adaptive_adjustments': defaultdict(list)
        }
        self.last_sample_time = time.time()
        self.sample_interval = 10.0  # Sample every 10 seconds
        
        # Bottleneck thresholds
        self.cpu_threshold = 80.0    # CPU usage %
        self.memory_threshold = 85.0 # Memory usage %
        self.disk_io_threshold = 80.0 # Disk usage %
        self.network_threshold = 95.0 # Network utilization %
        
    def record_system_metrics(self):
        """Record comprehensive system performance metrics."""
        current_time = time.time()
        if current_time - self.last_sample_time < self.sample_interval:
            return
            
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=0.1)
            cpu_count = psutil.cpu_count()
            
            # Memory metrics
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_available = memory.available
            
            # Disk I/O metrics
            disk_usage = psutil.disk_usage('/')
            disk_io = psutil.disk_io_counters()
            
            # Network metrics (if available)
            try:
                network_io = psutil.net_io_counters()
                network_sent = network_io.bytes_sent
                network_recv = network_io.bytes_recv
            except Exception:
                network_sent = network_recv = 0
            
            # Store metrics
            self.metrics['system_stats']['timestamps'].append(current_time)
            self.metrics['system_stats']['cpu_percent'].append(cpu_percent)
            self.metrics['system_stats']['cpu_count'].append(cpu_count)
            self.metrics['system_stats']['memory_percent'].append(memory_percent)
            self.metrics['system_stats']['memory_available'].append(memory_available)
            self.metrics['system_stats']['disk_usage_percent'].append(disk_usage.percent)
            self.metrics['system_stats']['network_sent'].append(network_sent)
            self.metrics['system_stats']['network_recv'].append(network_recv)
            
            # Detect bottlenecks
            self._detect_bottlenecks(cpu_percent, memory_percent, disk_usage.percent)
            
            self.last_sample_time = current_time
            
        except Exception as e:
            log(f"Performance monitoring error: {e}", 30)
    
    def _detect_bottlenecks(self, cpu_percent: float, memory_percent: float, disk_percent: float):
        """Detect system bottlenecks and log warnings."""
        if cpu_percent > self.cpu_threshold:
            self.metrics['bottlenecks']['cpu'] += 1
            if self.metrics['bottlenecks']['cpu'] % 5 == 1:  # Log every 5th detection
                log(f"CPU bottleneck detected: {cpu_percent:.1f}% usage", 30)
        
        if memory_percent > self.memory_threshold:
            self.metrics['bottlenecks']['memory'] += 1
            if self.metrics['bottlenecks']['memory'] % 5 == 1:
                log(f"Memory bottleneck detected: {memory_percent:.1f}% usage", 30)
        
        if disk_percent > self.disk_io_threshold:
            self.metrics['bottlenecks']['disk'] += 1
            if self.metrics['bottlenecks']['disk'] % 5 == 1:
                log(f"Disk bottleneck detected: {disk_percent:.1f}% usage", 30)
    
    def record_download_performance(self, domain: str, file_size: int, duration: float, success: bool):
        """Record download performance metrics."""
        speed = file_size / duration if duration > 0 else 0
        self.metrics['download_performance']['domain'].append(domain)
        self.metrics['download_performance']['file_size'].append(file_size)
        self.metrics['download_performance']['duration'].append(duration)
        self.metrics['download_performance']['speed'].append(speed)
        self.metrics['download_performance']['success'].append(success)
        self.metrics['download_performance']['timestamp'].append(time.time())
    
    def record_database_operation(self, operation: str, duration: float, batch_size: int = 1):
        """Record database operation performance."""
        self.metrics['database_performance']['operation'].append(operation)
        self.metrics['database_performance']['duration'].append(duration)
        self.metrics['database_performance']['batch_size'].append(batch_size)
        self.metrics['database_performance']['ops_per_second'].append(batch_size / duration if duration > 0 else 0)
        self.metrics['database_performance']['timestamp'].append(time.time())
    
    def record_adaptive_adjustment(self, component: str, adjustment: str, old_value: Any, new_value: Any):
        """Record adaptive system adjustments."""
        self.metrics['adaptive_adjustments']['component'].append(component)
        self.metrics['adaptive_adjustments']['adjustment'].append(adjustment)
        self.metrics['adaptive_adjustments']['old_value'].append(old_value)
        self.metrics['adaptive_adjustments']['new_value'].append(new_value)
        self.metrics['adaptive_adjustments']['timestamp'].append(time.time())
        
        log(f"Adaptive adjustment - {component}: {adjustment} ({old_value} -> {new_value})", 20)
    
    def get_performance_summary(self) -> dict[str, Any]:
        """Get comprehensive performance summary."""
        current_time = time.time()
        total_runtime = current_time - self.start_time
        
        # Calculate averages and summaries
        summary = {
            'total_runtime': total_runtime,
            'system_performance': self._get_system_summary(),
            'download_performance': self._get_download_summary(),
            'database_performance': self._get_database_summary(),
            'bottlenecks_detected': dict(self.metrics['bottlenecks']),
            'adaptive_adjustments': len(self.metrics['adaptive_adjustments']['component']),
        }
        
        return summary
    
    def _get_system_summary(self) -> dict[str, Any]:
        """Get system performance summary."""
        system_stats = self.metrics['system_stats']
        if not system_stats.get('cpu_percent'):
            return {'status': 'no_data'}
        
        return {
            'avg_cpu_percent': sum(system_stats['cpu_percent']) / len(system_stats['cpu_percent']),
            'max_cpu_percent': max(system_stats['cpu_percent']),
            'avg_memory_percent': sum(system_stats['memory_percent']) / len(system_stats['memory_percent']),
            'max_memory_percent': max(system_stats['memory_percent']),
            'samples_collected': len(system_stats['cpu_percent']),
        }
    
    def _get_download_summary(self) -> dict[str, Any]:
        """Get download performance summary."""
        download_stats = self.metrics['download_performance']
        if not download_stats.get('speed'):
            return {'status': 'no_data'}
        
        speeds = download_stats['speed']
        successes = download_stats['success']
        
        return {
            'total_downloads': len(speeds),
            'success_rate': sum(successes) / len(successes) if successes else 0.0,
            'avg_speed_mbps': (sum(speeds) / len(speeds)) / (1024 * 1024) if speeds else 0.0,
            'max_speed_mbps': max(speeds) / (1024 * 1024) if speeds else 0.0,
            'total_bytes': sum(download_stats.get('file_size', [])),
        }
    
    def _get_database_summary(self) -> dict[str, Any]:
        """Get database performance summary."""
        db_stats = self.metrics['database_performance']
        if not db_stats.get('ops_per_second'):
            return {'status': 'no_data'}
        
        ops_per_sec = db_stats['ops_per_second']
        batch_sizes = db_stats['batch_size']
        
        return {
            'total_operations': len(ops_per_sec),
            'avg_ops_per_second': sum(ops_per_sec) / len(ops_per_sec),
            'max_ops_per_second': max(ops_per_sec),
            'avg_batch_size': sum(batch_sizes) / len(batch_sizes) if batch_sizes else 0.0,
            'total_batches': len([b for b in batch_sizes if b > 1]),
        }


# Global performance monitor instance
_performance_monitor = ComprehensivePerformanceMonitor()


class ProgressManager:
    def __init__(self, manager: Manager) -> None:
        # File Download Bars
        self.manager = manager
        ui_options = manager.config_manager.global_settings_data.ui_options
        self.portrait = manager.parsed_args.cli_only_args.portrait
        self.file_progress = FileProgress(manager)
        self.scraping_progress = ScrapingProgress(manager)
        
        # Performance monitoring
        self.performance_monitor = _performance_monitor

        # Overall Progress Bars & Stats
        self.download_progress = DownloadsProgress(manager)
        self.download_stats_progress = DownloadStatsProgress()
        self.scrape_stats_progress = ScrapeStatsProgress()
        self.hash_progress = HashProgress(manager)
        self.sort_progress = SortProgress(1, manager)

        self.ui_refresh_rate = ui_options.refresh_rate

        self.hash_remove_layout: RenderableType = field(init=False)
        self.hash_layout: RenderableType = field(init=False)
        self.sort_layout: RenderableType = field(init=False)
        self.status_message: Progress = field(init=False)
        self.status_message_task_id: TaskID = field(init=False)

    @asynccontextmanager
    async def show_status_msg(self, msg: str | None) -> AsyncGenerator:
        try:
            self.status_message.update(self.status_message_task_id, description=msg, visible=bool(msg))
            yield
        finally:
            self.status_message.update(self.status_message_task_id, visible=False)

    def pause_or_resume(self):
        if self.manager.states.RUNNING.is_set():
            self.pause()
        else:
            self.resume()

    def pause(self, msg: str = ""):
        self.manager.states.RUNNING.clear()
        suffix = f" [{msg}]" if msg else ""
        self.activity.update(self.activity_task_id, description=f"Paused{suffix}")

    def resume(self):
        self.manager.states.RUNNING.set()
        self.activity.update(self.activity_task_id, description="Running Cyberdrop-DL")

    def startup(self) -> None:
        """Startup process for the progress manager."""
        spinner = SpinnerColumn(style="green", spinner_name="dots")
        activity = Progress(spinner, "[progress.description]{task.description}")
        self.status_message = Progress(spinner, "[progress.description]{task.description}")

        self.status_message_task_id = self.status_message.add_task("", total=100, completed=0, visible=False)
        self.activity_task_id = activity.add_task(f"Running Cyberdrop-DL: v{__version__}", total=100, completed=0)
        self.activity = activity

        simple_layout = Group(activity, self.download_progress.simple_progress)

        status_message_columns = Columns([activity, self.status_message], expand=False)

        horizontal_layout = Layout()
        vertical_layout = Layout()

        upper_layouts = (
            Layout(renderable=self.download_progress.get_progress(), name="Files", ratio=1, minimum_size=9),
            Layout(renderable=self.scrape_stats_progress.get_progress(), name="Scrape Failures", ratio=1),
            Layout(renderable=self.download_stats_progress.get_progress(), name="Download Failures", ratio=1),
        )

        lower_layouts = (
            Layout(renderable=self.scraping_progress.get_renderable(), name="Scraping", ratio=20),
            Layout(renderable=self.file_progress.get_renderable(), name="Downloads", ratio=20),
            Layout(renderable=status_message_columns, name="status_message", ratio=2),
        )

        horizontal_layout.split_column(Layout(name="upper", ratio=20), *lower_layouts)
        vertical_layout.split_column(Layout(name="upper", ratio=60), *lower_layouts)

        horizontal_layout["upper"].split_row(*upper_layouts)
        vertical_layout["upper"].split_column(*upper_layouts)

        self.horizontal_layout = horizontal_layout
        self.vertical_layout = vertical_layout
        self.activity_layout = activity
        self.simple_layout = simple_layout
        self.hash_remove_layout = self.hash_progress.get_removed_progress()
        self.hash_layout = self.hash_progress.get_renderable()
        self.sort_layout = self.sort_progress.get_renderable()

    @property
    def fullscreen_layout(self) -> Layout:
        if self.portrait:
            return self.vertical_layout
        return self.horizontal_layout

    def print_stats(self, start_time: float) -> None:
        """Prints the stats of the program."""
        if not self.manager.parsed_args.cli_only_args.print_stats:
            return
        end_time = time.perf_counter()
        runtime = timedelta(seconds=int(end_time - start_time))
        total_data_written = ByteSize(self.manager.storage_manager.total_data_written).human_readable(decimal=True)

        log_spacer(20)
        log("Printing Stats...\n", 20)
        config_path = self.manager.path_manager.config_folder / self.manager.config_manager.loaded_config
        config_path_text = get_console_hyperlink(config_path, text=self.manager.config_manager.loaded_config)
        input_file_text = get_input(self.manager)
        log_folder_text = get_console_hyperlink(self.manager.path_manager.log_folder)

        log_concat("Run Stats (config: ", config_path_text, ")", style="cyan")
        log_concat("  Input File: ", input_file_text, style="yellow")
        log_yellow(f"  Input URLs: {self.manager.scrape_mapper.count:,}")
        log_yellow(f"  Input URL Groups: {self.manager.scrape_mapper.group_count:,}")
        log_concat("  Log Folder: ", log_folder_text, style="yellow")
        log_yellow(f"  Total Runtime: {runtime}")
        log_yellow(f"  Total Downloaded Data: {total_data_written}")

        log_spacer(20, "")
        log_cyan("Download Stats:")
        log_green(f"  Downloaded: {self.download_progress.completed_files:,} files")
        log_yellow(f"  Skipped (By Config): {self.download_progress.skipped_files:,} files")
        log_yellow(f"  Skipped (Previously Downloaded): {self.download_progress.previously_completed_files:,} files")
        log_red(f"  Failed: {self.download_stats_progress.failed_files:,} files")

        log_spacer(20, "")
        log_cyan("Unsupported URLs Stats:")
        log_yellow(f"  Sent to Jdownloader: {self.scrape_stats_progress.sent_to_jdownloader:,}")
        log_yellow(f"  Skipped: {self.scrape_stats_progress.unsupported_urls_skipped:,}")

        self.print_dedupe_stats()

        log_spacer(20, "")
        log_cyan("Sort Stats:")
        log_green(f"  Audios: {self.sort_progress.audio_count:,}")
        log_green(f"  Images: {self.sort_progress.image_count:,}")
        log_green(f"  Videos: {self.sort_progress.video_count:,}")
        log_green(f"  Other Files: {self.sort_progress.other_count:,}")

        # Phase 2 Performance monitoring
        self.print_performance_stats()

        last_padding = log_failures(self.scrape_stats_progress.return_totals(), "Scrape Failures:")
        log_failures(self.download_stats_progress.return_totals(), "Download Failures:", last_padding)
    
    def print_performance_stats(self) -> None:
        """Print Phase 2 advanced performance statistics."""
        log_spacer(20, "")
        log_cyan("Phase 2 Performance Stats:")
        
        try:
            # Get comprehensive performance summary
            perf_summary = self.performance_monitor.get_performance_summary()
            
            # System performance
            sys_perf = perf_summary.get('system_performance', {})
            if sys_perf.get('status') != 'no_data':
                log_yellow(f"  System Performance:")
                log_yellow(f"    Average CPU Usage: {sys_perf.get('avg_cpu_percent', 0):.1f}%")
                log_yellow(f"    Peak CPU Usage: {sys_perf.get('max_cpu_percent', 0):.1f}%")
                log_yellow(f"    Average Memory Usage: {sys_perf.get('avg_memory_percent', 0):.1f}%")
                log_yellow(f"    Peak Memory Usage: {sys_perf.get('max_memory_percent', 0):.1f}%")
            
            # Download performance
            dl_perf = perf_summary.get('download_performance', {})
            if dl_perf.get('status') != 'no_data':
                log_green(f"  Download Performance:")
                log_green(f"    Success Rate: {dl_perf.get('success_rate', 0):.1%}")
                log_green(f"    Average Speed: {dl_perf.get('avg_speed_mbps', 0):.2f} MB/s")
                log_green(f"    Peak Speed: {dl_perf.get('max_speed_mbps', 0):.2f} MB/s")
                log_green(f"    Total Data: {ByteSize(dl_perf.get('total_bytes', 0)).human_readable(decimal=True)}")
            
            # Database performance
            db_perf = perf_summary.get('database_performance', {})
            if db_perf.get('status') != 'no_data':
                log_green(f"  Database Performance:")
                log_green(f"    Operations per Second: {db_perf.get('avg_ops_per_second', 0):.1f}")
                log_green(f"    Peak Operations per Second: {db_perf.get('max_ops_per_second', 0):.1f}")
                log_green(f"    Average Batch Size: {db_perf.get('avg_batch_size', 0):.1f}")
                log_green(f"    Total Batched Operations: {db_perf.get('total_batches', 0):,}")
            
            # Bottlenecks and adaptations
            bottlenecks = perf_summary.get('bottlenecks_detected', {})
            adaptations = perf_summary.get('adaptive_adjustments', 0)
            
            if bottlenecks or adaptations:
                log_cyan(f"  Adaptive Intelligence:")
                if bottlenecks:
                    log_yellow(f"    Bottlenecks Detected: CPU({bottlenecks.get('cpu', 0)}), Memory({bottlenecks.get('memory', 0)}), Disk({bottlenecks.get('disk', 0)})")
                log_green(f"    Adaptive Adjustments Made: {adaptations}")
            
            # Get retry statistics from download client
            try:
                from cyberdrop_dl.clients.download_client import _retry_manager
                retry_stats = _retry_manager.get_stats()
                if retry_stats:
                    log_cyan(f"  Intelligent Retry Stats:")
                    total_attempts = sum(stats.get('attempts', 0) for stats in retry_stats.values())
                    total_successes = sum(stats.get('successes', 0) for stats in retry_stats.values())
                    if total_attempts > 0:
                        success_rate = total_successes / total_attempts
                        log_green(f"    Retry Success Rate: {success_rate:.1%}")
                        log_yellow(f"    Total Retry Attempts: {total_attempts}")
                        log_green(f"    Domains with Retries: {len(retry_stats)}")
            except Exception:
                pass  # Retry stats not available
                
        except Exception as e:
            log(f"Error displaying performance stats: {e}", 30)

    def print_dedupe_stats(self) -> None:
        log_spacer(20, "")
        log_cyan("Dupe Stats:")
        log_yellow(f"  Newly Hashed: {self.hash_progress.hashed_files:,} files")
        log_yellow(f"  Previously Hashed: {self.hash_progress.prev_hashed_files:,} files")
        log_yellow(f"  Removed (Downloads): {self.hash_progress.removed_files:,} files")


def log_failures(failures: list[UiFailureTotal], title: str = "Failures:", last_padding: int = 0) -> int:
    log_spacer(20, "")
    log_cyan(title)
    if not failures:
        log_green("  None")
        return 0
    error_padding = last_padding
    error_codes = [f.error_code for f in failures if f.error_code is not None]
    if error_codes:
        error_padding = max(len(str(max(error_codes))), error_padding)
    for f in failures:
        error = f.error_code if f.error_code is not None else ""
        log_red(f"  {error:>{error_padding}}{' ' if error_padding else ''}{f.msg}: {f.total:,}")
    return error_padding


def get_input(manager: Manager) -> Text | str:
    if manager.parsed_args.cli_only_args.retry_all:
        return "--retry-all"
    if manager.parsed_args.cli_only_args.retry_failed:
        return "--retry-failed"
    if manager.parsed_args.cli_only_args.retry_maintenance:
        return "--retry-maintenance"
    if manager.scrape_mapper.using_input_file:
        return get_console_hyperlink(manager.path_manager.input_file)
    return "--links (CLI args)"


def get_console_hyperlink(file_path: Path, text: str = "") -> Text:
    full_path = file_path
    show_text = text or full_path
    file_url = URL(full_path.as_posix()).with_scheme("file")
    return Text(str(show_text), style=f"link {file_url}")


def concat_as_text(*text_or_str, style: str = "") -> Text:
    result = Text()
    for elem in text_or_str:
        if isinstance(elem, Text):
            text = elem
            if style and text.style != style:
                text.stylize(f"{style} {text.style}")
        else:
            text = Text(elem, style=style)

        result.append(text)
    return result


def log_concat(*text_or_str, style: str = "", **kwargs) -> None:
    text = concat_as_text(*text_or_str, style=style)
    log_with_color(text, style, **kwargs)
