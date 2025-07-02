import datetime
import json
import re
import shutil
import threading
import traceback
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from app import schemas
from app.chain.media import MediaChain
from app.chain.storage import StorageChain
from app.chain.tmdb import TmdbChain
from app.chain.transfer import TransferChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfoPath
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.db.transferhistory_oper import TransferHistoryOper
from app.helper.directory import DirectoryHelper
from app.log import logger
from app.modules.filemanager import FileManagerModule
from app.plugins import _PluginBase
from app.schemas import NotificationType, TransferInfo, TransferDirectoryConf
from app.schemas.types import EventType, MediaType, SystemConfigKey
from app.utils.string import StringUtils
from app.utils.system import SystemUtils

lock = threading.Lock()


class FileMonitorHandler(FileSystemEventHandler):
    """
    目录监控响应类
    """
    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync

    def on_created(self, event):
        self.sync.event_handler(event=event, text="创建", mon_path=self._watch_path, event_path=event.src_path)

    def on_moved(self, event):
        self.sync.event_handler(event=event, text="移动", mon_path=self._watch_path, event_path=event.dest_path)


class CloudLinkMonitor(_PluginBase):
    # 插件信息
    plugin_name = "多目录实时监控"
    plugin_desc = "监控多目录文件变化，自动转移媒体文件，支持轮询分发和持久化缓存。"
    plugin_icon = "Linkease_A.png"
    plugin_version = "2.8.2"  # 最终完整修复版
    plugin_author = "wonderful"
    author_url = "https://github.com/WonderMaker123/MoviePilot-Plugins2/"
    plugin_config_prefix = "cloudlinkmonitor_"
    plugin_order = 4
    auth_level = 1

    # --- 私有属性 ---
    _scheduler: Optional[BackgroundScheduler] = None
    _observer: List[Observer] = []
    _event = threading.Event()
    
    # 插件配置
    _enabled = False
    _notify = False
    _onlyonce = False
    _history = False
    _scrape = False
    _category = False
    _refresh = False
    _softlink = False
    _strm = False
    _cron: Optional[str] = None
    _size: int = 0
    _mode: str = "compatibility"
    _transfer_type: str = "softlink"
    _monitor_dirs: str = ""
    _exclude_keywords: str = ""
    _interval: int = 10

    # 核心逻辑状态
    _dirconf: Dict[str, List[Path]] = {}
    _transferconf: Dict[str, str] = {}
    _overwrite_mode: Dict[str, str] = {}
    _medias: Dict[str, Any] = {}
    
    # 持久化状态
    _state_file: Optional[Path] = None
    _cache_file: Optional[Path] = None
    _round_robin_index: Dict[str, int] = {}
    _allocation_cache: Dict[int, Path] = {}

    def _save_state_to_file(self):
        """将轮询索引持久化保存到文件"""
        if not self._state_file: return
        try:
            with self._state_file.open('w', encoding='utf-8') as f:
                json.dump(self._round_robin_index, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error(f"无法保存轮询状态到文件 {self._state_file}: {e}")

    def _load_state_from_file(self) -> Dict[str, int]:
        """从文件加载轮询索引"""
        if not self._state_file or not self._state_file.exists(): return {}
        try:
            with self._state_file.open('r', encoding='utf-8') as f:
                state = json.load(f)
                return state if isinstance(state, dict) else {}
        except Exception as e:
            logger.error(f"无法从 {self._state_file} 加载轮询状态: {e}")
            return {}

    def _save_cache_to_file(self):
        """将分配缓存持久化保存到文件"""
        if not self._cache_file: return
        try:
            with self._cache_file.open('w', encoding='utf-8') as f:
                serializable_cache = {tmdb_id: str(path) for tmdb_id, path in self._allocation_cache.items()}
                json.dump(serializable_cache, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error(f"无法保存分配缓存到文件 {self._cache_file}: {e}")

    def _load_cache_from_file(self) -> Dict[int, Path]:
        """从文件加载分配缓存"""
        if not self._cache_file or not self._cache_file.exists(): return {}
        try:
            with self._cache_file.open('r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                return {int(tmdb_id): Path(path) for tmdb_id, path in loaded_data.items()}
        except Exception as e:
            logger.error(f"无法从 {self._cache_file} 加载分配缓存: {e}")
            return {}

    def _update_cache_and_persist(self, tmdb_id: int, dest: Path):
        """更新缓存并立即持久化"""
        self._allocation_cache[tmdb_id] = dest
        self._save_cache_to_file()

    def _get_round_robin_destination(self, mon_path: str, mediainfo: MediaInfo) -> Optional[Path]:
        """
        根据媒体信息和轮询策略选择一个目标目录。
        【最终保底版逻辑】: 1.内存缓存 -> 2.历史记录 -> 3.物理扫描 -> 4.轮询选择
        """
        destinations = self._dirconf.get(mon_path)
        if not destinations:
            logger.error(f"监控源 {mon_path} 未配置目标目录")
            return None

        if len(destinations) == 1:
            return destinations[0]

        tmdb_id = mediainfo.tmdb_id

        # 1. 检查内存/持久化缓存
        if tmdb_id in self._allocation_cache:
            cached_dest = self._allocation_cache[tmdb_id]
            logger.info(f"为 '{mediainfo.title} ({mediainfo.year})' 命中缓存 -> {cached_dest}")
            return cached_dest

        # 2. 缓存未命中，查询历史数据库
        history_entry = self.transferhis.get_by_type_tmdbid(mtype=mediainfo.type.value, tmdbid=tmdb_id)
        if history_entry and history_entry.dest:
            historical_dest_path = Path(history_entry.dest)
            for dest in destinations:
                try:
                    if historical_dest_path.is_relative_to(dest):
                        logger.info(f"为 '{mediainfo.title} ({mediainfo.year})' 找到历史记录 -> {dest}")
                        self._update_cache_and_persist(tmdb_id, dest)
                        return dest
                except ValueError:
                    continue
        
        # 3. 物理扫描所有目标目录作为保底
        logger.info(f"缓存和历史未命中，为 '{mediainfo.title} ({mediainfo.year})' 启动物理目录扫描...")
        expected_folder_prefix = f"{mediainfo.title} ({mediainfo.year})"
        for dest in destinations:
            if not dest.is_dir(): continue
            try:
                for sub_dir in dest.iterdir():
                    if sub_dir.is_dir() and sub_dir.name.startswith(expected_folder_prefix):
                        logger.info(f"物理扫描命中！在 '{dest}' 找到已存在目录: '{sub_dir.name}'")
                        self._update_cache_and_persist(tmdb_id, dest)
                        return dest
            except OSError as e:
                logger.warning(f"扫描目录 {dest} 时出错: {e}")
                continue

        # 4. 如果以上全部未命中，执行轮询
        logger.info(f"首次转移 '{mediainfo.title} ({mediainfo.year})'，执行轮询...")
        last_index = self._round_robin_index.get(mon_path, -1)
        next_index = (last_index + 1) % len(destinations)
        
        self._round_robin_index[mon_path] = next_index
        self._save_state_to_file()

        chosen_dest = destinations[next_index]
        logger.info(f"轮询为 '{mon_path}' 选择索引 {next_index} -> {chosen_dest}")
        self._update_cache_and_persist(tmdb_id, chosen_dest)
        return chosen_dest

    def init_plugin(self, config: dict = None):
        self.transferhis = TransferHistoryOper()
        self.downloadhis = DownloadHistoryOper()
        self.transferchian = TransferChain()
        self.tmdbchain = TmdbChain()
        self.mediaChain = MediaChain()
        self.storagechain = StorageChain()
        self.filetransfer = FileManagerModule()
        
        self._state_file = self.get_data_path() / "cloudlinkmonitor_state.json"
        self._cache_file = self.get_data_path() / "cloudlinkmonitor_cache.json"
        self._dirconf, self._transferconf, self._overwrite_mode = {}, {}, {}
        self._round_robin_index = self._load_state_from_file()
        self._allocation_cache = self._load_cache_from_file()
        self._medias, self._observer = {}, []

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", False)
            self._onlyonce = config.get("onlyonce", False)
            self._history = config.get("history", True)
            self._scrape = config.get("scrape", False)
            self._category = config.get("category", True)
            self._refresh = config.get("refresh", True)
            self._softlink = config.get("softlink", False)
            self._strm = config.get("strm", False)
            self._mode = config.get("mode", "fast")
            self._transfer_type = config.get("transfer_type", "move")
            self._monitor_dirs = config.get("monitor_dirs", "")
            self._exclude_keywords = config.get("exclude_keywords", "")
            self._interval = int(config.get("interval", 10))
            self._cron = config.get("cron")
            self._size = int(config.get("size", 100))

        self.stop_service()
        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._notify:
                self._scheduler.add_job(self.send_msg, 'interval', seconds=self._interval)
            for line in self._monitor_dirs.split("\n"):
                if not line.strip(): continue
                conf_line = line
                _overwrite_mode = 'rename'
                if "@" in conf_line: conf_line, _overwrite_mode = conf_line.split("@", 1)
                _transfer_type = self._transfer_type
                if "#" in conf_line: conf_line, _transfer_type = conf_line.split("#", 1)
                if ':' not in conf_line: continue
                mon_path_str, dests_str = conf_line.split(":", 1)
                mon_path = mon_path_str.strip()
                dest_paths = [Path(p.strip()) for p in dests_str.split(',') if p.strip()]
                if not mon_path or not dest_paths: continue
                self._dirconf[mon_path] = dest_paths
                self._transferconf[mon_path] = _transfer_type
                self._overwrite_mode[mon_path] = _overwrite_mode
                if self._enabled: self.start_observer(mon_path)
            if self._onlyonce:
                logger.info("立即运行一次全量同步...")
                self._scheduler.add_job(self.sync_all, 'date', run_date=datetime.datetime.now(pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3))
                self._onlyonce = False
                self.__update_config()
            if self._scheduler and self._scheduler.get_jobs(): self._scheduler.start()

    def start_observer(self, mon_path: str):
        for target_path in self._dirconf.get(mon_path, []):
            try:
                if target_path.is_relative_to(Path(mon_path)):
                    logger.error(f"致命错误：目标目录 {target_path} 是监控目录 {mon_path} 的子目录！")
                    return
            except ValueError: pass
        try:
            observer = Observer(timeout=10) if self._mode == 'fast' else PollingObserver(timeout=10)
            observer.schedule(FileMonitorHandler(mon_path, self), path=mon_path, recursive=True)
            observer.daemon = True
            observer.start()
            self._observer.append(observer)
            logger.info(f"已启动对 '{mon_path}' 的监控 ({self._mode}模式)。")
        except Exception as e:
            logger.error(f"启动对 '{mon_path}' 的监控失败: {e}")

    def event_handler(self, event, mon_path: str, text: str, event_path: str):
        if not event.is_directory:
            self.__handle_file(event_path=event_path, mon_path=mon_path)

    def __handle_file(self, event_path: str, mon_path: str):
        file_path = Path(event_path)
        try:
            if not file_path.exists(): return
            with lock:
                if self.transferhis.get_by_src(event_path): return
                if any(s in event_path for s in ('/@Recycle/', '/#recycle/', '/.')) or '/@eaDir/' in event_path: return
                if self._exclude_keywords:
                    for keyword in self._exclude_keywords.split("\n"):
                        if keyword and re.search(keyword, event_path, re.IGNORECASE):
                            logger.info(f"'{event_path}' 命中排除关键字 '{keyword}'，不处理。")
                            return
                if file_path.suffix.lower() not in settings.RMT_MEDIAEXT: return
                if re.search(r"BDMV[/\\]STREAM", event_path, re.IGNORECASE):
                    file_path = Path(re.split(r"BDMV", event_path, flags=re.IGNORECASE)[0])
                    if self.transferhis.get_by_src(str(file_path)): return
                file_meta = MetaInfoPath(file_path)
                if not file_meta.name: return
                if self._size > 0 and file_path.is_file() and file_path.stat().st_size < self._size * 1024 * 1024: return
                file_item = self.storagechain.get_file_item(storage="local", path=file_path)
                if not file_item: return
                
                mediainfo = self.mediaChain.recognize_media(meta=file_meta)
                if not mediainfo:
                    logger.warn(f"无法识别媒体信息: {file_path.name}")
                    return

                target_dir_conf = DirectoryHelper().get_dir(mediainfo, src_path=Path(mon_path))
                if not target_dir_conf:
                    logger.warning(f"目录助手未能为 '{mediainfo.title}' 生成目录配置，将使用默认配置。")
                    target_dir_conf = TransferDirectoryConf()

                target_base_dir = self._get_round_robin_destination(mon_path, mediainfo)
                if not target_base_dir:
                    logger.error(f"无法为 '{mediainfo.title}' 获取轮询目标目录。")
                    return
                
                logger.info(f"[分发选择] 文件 '{file_path.name}' 的目标基准目录是: {target_base_dir}")
                
                target_dir_conf.library_path = target_base_dir
                target_dir_conf.transfer_type = self._transferconf.get(mon_path, self._transfer_type)
                target_dir_conf.overwrite_mode = self._overwrite_mode.get(mon_path, 'rename')
                target_dir_conf.scraping = self._scrape
                target_dir_conf.library_category_folder = self._category
                
                episodes_info = None
                if mediainfo.type == MediaType.TV:
                    episodes_info = self.tmdbchain.tmdb_episodes(tmdbid=mediainfo.tmdb_id,
                                                                season=1 if file_meta.begin_season is None else file_meta.begin_season)
                
                transferinfo = self.transferchian.transfer(fileitem=file_item, meta=file_meta, mediainfo=mediainfo,
                                                           target_directory=target_dir_conf, episodes_info=episodes_info)

                if not transferinfo or not transferinfo.success:
                    logger.error(f"文件转移失败: {file_path.name} - {transferinfo.message if transferinfo else '未知原因'}")
                    return

                logger.info(f"文件 '{file_path.name}' 成功转移到 '{transferinfo.target_item.path}'")
                
                if self._history: self.transferhis.add_success(fileitem=file_item, mode=target_dir_conf.transfer_type, meta=file_meta, mediainfo=mediainfo, transferinfo=transferinfo)
                if self._scrape: self.mediaChain.scrape_metadata(fileitem=transferinfo.target_diritem, meta=file_meta, mediainfo=mediainfo)
                if self._notify: self.add_to_notification_queue(file_path, mediainfo, file_meta, transferinfo)
                if self._refresh: self.eventmanager.send_event(EventType.TransferComplete, {'meta': file_meta, 'mediainfo': mediainfo, 'transferinfo': transferinfo})
                if self._softlink: self.eventmanager.send_event(EventType.PluginAction, {'file_path': str(transferinfo.target_item.path), 'action': 'softlink_file'})
                if self._strm: self.eventmanager.send_event(EventType.PluginAction, {'file_path': str(transferinfo.target_item.path), 'action': 'cloudstrm_file'})
                if target_dir_conf.transfer_type == "move": self.cleanup_empty_dirs(file_path, mon_path)
        except Exception as e:
            logger.error(f"处理文件 '{event_path}' 时发生未知错误: {e}\n{traceback.format_exc()}")
            
    def cleanup_empty_dirs(self, file_path: Path, mon_path_str: str):
        try:
            parent_dir = file_path.parent
            mon_path = Path(mon_path_str)
            while parent_dir.is_relative_to(mon_path) and parent_dir != mon_path:
                if not any(parent_dir.iterdir()):
                    logger.info(f"移动模式，删除空目录：{parent_dir}")
                    shutil.rmtree(parent_dir, ignore_errors=True)
                    parent_dir = parent_dir.parent
                else: break
        except Exception as
