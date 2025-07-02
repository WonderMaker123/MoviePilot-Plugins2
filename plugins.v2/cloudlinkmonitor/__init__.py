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
    """目录监控响应类"""
    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync

    def on_created(self, event):
        self.sync.event_handler(event=event, text="创建", mon_path=self._watch_path, event_path=event.src_path)

    def on_moved(self, event):
        self.sync.event_handler(event=event, text="移动", mon_path=self._watch_path, event_path=event.dest_path)


class CloudLinkMonitor(_PluginBase):
    plugin_name = "多目录实时监控"
    plugin_desc = "监控多目录文件变化，自动转移媒体文件，支持轮询分发和持久化缓存。"
    plugin_icon = "Linkease_A.png"
    plugin_version = "2.9.0"  # 整合目录助手逻辑，最终稳定版
    plugin_author = "wonderful"
    author_url = "https://github.com/WonderMaker123/MoviePilot-Plugins2/"
    plugin_config_prefix = "cloudlinkmonitor_"
    plugin_order = 4
    auth_level = 1

    _scheduler: Optional[BackgroundScheduler] = None
    _observer: List[Observer] = []
    _event = threading.Event()
    
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

    _dirconf: Dict[str, List[Path]] = {}
    _transferconf: Dict[str, str] = {}
    _overwrite_mode: Dict[str, str] = {}
    _medias: Dict[str, Any] = {}
    
    _state_file: Optional[Path] = None
    _cache_file: Optional[Path] = None
    _round_robin_index: Dict[str, int] = {}
    _allocation_cache: Dict[int, Path] = {}

    # 并发锁
    _destination_lock: Optional[threading.Lock] = None

    def _save_state_to_file(self):
        if not self._state_file: return
        try:
            with self._state_file.open('w', encoding='utf-8') as f:
                json.dump(self._round_robin_index, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error(f"无法保存轮询状态到文件 {self._state_file}: {e}")

    def _load_state_from_file(self) -> Dict[str, int]:
        if not self._state_file or not self._state_file.exists(): return {}
        try:
            with self._state_file.open('r', encoding='utf-8') as f:
                state = json.load(f)
                return state if isinstance(state, dict) else {}
        except Exception as e:
            logger.error(f"无法从 {self._state_file} 加载轮询状态: {e}")
            return {}

    def _save_cache_to_file(self):
        if not self._cache_file: return
        try:
            with self._cache_file.open('w', encoding='utf-8') as f:
                serializable_cache = {tmdb_id: str(path) for tmdb_id, path in self._allocation_cache.items()}
                json.dump(serializable_cache, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error(f"无法保存分配缓存到文件 {self._cache_file}: {e}")

    def _load_cache_from_file(self) -> Dict[int, Path]:
        if not self._cache_file or not self._cache_file.exists(): return {}
        try:
            with self._cache_file.open('r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                return {int(tmdb_id): Path(path) for tmdb_id, path in loaded_data.items()}
        except Exception as e:
            logger.error(f"无法从 {self._cache_file} 加载分配缓存: {e}")
            return {}

    def _update_cache_and_persist(self, tmdb_id: int, dest: Path):
        self._allocation_cache[tmdb_id] = dest
        self._save_cache_to_file()

    def _get_round_robin_destination(self, mon_path: str, mediainfo: MediaInfo) -> Optional[Path]:
        # 使用 with 语句包裹整个函数，实现自动加锁和解锁，防止并发冲突
        with self._destination_lock:
            destinations = self._dirconf.get(mon_path)
            if not destinations:
                logger.error(f"监控源 {mon_path} 未配置目标目录")
                return None
            if len(destinations) == 1:
                return destinations[0]

            tmdb_id = mediainfo.tmdb_id
            if tmdb_id in self._allocation_cache:
                cached_dest = self._allocation_cache[tmdb_id]
                logger.info(f"为 '{mediainfo.title} ({mediainfo.year})' 命中缓存 -> {cached_dest}")
                return cached_dest

            history_entry = self.transferhis.get_by_type_tmdbid(mtype=mediainfo.type.value, tmdbid=tmdb_id)
            if history_entry and history_entry.dest:
                historical_dest_path = Path(history_entry.dest)
                for dest in destinations:
                    try:
                        if historical_dest_path.is_relative_to(dest):
                            logger.info(f"为 '{mediainfo.title} ({mediainfo.year})' 找到历史记录 -> {dest}")
                            self._update_cache_and_persist(tmdb_id, dest)
                            return dest
                    except ValueError: continue
            
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
        # 初始化并发锁
        self._destination_lock = threading.Lock()
        
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

                # 【最终修复】整合目录助手逻辑
                # 1. 首先确定轮询的目标盘（根目录）
                target_base_dir = self._get_round_robin_destination(mon_path, mediainfo)
                if not target_base_dir:
                    logger.error(f"无法为 '{mediainfo.title}' 获取轮询目标目录。")
                    return
                
                # 2. 然后向主程序的目录助手请求目录结构配置
                target_dir_conf = DirectoryHelper().get_dir(mediainfo, src_path=Path(mon_path))

                # 3. 判断目录助手是否返回了有效的配置
                if not target_dir_conf:
                    logger.warning(f"目录助手未能为 '{mediainfo.title}' 生成目录配置，将使用插件的默认后备设置。")
                    # 如果助手失败，则手动创建一个配置对象
                    target_dir_conf = TransferDirectoryConf()
                    target_dir_conf.library_category_folder = self._category
                    target_dir_conf.scraping = self._scrape
                    # 关键：手动设置重命名为True，这通常会触发标准文件夹结构的创建
                    target_dir_conf.renaming = True 
                else:
                    logger.info(f"目录助手成功为 '{mediainfo.title}' 生成目录配置。")
                    # 如果助手成功，我们也需要确保刮削设置与插件设置一致
                    target_dir_conf.scraping = self._scrape

                # 4. 无论助手是否成功，都将轮询选出的目标盘符作为最终的根路径
                target_dir_conf.library_path = target_base_dir
                
                # 5. 无论助手是否成功，都使用插件自身定义的转移方式和覆盖模式
                target_dir_conf.transfer_type = self._transferconf.get(mon_path, self._transfer_type)
                target_dir_conf.overwrite_mode = self._overwrite_mode.get(mon_path, 'rename')

                logger.info(f"[分发选择] 文件 '{file_path.name}' 的目标基准目录是: {target_dir_conf.library_path}")

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
                # 注意：刮削操作由转移链根据 target_dir_conf.scraping 的值来决定，这里无需重复调用
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
            while parent_dir != mon_path and str(parent_dir).startswith(mon_path_str):
                if not any(parent_dir.iterdir()):
                    logger.info(f"移动模式，删除空目录：{parent_dir}")
                    shutil.rmtree(parent_dir, ignore_errors=True)
                    parent_dir = parent_dir.parent
                else: break
        except Exception as e:
            logger.error(f"删除空目录时出错: {e}")
            
    def add_to_notification_queue(self, file_path, mediainfo, file_meta, transferinfo):
        if mediainfo.type == MediaType.TV:
            season_str = str(file_meta.season) if file_meta.season is not None else ''
            season_num_match = re.search(r'\d+', season_str)
            season_num = int(season_num_match.group(0)) if season_num_match else 1
            key = f"{mediainfo.title} ({mediainfo.year}) S{season_num:02d}"
        else:
            key = f"{mediainfo.title} ({mediainfo.year})"

        if key not in self._medias:
            self._medias[key] = {"files": [], "time": datetime.datetime.now()}
        self._medias[key]["files"].append({"path": str(file_path), "mediainfo": mediainfo, "file_meta": file_meta, "transferinfo": transferinfo})
        self._medias[key]["time"] = datetime.datetime.now()

    def send_msg(self):
        if not self._medias: return
        now = datetime.datetime.now()
        for key, data in list(self._medias.items()):
            if (now - data['time']).total_seconds() > self._interval:
                files = data['files']
                first_item = files[0]
                mediainfo = first_item['mediainfo']
                total_size = sum(f['transferinfo'].total_size for f in files)
                file_count = len(files)
                final_transfer_info = first_item['transferinfo']
                final_transfer_info.total_size = total_size
                final_transfer_info.file_count = file_count
                season_episode = None
                if mediainfo.type == MediaType.TV:
                    episodes = sorted([f['file_meta'].begin_episode for f in files if f['file_meta'].begin_episode])
                    season_str = str(first_item['file_meta'].season) if first_item['file_meta'].season is not None else ''
                    season_num_match = re.search(r'\d+', season_str)
                    season_num = int(season_num_match.group(0)) if season_num_match else 1
                    season_episode = f"S{season_num:02d} {StringUtils.format_ep(episodes)}"
                self.transferchian.send_transfer_message(meta=first_item['file_meta'], mediainfo=mediainfo, transferinfo=final_transfer_info, season_episode=season_episode)
                del self._medias[key]

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled, "notify": self._notify, "onlyonce": self._onlyonce, "history": self._history,
            "scrape": self._scrape, "category": self._category, "refresh": self._refresh, "softlink": self._softlink,
            "strm": self._strm, "mode": self._mode, "transfer_type": self._transfer_type,
            "monitor_dirs": self._monitor_dirs, "exclude_keywords": self._exclude_keywords,
            "interval": self._interval, "cron": self._cron, "size": self._size
        })

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event):
        if event and event.event_data and event.event_data.get("action") == "cloud_link_sync":
            self.post_message(channel=event.event_data.get("channel"), title="开始同步监控目录 ...", userid=event.event_data.get("user"))
            self.sync_all()
            self.post_message(channel=event.event_data.get("channel"), title="监控目录同步完成！", userid=event.event_data.get("user"))

    def sync_all(self):
        logger.info("开始全量同步监控目录 ...")
        for mon_path in self._dirconf.keys():
            logger.info(f"处理监控目录 {mon_path} ...")
            try:
                list_files = [p for p in Path(mon_path).rglob('*') if p.suffix.lower() in settings.RMT_MEDIAEXT]
                logger.info(f"发现 {len(list_files)} 个媒体文件")
                for file_path in list_files:
                    self.__handle_file(event_path=str(file_path), mon_path=mon_path)
            except Exception as e:
                logger.error(f"处理目录 {mon_path} 时出错: {e}")
        logger.info("全量同步监控目录完成！")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{"cmd": "/cloud_link_sync", "event": EventType.PluginAction, "desc": "多目录监控同步", "data": {"action": "cloud_link_sync"}}]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{"path": "/cloud_link_sync", "endpoint": self.sync, "methods": ["GET"], "summary": "多目录监控同步"}]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [{"id": "CloudLinkMonitor", "name": "多目录监控全量同步", "trigger": CronTrigger.from_crontab(self._cron),
                     "func": self.sync_all, "kwargs": {}}]
        return []

    def sync(self) -> schemas.Response:
        self.sync_all()
        return schemas.Response(success=True)

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        placeholder_text = ('每一行一个配置，支持单目标或多目标轮询分发。\n'
                            '【单目标】: /监控目录:/目标目录\n'
                            '【多目标轮询】: /监控目录:/目标1,/目标2,/目标3\n'
                            '【自定义转移】: /监控目录:/目标目录#转移方式 (例如 #move)\n'
                            '【自定义覆盖】: /监控目录:/目标目录@覆盖方式 (例如 @rename)')
        return ([{'component': 'VForm', 'content': [
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]}
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'history', 'label': '存储历史记录'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'scrape', 'label': '是否刮削'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'category', 'label': '是否二级分类'}}]}
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'refresh', 'label': '刷新媒体库'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'softlink', 'label': '联动实时软连接'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'strm', 'label': '联动Strm生成'}}]}
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSelect', 'props': {'model': 'mode', 'label': '监控模式', 'items': [{'title': '兼容模式', 'value': 'compatibility'}, {'title': '性能模式', 'value': 'fast'}]}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSelect', 'props': {'model': 'transfer_type', 'label': '转移方式', 'items': [{'title': '移动', 'value': 'move'}, {'title': '复制', 'value': 'copy'}, {'title': '硬链接', 'value': 'link'}, {'title': '软链接', 'value': 'softlink'}, {'title': 'Rclone复制', 'value': 'rclone_copy'}, {'title': 'Rclone移动', 'value': 'rclone_move'}]}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '定时任务', 'placeholder': '留空则禁用'}}]}
            ]},
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VTextField', 'props': {'model': 'interval', 'label': '入库消息延迟(秒)', 'type': 'number'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VTextField', 'props': {'model': 'size', 'label': '最小文件大小(MB)', 'type': 'number'}}]}
            ]},
            {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VTextarea', 'props': {'model': 'monitor_dirs', 'label': '监控目录', 'rows': 5, 'placeholder': placeholder_text}}]}]},
            {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VTextarea', 'props': {'model': 'exclude_keywords', 'label': '排除关键词', 'rows': 2, 'placeholder': '每一行一个关键词'}}]}]}
        ]}], 
        {"enabled": False, "notify": True, "onlyonce": False, "history": True, "scrape": False, "category": True,
         "refresh": True, "softlink": False, "strm": False, "mode": "fast", "transfer_type": "move",
         "monitor_dirs": "", "exclude_keywords": "", "interval": 10, "cron": "", "size": 100})

    def get_page(self) -> List[dict]:
        return []

    def stop_service(self):
        if self._observer:
            for observer in self._observer:
                try:
                    observer.stop()
                    observer.join()
                except Exception as e:
                    logger.error(f"停止监控器时出错: {e}")
        self._observer = []
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown()
        self._scheduler = None
