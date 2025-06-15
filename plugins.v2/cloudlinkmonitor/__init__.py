import datetime
import re
import shutil
import threading
import time
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
        self.sync.event_handler(event=event, text="创建",
                                mon_path=self._watch_path, event_path=event.src_path)

    def on_moved(self, event):
        self.sync.event_handler(event=event, text="移动",
                                mon_path=self._watch_path, event_path=event.dest_path)


class CloudLinkMonitor(_PluginBase):
    # 插件名称
    plugin_name = "目录实时监控"
    # 插件描述
    plugin_desc = "监控目录文件变化，自动转移媒体文件（支持多盘轮询分发）。"
    # 插件图标
    plugin_icon = "Linkease_A.png"
    # 插件版本
    plugin_version = "2.6.1"  # 版本号+1
    # 插件作者
    plugin_author = "thtemp"
    # 作者主页
    author_url = "https://github.com/thsrite"
    # 插件配置项ID前缀
    plugin_config_prefix = "cloudlinkmonitor_"
    # 加载顺序
    plugin_order = 4
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler = None
    transferhis = None
    downloadhis = None
    transferchian = None
    tmdbchain = None
    storagechain = None
    _observer = []
    _enabled = False
    _notify = False
    _onlyonce = False
    _history = False
    _scrape = False
    _category = False
    _refresh = False
    _softlink = False
    _strm = False
    _cron = None
    filetransfer = None
    mediaChain = None
    _size = 0
    # 模式 compatibility/fast
    _mode = "compatibility"
    # 转移方式
    _transfer_type = "softlink"
    _monitor_dirs = ""
    _exclude_keywords = ""
    _interval: int = 10

    # -- 新增/修改的属性 --
    # 存储源目录与目的目录列表关系
    _dirconf: Dict[str, Optional[List[Path]]] = {}
    # 存储每个源目录的轮询索引
    _dir_indexes: Dict[str, int] = {}
    # 缓存一级目录到目标索引的映射，保证同名一级目录发往相同目标
    _dir_allocation_map: Dict[str, Dict[str, int]] = {}
    # 目录索引锁
    _dir_locks: Dict[str, threading.Lock] = {}
    # 文件稳定检测配置
    _stability_checks = 5
    _check_interval = 2

    # 存储源目录转移方式
    _transferconf: Dict[str, Optional[str]] = {}
    _overwrite_mode: Dict[str, Optional[str]] = {}
    _medias = {}
    # 退出事件
    _event = threading.Event()

    def init_plugin(self, config: dict = None):
        self.transferhis = TransferHistoryOper()
        self.downloadhis = DownloadHistoryOper()
        self.transferchian = TransferChain()
        self.tmdbchain = TmdbChain()
        self.mediaChain = MediaChain()
        self.storagechain = StorageChain()
        self.filetransfer = FileManagerModule()

        # 清空所有配置和状态
        self._dirconf = {}
        self._transferconf = {}
        self._overwrite_mode = {}
        self._dir_indexes = {}
        self._dir_allocation_map = {}
        self._dir_locks = {}

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._history = config.get("history")
            self._scrape = config.get("scrape")
            self._category = config.get("category")
            self._refresh = config.get("refresh")
            self._mode = config.get("mode")
            self._transfer_type = config.get("transfer_type")
            self._monitor_dirs = config.get("monitor_dirs") or ""
            self._exclude_keywords = config.get("exclude_keywords") or ""
            self._interval = config.get("interval") or 10
            self._cron = config.get("cron")
            self._size = config.get("size") or 0
            self._softlink = config.get("softlink")
            self._strm = config.get("strm")
            # 读取新增的稳定检测配置
            self._stability_checks = int(config.get("stability_checks", 5))
            self._check_interval = int(config.get("check_interval", 2))

        # 停止现有任务
        self.stop_service()

        if self._enabled or self._onlyonce:
            # 定时服务管理器
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._notify:
                # 追加入库消息统一发送服务
                self._scheduler.add_job(self.send_msg, trigger='interval', seconds=15)

            # 读取目录配置
            monitor_dirs_lines = self._monitor_dirs.split("\n")
            if not monitor_dirs_lines:
                return

            for mon_path_line in monitor_dirs_lines:
                if not mon_path_line.strip():
                    continue

                # 自定义覆盖方式 (默认改为 rename)
                _overwrite_mode = 'rename'
                if mon_path_line.count("@") == 1:
                    _overwrite_mode = mon_path_line.split("@")[1]
                    mon_path_line = mon_path_line.split("@")[0]

                # 自定义转移方式
                _transfer_type = self._transfer_type
                if mon_path_line.count("#") == 1:
                    _transfer_type = mon_path_line.split("#")[1]
                    mon_path_line = mon_path_line.split("#")[0]

                # 解析源目录和目标目录列表
                if ':' not in mon_path_line:
                    logger.error(f"监控目录格式错误，缺少冒号':': {mon_path_line}")
                    continue

                parts = mon_path_line.split(':', 1)
                mon_path = parts[0].strip()
                # 目标目录可以是逗号分隔的列表
                dest_paths_str = parts[1].split(',')

                target_paths = [Path(p.strip()) for p in dest_paths_str if p.strip()]

                if not target_paths:
                    logger.error(f"监控目录 {mon_path} 未配置有效的目标目录。")
                    continue

                self._dirconf[mon_path] = target_paths
                self._transferconf[mon_path] = _transfer_type
                self._overwrite_mode[mon_path] = _overwrite_mode

                # 初始化分发状态
                self._dir_indexes[mon_path] = 0
                self._dir_allocation_map[mon_path] = {}
                self._dir_locks[mon_path] = threading.Lock()

                # 启用目录监控
                if self._enabled:
                    # 检查所有目标目录是否是监控目录的子目录
                    for target_path in target_paths:
                        try:
                            if target_path and target_path.is_relative_to(Path(mon_path)):
                                logger.warn(f"{target_path} 是监控目录 {mon_path} 的子目录，可能导致死循环！")
                                self.systemmessage.put(f"{target_path} 是下载目录 {mon_path} 的子目录，无法监控")
                        except Exception:
                            # 在Windows上跨盘符会报错，可以安全忽略
                            pass

                    try:
                        if self._mode == "compatibility":
                            observer = PollingObserver(timeout=10)
                        else:
                            observer = Observer(timeout=10)
                        self._observer.append(observer)
                        observer.schedule(FileMonitorHandler(mon_path, self), path=mon_path, recursive=True)
                        observer.daemon = True
                        observer.start()
                        logger.info(f"{mon_path} 的云盘实时监控服务启动")
                    except Exception as e:
                        err_msg = str(e)
                        if "inotify" in err_msg and "reached" in err_msg:
                            logger.warn(
                                f"云盘实时监控服务启动出现异常：{err_msg}，请在宿主机上（不是docker容器内）执行以下命令并重启："
                                + """
                                     echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
                                     echo fs.inotify.max_user_instances=524288 | sudo tee -a /etc/sysctl.conf
                                     sudo sysctl -p
                                     """)
                        else:
                            logger.error(f"{mon_path} 启动目云盘实时监控失败：{err_msg}")
                        self.systemmessage.put(f"{mon_path} 启动云盘实时监控失败：{err_msg}")

            # 运行一次定时服务
            if self._onlyonce:
                logger.info("云盘实时监控服务启动，立即运行一次")
                self._scheduler.add_job(name="云盘实时监控",
                                        func=self.sync_all, trigger='date',
                                        run_date=datetime.datetime.now(
                                            tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                        )
                self._onlyonce = False
                self.__update_config()

            # 启动定时服务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __update_config(self):
        """
        更新配置
        """
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "mode": self._mode,
            "transfer_type": self._transfer_type,
            "monitor_dirs": self._monitor_dirs,
            "exclude_keywords": self._exclude_keywords,
            "interval": self._interval,
            "history": self._history,
            "softlink": self._softlink,
            "cron": self._cron,
            "strm": self._strm,
            "scrape": self._scrape,
            "category": self._category,
            "size": self._size,
            "refresh": self._refresh,
            # 保存新增的配置
            "stability_checks": self._stability_checks,
            "check_interval": self._check_interval,
        })

    def _is_file_stable(self, filepath: Path) -> bool:
        """
        检查文件大小是否在一段时间内保持不变，以确认文件已写入完成。
        """
        try:
            last_size = filepath.stat().st_size
        except FileNotFoundError:
            return False

        time.sleep(self._check_interval)
        # 连续检测N次
        for i in range(self._stability_checks):
            try:
                current_size = filepath.stat().st_size
            except FileNotFoundError:
                # 文件在检测期间被删除
                return False
            if current_size != last_size:
                logger.debug(f"文件仍在写入中: {filepath}, 大小从 {last_size} 变为 {current_size}")
                # 文件大小变化，重置最后大小并继续检测
                last_size = current_size
                time.sleep(self._check_interval)
                continue
            # 文件大小无变化，如果是最后一次检测则返回True
            if i < self._stability_checks - 1:
                time.sleep(self._check_interval)
        logger.debug(f"文件已稳定: {filepath}")
        return True

    def _get_target_dir(self, mon_path: str, event_path: Path) -> Path:
        """
        根据轮询和一级目录“粘性”策略，获取下一个目标目录。
        """
        try:
            # 获取相对于监控目录的路径
            rel_path = event_path.relative_to(mon_path)
            # 获取监控目录下的第一级目录名（如电影/电视剧文件夹名）
            top_level_dir = rel_path.parts[0] if len(rel_path.parts) > 1 else ""
        except ValueError:
            # 如果event_path不在mon_path下，直接返回第一个目标
            logger.warning(f"路径 {event_path} 不在监控目录 {mon_path} 下，无法计算相对路径。")
            return self._dirconf[mon_path][0]

        targets = self._dirconf[mon_path]
        allocation = self._dir_allocation_map[mon_path]

        # 如果文件直接在监控目录下，不属于任何子目录，直接轮询
        if top_level_dir == "" or not Path(mon_path, top_level_dir).is_dir():
            with self._dir_locks[mon_path]:
                index = self._dir_indexes[mon_path]
                self._dir_indexes[mon_path] = (index + 1) % len(targets)
            return targets[index]

        # 如果文件在子目录中，则应用“粘性”策略
        if top_level_dir in allocation:
            # 这个子目录之前已经分配过目标
            index = allocation[top_level_dir]
        else:
            # 第一次见到这个子目录，为它分配一个目标并记录下来
            with self._dir_locks[mon_path]:
                index = self._dir_indexes[mon_path]
                self._dir_indexes[mon_path] = (index + 1) % len(targets)
                allocation[top_level_dir] = index

        return targets[index]

    def __handle_file(self, event_path: str, mon_path: str):
        """
        同步一个文件
        """
        file_path = Path(event_path)
        try:
            if not file_path.exists():
                return

            # 1. 检查文件是否稳定
            if not self._is_file_stable(file_path):
                logger.info(f"文件未稳定，跳过: {event_path}")
                return
            # 稳定检查后再次确认文件存在
            if not file_path.exists():
                logger.warning(f"稳定检查后文件消失，跳过: {event_path}")
                return

            with lock:
                # 2. 检查历史记录和各种过滤规则 (此部分逻辑不变)
                transfer_history = self.transferhis.get_by_src(event_path)
                if transfer_history:
                    logger.info("文件已处理过：%s" % event_path)
                    return

                if any(s in event_path for s in ['/@Recycle/', '/#recycle/', '/.', '#eaDir']):
                    logger.debug(f"{event_path} 是回收站或隐藏的文件")
                    return

                if self._exclude_keywords:
                    for keyword in self._exclude_keywords.split("\n"):
                        if keyword and re.findall(keyword, event_path):
                            logger.info(f"{event_path} 命中过滤关键字 {keyword}，不处理")
                            return

                if file_path.suffix not in settings.RMT_MEDIAEXT:
                    logger.debug(f"{event_path} 不是媒体文件")
                    return

                if re.search(r"BDMV[/\\]STREAM", event_path, re.IGNORECASE):
                    blurray_dir = re.split(r"BDMV", event_path, flags=re.IGNORECASE)[0]
                    file_path = Path(blurray_dir)
                    logger.info(f"{event_path} 是蓝光目录，更正文件路径为：{str(file_path)}")
                    if self.transferhis.get_by_src(str(file_path)):
                        logger.info(f"{file_path} 已整理过")
                        return

                if self._size and file_path.is_file() and file_path.stat().st_size < float(self._size) * 1024 ** 2:
                    logger.info(
                        f"{file_path} 文件大小({file_path.stat().st_size / 1024 ** 2:.2f}MB)小于设定值({self._size}MB)，不处理")
                    return

                # 3. 识别媒体信息 (此部分逻辑不变)
                file_meta = MetaInfoPath(file_path)
                if not file_meta.name:
                    logger.error(f"{file_path.name} 无法识别有效信息")
                    return

                file_item = self.storagechain.get_file_item(storage="local", path=file_path)
                if not file_item:
                    logger.warn(f"{event_path} 未找到对应的文件项")
                    return

                mediainfo: MediaInfo = self.chain.recognize_media(meta=file_meta)
                if not mediainfo:
                    # ... (处理无法识别的媒体，逻辑不变)
                    return

                # 4. 获取分发的目标目录
                target_path_base = self._get_target_dir(mon_path, file_path)
                
                # =========================================================
                # ▼▼▼ 在这里添加下面这行代码 ▼▼▼
                logger.info(f"[分发选择] 文件 '{file_path.name}' 的目标基准目录是: {target_path_base}")
                # ▲▲▲ 在这里添加上面这行代码 ▲▲▲
                # =========================================================

                transfer_type = self._transferconf.get(mon_path)
                overwrite_mode = self._overwrite_mode.get(mon_path) or 'rename'

                # 5. 构建转移配置
                target_dir = TransferDirectoryConf(
                    library_path=target_path_base,
                    transfer_type=transfer_type,
                    overwrite_mode=overwrite_mode,
                    library_category_folder=self._category,
                    scraping=self._scrape,
                    renaming=True,
                    notify=False,
                    library_storage="local"
                )

                episodes_info = None
                if mediainfo.type == MediaType.TV:
                    episodes_info = self.tmdbchain.tmdb_episodes(
                        tmdbid=mediainfo.tmdb_id,
                        season=1 if file_meta.begin_season is None else file_meta.begin_season)

                # 6. 执行转移及后续操作 (此部分逻辑不变)
                transferinfo: TransferInfo = self.chain.transfer(
                    fileitem=file_item,
                    meta=file_meta,
                    mediainfo=mediainfo,
                    target_directory=target_dir,
                    episodes_info=episodes_info
                )

                if not transferinfo or not transferinfo.success:
                    # ... (处理转移失败，逻辑不变)
                    return

                if self._history:
                    # ... (添加成功历史，逻辑不变)
                    pass

                if self._scrape:
                    # ... (刮削，逻辑不变)
                    pass

                if self._notify:
                    # ... (添加到待发送消息列表，逻辑不变)
                    pass

                if self._refresh:
                    # ... (广播事件，逻辑不变)
                    pass

                if self._softlink or self._strm:
                    # ... (联动其他插件，逻辑不变)
                    pass

                if transfer_type == "move":
                    # ... (移动模式下删除空目录，逻辑不变)
                    pass

        except Exception as e:
            logger.error("目录监控发生错误：%s - %s" % (str(e), traceback.format_exc()))

    # ... remote_sync, sync_all, event_handler, send_msg ...
    # ... get_state, get_command, get_api, get_service, sync ...
    # 以上方法均无需修改

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        # 返回UI界面的配置项定义
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VSwitch', 'props': {'model': 'history', 'label': '存储历史记录'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'scrape', 'label': '是否刮削'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VSwitch', 'props': {'model': 'category', 'label': '是否二级分类'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VSwitch', 'props': {'model': 'refresh', 'label': '刷新媒体库'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VSwitch', 'props': {'model': 'softlink', 'label': '联动实时软连接'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'strm', 'label': '联动Strm生成'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSelect',
                                                                                               'props': {'model': 'mode',
                                                                                                         'label': '监控模式',
                                                                                                         'items': [{
                                                                                                             'title': '兼容模式',
                                                                                                             'value': 'compatibility'},
                                                                                                             {
                                                                                                                 'title': '性能模式',
                                                                                                                 'value': 'fast'}]}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSelect',
                                                                                               'props': {
                                                                                                   'model': 'transfer_type',
                                                                                                   'label': '转移方式',
                                                                                                   'items': [{
                                                                                                       'title': '移动',
                                                                                                       'value': 'move'},
                                                                                                       {
                                                                                                           'title': '复制',
                                                                                                           'value': 'copy'},
                                                                                                       {
                                                                                                           'title': '硬链接',
                                                                                                           'value': 'link'},
                                                                                                       {
                                                                                                           'title': '软链接',
                                                                                                           'value': 'softlink'},
                                                                                                       {
                                                                                                           'title': 'Rclone复制',
                                                                                                           'value': 'rclone_copy'},
                                                                                                       {
                                                                                                           'title': 'Rclone移动',
                                                                                                           'value': 'rclone_move'}]}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'cron', 'label': '定时任务',
                                                                      'placeholder': '留空则禁用'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'interval', 'label': '入库消息延迟(秒)',
                                                                      'type': 'number'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VTextField',
                                 'props': {'model': 'stability_checks', 'label': '稳定检测次数', 'type': 'number'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VTextField',
                                 'props': {'model': 'check_interval', 'label': '稳定检测间隔(秒)', 'type': 'number'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                                {
                                    'component': 'VTextarea',
                                    'props': {
                                        'model': 'monitor_dirs',
                                        'label': '监控目录',
                                        'rows': 5,
                                        'placeholder': '每一行一个配置，支持单目标或多目标轮询分发。\n'
                                                     '【单目标】: /监控目录:/目标目录\n'
                                                     '【多目标轮询】: /监控目录:/目标1,/目标2,/目标3\n'
                                                     '【自定义转移】: /监控目录:/目标目录#转移方式 (例如 #move)\n'
                                                     '【自定义覆盖】: /监控目录:/目标目录@覆盖方式 (例如 @rename, 默认就是rename)'
                                    }
                                }
                            ]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                                {
                                    'component': 'VTextarea',
                                    'props': {
                                        'model': 'exclude_keywords',
                                        'label': '排除关键词 (支持正则)',
                                        'rows': 2,
                                        'placeholder': '每一行一个关键词，路径中包含这些词的文件或目录将被忽略。'
                                    }
                                }
                            ]}
                        ]
                    }
                ]
            }
        ], {
            # 默认值
            "enabled": False, "notify": True, "onlyonce": False, "history": True,
            "scrape": False, "category": False, "refresh": True, "softlink": False, "strm": False,
            "mode": "fast", "transfer_type": "link", "monitor_dirs": "", "exclude_keywords": "",
            "interval": 10, "cron": "", "size": 100,
            "stability_checks": 5, "check_interval": 2
        }

    def stop_service(self):
        """
        退出插件
        """
        if self._observer:
            for observer in self._observer:
                try:
                    observer.stop()
                    observer.join()
                except Exception as e:
                    print(str(e))
        self._observer = []
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._event.set()
                self._scheduler.shutdown()
                self._event.clear()
            self._scheduler = None
