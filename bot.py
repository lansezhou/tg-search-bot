import concurrent.futures
import math
import os
import re
import string
import random
import jvav as jv
import yaml
import asyncio
import threading
import langdetect
import telebot
from pyrogram import Client
from telebot import apihelper, types
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from database import BotFileDb, BotCacheDb
from requests import get
from requests.compat import quote
import logging
from logging.handlers import RotatingFileHandler


class Logger:

    def __init__(self, path_log_file: str, log_level=logging.INFO):
        self.logger = logging.getLogger()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        r_file_handler = RotatingFileHandler(
            path_log_file, maxBytes=1024 * 1024 * 16, backupCount=1
        )
        r_file_handler.setFormatter(formatter)
        self.logger.addHandler(r_file_handler)
        self.logger.addHandler(stream_handler)
        self.logger.setLevel(log_level)


class BotConfig:
    def __init__(self, path_config_file: str):
        with open(path_config_file, "r", encoding="utf8") as f:
            config = yaml.safe_load(f)
        self.tg_chat_id = str(config["tg_chat_id"]) if config["tg_chat_id"] else ""
        self.tg_bot_token = (
            str(config["tg_bot_token"]) if config["tg_bot_token"] else ""
        )
        self.tg_api_id = str(config["tg_api_id"]) if config["tg_api_id"] else ""
        self.tg_api_hash = str(config["tg_api_hash"]) if config["tg_api_hash"] else ""
        self.redis_host = str(config["redis_host"]) if config["redis_host"] else ""
        self.redis_port = str(config["redis_port"]) if config["redis_port"] else ""
        self.redis_password = (
            str(config["redis_password"]) if config["redis_password"] else ""
        )
        self.enable_nsfw = str(config["enable_nsfw"]) if config["enable_nsfw"] else "0"
        self.use_proxy = str(config["use_proxy"]) if config["use_proxy"] else "0"
        self.proxy_addr = str(config["proxy_addr"]) if config["proxy_addr"] else ""
        # set
        self.proxy_json = {"http": "", "https": ""}
        self.proxy_json_pikpak = {}
        if self.use_proxy == "1":
            self.proxy_json = {"http": self.proxy_addr, "https": self.proxy_addr}
            t1 = self.proxy_addr.find(":")
            t2 = self.proxy_addr.rfind(":")
            self.proxy_json_pikpak = {
                "scheme": self.proxy_addr[:t1],
                "hostname": self.proxy_addr[t1 + 3: t2],
                "port": int(self.proxy_addr[t2 + 1:]),
            }
            LOG.info(f'设置代理: "{self.proxy_addr}"')
        else:
            self.proxy_addr = ""
        LOG.info("成功读取并加载配置文件。")


# URL
BASE_URL_TG = "https://t.me"
PIKPAK_BOT_NAME = "PikPak6_Bot"
URL_PROJECT_ADDRESS = "https://github.com/akynazh/tg-search-bot"
URL_PIKPAK_BOT = f"{BASE_URL_TG}/{PIKPAK_BOT_NAME}"
# PATH
PATH_ROOT = f'{os.path.expanduser("~")}/.tg_search_bot'
PATH_LOG_FILE = f"{PATH_ROOT}/log.txt"
PATH_RECORD_FILE = f"{PATH_ROOT}/record.json"
PATH_SESSION_FILE = f"{PATH_ROOT}/session"
PATH_CONFIG_FILE = f"{PATH_ROOT}/config.yaml"
# BASE
LOG = Logger(path_log_file=PATH_LOG_FILE).logger
BOT_CFG = BotConfig(PATH_CONFIG_FILE)
apihelper.proxy = BOT_CFG.proxy_json
BOT = telebot.TeleBot(BOT_CFG.tg_bot_token)
BOT_DB = BotFileDb(PATH_RECORD_FILE)
BOT_CACHE_DB = BotCacheDb(
    host=BOT_CFG.redis_host,
    port=int(BOT_CFG.redis_port),
    password=BOT_CFG.redis_password,
    use_cache="1",
)
BASE_UTIL = jv.BaseUtil(BOT_CFG.proxy_addr)
DMM_UTIL = jv.DmmUtil(BOT_CFG.proxy_addr)
JBUS_UTIL = jv.JavBusUtil(BOT_CFG.proxy_addr)
JDB_UTIL = jv.JavDbUtil(BOT_CFG.proxy_addr)
JLIB_UTIL = jv.JavLibUtil(BOT_CFG.proxy_addr)
SUKEBEI_UTIL = jv.SukebeiUtil(BOT_CFG.proxy_addr)
TRANS_UTIL = jv.TransUtil(BOT_CFG.proxy_addr)
WIKI_UTIL = jv.WikiUtil(BOT_CFG.proxy_addr)
VGLE_UTIL = jv.AvgleUtil(BOT_CFG.proxy_addr)
EXECUTOR = concurrent.futures.ThreadPoolExecutor()
ID_PAT = re.compile(r"[a-z0-9]+[-_](?:ppv-)?[a-z0-9]+")
BOT_CMDS = {
    "help": "查看命令帮助",
    "stars": "查看我的演员",
    "ids": "查看我的番号",
    "nice": "随机获取高评分影片",
    "new": "随机获取最新影片",
    "rank": "获取DMM演员排行榜",
    "record": "获取已保存记录文件",
    "star": "按演员名字搜索演员",
    "id": "按番号搜索影片",
}
MSG_HELP = f"""只需发送影片标题、关键词或番号，我会处理剩下的工作！

"""
for cmd, content in BOT_CMDS.items():
    MSG_HELP += f"""/{cmd}  {content}
"""
MSG_HELP += f"""
[NSFW: {"已开启" if BOT_CFG.enable_nsfw == "1" else "已关闭"}]"""


class BotKey:
    KEY_GET_SAMPLE_BY_ID = "k0_0"
    KEY_GET_MORE_MAGNETS_BY_ID = "k0_1"
    KEY_SEARCH_STAR_BY_NAME = "k0_2"
    KEY_GET_TOP_STARS = "k0_3"
    KEY_WATCH_PV_BY_ID = "k1_0"
    KEY_WATCH_FV_BY_ID = "k1_1"
    KEY_GET_V_BY_ID = "k2_0"
    KEY_RANDOM_GET_V_BY_STAR_ID = "k2_1"
    KEY_RANDOM_GET_V_NICE = "k2_2"
    KEY_RANDOM_GET_V_NEW = "k2_3"
    KEY_GET_NEW_VS_BY_STAR_NAME_ID = "k2_4"
    KEY_GET_NICE_VS_BY_STAR_NAME = "k2_5"
    KEY_RECORD_STAR_BY_STAR_NAME_ID = "k3_0"
    KEY_RECORD_V_BY_ID_STAR_IDS = "k3_1"
    KEY_GET_STARS_RECORD = "k3_2"
    KEY_GET_VS_RECORD = "k3_3"
    KEY_GET_STAR_DETAIL_RECORD_BY_STAR_NAME_ID = "k3_4"
    KEY_GET_V_DETAIL_RECORD_BY_ID = "k3_5"
    KEY_UNDO_RECORD_STAR_BY_STAR_NAME_ID = "k3_6"
    KEY_UNDO_RECORD_V_BY_ID = "k3_7"
    KEY_DEL_V_CACHE = "k4_1"


class BotUtils:
    v_utils = [JDB_UTIL, JBUS_UTIL, SUKEBEI_UTIL]

    def send_action_typing(self):
        BOT.send_chat_action(chat_id=BOT_CFG.tg_chat_id, action="typing")

    def send_msg(self, msg: str, pv=False, markup=None):
        BOT.send_message(
            chat_id=BOT_CFG.tg_chat_id,
            text=msg,
            disable_web_page_preview=not pv,
            parse_mode="HTML",
            reply_markup=markup,
        )

    def send_msg_code_op(self, code: int, op: str):
        if code == 200:
            self.send_msg(f"操作执行: {op}\n执行结果: 成功 ^_^")
        elif code == 404:
            self.send_msg(
                f"操作执行: {op}\n执行结果: 未找到结果 Q_Q"
            )
        elif code == 500:
            self.send_msg(
                f"操作执行: {op}\n执行结果: 服务器错误，请重试或检查日志 Q_Q"
            )
        elif code == 502:
            self.send_msg(
                f"操作执行: {op}\n执行结果: 网络请求失败，请重试或检查网络 Q_Q"
            )

    def send_msg_success_op(self, op: str):
        self.send_msg(f"操作执行: {op}\n执行结果: 成功 ^_^")

    def send_msg_fail_reason_op(self, reason: str, op: str):
        self.send_msg(
            f"操作执行: {op}\n执行结果: 失败，{reason} Q_Q"
        )

    def check_success(self, code: int, op: str):
        if code == 200:
            return True
        if code == 404:
            self.send_msg_fail_reason_op(reason="未找到结果", op=op)
        elif code == 500:
            self.send_msg_fail_reason_op(reason="服务器错误", op=op)
        elif code == 502:
            self.send_msg_fail_reason_op(reason="网络请求失败", op=op)
        return False

    def create_btn_by_key(self, key_type: str, obj):
        if key_type == BotKey.KEY_GET_STAR_DETAIL_RECORD_BY_STAR_NAME_ID:
            return InlineKeyboardButton(
                text=obj["name"], callback_data=f'{obj["name"]}|{obj["id"]}:{key_type}'
            )
        elif key_type == BotKey.KEY_GET_V_DETAIL_RECORD_BY_ID:
            return InlineKeyboardButton(text=obj, callback_data=f"{obj}:{key_type}")
        elif key_type == BotKey.KEY_SEARCH_STAR_BY_NAME:
            return InlineKeyboardButton(text=obj, callback_data=f"{obj}:{key_type}")
        elif key_type == BotKey.KEY_GET_V_BY_ID:
            return InlineKeyboardButton(
                text=f'{obj["id"]} | {obj["rate"]}',
                callback_data=f'{obj["id"]}:{key_type}',
            )

    def send_msg_btns(
            self,
            max_btn_per_row: int,
            max_row_per_msg: int,
            key_type: str,
            title: str,
            objs: list,
            extra_btns=[],
            page_btns=[],
    ):
        markup = InlineKeyboardMarkup()
        row_count = 0
        btns = []
        for obj in objs:
            btns.append(self.create_btn_by_key(key_type, obj))
            if len(btns) == max_btn_per_row:
                markup.row(*btns)
                row_count += 1
                btns = []
            if row_count == max_row_per_msg:
                for extra_btn in extra_btns:
                    markup.row(*extra_btn)
                if page_btns != []:
                    markup.row(*page_btns)
                self.send_msg(msg=title, markup=markup)
                row_count = 0
                markup = InlineKeyboardMarkup()
        if btns != []:
            markup.row(*btns)
            row_count += 1
        if row_count != 0:
            for extra_btn in extra_btns:
                markup.row(*extra_btn)
            if page_btns != []:
                markup.row(*page_btns)
            self.send_msg(msg=title, markup=markup)

    def get_page_elements(
            self, objs: list, page: int, col: int, row: int, key_type: str
    ):
        """
        Get the list of objects on the current page, list of pagination buttons, and the title of quantity.

        :param list objs: All objects
        :param int page: Current page
        :param int col: Number of columns on the current page
        :param int row: Number of rows on the current page
        :param str key_type: Key type
        :return tuple[list, list, str]: List of objects on the current page, list of pagination buttons, title of quantity
        """
        record_count_total = len(objs)
        record_count_per_page = col * row
        if record_count_per_page > record_count_total:
            page_count = 1
        else:
            page_count = math.ceil(record_count_total / record_count_per_page)
        if page > page_count:
            page = page_count
        start_idx = (page - 1) * record_count_per_page
        objs = objs[start_idx: start_idx + record_count_per_page]
        if page == 1:
            to_previous = 1
        else:
            to_previous = page - 1
        if page == page_count:
            to_next = page_count
        else:
            to_next = page + 1
        btn_to_first = InlineKeyboardButton(text="<<", callback_data=f"1:{key_type}")
        btn_to_previous = InlineKeyboardButton(
            text="<", callback_data=f"{to_previous}:{key_type}"
        )
        btn_to_current = InlineKeyboardButton(
            text=f"-{page}-", callback_data=f"{page}:{key_type}"
        )
        btn_to_next = InlineKeyboardButton(
            text=">", callback_data=f"{to_next}:{key_type}"
        )
        btn_to_last = InlineKeyboardButton(
            text=">>", callback_data=f"{page_count}:{key_type}"
        )
        # Get the title of quantity
        title = f"总计: <b>{record_count_total}</b>, 总页数: <b>{page_count}</b>"
        return (
            objs,
            [btn_to_first, btn_to_previous, btn_to_current, btn_to_next, btn_to_last],
            title,
        )

    def check_if_enable_nsfw(self):
        if BOT_CFG.enable_nsfw == "0":
            self.send_msg("[NSFW] 禁止访问!")
            return False
        return True

    def get_stars_record(self, page=1):
        record, is_star_exists, _ = BOT_DB.check_has_record()
        if not record or not is_star_exists:
            self.send_msg_fail_reason_op(
                reason="暂无演员记录", op="获取演员记录"
            )
            return
        stars = record["stars"]
        stars.reverse()
        col, row = 4, 5
        objs, page_btns, title = self.get_page_elements(
            objs=stars,
            page=page,
            col=col,
            row=row,
            key_type=BotKey.KEY_GET_STARS_RECORD,
        )
        self.send_msg_btns(
            max_btn_per_row=col,
            max_row_per_msg=row,
            key_type=BotKey.KEY_GET_STAR_DETAIL_RECORD_BY_STAR_NAME_ID,
            title="<b>我的演员: </b>" + title,
            objs=objs,
            page_btns=page_btns,
        )

    def get_star_detail_record_by_name_id(self, star_name: str, star_id: str):
        record, is_stars_exists, is_vs_exists = BOT_DB.check_has_record()
        if not record:
            self.send_msg_fail_reason_op(
                reason="该演员暂无记录",
                op=f"获取演员 <code>{star_name}</code> 的详细信息",
            )
            return
        star_vs = []
        cur_star_exists = False
        if is_vs_exists:
            vs = record["vs"]
            vs.reverse()
            for v in vs:
                if star_id in v["stars"]:
                    star_vs.append(v["id"])
        if is_stars_exists:
            stars = record["stars"]
            for star in stars:
                if star["id"].lower() == star_id.lower():
                    cur_star_exists = True
        extra_btn1 = InlineKeyboardButton(
            text="随机",
            callback_data=f"{star_name}|{star_id}:{BotKey.KEY_RANDOM_GET_V_BY_STAR_ID}",
        )
        extra_btn2 = InlineKeyboardButton(
            text="最新",
            callback_data=f"{star_name}|{star_id}:{BotKey.KEY_GET_NEW_VS_BY_STAR_NAME_ID}",
        )
        extra_btn3 = InlineKeyboardButton(
            text="高分",
            callback_data=f"{star_name}:{BotKey.KEY_GET_NICE_VS_BY_STAR_NAME}",
        )
        if cur_star_exists:
            extra_btn4 = InlineKeyboardButton(
                text="取消收藏",
                callback_data=f"{star_name}|{star_id}:{BotKey.KEY_UNDO_RECORD_STAR_BY_STAR_NAME_ID}",
            )
        else:
            extra_btn4 = InlineKeyboardButton(
                text="收藏",
                callback_data=f"{star_name}|{star_id}:{BotKey.KEY_RECORD_STAR_BY_STAR_NAME_ID}",
            )
        title = f'<code>{star_name}</code> | <a href="{WIKI_UTIL.BASE_URL_JAPAN_WIKI}/{star_name}">Wiki</a> | <a href="{JBUS_UTIL.base_url_search_by_star_id}/{star_id}">Javbus</a>'
        if len(star_vs) == 0:
            markup = InlineKeyboardMarkup()
            markup.row(extra_btn1, extra_btn2, extra_btn3, extra_btn4)
            self.send_msg(msg=title, markup=markup)
            return
        self.send_msg_btns(
            max_btn_per_row=4,
            max_row_per_msg=10,
            key_type=BotKey.KEY_GET_V_DETAIL_RECORD_BY_ID,
            title=title,
            objs=star_vs,
            extra_btns=[[extra_btn1, extra_btn2, extra_btn3, extra_btn4]],
        )

    def get_vs_record(self, page=1):
        record, _, is_vs_exists = BOT_DB.check_has_record()
        if not record or not is_vs_exists:
            self.send_msg_fail_reason_op(
                reason="暂无番号收藏",
                op="获取番号收藏",
            )
            return
        vs = [v["id"] for v in record["vs"]]
        vs.reverse()
        extra_btn1 = InlineKeyboardButton(
            text="随机高分",
            callback_data=f"0:{BotKey.KEY_RANDOM_GET_V_NICE}",
        )
        extra_btn2 = InlineKeyboardButton(
            text="随机最新", callback_data=f"0:{BotKey.KEY_RANDOM_GET_V_NEW}"
        )
        col, row = 4, 10
        objs, page_btns, title = self.get_page_elements(
            objs=vs, page=page, col=col, row=row, key_type=BotKey.KEY_GET_VS_RECORD
        )
        self.send_msg_btns(
            max_btn_per_row=col,
            max_row_per_msg=row,
            key_type=BotKey.KEY_GET_V_DETAIL_RECORD_BY_ID,
            title="<b>我的番号: </b>" + title,
            objs=objs,
            extra_btns=[[extra_btn1, extra_btn2]],
            page_btns=page_btns,
        )

    def get_v_detail_record_by_id(self, id: str):
        record, _, is_vs_exists = BOT_DB.check_has_record()
        vs = record["vs"]
        cur_v_exists = False
        for v in vs:
            if id.lower() == v["id"].lower():
                cur_v_exists = True
        markup = InlineKeyboardMarkup()
        btn = InlineKeyboardButton(
            text=f"获取",
            callback_data=f"{id}:{BotKey.KEY_GET_V_BY_ID}",
        )
        if cur_v_exists:
            markup.row(
                btn,
                InlineKeyboardButton(
                    text=f"移除",
                    callback_data=f"{id}:{BotKey.KEY_UNDO_RECORD_V_BY_ID}",
                ),
            )
        else:
            markup.row(btn)
        self.send_msg(msg=f"<code>{id}</code>", markup=markup)

    def search_bts(self, q):
        def append_trackers():
            """Returns the base tracker list"""
            trackers = [
                "udp://tracker.coppersurfer.tk:6969/announce",
                "udp://tracker.openbittorrent.com:6969/announce",
                "udp://9.rarbg.to:2710/announce",
                "udp://9.rarbg.me:2780/announce",
                "udp://9.rarbg.to:2730/announce",
                "udp://tracker.opentrackr.org:1337",
                "http://p4p.arenabg.com:1337/announce",
                "udp://tracker.torrent.eu.org:451/announce",
                "udp://tracker.tiny-vps.com:6969/announce",
                "udp://open.stealth.si:80/announce",
            ]
            trackers = [quote(tr) for tr in trackers]
            return "&tr=".join(trackers)

        def category_name(category):
            """Translates the category code to a name"""
            names = ["", "音频", "视频", "应用", "游戏", "NSFW", "其他"]
            category = int(category[0])
            category = category if category < len(names) - 1 else -1
            return names[category]

        def size_as_str(size):
            """Formats the file size in bytes to kb, mb or gb accordingly"""
            size = int(size)
            size_str = f"{size} b"
            if size >= 1024:
                size_str = f"{(size / 1024):.2f} kb"
            if size >= 1024 ** 2:
                size_str = f"{(size / 1024 ** 2):.2f} mb"
            if size >= 1024 ** 3:
                size_str = f"{(size / 1024 ** 3):.2f} gb"
            return size_str

        def magnet_link(ih, name):
            """Creates the magnet URI"""
            return f"magnet:?xt=urn:btih:{ih}&dn={quote(name)}&tr={append_trackers()}"

        agent = BASE_UTIL.ua()
        url = f"https://apibay.org/q.php?q={quote(q)}"
        results = get(url, headers={"agent": agent})
        if not results.status_code == 200:
            return None
        matches = []
        data = results.json()
        if data and "no results" in data[0]["name"].lower():
            return matches
        for d in data:
            matches.append(
                {
                    "seeders": d["seeders"],
                    "leechers": d["leechers"],
                    "name": d["name"],
                    "category": category_name(d["category"]),
                    "size": size_as_str(d["size"]),
                    "magnet": magnet_link(d["info_hash"], d["name"]),
                }
            )
        return matches

    def get_v_by_id(
            self,
            id: str,
            send_to_pikpak=True,
            is_nice=True,
            is_uncensored=True,
            magnet_max_count=3,
            not_send=False,
    ):
        """
        Get based on id

        :param str id: Number
        :param bool send_to_pikpak: Whether to send to pikpak, default is yes
        :param bool is_nice: Whether to filter out HD, subtitled magnet links, default is yes
        :param bool is_uncensored: Whether to filter out uncensored magnet links, default is yes
        :param int magnet_max_count: Maximum id of magnet links after filtering, default is 3
        :param not_send: Whether not to send results, default is to send
        :return dict: When not sending results, return the obtained results (if any)
        """
        if not self.check_if_enable_nsfw():
            return {}
        op_get_v_by_id = f"搜索番号: <code>{id}</code>"
        v = BOT_CACHE_DB.get_cache(key=id, type=BotCacheDb.TYPE_V)
        v_score = None
        is_cache = False
        if not v or not_send:
            for util in self.v_utils:
                code, v = util.get_av_by_id(
                    id=id,
                    is_nice=is_nice,
                    is_uncensored=is_uncensored,
                    magnet_max_count=magnet_max_count,
                )
                if code == 200:
                    v_util = util
                    break
            if not v_util:
                if not not_send:
                    self.send_msg_v_not_found(
                        v_id=id,
                        op=op_get_v_by_id,
                        reason="找不到结果，请稍后重试。",
                    )
                return
            if "score" not in v.keys():
                _, v["score"] = DMM_UTIL.get_score_by_id(id)
            if not not_send:
                if len(v["magnets"]) == 0:
                    BOT_CACHE_DB.set_cache(
                        key=id, value=v, type=BotCacheDb.TYPE_V, expire=3600 * 24 * 1
                    )
                else:
                    BOT_CACHE_DB.set_cache(key=id, value=v, type=BotCacheDb.TYPE_V)
        else:
            v_score = v["score"]
            is_cache = True
        if not_send:
            return v
        v_id = id
        v_title = v["title"]
        v_img = v["img"]
        v_date = v["date"]
        v_tags = v["tags"]
        v_stars = v["stars"]
        v_magnets = v["magnets"]
        v_url = v["url"]
        msg = ""
        if v_title != "":
            v_title = v_title.replace("<", "").replace(">", "")
            msg += f"""【标题】<a href="{v_url}">{v_title}</a>
"""
        msg += f"""【番号】<code>{v_id}</code>
"""
        if v_date != "":
            msg += f"""【日期】{v_date}
"""
        if v_score:
            msg += f"""【评分】{v_score}
"""
        if v_stars != []:
            show_star_name = v_stars[0]["name"]
            show_star_id = v_stars[0]["id"]
            stars_msg = ""
            for star in v_stars:
                stars_msg += f"""【演员】<code>{star["name"]}</code>
"""
            msg += stars_msg
        if v_tags:
            v_tags = " ".join(v_tags).replace("<", "").replace(">", "")
            msg += f"""【标签】{v_tags}
"""
        msg += f"""【其他】<a href="{URL_PIKPAK_BOT}">Pikpak</a> | <a href="{URL_PROJECT_ADDRESS}">项目地址</a>
"""
        magnet_send_to_pikpak = ""
        for i, magnet in enumerate(v_magnets):
            if i == 0:
                magnet_send_to_pikpak = magnet["link"]
            magnet_tags = ""
            if magnet["uc"] == "1":
                magnet_tags += "无码 "
            if magnet["hd"] == "1":
                magnet_tags += "高清 "
            if magnet["zm"] == "1":
                magnet_tags += "字幕 "
            msg_tmp = f"""【{magnet_tags}磁力-{string.ascii_letters[i].upper()} {magnet["size"]}】<code>{magnet["link"]}</code>
"""
            if len(msg + msg_tmp) >= 2000:
                break
            msg += msg_tmp
        pv_btn = InlineKeyboardButton(
            text="预览", callback_data=f"{v_id}:{BotKey.KEY_WATCH_PV_BY_ID}"
        )
        fv_btn = InlineKeyboardButton(
            text="观看", callback_data=f"{v_id}:{BotKey.KEY_WATCH_FV_BY_ID}"
        )
        sample_btn = InlineKeyboardButton(
            text="截图", callback_data=f"{v_id}:{BotKey.KEY_GET_SAMPLE_BY_ID}"
        )
        more_btn = InlineKeyboardButton(
            text="更多",
            callback_data=f"{v_id}:{BotKey.KEY_GET_MORE_MAGNETS_BY_ID}",
        )
        if len(v_magnets) != 0:
            markup = InlineKeyboardMarkup().row(sample_btn, pv_btn, fv_btn, more_btn)
        else:
            markup = InlineKeyboardMarkup().row(sample_btn, pv_btn, fv_btn)
        star_record_btn = None
        if len(v_stars) == 1:
            if BOT_DB.check_star_exists_by_id(star_id=show_star_id):
                star_record_btn = InlineKeyboardButton(
                    text=f"信息",
                    callback_data=f"{show_star_name}|{show_star_id}:{BotKey.KEY_GET_STAR_DETAIL_RECORD_BY_STAR_NAME_ID}",
                )
            else:
                star_record_btn = InlineKeyboardButton(
                    text=f"收藏",
                    callback_data=f"{show_star_name}|{show_star_id}:{BotKey.KEY_RECORD_STAR_BY_STAR_NAME_ID}",
                )
        star_ids = ""
        for i, star in enumerate(v_stars):
            star_ids += star["id"] + "|"
            if i >= 5:
                star_ids += "...|"
                break
        if star_ids != "":
            star_ids = star_ids[: len(star_ids) - 1]
        v_record_btn = None
        if BOT_DB.check_id_exists(id=v_id):
            v_record_btn = InlineKeyboardButton(
                text=f"信息",
                callback_data=f"{v_id}:{BotKey.KEY_GET_V_DETAIL_RECORD_BY_ID}",
            )
        else:
            v_record_btn = InlineKeyboardButton(
                text=f"收藏",
                callback_data=f"{v_id}|{star_ids}:{BotKey.KEY_RECORD_V_BY_ID_STAR_IDS}",
            )
        renew_btn = None
        if is_cache:
            renew_btn = InlineKeyboardButton(
                text="重试", callback_data=f"{v_id}:{BotKey.KEY_DEL_V_CACHE}"
            )
        if star_record_btn and renew_btn:
            markup.row(v_record_btn, star_record_btn, renew_btn)
        elif star_record_btn:
            markup.row(v_record_btn, star_record_btn)
        elif renew_btn:
            markup.row(v_record_btn, renew_btn)
        else:
            markup.row(v_record_btn)
        if v_img == "":
            self.send_msg(msg=msg, markup=markup)
        else:
            try:
                BOT.send_photo(
                    chat_id=BOT_CFG.tg_chat_id,
                    photo=v_img,
                    caption=msg,
                    parse_mode="HTML",
                    reply_markup=markup,
                )
            except Exception:
                self.send_msg(msg=msg, markup=markup)
        if magnet_send_to_pikpak != "" and send_to_pikpak:
            self.send_magnet_to_pikpak(magnet_send_to_pikpak, v_id)

    def send_magnet_to_pikpak(self, magnet: str, id: str):
        op_send_magnet_to_pikpak = (
            f"发送番号 {id} 的磁力链接A到Pikpak: <code>{magnet}</code>"
        )
        if self.send_msg_to_pikpak(magnet):
            self.send_msg_success_op(op_send_magnet_to_pikpak)
        else:
            self.send_msg_fail_reason_op(
                reason="请自行验证网络或日志。",
                op=op_send_magnet_to_pikpak,
            )

    def get_sample_by_id(self, id: str):
        op_get_sample = f"获取番号 <code>{id}</code> 的截图。"
        samples = BOT_CACHE_DB.get_cache(key=id, type=BotCacheDb.TYPE_SAMPLE)
        if not samples:
            code, samples = JBUS_UTIL.get_samples_by_id(id)
            if not self.check_success(code, op_get_sample):
                return
            BOT_CACHE_DB.set_cache(key=id, value=samples, type=BotCacheDb.TYPE_SAMPLE)
        samples_imp = []
        sample_error = False
        for sample in samples:
            samples_imp.append(InputMediaPhoto(sample))
            if len(samples_imp) == 10:
                try:
                    BOT.send_media_group(chat_id=BOT_CFG.tg_chat_id, media=samples_imp)
                    samples_imp = []
                except Exception:
                    sample_error = True
                    self.send_msg_fail_reason_op(
                        reason="图片解析失败。", op=op_get_sample
                    )
                    break
        if samples_imp != [] and not sample_error:
            try:
                BOT.send_media_group(chat_id=BOT_CFG.tg_chat_id, media=samples_imp)
            except Exception:
                self.send_msg_fail_reason_op(
                    reason="图片解析失败。", op=op_get_sample
                )

    def watch_v_by_id(self, id: str, type: int):
        id = id.lower()
        if id.find("fc2") != -1 and id.find("ppv") == -1:
            id = id.replace("fc2", "fc2-ppv")
        if type == 0:
            pv = BOT_CACHE_DB.get_cache(key=id, type=BotCacheDb.TYPE_PV)
            if not pv:
                op_watch_v = f"获取番号 <code>{id}</code> 的预览视频。"
                futures = {}
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures[executor.submit(DMM_UTIL.get_pv_by_id, id)] = 1
                    futures[executor.submit(VGLE_UTIL.get_pv_by_id, id)] = 2
                    for future in concurrent.futures.as_completed(futures):
                        if futures[future] == 1:
                            code_dmm, pv_dmm = future.result()
                        elif futures[future] == 2:
                            code_vgle, pv_vgle = future.result()
                if code_dmm != 200 and code_vgle != 200:
                    if code_dmm == 502 or code_vgle == 502:
                        self.send_msg_code_op(502, op_watch_v)
                    else:
                        self.send_msg_code_op(404, op_watch_v)
                    return
                from_site = ""
                pv_src = ""
                if code_dmm == 200:
                    from_site = "dmm"
                    pv_src = pv_dmm
                elif code_vgle == 200:
                    from_site = "avgle"
                    pv_src = pv_vgle
                pv_cache = {"from": from_site, "src": pv_src}
                BOT_CACHE_DB.set_cache(key=id, value=pv_cache, type=BotCacheDb.TYPE_PV)
            else:
                from_site = pv["from"]
                pv_src = pv["src"]
            if from_site == "dmm":
                self.send_msg(msg=pv_src, pv=True)
            elif from_site == "avgle":
                self.send_msg(msg=pv_src, pv=True)
        elif type == 1:
            fv = BOT_CACHE_DB.get_cache(key=id, type=BotCacheDb.TYPE_FV)
            if not fv:
                op_watch_v = f"获取番号 <code>{id}</code> 的完整视频。"
                code, fv = VGLE_UTIL.get_fv_by_id(id)
                if not self.check_success(code, op_watch_v):
                    return
                fv_cache = {"from": "avgle", "src": fv}
                BOT_CACHE_DB.set_cache(key=id, value=fv_cache, type=BotCacheDb.TYPE_FV)
            else:
                fv_src = fv["src"]
                self.send_msg(msg=fv_src, pv=True)

    def get_more_magnets_by_id(self, id: str):
        op_get_more_magnets = f"获取番号 <code>{id}</code> 的更多磁力链接。"
        v = BOT_CACHE_DB.get_cache(key=id, type=BotCacheDb.TYPE_V)
        if not v:
            v = self.get_v_by_id(
                id=id,
                send_to_pikpak=False,
                is_nice=False,
                is_uncensored=False,
                magnet_max_count=20,
                not_send=True,
            )
            if not v:
                self.send_msg_fail_reason_op(
                    reason="未找到结果。", op=op_get_more_magnets
                )
                return
        v_magnets = v["magnets"]
        if len(v_magnets) <= 3:
            self.send_msg_fail_reason_op(
                reason="没有更多磁力链接。", op=op_get_more_magnets
            )
            return
        v_id = v["id"]
        v_title = v["title"]
        msg = f"""【标题】{v_title}
【番号】<code>{v_id}</code>
"""
        for i, magnet in enumerate(v_magnets):
            if i < 3:
                continue
            magnet_tags = ""
            if magnet["uc"] == "1":
                magnet_tags += "无码 "
            if magnet["hd"] == "1":
                magnet_tags += "高清 "
            if magnet["zm"] == "1":
                magnet_tags += "字幕 "
            msg_tmp = f"""【{magnet_tags}磁力-{string.ascii_letters[i].upper()} {magnet["size"]}】<code>{magnet["link"]}</code>
"""
            if len(msg + msg_tmp) >= 2000:
                break
            msg += msg_tmp
        self.send_msg(msg=msg)

    def search_star_by_name(self, star_name: str, page=1):
        op_search_star = f"搜索演员: <code>{star_name}</code>"
        stars = BOT_CACHE_DB.get_cache(key=star_name, type=BotCacheDb.TYPE_STAR)
        if not stars:
            code, stars = DMM_UTIL.get_stars_by_name(star_name)
            if not self.check_success(code, op_search_star):
                return
            BOT_CACHE_DB.set_cache(
                key=star_name, value=stars, type=BotCacheDb.TYPE_STAR
            )
        col, row = 4, 5
        objs, page_btns, title = self.get_page_elements(
            objs=stars,
            page=page,
            col=col,
            row=row,
            key_type=BotKey.KEY_SEARCH_STAR_BY_NAME,
        )
        self.send_msg_btns(
            max_btn_per_row=col,
            max_row_per_msg=row,
            key_type=BotKey.KEY_GET_STAR_DETAIL_RECORD_BY_STAR_NAME_ID,
            title="<b>演员列表: </b>" + title,
            objs=objs,
            page_btns=page_btns,
        )

    def get_top_stars(self, page=1):
        op_get_top_stars = "获取DMM演员排行榜"
        stars = BOT_CACHE_DB.get_cache(key="top_stars", type=BotCacheDb.TYPE_STAR)
        if not stars:
            code, stars = DMM_UTIL.get_top_stars()
            if not self.check_success(code, op_get_top_stars):
                return
            BOT_CACHE_DB.set_cache(
                key="top_stars", value=stars, type=BotCacheDb.TYPE_STAR
            )
        col, row = 4, 5
        objs, page_btns, title = self.get_page_elements(
            objs=stars,
            page=page,
            col=col,
            row=row,
            key_type=BotKey.KEY_GET_TOP_STARS,
        )
        self.send_msg_btns(
            max_btn_per_row=col,
            max_row_per_msg=row,
            key_type=BotKey.KEY_GET_STAR_DETAIL_RECORD_BY_STAR_NAME_ID,
            title="<b>演员排行榜: </b>" + title,
            objs=objs,
            page_btns=page_btns,
        )

    def random_get_v_by_star_id(self, star_name: str, star_id: str):
        op_random_get_v = f"随机获取演员 <code>{star_name}</code> 的影片"
        code, v = DMM_UTIL.get_random_av_by_star_id(star_id)
        if not self.check_success(code, op_random_get_v):
            return
        self.get_v_by_id(id=v["id"])

    def random_get_v_nice(self):
        op_random_get_v_nice = "随机获取高评分影片"
        code, v = DMM_UTIL.get_random_av_nice()
        if not self.check_success(code, op_random_get_v_nice):
            return
        self.get_v_by_id(id=v["id"])

    def random_get_v_new(self):
        op_random_get_v_new = "随机获取最新影片"
        code, v = DMM_UTIL.get_random_av_new()
        if not self.check_success(code, op_random_get_v_new):
            return
        self.get_v_by_id(id=v["id"])

    def get_new_vs_by_star_name_id(self, star_name: str, star_id: str):
        op_get_new_vs = f"获取演员 <code>{star_name}</code> 的最新影片"
        code, vs = DMM_UTIL.get_new_avs_by_star_id(star_id)
        if not self.check_success(code, op_get_new_vs):
            return
        if len(vs) == 0:
            self.send_msg_fail_reason_op(
                reason="未找到结果。", op=op_get_new_vs
            )
            return
        self.send_msg_btns(
            max_btn_per_row=4,
            max_row_per_msg=10,
            key_type=BotKey.KEY_GET_V_BY_ID,
            title=f"<b>{star_name} 的最新影片: </b>",
            objs=vs,
        )

    def get_nice_vs_by_star_name(self, star_name: str):
        op_get_nice_vs = f"获取演员 <code>{star_name}</code> 的高分影片"
        code, vs = DMM_UTIL.get_nice_avs_by_star_name(star_name)
        if not self.check_success(code, op_get_nice_vs):
            return
        if len(vs) == 0:
            self.send_msg_fail_reason_op(
                reason="未找到结果。", op=op_get_nice_vs
            )
            return
        self.send_msg_btns(
            max_btn_per_row=4,
            max_row_per_msg=10,
            key_type=BotKey.KEY_GET_V_BY_ID,
            title=f"<b>{star_name} 的高分影片: </b>",
            objs=vs,
        )

    def record_star_by_star_name_id(self, star_name: str, star_id: str):
        op_record_star = f"收藏演员: <code>{star_name}</code>"
        if BOT_DB.check_star_exists_by_id(star_id):
            self.send_msg_fail_reason_op(
                reason="该演员已存在。", op=op_record_star
            )
            return
        code = BOT_DB.record_star_by_name_id(star_name, star_id)
        self.send_msg_code_op(code, op_record_star)

    def record_v_by_id_star_ids(self, id: str, star_ids: str):
        op_record_v = f"收藏番号: <code>{id}</code>"
        if BOT_DB.check_id_exists(id):
            self.send_msg_fail_reason_op(
                reason="该番号已存在。", op=op_record_v
            )
            return
        star_ids = star_ids.split("|")
        code = BOT_DB.record_v_by_id_star_ids(id, star_ids)
        self.send_msg_code_op(code, op_record_v)

    def undo_record_star_by_star_name_id(self, star_name: str, star_id: str):
        op_undo_record_star = f"取消收藏演员: <code>{star_name}</code>"
        if not BOT_DB.check_star_exists_by_id(star_id):
            self.send_msg_fail_reason_op(
                reason="该演员不存在。", op=op_undo_record_star
            )
            return
        code = BOT_DB.undo_record_star_by_id(star_id)
        self.send_msg_code_op(code, op_undo_record_star)

    def undo_record_v_by_id(self, id: str):
        op_undo_record_v = f"取消收藏番号: <code>{id}</code>"
        if not BOT_DB.check_id_exists(id):
            self.send_msg_fail_reason_op(
                reason="该番号不存在。", op=op_undo_record_v
            )
            return
        code = BOT_DB.undo_record_v_by_id(id)
        self.send_msg_code_op(code, op_undo_record_v)

    def del_v_cache(self, id: str):
        op_del_v_cache = f"删除番号 <code>{id}</code> 的缓存"
        BOT_CACHE_DB.del_cache(key=id, type=BotCacheDb.TYPE_V)
        self.send_msg_success_op(op_del_v_cache)

    def send_msg_to_pikpak(self, msg: str):
        try:
            app = Client(
                "pikpak",
                api_id=BOT_CFG.tg_api_id,
                api_hash=BOT_CFG.tg_api_hash,
                proxy=BOT_CFG.proxy_json_pikpak,
            )
            app.start()
            app.send_message(PIKPAK_BOT_NAME, msg)
            app.stop()
            return True
        except Exception as e:
            LOG.error(f"发送消息到Pikpak失败: {e}")
            return False

    def send_msg_v_not_found(self, v_id: str, op: str, reason: str):
        msg = f"""操作执行: {op}
执行结果: 失败，{reason} Q_Q
"""
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton(
                text="重试", callback_data=f"{v_id}:{BotKey.KEY_DEL_V_CACHE}"
            )
        )
        self.send_msg(msg=msg, markup=markup)


BOT_UTILS = BotUtils()


@BOT.message_handler(commands=list(BOT_CMDS.keys()))
def handle_cmds(message):
    cmd = message.text[1:]
    if cmd == "help":
        BOT_UTILS.send_msg(msg=MSG_HELP)
    elif cmd == "stars":
        BOT_UTILS.get_stars_record()
    elif cmd == "ids":
        BOT_UTILS.get_vs_record()
    elif cmd == "nice":
        BOT_UTILS.random_get_v_nice()
    elif cmd == "new":
        BOT_UTILS.random_get_v_new()
    elif cmd == "rank":
        BOT_UTILS.get_top_stars()
    elif cmd == "record":
        BOT.send_document(
            chat_id=BOT_CFG.tg_chat_id,
            document=open(PATH_RECORD_FILE, "rb"),
            caption="记录文件",
        )
    elif cmd == "star":
        BOT_UTILS.send_msg(msg="请发送演员名字: ")
        BOT.register_next_step_handler(message, handle_star_search)
    elif cmd == "id":
        BOT_UTILS.send_msg(msg="请发送番号: ")
        BOT.register_next_step_handler(message, handle_id_search)


def handle_star_search(message):
    star_name = message.text
    BOT_UTILS.search_star_by_name(star_name)


def handle_id_search(message):
    id = message.text
    BOT_UTILS.get_v_by_id(id)


@BOT.message_handler(content_types=["text"])
def handle_text(message):
    text = message.text
    if text.startswith("/"):
        BOT_UTILS.send_msg(msg="未知命令，请使用 /help 查看帮助")
        return
    if ID_PAT.search(text):
        id = ID_PAT.search(text).group()
        BOT_UTILS.get_v_by_id(id)
    else:
        BOT_UTILS.send_msg(msg="暂不支持关键词搜索，请直接发送番号")


@BOT.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    data = call.data
    if ":" not in data:
        return
    key, key_type = data.split(":", 1)
    if key_type == BotKey.KEY_GET_SAMPLE_BY_ID:
        BOT_UTILS.get_sample_by_id(key)
    elif key_type == BotKey.KEY_GET_MORE_MAGNETS_BY_ID:
        BOT_UTILS.get_more_magnets_by_id(key)
    elif key_type == BotKey.KEY_SEARCH_STAR_BY_NAME:
        BOT_UTILS.search_star_by_name(key)
    elif key_type == BotKey.KEY_GET_TOP_STARS:
        BOT_UTILS.get_top_stars(int(key))
    elif key_type == BotKey.KEY_WATCH_PV_BY_ID:
        BOT_UTILS.watch_v_by_id(key, 0)
    elif key_type == BotKey.KEY_WATCH_FV_BY_ID:
        BOT_UTILS.watch_v_by_id(key, 1)
    elif key_type == BotKey.KEY_GET_V_BY_ID:
        BOT_UTILS.get_v_by_id(key)
    elif key_type == BotKey.KEY_RANDOM_GET_V_BY_STAR_ID:
        star_name, star_id = key.split("|")
        BOT_UTILS.random_get_v_by_star_id(star_name, star_id)
    elif key_type == BotKey.KEY_RANDOM_GET_V_NICE:
        BOT_UTILS.random_get_v_nice()
    elif key_type == BotKey.KEY_RANDOM_GET_V_NEW:
        BOT_UTILS.random_get_v_new()
    elif key_type == BotKey.KEY_GET_NEW_VS_BY_STAR_NAME_ID:
        star_name, star_id = key.split("|")
        BOT_UTILS.get_new_vs_by_star_name_id(star_name, star_id)
    elif key_type == BotKey.KEY_GET_NICE_VS_BY_STAR_NAME:
        BOT_UTILS.get_nice_vs_by_star_name(key)
    elif key_type == BotKey.KEY_RECORD_STAR_BY_STAR_NAME_ID:
        star_name, star_id = key.split("|")
        BOT_UTILS.record_star_by_star_name_id(star_name, star_id)
    elif key_type == BotKey.KEY_RECORD_V_BY_ID_STAR_IDS:
        v_id, star_ids = key.split("|")
        BOT_UTILS.record_v_by_id_star_ids(v_id, star_ids)
    elif key_type == BotKey.KEY_GET_STARS_RECORD:
        BOT_UTILS.get_stars_record(int(key))
    elif key_type == BotKey.KEY_GET_VS_RECORD:
        BOT_UTILS.get_vs_record(int(key))
    elif key_type == BotKey.KEY_GET_STAR_DETAIL_RECORD_BY_STAR_NAME_ID:
        star_name, star_id = key.split("|")
        BOT_UTILS.get_star_detail_record_by_name_id(star_name, star_id)
    elif key_type == BotKey.KEY_GET_V_DETAIL_RECORD_BY_ID:
        BOT_UTILS.get_v_detail_record_by_id(key)
    elif key_type == BotKey.KEY_UNDO_RECORD_STAR_BY_STAR_NAME_ID:
        star_name, star_id = key.split("|")
        BOT_UTILS.undo_record_star_by_star_name_id(star_name, star_id)
    elif key_type == BotKey.KEY_UNDO_RECORD_V_BY_ID:
        BOT_UTILS.undo_record_v_by_id(key)
    elif key_type == BotKey.KEY_DEL_V_CACHE:
        BOT_UTILS.del_v_cache(key)


if __name__ == "__main__":
    LOG.info("开始启动机器人...")
    BOT.infinity_polling()
