"""
icq_core.py — ICQ/OSCAR client core library
============================================
Чистое ядро без UI. Использование:

    client = ICQClient("12345678", "password")
    client.on_contact_online = lambda c: print(f"{c.display_name} онлайн")
    client.on_message        = lambda m: print(f"{m.sender_uin}: {m.text}")
    await client.set_status(Status.FREE)
    asyncio.run(client.run())
"""

from __future__ import annotations

import asyncio
import logging
import struct
import time, re
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger("icq_core")


class AuthError(ConnectionError):
    """Ошибка аутентификации — неверный UIN/пароль. Повторное подключение не поможет."""
    pass


SERVER = "195.66.114.37" #"195.66.114.37"
PORT   = 5190

ICQ_MAX_CHARS = 2000


CAP_QIP2005          = bytes.fromhex("563FC8090B6F41514950203230303561")
CAP_QIP_GENERIC      = bytes.fromhex("563FC8090B6F41514950202020202021")
CAP_TYPING           = bytes.fromhex("563FC8090B6F41BD9F79422609DFA2F3")
CAP_RTF              = bytes.fromhex("97B12751243C4334AD22D6ABF73F1492")
CAP_AIM_SERVERRELAY  = bytes.fromhex("094613494C7F11D18222444553540000")
CAP_UTF8             = bytes.fromhex("0946134E4C7F11D18222444553540000")
CAP_XTRAZ            = bytes.fromhex("1A093C6CD7FD4EC59D51A6474E34F5A0")

# Уточнено по декомпилированному исходнику Jimm (class_20.java):
# прежние значения CAP_JIMM/CAP_MIRANDA не соответствовали реальным байтам.
CAP_JIMM    = bytes.fromhex("4A696D6D200000000000000000000000")
CAP_MIRANDA = bytes.fromhex("4D6972616E64614D0006030000030807")
CAP_ICQ6    = bytes.fromhex("3B7248ED5EDE4D6993F729D5BDF8A27F")
CAP_ICQ7    = bytes.fromhex("3FF19BEB53714657BCDEA39142E55D99")

KNOWN_CAPS: Dict[bytes, str] = {
    CAP_QIP2005:     "QIP 2005a",
    CAP_QIP_GENERIC: "QIP",
    CAP_JIMM:        "Jimm",
    CAP_MIRANDA:     "Miranda",
    CAP_ICQ6:        "ICQ 6",
    CAP_ICQ7:        "ICQ 7",
    b'Jasmine ICQ ####': "Jasmine",
    b'Jasmine ver\xff\x05\x05\x03\x00': "Jasmine",
}

CLI_READY_DATA = bytes([
    0x00,0x01,0x00,0x04,0x01,0x10,0x16,0x4f,
    0x00,0x02,0x00,0x01,0x01,0x10,0x16,0x4f,
    0x00,0x03,0x00,0x01,0x01,0x10,0x16,0x4f,
    0x00,0x04,0x00,0x01,0x01,0x10,0x16,0x4f,
    0x00,0x06,0x00,0x01,0x01,0x10,0x16,0x4f,
    0x00,0x09,0x00,0x01,0x01,0x10,0x16,0x4f,
    0x00,0x0a,0x00,0x01,0x01,0x10,0x16,0x4f,
    0x00,0x0b,0x00,0x01,0x01,0x10,0x16,0x4f,
])

XSTATUS_TABLE: List[Tuple[str, str]] = [
    ("63627337A03F49FF80E5F709CDE0A4EE", "shopping"),
    ("5A581EA1E580430CA06F612298B7E4C7", "duck"),
    ("83C9B78E77E74378B2C5FB6CFCC35BEC", "tired"),
    ("E601E41C33734BD1BC06811D6C323D81", "party"),
    ("8C50DBAE81ED4786ACCA16CC3213C7B7", "beer"),
    ("3FB0BD36AF3B4A609EEFCF190F6A5A7F", "thinking"),
    ("F8E8D7B282C4414290F810C6CE0A89A6", "eating"),
    ("80537DE2A4674A76B3546DFD075F5EC6", "tv"),
    ("F18AB52EDC57491D99DC6444502457AF", "friends"),
    ("1B78AE31FA0B4D3893D1997EEEAFB218", "coffee"),
    ("61BEE0DD8BDD475D8DEE5F4BAACF19A7", "music"),
    ("488E14898ACA4A0882AA77CE7A165208", "business"),
    ("107A9A1812324DA4B6CD0879DB780F09", "camera"),
    ("6F4930984F7C4AFFA27634A03BCEAEA7", "funny"),
    ("1292E5501B644F66B206B29AF378E48D", "phone"),
    ("D4A611D08F014EC09223C5B6BEC6CCF0", "games"),
    ("609D52F8A29A49A6B2A02524C5E9D260", "college"),
    ("1F7A4071BF3B4E60BC324C5787B04CF1", "sick"),
    ("785E8C4840D34C65886F04CF3F3F43DF", "sleeping"),
    ("A6ED557E6BF744D4A5D4D2E7D95CE81F", "surfing"),
    ("12D07E3EF885489E8E97A72A6551E58D", "internet"),
    ("BA74DB3E9E24434B87B62F6B8DFEE50F", "engineering"),
    ("634F6BD8ADD24AA1AAB9115BC26D05A1", "typing"),
    ("01D8D7EEAC3B492AA58DD3D877E66B92", "angry"),
    ("2CE0E4E57C6443709C3A7A1CE878A7DC", "unknown"),
    ("101117C9A3B040F981AC49E159FBD5D4", "ppc"),
    ("160C60BBDD4443F39140050F00E6C009", "mobile"),
    ("6443C6AF22604517B58CD7DF8E290352", "man"),
    ("16F5B76FA9D240358CC5C084703C98FA", "wc"),
    ("631436FF3F8A40D0A5CB7B66E051B364", "question"),
    ("B70867F538254327A1FFCF4CC1939797", "way"),
    ("DDCF0EA971954048A9C6413206D6F280", "heart"),
    ("3FB0BD36AF3B4A609EEFCF190F6A5A7E", "smoking"),
    ("E601E41C33734BD1BC06811D6C323D82", "sex"),
    ("D4E2B0BA334E4FA598D0117DBF4D3CC8", "search"),
    ("0072D9084AD143DD91996F026966026F", "diary"),
]

XSTATUS_BY_NAME: Dict[str, str] = {
    name: guid_hex.upper() for guid_hex, name in XSTATUS_TABLE
}

_XSTATUS_BY_GUID: Dict[str, str] = {
    g.upper(): n for g, n in XSTATUS_TABLE
}


class Status(IntEnum):
    ONLINE  = 0x00000000
    AWAY    = 0x00000001
    DND     = 0x00000002
    NA      = 0x00000004
    FREE    = 0x00000020
    OFFLINE = 0xFFFFFFFF

    @classmethod
    def from_flags(cls, flags: int) -> "Status":
        if flags & 0x0002:
            return cls.DND
        if flags & 0x0004:
            return cls.NA
        if flags & 0x0001:
            return cls.AWAY
        if flags & 0x0020:
            return cls.FREE
        if flags == 0x00000000:
            return cls.ONLINE
        return cls.ONLINE

    @property
    def label(self) -> str:
        return {
            Status.ONLINE:  "ONLINE",
            Status.AWAY:    "AWAY",
            Status.DND:     "DND",
            Status.NA:      "NA",
            Status.FREE:    "FREE",
            Status.OFFLINE: "OFFLINE",
        }.get(self, "UNKNOWN")



@dataclass
class Group:
    group_id: int
    name:     str


@dataclass
class Contact:
    uin:      str
    name:     str
    group_id: int
    item_id:  int

    status:      Status = Status.OFFLINE
    status_msg:  str    = ""
    client:      str    = "Unknown"
    xstatus:     str    = ""
    xstatus_msg: str    = ""
    signon_time:  int  = 0
    online_secs:  int  = 0
    idle_secs:    int  = 0
    pending_auth: bool = False   # True = сервер требует авторизацию для этого UIN

    @property
    def display_name(self) -> str:
        return self.name if self.name else self.uin

    @property
    def is_online(self) -> bool:
        return self.status != Status.OFFLINE


@dataclass
class Message:
    sender_uin:  str
    text:        str
    timestamp:   float = field(default_factory=time.time)
    is_outgoing: bool  = False


@dataclass
class UserInfo:
    """Анкета пользователя (из SNAC 0x15/0x03 CLI_METAREQINFO)."""
    uin:        str  = ""
    nick:       str  = ""
    first_name: str  = ""
    last_name:  str  = ""
    email:      str  = ""
    city:       str  = ""
    state:      str  = ""
    phone:      str  = ""
    fax:        str  = ""
    address:    str  = ""
    cell_phone: str  = ""
    age:        int  = 0
    gender:     str  = ""   # "M", "F" или ""
    home_page:  str  = ""
    birthday:   str  = ""   # "DD.MM.YYYY"
    about:      str  = ""
    work_city:  str  = ""
    work_state: str  = ""
    work_phone: str  = ""
    work_fax:   str  = ""
    work_addr:  str  = ""
    work_name:  str  = ""   # название компании
    work_dep:   str  = ""   # отдел
    work_pos:   str  = ""   # должность
    auth_required: bool = False  # True = требовать авторизацию при добавлении

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


@dataclass
class SearchResult:
    """Одна запись из результатов поиска пользователей."""
    uin:        str
    nick:       str
    first_name: str
    last_name:  str
    email:      str
    auth_req:   bool   = False  # требуется авторизация
    online:     bool   = False
    gender:     str    = ""
    age:        int    = 0



def _xor_password(password: str) -> bytes:
    key = [0xF3,0x26,0x81,0xC4,0x39,0x86,0xDB,0x92,
           0x71,0xA3,0xB9,0xE6,0x53,0x7A,0x95,0x7C]
    return bytes(key[i % len(key)] ^ ord(ch) for i, ch in enumerate(password))


def _make_tlv(t: int, v: bytes) -> bytes:
    return struct.pack("!HH", t, len(v)) + v


def _parse_tlvs(data: bytes) -> Dict[int, bytes]:
    out, p = {}, 0
    while p + 4 <= len(data):
        t, l = struct.unpack_from("!HH", data, p)
        if p + 4 + l > len(data):
            break
        out[t] = data[p + 4: p + 4 + l]
        p += 4 + l
    return out


def _make_snac(fam: int, sub: int, flags: int = 0,
               reqid: int = None, payload: bytes = b"") -> bytes:
    if reqid is None:
        reqid = int(time.time()) & 0xFFFFFFFF
    return struct.pack("!HHHI", fam, sub, flags, reqid) + payload


def _pack_flap(channel: int, seq: int, payload: bytes) -> bytes:
    return struct.pack("!BBHH", 0x2A, channel, seq, len(payload)) + payload


def _decode_text(raw: bytes) -> str:
    clean = raw.rstrip(b"\x00").strip()
    if not clean:
        return ""
    if len(clean) % 2 == 0:
        even, odd = clean[0::2], clean[1::2]
        if all(b in (0x00, 0x04) for b in even) and any(b != 0x00 for b in odd):
            return clean.decode("utf-16-be", errors="replace")
    try:
        return clean.decode("utf-8")
    except UnicodeDecodeError:
        return clean.decode("cp1251", errors="replace")


def _detect_client(caps_blob: bytes) -> str:
    if not caps_blob:
        return "Unknown"
    caps = [caps_blob[i:i+16] for i in range(0, len(caps_blob), 16)
            if len(caps_blob[i:i+16]) == 16]
    for cap in caps:
        if cap in KNOWN_CAPS:
            return KNOWN_CAPS[cap]
    return "Unknown"


def _detect_xstatus_from_caps(caps_blob: bytes) -> str:
    if not caps_blob:
        return ""
    for i in range(0, len(caps_blob) - 15, 16):
        name = _XSTATUS_BY_GUID.get(caps_blob[i:i+16].hex().upper(), "")
        if name:
            return name
    return ""


def _guid_to_name(guid_bytes: bytes) -> str:
    return _XSTATUS_BY_GUID.get(guid_bytes.hex().upper(), "")


def _split_text(text: str) -> List[str]:
    parts = []
    while text:
        if len(text) <= ICQ_MAX_CHARS:
            parts.append(text)
            break
        chunk = text[:ICQ_MAX_CHARS]
        cut = chunk.rfind("\n")
        if cut <= 0:
            cut = chunk.rfind(" ")
        if cut <= 0:
            cut = ICQ_MAX_CHARS
        parts.append(chunk[:cut].rstrip())
        text = text[cut:].lstrip()
    return [p for p in parts if p]



CLI_META_REQINFO_TYPE    = 0x04D0   # запрос basic info (не работает, по Jimm)
CLI_META_REQMOREINFO_TYPE= 0x04B2   # запрос полной анкеты (по Jimm: CLI_META_REQMOREINFO_TYPE)
CLI_META_REQWORKINFO_TYPE= 0x04BA   # запрос work info
CLI_META_REQABOUT_TYPE   = 0x04C8   # запрос about/notes
CLI_SET_FULLINFO_TYPE    = 0x0C3A   # сохранение анкеты (SaveInfoAction)

SRV_META_GENERAL_TYPE    = 0x00C8   # ответ: basic info
SRV_META_MORE_TYPE       = 0x00DC   # ответ: more info
SRV_META_WORK_TYPE       = 0x00D2   # ответ: work info
SRV_META_ABOUT_TYPE      = 0x00E6   # ответ: about
SRV_META_END_TYPE        = 0x00FA   # конец серии

SEARCH_TLV_UIN         = 0x3601
SEARCH_TLV_NICK        = 0x5401
SEARCH_TLV_FIRSTNAME   = 0x4001
SEARCH_TLV_LASTNAME    = 0x4A01
SEARCH_TLV_EMAIL       = 0x5E01
SEARCH_TLV_CITY        = 0x9001
SEARCH_TLV_KEYWORD     = 0x2602
SEARCH_TLV_GENDER      = 0x7C01
SEARCH_TLV_ONLYONLINE  = 0x3002

SAVE_TLV_NICK      = 0x0154
SAVE_TLV_FIRSTNAME = 0x0140
SAVE_TLV_LASTNAME  = 0x014A
SAVE_TLV_EMAIL     = 0x015E
SAVE_TLV_BDAY      = 0x023A
SAVE_TLV_CITY      = 0x0190
SAVE_TLV_HOME_PAGE = 0x0213
SAVE_TLV_ABOUT     = 0x0258
SAVE_TLV_GENDER    = 0x017C
# SAVE_TLV_AUTH_REQUIRED не используется: auth управляется через META_INFO_SET_PERMS (0x0424)

# Смена пароля — META_INFO_SET_PASSWORD (по EditInfo.java из Jimm и
# mr_set_user_pass_info из iserverd, isdcore/v7_proto/snac_families/sn15_ext_messages.cpp).
# Jimm пишет подкоманду как Util.putWord(buf, 0, 0x2e04) big-endian (байты 2E 04),
# что при чтении сервером как LE-слово даёт 0x042E — это и есть META_INFO_SET_PASSWORD
# из v7_defines.h iserverd. Тело — НЕ TLV-цепочка, а «сырая» строка формата
# v7_extract_string: LE(length:2) + сами байты пароля, без нуль-терминатора
# (max_len=32, char password[33] на сервере).
META_INFO_SET_PASSWORD = 0x042E
META_INFO_PASS_ACK     = 0x00AA   # ack для set password packet (v7_defines.h)

CLI_ROSTERADD_CMD    = 0x0008   # добавить запись SSI
CLI_ROSTERDELETE_CMD = 0x000A   # удалить запись SSI
SRV_UPDATEACK_CMD    = 0x000E   # подтверждение от сервера
CLI_ADDSTART_CMD     = 0x0011   # начало пакетного изменения SSI
CLI_ADDEND_CMD       = 0x0012   # конец пакетного изменения SSI
CLI_ROSTERUPDATE_CMD = 0x0009   # обновление записи SSI

META_REQ_OFFLINE_MSG  = 0x003C  # запросить оффлайн-очередь
META_ACK_OFFLINE_MSG  = 0x003E  # подтвердить получение (удалить с сервера)
OFFLINE_MSG_RESPONSE  = 0x0041  # одно оффлайн-сообщение
OFFLINE_MSG_EOF       = 0x0042  # конец очереди

SSI_AUTH_SEND_REQ   = 0x0018   # c→s: отправить запрос авторизации
SSI_AUTH_REQ        = 0x0019   # s→c: входящий запрос авторизации
SSI_AUTH_SEND_REPLY = 0x001A   # c→s: ответ на запрос (grant/deny)
SSI_AUTH_REPLY      = 0x001B   # s→c: ответ на наш запрос (granted/denied)
SSI_YOU_WERE_ADDED  = 0x001C   # s→c: тебя добавили в список


def _read_asciiz_le(data: bytes, pos: int) -> Tuple[str, int]:
    """
    Читает строку в формате ICQ-метасервера: LE(2) длина + байты + null.
    Возвращает (строка, новая_позиция).
    """
    if pos + 2 > len(data):
        return "", pos
    length = struct.unpack_from("<H", data, pos)[0]
    pos += 2
    if length == 0:
        return "", pos
    raw = data[pos:pos + length]
    pos += length
    text = raw.rstrip(b"\x00")
    for enc in ("utf-8", "cp1251", "latin-1"):
        try:
            return text.decode(enc), pos
        except UnicodeDecodeError:
            continue
    return text.decode("latin-1", errors="replace"), pos


def _make_email_tlv_le(email: str, is_hidden: bool = False) -> bytes:
    try:
        encoded = email.encode("ascii")
    except UnicodeEncodeError:
        encoded = email.encode("utf-8")
    str_len = len(encoded) + 1          # +1 за \x00
    flag = 0x01 if is_hidden else 0x00
    value = struct.pack("<H", str_len) + encoded + b"\x00" + struct.pack("B", flag)
    return struct.pack("<HH", SAVE_TLV_EMAIL, len(value)) + value


def _make_empty_email_tlv_le() -> bytes:
    """Пустой email-слот: str_len=1, email="", \x00, flag=0x01"""
    value = struct.pack("<H", 1) + b"\x00" + struct.pack("B", 0x01)
    return struct.pack("<HH", SAVE_TLV_EMAIL, len(value)) + value


def _make_asciiz_tlv_le(tlv_id: int, text: str) -> bytes:
    """
    Строит строковый TLV в LE-формате для ICQ meta-сервиса.
    Строго по Util.writeAsciizTLV из Jimm:
      LE(type:2) + LE(raw.length+3:2) + LE(raw.length+1:2) + raw_bytes + 0x00
    Внешняя длина TLV = inner_len(2) + data + null = raw+3
    Внутренняя длина  = raw + null = raw+1
    """
    raw = (text or "").encode("utf-8")
    outer_len = len(raw) + 3   # inner_len_field(2) + raw + null
    inner_len = len(raw) + 1   # raw + null
    return struct.pack("<HH", tlv_id, outer_len) + struct.pack("<H", inner_len) + raw + b"\x00"


def _make_meta_snac(my_uin: str, subtype: int,
                    data: bytes = b"", seq_id: int = 0) -> bytes:
    """
    Строит payload SNAC 0x15/0x02 (CLI_TOICQSRV) — обёртка ICQ meta-сервиса.

    Структура строго по ToIcqSrvPacket.java (без extData, т.е. стандартная ветка):
      TLV(0x0001):
        LE(length : 2)     = 8 + len(data)   ← "8 + this.data.length"
        LE(uin : 4)
        LE(0x07D0 : 2)     ← CLI_META_SUBCMD
        LE(seq_id : 2)     ← icqSequence
        LE(subtype : 2)    ← тип подкоманды (это первые 2 байта data в Jimm)
        <data>             ← остальные данные подкоманды
    """
    uin_int = int(my_uin)
    inner = bytearray()
    inner += struct.pack("<I", uin_int)   # UIN (LE, 4 байта)
    inner += struct.pack("<H", 0x07D0)   # CLI_META_SUBCMD (LE)
    inner += struct.pack("<H", seq_id)   # icqSequence (LE)
    inner += struct.pack("<H", subtype)  # subcommand type (LE) — первые 2 байта data
    inner += data                        # остальные байты подкоманды

    this_data_len = 2 + len(data)  # subtype(2) + payload
    length_field = 8 + this_data_len

    tlv_body = struct.pack("<H", length_field) + bytes(inner)
    return _make_tlv(0x0001, tlv_body)


def _make_offline_req_snac(my_uin: str, req_cmd: int,
                           extra: bytes = b"") -> bytes:
    """
    Строит payload SNAC 0x15/0x02 для запросов оффлайн-очереди верхнего
    уровня (META_REQ_OFFLINE_MSG / META_ACK_OFFLINE_MSG).

    В отличие от _make_meta_snac (который всегда оборачивает данные в
    req_cmd=META_REQ_INFORMATION/0x07D0 — это подходит только для
    подкоманд META_INFO_*), здесь req_cmd пишется напрямую, как этого
    ожидает process_ime_multi_req на сервере (sn15_ext_messages.cpp):

      TLV(0x0001):
        LE(remaining_size:2) = 4(uin) + 2(req_cmd) + len(extra)
        LE(uin:4)
        LE(req_cmd:2)        ← META_REQ_OFFLINE_MSG (0x003C) или
                                META_ACK_OFFLINE_MSG (0x003E)
        <extra>               ← LE(req_seq:2) для META_REQ_OFFLINE_MSG,
                                ничего для META_ACK_OFFLINE_MSG
    """
    uin_int = int(my_uin)
    body = struct.pack("<I", uin_int) + struct.pack("<H", req_cmd) + extra
    tlv_body = struct.pack("<H", len(body)) + body
    return _make_tlv(0x0001, tlv_body)


def _read_search_string(data: bytes, pos: int) -> Tuple[str, int]:
    """
    Читает строку из результата поиска по SearchAction.java:
      LE(len:2) + bytes (без null-терминатора — в отличие от asciiz!).
    Jimm: Util.byteArrayToString(data, marker+2, getWord(data,marker,false))
    """
    if pos + 2 > len(data):
        return "", pos
    length = struct.unpack_from("<H", data, pos)[0]
    pos += 2
    if length == 0:
        return "", pos
    raw = data[pos:pos + length]
    pos += length
    raw = raw.rstrip(b"\x00")
    for enc in ("utf-8", "cp1251", "latin-1"):
        try:
            return raw.decode(enc), pos
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace"), pos


def _parse_search_result(data: bytes) -> Optional["SearchResult"]:
    """
    Разбирает одну запись результата поиска (из SRV_FROMICQSRV ответа).
    Структура строго по SearchAction.java (marker — относительно data):
      LE(uin:4) + LE(skip:2) + строки*4 (LE_len+bytes без null):
        nick, first, last, email
      + BYTE(auth) + LE(status:2) + BYTE(gender) + LE(age:2)

    Jimm после success делает marker+=3 (пропускает success(1)+2 байта),
    затем читает UIN. То есть перед UIN идут 2 неизвестных байта (skip).
    """
    try:
        pos = 0
        pos += 2
        if pos + 4 > len(data):
            return None
        uin = struct.unpack_from("<I", data, pos)[0]; pos += 4

        strings: List[str] = []
        for _ in range(4):
            s, pos = _read_search_string(data, pos)
            strings.append(s)

        if pos >= len(data):
            return None

        auth_byte = data[pos]; pos += 1
        auth_req = (auth_byte == 0)

        status = 0
        if pos + 2 <= len(data):
            status = struct.unpack_from("<H", data, pos)[0]; pos += 2

        gender = ""
        if pos < len(data):
            g = data[pos]; pos += 1
            gender = {1: "F", 2: "M"}.get(g, "")

        age = 0
        if pos + 2 <= len(data):
            age = struct.unpack_from("<H", data, pos)[0]

        online = (status != 0x0000)

        return SearchResult(
            uin=str(uin),
            nick=strings[0],
            first_name=strings[1],
            last_name=strings[2],
            email=strings[3],
            auth_req=auth_req,
            online=online,
            gender=gender,
            age=age,
        )
    except Exception:
        return None



def _decode_ssi_nick(raw: bytes) -> str:
    if not raw:
        return ""
    for enc in ("utf-8", "cp1251", "latin-1"):
        try:
            return raw.decode(enc).strip("\x00").strip()
        except UnicodeDecodeError:
            continue
    return ""


def _parse_ssi(data: bytes) -> Tuple[List[Group], List[Contact]]:
    groups:   List[Group]   = []
    contacts: List[Contact] = []

    if len(data) < 3:
        return groups, contacts

    pos = 1
    if pos + 2 > len(data):
        return groups, contacts
    item_count = struct.unpack_from("!H", data, pos)[0]
    pos += 2

    for _ in range(item_count):
        if pos + 10 > len(data):
            break
        name_len  = struct.unpack_from("!H", data, pos)[0]; pos += 2
        name      = data[pos:pos+name_len].decode("utf-8", errors="ignore"); pos += name_len
        group_id  = struct.unpack_from("!H", data, pos)[0]; pos += 2
        item_id   = struct.unpack_from("!H", data, pos)[0]; pos += 2
        item_type = struct.unpack_from("!H", data, pos)[0]; pos += 2
        tlv_len   = struct.unpack_from("!H", data, pos)[0]; pos += 2
        tlv_data  = data[pos:pos+tlv_len]; pos += tlv_len
        tlvs      = _parse_tlvs(tlv_data)

        if item_type == 0x0001:
            if not name and group_id == 0 and item_id == 0:
                continue
            groups.append(Group(group_id=group_id, name=name or f"Group{group_id}"))

        elif item_type == 0x0000:
            nick = _decode_ssi_nick(tlvs.get(0x0131, b""))
            pending_auth = (0x0066 in tlvs)
            contacts.append(Contact(uin=name, name=nick,
                                    group_id=group_id, item_id=item_id,
                                    pending_auth=pending_auth))

    return groups, contacts



def _parse_tlv001d(data: bytes) -> Tuple[str, str]:
    xstatus_name = ""
    xstatus_msg  = ""
    ps  = 0
    ln  = len(data)

    while ps < ln - 1:
        if ps + 3 > ln:
            break
        ext_tlv = struct.unpack_from("!H", data, ps)[0]; ps += 3
        if ps >= ln:
            break
        ext_len = data[ps]
        idx = ps

        if ext_len > 0:
            if ext_tlv == 0x000E:
                raw = data[idx+1:idx+1+ext_len]
                try:
                    mood_str = raw.decode("ascii", errors="ignore").strip("\x00")
                    if mood_str.startswith("icqmood"):
                        idx_str = mood_str[7:]
                        mood_idx = int(idx_str)
                        if 0 <= mood_idx < len(XSTATUS_TABLE):
                            xstatus_name = XSTATUS_TABLE[mood_idx][1]
                except (ValueError, IndexError):
                    pass

            elif ext_tlv == 0x0002:
                try:
                    text_pos = idx + 1
                    if text_pos + 2 <= ln:
                        text_len = struct.unpack_from("!H", data, text_pos)[0]
                        text_pos += 2
                        if text_pos + text_len <= ln:
                            raw_text = data[text_pos:text_pos+text_len]
                            xstatus_msg = _decode_text(raw_text)
                except Exception:
                    pass

        ps += ext_len + 1

    return xstatus_name, xstatus_msg



def _extract_ch1_text(data: bytes, offset: int) -> Optional[str]:
    while offset + 4 <= len(data):
        tlv_type, tlv_len = struct.unpack_from("!HH", data, offset); offset += 4
        if offset + tlv_len > len(data):
            break
        value = data[offset:offset+tlv_len]; offset += tlv_len
        if tlv_type != 0x0002 or len(value) < 4:
            continue
        inner = 0
        while inner + 4 <= len(value):
            it, il = struct.unpack_from("!HH", value, inner); inner += 4
            if inner + il > len(value):
                break
            iv = value[inner:inner+il]; inner += il
            if it == 0x0101 and len(iv) >= 4:
                charset = struct.unpack_from("!H", iv, 0)[0]
                text_data = iv[4:]
                if charset == 2:
                    return text_data.decode("utf-16-be", errors="replace").strip("\x00").strip()
                return _decode_text(text_data) or None
    return None


def _extract_ch2_text(data: bytes) -> Optional[str]:
    try:
        pos = 10 + 8 + 2
        uin_len = data[pos]; pos += 1 + uin_len + 4

        def tlvs_be(d):
            out, p = {}, 0
            while p + 4 <= len(d):
                t, l = struct.unpack_from(">HH", d, p)
                if p + 4 + l > len(d): break
                out[t] = d[p+4:p+4+l]; p += 4 + l
            return out

        outer  = tlvs_be(data[pos:])
        raw5   = outer.get(0x0005)
        if not raw5 or len(raw5) < 26: return None
        if struct.unpack_from(">H", raw5, 0)[0] != 0: return None
        sub    = tlvs_be(raw5[26:])
        r2711  = sub.get(0x2711)
        if not r2711 or len(r2711) < 36: return None
        if r2711[4:20] != b"\x00"*16: return None
        p = 36
        while p + 4 <= len(r2711):
            t = struct.unpack_from("<H", r2711, p)[0]
            l = struct.unpack_from("<H", r2711, p+2)[0]
            if p + 4 + l > len(r2711): p += 1; continue
            if t == 0x0001 and l > 0:
                text = _decode_text(r2711[p+4:p+4+l])
                if text: return text
            p += 4 + l
    except Exception:
        pass
    return None



def _xml_unescape(text: str) -> str:
    return (text
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&")
            .replace("&quot;", '"')
            .replace("&apos;", "'"))


def _xml_escape(text: str) -> str:
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))



def _parse_xtraz_request(text: str) -> Optional[str]:
    """
    Парсит входящий xTraz-запрос (уже декодированный текст).
    Возвращает UIN отправителя если это запрос xStatus (srvmng/cAwaySrv).

    Алгоритм точно по XtrazSM.a(String s, String s1):
        1. Найти <QUERY>...</QUERY> и <NOTIFY>...</NOTIFY>
        2. DeMangleXml(query) → проверить <PluginID>srvMng</PluginID>
        3. DeMangleXml(notify) → проверить наличие "AwayStat"
        4. Извлечь <senderId>
    """
    try:
        qi = text.find("<QUERY>")
        qi_end = text.find("</QUERY>")
        ni = text.find("<NOTIFY>")
        ni_end = text.find("</NOTIFY>")

        if qi < 0 or qi_end < 0 or ni < 0 or ni_end < 0:
            return None

        query_raw  = text[qi + 7 : qi_end]
        notify_raw = text[ni + 8 : ni_end]

        query  = _xml_unescape(query_raw)
        notify = _xml_unescape(notify_raw)

        plugin_m = re.search(r"<PluginID>\s*([^<]+)\s*</PluginID>", query, re.I)
        if not plugin_m or plugin_m.group(1).strip().lower() != "srvmng":
            return None

        if "AwayStat" not in notify and "awayssrv" not in notify.lower():
            return None

        sender_m = re.search(r"<senderId>\s*([^<]+)\s*</senderId>", notify)
        if not sender_m:
            return None

        return sender_m.group(1).strip()
    except Exception:
        return None


def _parse_xtraz_response(text: str) -> Optional[Tuple[str, str]]:
    """
    Парсит xTraz-ответ из строки (уже декодированный текст).
    Возвращает (title, desc) или None.

    Алгоритм точно по XtrazSM.b(String s, String s1):
        1. Найти <NR><RES>...</RES></NR>
        2. DeMangleXml(внутреннее) → найти <val srv_id=
        3. Извлечь <title> и <desc>
    """
    try:
        nr_start = text.find("<NR><RES>")
        nr_end   = text.find("</RES></NR>")
        if nr_start < 0 or nr_end < 0:
            return None

        inner_raw = text[nr_start + 9 : nr_end]

        inner = _xml_unescape(inner_raw)

        if "<val srv_id=" not in inner and "val srv_id=" not in inner:
            return None
        if "CASXtraSetAwayMessage" not in inner:
            return None

        title_m = re.search(r"<title>(.*?)</title>", inner, re.DOTALL)
        desc_m  = re.search(r"<desc>(.*?)</desc>",   inner, re.DOTALL)

        title = title_m.group(1).strip() if title_m else ""
        desc  = desc_m.group(1).strip()  if desc_m  else ""

        if not title and not desc:
            return None

        return title, desc
    except Exception:
        return None



def _parse_xtraz_request_bytes(data: bytes) -> Optional[str]:
    """
    Парсит входящий xTraz-запрос (bytes, duck-вариант).
    Возвращает UIN отправителя если это запрос xStatus, иначе None.
    """
    try:
        text = data.decode("utf-8", errors="ignore")
        text = _xml_unescape(text)

        if "<QUERY>" not in text or "<NOTIFY>" not in text:
            return None

        plugin_m = re.search(r"<PluginID>\s*([^<]+)\s*</PluginID>", text, re.I)
        if not plugin_m or plugin_m.group(1).strip().lower() != "srvmng":
            return None

        srv_id_m = re.search(r"<id>\s*([^<]+)\s*</id>", text, re.I)
        if not srv_id_m or srv_id_m.group(1).strip().lower() != "cawayssrv".lower():
            if "cAwaySrv" not in text and "cawayssrv" not in text.lower():
                return None

        sender_m = re.search(r"<senderId>\s*([^<]+)\s*</senderId>", text)
        if not sender_m:
            return None

        return sender_m.group(1).strip()
    except Exception:
        return None


def _parse_xtraz_response_bytes(data: bytes) -> Optional[Tuple[str, str]]:
    """
    Парсит входящий xTraz-ответ (bytes, duck-вариант).
    Возвращает (title, desc) или None.
    """
    try:
        text = data.decode("utf-8", errors="ignore")

        text = _xml_unescape(text)
        if "&lt;" in text or "&gt;" in text:
            text = _xml_unescape(text)

        if "CASXtraSetAwayMessage" not in text:
            return None
        if "OnRemoteNotification" not in text:
            return None

        title_m = re.search(r"<title>(.*?)</title>", text, re.DOTALL)
        desc_m  = re.search(r"<desc>(.*?)</desc>",  text, re.DOTALL)

        title = title_m.group(1).strip() if title_m else ""
        desc  = desc_m.group(1).strip()  if desc_m  else ""

        if title in ("", " ") and desc in ("", " "):
            return None

        return title, desc
    except Exception:
        return None



def _extract_xtraz_xml_from_2711(data_2711: bytes) -> Optional[str]:
    """
    Извлекает XML из TLV 0x2711 (содержимое из SNAC 4/7 channel 2).

    Структура TLV 0x2711 (Jimm ActionListener):
      Пропускаем: LE(2) + BYTE(1) + 16×0 + DWORD_LE(4) + DWORD_LE(4)
                  + LE(2) + LE(2) + LE(2) + 12×0
                = 2+1+16+4+4+2+2+2+12 = 45 байт
      Потом: BYTE msgType (LE word = 2 байта), пропустить +2+2
             (status + priority)
      Потом: LE textLen + text (plugin name)
      Потом смотрим msgType:
        если 26 (0x1A): пропустить ещё 3(2+1) + j7(2) + 18(16+2) +
                         DWORD_LE(4 = pluginLen) + plugin(42) +
                         15(4+4+4+2+1) + 8(4+4) → весь хвост = XML

    Для надёжности — ищем маркер "Script Plug-in: Remote Notification Arrive"
    и разбираем смещения строго по ActionListener.java.
    """
    try:
        marker = data_2711.find(b"Script Plug-in: Remote Notification Arrive")
        if marker < 0:
            return None

        pos = marker + 42 + 15 + 8

        if pos >= len(data_2711):
            return None

        raw = data_2711[pos:]

        for enc in ("utf-8", "cp1251", "latin-1"):
            try:
                return raw.decode(enc).strip("\x00").strip()
            except UnicodeDecodeError:
                continue
        return None
    except Exception:
        return None


def _extract_xtraz_xml_from_relay(data: bytes) -> Optional[str]:
    """
    Извлекает XML из SNAC 4/11 (server relay / CLI_ACKMSG).

    Алгоритм строго по ActionListener.java (CLI_ACKMSG_COMMAND):
      cookie(8) + channel(2) + uin_len(1) + uin(N) + fixed(47) → msgType(LE) + ...
      если msgType == 26 и plugin == "Script Plug-in: Remote Notification Arrive":
        xml = tail

    Fallback: если строгий разбор не сработал — маркерный поиск по всему пакету.
    """
    try:
        pos = 10 + 8
        pos += 2

        j5 = data[pos]; pos += 1
        pos += j5

        pos += 47

        if pos + 2 > len(data):
            raise ValueError("too short after fixed block")

        msg_type = struct.unpack_from("<H", data, pos)[0]
        pos += 6

        if msg_type != 26:
            raise ValueError(f"msgType={msg_type}, not 26")

        pos += 3

        if pos + 2 > len(data):
            raise ValueError("too short before j7")
        j7 = struct.unpack_from("<H", data, pos)[0]; pos += 2
        if j7 != 79:
            raise ValueError(f"j7={j7}, not 79")

        pos += 18

        if pos + 4 > len(data):
            raise ValueError("too short before plugin_len")
        plugin_len = struct.unpack_from("<I", data, pos)[0]; pos += 4

        if pos + plugin_len > len(data):
            raise ValueError("plugin_len out of bounds")
        plugin_name = data[pos:pos+plugin_len].decode("ascii", errors="ignore")
        pos += plugin_len

        pos += 15

        if plugin_name.strip("\x00") != "Script Plug-in: Remote Notification Arrive":
            raise ValueError(f"unexpected plugin: {plugin_name!r}")

        pos += 8

        if pos >= len(data):
            raise ValueError("no xml tail")

        raw = data[pos:]
        for enc in ("utf-8", "cp1251", "latin-1"):
            try:
                return raw.decode(enc).strip("\x00").strip()
            except UnicodeDecodeError:
                continue
        return None

    except Exception:
        try:
            marker = b"Script Plug-in: Remote Notification Arrive"
            idx = data.find(marker)
            if idx < 0:
                return None
            pos = idx + len(marker) + 15 + 8
            if pos >= len(data):
                return None
            raw = data[pos:]
            for enc in ("utf-8", "cp1251", "latin-1"):
                try:
                    return raw.decode(enc).strip("\x00").strip()
                except UnicodeDecodeError:
                    continue
        except Exception:
            pass
        return None



class ICQClient:
    """
    Асинхронный ICQ/OSCAR клиент.

    Колбэки (устанавливаются снаружи):
        on_connected()
        on_disconnected()
        on_roster(groups: List[Group], contacts: List[Contact])
        on_contact_online(contact: Contact)
        on_contact_offline(contact: Contact)
        on_contact_status(contact: Contact)
        on_message(message: Message)
        on_typing(uin: str, is_typing: bool)
        on_error(exc: Exception)
        on_xstatus_updated(contact: Contact)
        on_my_info(info: UserInfo)          ← своя анкета получена/обновлена
        on_user_info(info: UserInfo)        ← анкета другого пользователя
        on_search_result(result: SearchResult)  ← одна запись в процессе поиска
        on_search_done(results: List[SearchResult])  ← поиск завершён
        on_offline_message(message: Message)    ← оффлайн-сообщение из очереди
        on_auth_request(uin: str, message: str) ← кто-то просит авторизацию
        on_auth_reply(uin: str, granted: bool, message: str)  ← ответ на наш запрос
        on_you_were_added(uin: str)             ← тебя добавили в список

    Публичный API:
        await client.request_my_info()               → заполняет my_info / my_nick
        await client.request_user_info(uin)
        await client.save_my_info(info: UserInfo) → bool
        await client.search_users(uin/nick/email/…) → List[SearchResult]
        await client.add_contact(uin, nick, group_id) → bool
        await client.remove_contact(uin)              → bool
        await client.send_message(to_uin, text)
        await client.send_typing(to_uin, is_typing)
        await client.set_status(status, message)
        await client.set_xstatus(name, title, desc)
        await client.request_xstatus(to_uin)

    Атрибуты:
        client.my_nick  — никнейм, сохраняется после request_my_info() / save_my_info()
        client.my_info  — UserInfo собственной анкеты
        client.contacts — Dict[uin, Contact]
        client.groups   — Dict[group_id, Group]
    """

    def __init__(self, uin: str, password: str,
                 server: str = SERVER, port: int = PORT):
        self.uin      = uin
        self.password = password
        self.server   = server
        self.port     = port

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._seq    = 0
        self._cookie: Optional[bytes] = None
        self._running           = False
        self._stop_requested    = False
        self._intentional_stop  = False

        self.groups:   Dict[int, Group]   = {}
        self.contacts: Dict[str, Contact] = {}

        self._my_status:        Status = Status.FREE
        self._my_status_msg:    str    = ""
        self._my_xstatus_name:  str    = ""
        self._my_xstatus_guid:  bytes  = b""
        self._my_xstatus_title: str    = ""
        self._my_xstatus_desc:  str    = ""

        self.my_nick: str      = ""
        self.my_info: Optional[UserInfo] = None

        self._pending_info:   Dict[int, Tuple[str, UserInfo, Set[int]]] = {}
        self._pending_save:   Dict[int, asyncio.Future] = {}
        self._pending_search: Dict[int, List[SearchResult]] = {}
        self._search_futures: Dict[int, asyncio.Future] = {}
        self._meta_seq: int = 0

        self._pending_ssi_ack: Dict[int, asyncio.Future] = {}
        self._client_cache: Dict[str, str] = {}
        self._message_tasks: Set[asyncio.Task] = set()

        self.on_connected:       Optional[Callable] = None
        self.on_disconnected:    Optional[Callable] = None
        self.on_reconnecting:    Optional[Callable] = None
        self.on_roster:          Optional[Callable] = None
        self.on_contact_online:  Optional[Callable] = None
        self.on_contact_offline: Optional[Callable] = None
        self.on_contact_status:  Optional[Callable] = None
        self.on_message:         Optional[Callable] = None
        self.on_typing:          Optional[Callable] = None
        self.on_error:           Optional[Callable] = None
        self.on_xstatus_updated: Optional[Callable] = None
        self.on_user_info:       Optional[Callable] = None
        self.on_my_info:         Optional[Callable] = None
        self.on_search_result:   Optional[Callable] = None
        self.on_search_done:     Optional[Callable] = None
        self.on_offline_message: Optional[Callable] = None
        self.on_auth_request:    Optional[Callable] = None
        self.on_auth_reply:      Optional[Callable] = None
        self.on_you_were_added:  Optional[Callable] = None
        self.on_password_changed: Optional[Callable] = None


    async def set_status(self, status: Status, message: str = ""):
        self._my_status     = status
        self._my_status_msg = message
        if self._running:
            await self._apply_status()
            log.info(f"Status changed to {status.label}: {message}")

    async def set_xstatus(self, name: str, title: str = "", desc: str = ""):
        name_lower = name.lower()
        guid_hex = XSTATUS_BY_NAME.get(name_lower, "")
        if not guid_hex:
            log.warning(f"Unknown xStatus name: {name}")
            return
        self._my_xstatus_name  = name_lower
        self._my_xstatus_guid  = bytes.fromhex(guid_hex)
        self._my_xstatus_title = title
        self._my_xstatus_desc  = desc
        if self._running:
            await self._send_cli_setuserinfo()
            log.info(f"xStatus set to {name_lower}: '{title}' - '{desc}'")

    async def send_message(self, to_uin: str, text: str):
        if not text.strip():
            return
        parts = _split_text(text)
        for i, part in enumerate(parts):
            encoded = part.encode("utf-16-be")
            msg_tlv = struct.pack("!HHI", 0x0101, len(encoded)+4, 0x00020000) + encoded
            payload = (struct.pack("!Q", int(time.time()))
                       + struct.pack("!H", 1)
                       + struct.pack("!B", len(to_uin)) + to_uin.encode("ascii")
                       + struct.pack("!HH", 0x0002, len(msg_tlv)) + msg_tlv)
            await self._send_snac(0x0004, 0x0006, payload)
            log.info(f"→ {to_uin} [{i+1}]: {part[:80]}")
            if i < len(parts) - 1:
                await asyncio.sleep(0.5)

    async def send_typing(self, to_uin: str, is_typing: bool = True):
        uin_b = to_uin.encode("ascii")
        payload = (b"\x00"*8 + b"\x00\x01"
                   + struct.pack("!B", len(uin_b)) + uin_b
                   + struct.pack("!H", 0x0002 if is_typing else 0x0000))
        await self._send_snac(0x0004, 0x0014, payload, reqid=0)

    def get_contact(self, uin: str) -> Optional[Contact]:
        return self.contacts.get(uin)

    def get_online_contacts(self) -> List[Contact]:
        return [c for c in self.contacts.values() if c.is_online]


    async def request_offline_messages(self):
        """
        Запрашивает оффлайн-очередь (SNAC 0x15/0x02, subtype 0x003C).

        Сервер пришлёт пачку OFFLINE_MSG_RESPONSE (0x0041), каждый из которых
        будет доставлен в колбэк on_offline_message(Message).
        После последнего придёт OFFLINE_MSG_EOF (0x0042) — мы автоматически
        подтвердим получение (META_ACK_OFFLINE_MSG 0x003E), чтобы сервер
        удалил очередь.

        Обычно вызывается один раз сразу после on_connected / on_roster.
        """
        seq_id = self._next_meta_seq()
        extra = struct.pack("<H", seq_id)
        payload = _make_offline_req_snac(self.uin, META_REQ_OFFLINE_MSG, extra=extra)
        await self._send_snac(0x0015, 0x0002, payload)
        log.debug(f"request_offline_messages: seq={seq_id}")


    async def send_auth_request(self, to_uin: str, message: str = ""):
        """
        SNAC 13/18: BYTE(uin_len) + uin + WORD(reason_len) + reason + WORD(chs_flg=0)

        Структура по process_ssi_auth_req на сервере (iserverd):
          to_uin  = read_buin(pack)          — BYTE(len) + uin
          reason  = v7_extract_string(pack)  — WORD(len) + string
          chs_flg = pack >> WORD             — всегда читается, 0 = нет charset

        WORD(0x0000) в конце обязателен — без него сервер может не пробросить
        запрос целевому пользователю.
        """
        uin_b = to_uin.encode("ascii")
        msg_b = message.encode("utf-8")
        payload = (struct.pack("!B", len(uin_b)) + uin_b
                 + struct.pack("!H", len(msg_b)) + msg_b
                 + struct.pack("!H", 0x0000))  # chs_flg = 0
        await self._send_snac(0x0013, SSI_AUTH_SEND_REQ, payload)
        log.info(f"send_auth_request → {to_uin}")
        

    async def send_auth_reply(self, to_uin: str, granted: bool, message: str = ""):
        """
        SNAC 13/1A: BYTE(uin_len) + uin + BYTE(flag) + WORD(msg_len) + msg + WORD(charset_flag)

        Структура по process_ssi_auth_rep на сервере (iserverd):
          read_buin()          — BYTE(uin_len) + uin
          pack >> auth_state   — BYTE: 1=grant, 0=deny
          v7_extract_string()  — WORD(len) + string (reason/message)
          pack >> chs_flg      — WORD: charset flag (0 = нет charset)

        WORD(0x0000) в конце обязателен — сервер всегда читает charset_flag,
        и если пакет обрывается раньше, grant_ssi_authorization не вызывается.
        """
        uin_b = to_uin.encode("ascii")
        msg_b = message.encode("utf-8")
        auth_byte = 0x01 if granted else 0x00
        payload = (struct.pack("!B", len(uin_b)) + uin_b
                 + struct.pack("!B", auth_byte)
                 + struct.pack("!H", len(msg_b)) + msg_b
                 + struct.pack("!H", 0x0000))  # charset_flag = 0
        await self._send_snac(0x0013, SSI_AUTH_SEND_REPLY, payload)
        log.info(f"send_auth_reply → {to_uin}: {'granted' if granted else 'denied'}")


    async def set_require_auth(self, require: bool) -> bool:
        """
        Включает/выключает требование авторизации для своего аккаунта.

        Использует META_INFO_SET_PERMS (подкоманда 0x0424) SNAC 0x15/0x02.
        Сервер (iserverd) читает три байта подряд из TLV-тела:
          BYTE auth      — 0=требовать авторизацию, 1=не требовать (инвертировано!)
          BYTE webaware  — сохраняем текущее значение из my_info
          BYTE dc_perms  — сохраняем текущее значение (1 = только из списка контактов)

        Значение auth=0 означает «требовать авторизацию» (userinfo.auth != 1 → ошибка).
        """
        META_INFO_SET_PERMS = 0x0424
        META_INFO_PERMS_ACK = 0x00A0

        # auth=0 → требовать авторизацию; auth=1 → не требовать
        auth_byte    = 0 if require else 1
        webaware     = 0
        dc_perms     = 1
        if self.my_info:
            # Сохраняем остальные флаги если анкета уже загружена
            pass  # webaware/dc_perms не хранятся в UserInfo, используем дефолты

        body = struct.pack("BBB", auth_byte, webaware, dc_perms)

        seq = self._next_meta_seq()
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_save[seq] = fut

        payload = _make_meta_snac(self.uin, META_INFO_SET_PERMS, body, seq_id=seq)
        await self._send_snac(0x0015, 0x0002, payload)
        log.info(f"set_require_auth({require}): sending SET_PERMS, auth_byte={auth_byte}")

        try:
            result = await asyncio.wait_for(fut, timeout=10.0)
            if result and self.my_info:
                self.my_info.auth_required = require
            log.info(f"set_require_auth({require}) -> {'ok' if result else 'FAILED'}")
            return result
        except asyncio.TimeoutError:
            self._pending_save.pop(seq, None)
            log.warning("set_require_auth: timeout")
            return False


    async def change_password(self, new_password: str,
                               current_password: Optional[str] = None) -> bool:
        """
        Меняет пароль аккаунта на ICQ-сервере.

        Использует META_INFO_SET_PASSWORD (подкоманда 0x042E) SNAC 0x15/0x02 —
        строго по EditInfo.java (Jimm, обработчик команды _CmdChange) и
        mr_set_user_pass_info (iserverd, sn15_ext_messages.cpp).

        В отличие от save_my_info/set_require_auth, тело запроса — это НЕ
        TLV-цепочка (SAVE_TLV_*), а «сырая» строка в формате v7_extract_string,
        который использует сервер для пароля:
            LE(length : 2) + password_bytes   (без нуль-терминатора)
        Сервер хранит пароль в char[33] и ограничивает длину 32 байтами
        (см. v7_extract_string(password, tlv, sizeof(password)-1, ...)).

        current_password — необязательная клиентская проверка (как в Jimm,
        где смена блокируется, если введённый "текущий" пароль не совпадает
        с сохранённым). Сервер сам текущий пароль не запрашивает и не проверяет,
        поэтому эта сверка — только защита от опечаток на стороне клиента.

        Возвращает True, если сервер подтвердил смену (META_INFO_PASS_ACK,
        success=0x0A). При успехе обновляет self.password, чтобы последующие
        переподключения (в т.ч. автоматические) использовали новый пароль.

        Дополнительно вызывает колбэк on_password_changed(success: bool) —
        и при успехе/ошибке от сервера, и при таймауте (success=False).
        Использовать можно и то, и другое: await client.change_password(...)
        для получения результата в месте вызова, и/или колбэк, если решение
        принимается где-то ещё в асинхронном клиенте.
        """
        if current_password is not None and current_password != self.password:
            log.warning("change_password: указанный текущий пароль не совпадает, отмена")
            return False

        pw_bytes = new_password.encode("utf-8")
        if not (1 <= len(pw_bytes) <= 32):
            log.warning("change_password: пароль должен быть от 1 до 32 байт")
            return False

        seq  = self._next_meta_seq()
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_save[seq] = fut

        # LE(length:2) + пароль + нуль-терминатор (как в Jimm; сервер его не
        # читает, т.к. v7_extract_string сама останавливается по length, но
        # он безвреден и повышает совместимость с другими реализациями сервера).
        body = struct.pack("<H", len(pw_bytes)) + pw_bytes + b"\x00"
        payload = _make_meta_snac(self.uin, META_INFO_SET_PASSWORD, body, seq_id=seq)
        await self._send_snac(0x0015, 0x0002, payload)
        log.info("change_password: отправлен запрос SET_PASSWORD")

        try:
            result = await asyncio.wait_for(fut, timeout=10.0)
            if result:
                self.password = new_password
            log.info(f"change_password -> {'ok' if result else 'FAILED'}")
            await self._fire(self.on_password_changed, result)
            return result
        except asyncio.TimeoutError:
            self._pending_save.pop(seq, None)
            log.warning("change_password: timeout")
            await self._fire(self.on_password_changed, False)
            return False

    async def request_my_info(self):
        """
        Запрашивает собственную анкету с ICQ-сервера.
        После получения заполняет self.my_info и self.my_nick,
        вызывает колбэк on_my_info(info: UserInfo).
        """
        await self._request_user_info_impl(self.uin, is_own=True)
        log.info(f"Requested own profile (uin={self.uin})")

    async def request_user_info(self, uin: str):
        """
        Запрашивает анкету любого пользователя по UIN.
        Результат приходит через колбэк on_user_info(info: UserInfo).
        """
        await self._request_user_info_impl(uin, is_own=False)
        log.info(f"Requested user info for {uin}")

    async def _request_user_info_impl(self, uin: str, is_own: bool):
        """
        Отправляет meta-запрос анкеты строго по RequestInfoAction.java.

        Jimm использует CLI_META_REQMOREINFO_TYPE = 0x04B2 с данными:
          LE(subtype : 2)  — уже в _make_meta_snac как параметр subtype
          LE(uin : 4)      — UIN запрашиваемого пользователя
        Это единственный запрос; сервер отвечает серией пакетов:
          0x00C8 (general), 0x00DC (more), 0x00D2 (work), 0x00E6 (about), 0x00FA (end)
        """
        seq = self._next_meta_seq()
        info = UserInfo(uin=uin)
        expected = {SRV_META_GENERAL_TYPE, SRV_META_MORE_TYPE,
                    SRV_META_WORK_TYPE, SRV_META_ABOUT_TYPE, SRV_META_END_TYPE}
        self._pending_info[seq] = (uin, info, expected, is_own)

        uin_int = int(uin)
        data = struct.pack("<I", uin_int)
        payload = _make_meta_snac(self.uin, CLI_META_REQMOREINFO_TYPE, data, seq_id=seq)
        await self._send_snac(0x0015, 0x0002, payload)

    async def save_my_info(self, info: UserInfo) -> bool:
        """
        Сохраняет анкету на ICQ-сервере (SNAC 0x15/0x02, CLI_SET_FULLINFO).
        Строго по SaveInfoAction.java.
        Возвращает True при успехе, False при ошибке/таймауте.
        """
        seq  = self._next_meta_seq()
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_save[seq] = fut

        body = self._build_save_info_body(info)
        payload = _make_meta_snac(self.uin, CLI_SET_FULLINFO_TYPE, body, seq_id=seq)
        await self._send_snac(0x0015, 0x0002, payload)
        log.info(f"Saving own profile (nick={info.nick!r})")

        try:
            result = await asyncio.wait_for(fut, timeout=10.0)
            if result:
                if info.nick:
                    self.my_nick = info.nick
                self.my_info = info
            return result
        except asyncio.TimeoutError:
            self._pending_save.pop(seq, None)
            log.warning("save_my_info: timeout")
            return False
        

    def _build_save_info_body(self, info: UserInfo) -> bytes:
        buf = bytearray()
        buf += _make_asciiz_tlv_le(SAVE_TLV_NICK,      info.nick)
        buf += _make_asciiz_tlv_le(SAVE_TLV_FIRSTNAME, info.first_name)
        buf += _make_asciiz_tlv_le(SAVE_TLV_LASTNAME,  info.last_name)
        buf += _make_asciiz_tlv_le(SAVE_TLV_HOME_PAGE, info.home_page)
        buf += _make_asciiz_tlv_le(SAVE_TLV_ABOUT,     info.about)
        buf += _make_asciiz_tlv_le(SAVE_TLV_CITY,      info.city)

        if info.email:
            buf += _make_email_tlv_le(info.email, is_hidden=False)
            empty_count = 9
        else:
            empty_count = 10

        for _ in range(empty_count):
            buf += _make_empty_email_tlv_le()

        if info.birthday:
            parts = info.birthday.split(".")
            if len(parts) == 3:
                try:
                    day, mon, year = int(parts[0]), int(parts[1]), int(parts[2])
                    buf += struct.pack("<HH", SAVE_TLV_BDAY, 6)
                    buf += struct.pack("<HHH", year, mon, day)
                except ValueError:
                    pass

        gender_byte = {"M": 2, "F": 1}.get(info.gender.upper(), 0)
        buf += struct.pack("<HHB", SAVE_TLV_GENDER, 1, gender_byte)

        return bytes(buf)


    async def search_users(
        self,
        uin:         str  = "",
        nick:        str  = "",
        first_name:  str  = "",
        last_name:   str  = "",
        email:       str  = "",
        city:        str  = "",
        keyword:     str  = "",
        only_online: bool = False,
        timeout:     float = 30.0,
    ) -> List[SearchResult]:
        """
        Поиск пользователей (SNAC 0x15/0x02, тип 0x055F).
        Строго по SearchAction.java.
        Параллельно вызывает on_search_result(result) для каждой записи,
        on_search_done(results) по завершении.
        Возвращает список результатов.
        """
        seq  = self._next_meta_seq()
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_search[seq] = []
        self._search_futures[seq] = fut

        body = self._build_search_body(
            uin=uin, nick=nick, first_name=first_name,
            last_name=last_name, email=email, city=city,
            keyword=keyword, only_online=only_online,
        )
        payload = _make_meta_snac(self.uin, 0x055F, body, seq_id=seq)
        await self._send_snac(0x0015, 0x0002, payload)
        log.info(f"Search sent: uin={uin!r} nick={nick!r} email={email!r}")

        try:
            results = await asyncio.wait_for(fut, timeout=timeout)
            await self._fire(self.on_search_done, results)
            return results
        except asyncio.TimeoutError:
            results = self._pending_search.pop(seq, [])
            self._search_futures.pop(seq, None)
            log.warning("search_users: timeout")
            await self._fire(self.on_search_done, results)
            return results

    def _build_search_body(self, uin="", nick="", first_name="", last_name="",
                           email="", city="", keyword="",
                           only_online=False) -> bytes:
        """
        Строит тело поискового запроса строго по SearchAction.java / Util.writeAsciizTLV.

        Jimm writeAsciizTLV(type, stream, value) — вызывается без bigEndian => bigEndian=true:
          BE(type:2)  LE(outer_len=raw+3:2)  LE(inner_len=raw+1:2)  raw_bytes  0x00

        Для UIN (числовое поле):
          BE(type:2)  LE(4:2)  LE(uin_value:4)

        Для однобайтовых полей (ONLYONLINE, GENDER):
          BE(type:2)  LE(1:2)  byte
        """
        buf = bytearray()

        if uin:
            try:
                buf += struct.pack("!H", SEARCH_TLV_UIN)
                buf += struct.pack("<H", 4)
                buf += struct.pack("<I", int(uin))
            except ValueError:
                pass

        def _search_str_tlv(tlv_type: int, text: str) -> bytes:
            raw = (text or "").encode("utf-8")
            return (struct.pack("!H", tlv_type) +
                    struct.pack("<H", len(raw) + 3) +
                    struct.pack("<H", len(raw) + 1) +
                    raw + b"\x00")

        if nick:
            buf += _search_str_tlv(SEARCH_TLV_NICK, nick)
        if first_name:
            buf += _search_str_tlv(SEARCH_TLV_FIRSTNAME, first_name)
        if last_name:
            buf += _search_str_tlv(SEARCH_TLV_LASTNAME, last_name)
        if email:
            buf += _search_str_tlv(SEARCH_TLV_EMAIL, email)
        if city:
            buf += _search_str_tlv(SEARCH_TLV_CITY, city)
        if keyword:
            buf += _search_str_tlv(SEARCH_TLV_KEYWORD, keyword)

        buf += struct.pack("!H", SEARCH_TLV_ONLYONLINE)
        buf += struct.pack("<H", 1)
        buf += struct.pack("B", 1 if only_online else 0)

        return bytes(buf)


    def _pack_contact_entry(self, uin: str, group_id: int, item_id: int,
                            nick: str = "", include_nick: bool = True,
                            auth_required: bool = False) -> bytes:
        """
        Формирует SSI-запись контакта по packRosterItem(cItem, groupID) из Jimm.

        Структура (все поля BE):
          BE(name_len:2) + name_bytes
          BE(group_id:2)
          BE(item_id:2)
          BE(type=0:2)         ← тип "buddy"
          BE(adddata_len:2)
          [TLV 0x0131: BE(0x0131:2) + BE(nick_len:2) + nick_utf8]
        """
        name_b = uin.encode("ascii")
        entry = bytearray()
        entry += struct.pack("!H", len(name_b)) + name_b
        entry += struct.pack("!HHH", group_id, item_id, 0x0000)

        tlvs = bytearray()
        if include_nick and nick:
            nick_b = nick.encode("utf-8")
            tlvs += struct.pack("!HH", 0x0131, len(nick_b)) + nick_b
        if auth_required:
            tlvs += struct.pack("!HH", 0x0066, 0)  # TLV 0x0066: авторизация не выдана
        entry += struct.pack("!H", len(tlvs)) + tlvs

        return bytes(entry)

    def _pack_group_entry(self, group_id: int, group_name: str,
                          contact_item_ids: list) -> bytes:
        """
        Формирует SSI-запись группы по packRosterItem(gItem) из Jimm.

        Структура (BE):
          BE(name_len:2) + name_bytes
          BE(group_id:2)
          BE(0:2)              ← item_id группы всегда 0
          BE(1:2)              ← тип "group"
          BE(adddata_len:2)
          [TLV 0x00C8: BE(0x00C8:2) + BE(ids_len:2) + BE(item_id:2)*N]
        """
        name_b = group_name.encode("utf-8")
        entry = bytearray()
        entry += struct.pack("!H", len(name_b)) + name_b
        entry += struct.pack("!HHH", group_id, 0, 0x0001)

        if contact_item_ids:
            ids_data = b"".join(struct.pack("!H", iid) for iid in contact_item_ids)
            c8_tlv = struct.pack("!HH", 0x00C8, len(ids_data)) + ids_data
            entry += struct.pack("!H", len(c8_tlv)) + c8_tlv
        else:
            entry += struct.pack("!H", 0)

        return bytes(entry)

    async def add_contact(self, uin: str, nick: str = "",
                          group_id: int = 0,
                          auth_required: bool = False) -> bool:
        """
        Добавляет пользователя в контакт-лист строго по UpdateContactListAction.java.

        Последовательность (ACTION_ADD, STATE_ADD2):
          1. CLI_ADDSTART  (0x13/0x11)
          2. CLI_ROSTERADD (0x13/0x08) — запись контакта
          3. [ждём ACK]
          4. CLI_ROSTERUPDATE (0x13/0x09) — обновляем группу со списком item_id
          5. CLI_ADDEND    (0x13/0x12)
        """
        existing = self.contacts.get(uin)
        if existing and existing.item_id != 0:
            # Контакт уже в SSI (item_id != 0 означает реальную SSI-запись)
            log.info(f"add_contact: {uin} уже есть в контакт-листе (item_id={existing.item_id:#06x}), пропускаем")
            return False
        # Если item_id == 0 — контакт появился только через buddy_online (временный объект),
        # SSI-записи нет — продолжаем добавление

        item_id  = self._rand_item_id()
        nick_str = nick or uin

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_ssi_ack[item_id] = fut

        await self._send_snac(0x0013, CLI_ADDSTART_CMD)

        entry = self._pack_contact_entry(uin, group_id, item_id, nick_str,
                                            auth_required=auth_required)
        await self._send_snac(0x0013, CLI_ROSTERADD_CMD, entry)
        log.info(f"add_contact: {uin} ({nick_str}) → группа {group_id}, item_id={item_id:#06x}")

        try:
            ack = await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            self._pending_ssi_ack.pop(item_id, None)
            log.warning(f"add_contact {uin}: timeout waiting for ACK")
            await self._send_snac(0x0013, CLI_ADDEND_CMD)
            return False

        # ack == True: добавлен без авторизации
        # ack == "auth_required": сервер требует авторизацию от добавляемого
        # ack == False: ошибка
        ok = (ack is True)
        needs_auth = (ack == "auth_required")

        if ok or needs_auth:
            group = self.groups.get(group_id)
            group_name = group.name if group else f"Group{group_id}"
            existing_ids = [c.item_id for c in self.contacts.values()
                            if c.group_id == group_id]
            existing_ids.append(item_id)
            group_entry = self._pack_group_entry(group_id, group_name, existing_ids)
            await self._send_snac(0x0013, CLI_ROSTERUPDATE_CMD, group_entry)

            # Сохраняем статус если контакт уже известен (мог прийти через buddy_online раньше SSI)
            prev_status   = self.contacts[uin].status   if uin in self.contacts else Status.OFFLINE
            prev_client   = self.contacts[uin].client   if uin in self.contacts else ""
            prev_xstatus  = self.contacts[uin].xstatus  if uin in self.contacts else ""
            c = Contact(uin=uin, name=nick_str, group_id=group_id, item_id=item_id,
                        pending_auth=(auth_required or needs_auth))
            c.status  = prev_status
            c.client  = prev_client
            c.xstatus = prev_xstatus
            self.contacts[uin] = c

        await self._send_snac(0x0013, CLI_ADDEND_CMD)

        if ok and auth_required:
            await self.send_auth_request(uin)
        elif needs_auth:
            # Сервер требует авторизацию — отправляем запрос автоматически
            log.info(f"add_contact {uin}: auth required by target, sending auth request")
            await self.send_auth_request(uin)

        if needs_auth:
            return "auth_required"
        return ok

    async def create_group(self, name: str) -> Tuple[bool, Optional["Group"]]:
        """
        Создаёт новую группу в SSI.

        Последовательность:
          1. CLI_ADDSTART  (0x13/0x11)
          2. CLI_ROSTERADD (0x13/0x08) — запись группы (item_id=0, type=0x0001)
          3. [ждём ACK]
          4. CLI_ADDEND    (0x13/0x12)
        Возвращает (True, Group) при успехе.
        """
        import random
        group_id = random.randint(0x0001, 0x7FFF)
        while group_id in self.groups:
            group_id = random.randint(0x0001, 0x7FFF)

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        ack_key = group_id | 0x8000
        self._pending_ssi_ack[ack_key] = fut

        await self._send_snac(0x0013, CLI_ADDSTART_CMD)

        entry = self._pack_group_entry(group_id, name, [])
        await self._send_snac(0x0013, CLI_ROSTERADD_CMD, entry)
        log.info(f"create_group: '{name}' group_id={group_id:#06x}")

        try:
            ok = await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            self._pending_ssi_ack.pop(ack_key, None)
            log.warning(f"create_group '{name}': timeout")
            await self._send_snac(0x0013, CLI_ADDEND_CMD)
            return False, None

        await self._send_snac(0x0013, CLI_ADDEND_CMD)

        if ok:
            g = Group(group_id=group_id, name=name)
            self.groups[group_id] = g
            log.info(f"create_group OK: '{name}' id={group_id}")
            return True, g

        return False, None

    async def delete_group(self, group_id: int) -> bool:
        """
        Удаляет группу из SSI.

        Последовательность:
          1. CLI_ADDSTART     (0x13/0x11)
          2. CLI_ROSTERDELETE (0x13/0x0A) — запись группы
          3. [ждём ACK]
          4. CLI_ADDEND       (0x13/0x12)
        Группа должна быть пустой (без контактов).
        """
        group = self.groups.get(group_id)
        if not group:
            log.warning(f"delete_group: group_id={group_id} not found")
            return False

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        ack_key = group_id | 0x8000
        self._pending_ssi_ack[ack_key] = fut

        await self._send_snac(0x0013, CLI_ADDSTART_CMD)

        entry = self._pack_group_entry(group_id, group.name, [])
        await self._send_snac(0x0013, CLI_ROSTERDELETE_CMD, entry)
        log.info(f"delete_group: '{group.name}' group_id={group_id:#06x}")

        try:
            ok = await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            self._pending_ssi_ack.pop(ack_key, None)
            log.warning(f"delete_group '{group.name}': timeout")
            await self._send_snac(0x0013, CLI_ADDEND_CMD)
            return False

        await self._send_snac(0x0013, CLI_ADDEND_CMD)

        if ok:
            del self.groups[group_id]
            log.info(f"delete_group OK: '{group.name}'")
            return True

        return False

    async def remove_contact(self, uin: str) -> bool:
        """
        Удаляет пользователя из контакт-листа строго по UpdateContactListAction.java.

        Последовательность (ACTION_DEL, STATE_DELETE_CONTACT):
          1. CLI_ADDSTART   (0x13/0x11)
          2. CLI_ROSTERDELETE (0x13/0x0A) — запись контакта
          3. [ждём ACK]
          4. CLI_ROSTERUPDATE (0x13/0x09) — обновляем группу без удалённого item_id
          5. CLI_ADDEND     (0x13/0x12)
        """
        c = self.contacts.get(uin)
        if not c:
            log.warning(f"remove_contact: {uin} not in roster")
            return False

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_ssi_ack[c.item_id] = fut

        await self._send_snac(0x0013, CLI_ADDSTART_CMD)

        entry = self._pack_contact_entry(uin, c.group_id, c.item_id, include_nick=False)
        await self._send_snac(0x0013, CLI_ROSTERDELETE_CMD, entry)
        log.info(f"remove_contact: {uin}, item_id={c.item_id:#06x}")

        try:
            ok = await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            self._pending_ssi_ack.pop(c.item_id, None)
            log.warning(f"remove_contact {uin}: timeout")
            await self._send_snac(0x0013, CLI_ADDEND_CMD)
            return False

        if ok:
            group = self.groups.get(c.group_id)
            group_name = group.name if group else f"Group{c.group_id}"
            remaining_ids = [ct.item_id for ct in self.contacts.values()
                             if ct.group_id == c.group_id and ct.uin != uin]
            group_entry = self._pack_group_entry(c.group_id, group_name, remaining_ids)
            await self._send_snac(0x0013, CLI_ROSTERUPDATE_CMD, group_entry)

            del self.contacts[uin]

        await self._send_snac(0x0013, CLI_ADDEND_CMD)
        return ok

    async def rename_contact(self, uin: str, new_nick: str) -> bool:
        """
        Переименовывает контакт в SSI (обновляет TLV 0x0131 с новым ником).

        Последовательность (аналог ACTION_MODIFY в Jimm):
          1. CLI_ADDSTART  (0x13/0x11)
          2. CLI_ROSTERUPDATE (0x13/0x09) — запись контакта с новым ником
          3. CLI_ADDEND    (0x13/0x12)
        """
        c = self.contacts.get(uin)
        if c is None:
            log.warning(f"rename_contact: {uin} not in roster")
            return False

        nick_str = new_nick.strip() or uin
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_ssi_ack[c.item_id] = fut

        await self._send_snac(0x0013, CLI_ADDSTART_CMD)

        entry = self._pack_contact_entry(uin, c.group_id, c.item_id, nick_str)
        await self._send_snac(0x0013, CLI_ROSTERUPDATE_CMD, entry)
        log.info(f"rename_contact: {uin} → {nick_str!r}")

        try:
            ok = await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            self._pending_ssi_ack.pop(c.item_id, None)
            log.warning(f"rename_contact {uin}: timeout waiting for ACK")
            await self._send_snac(0x0013, CLI_ADDEND_CMD)
            return False

        if ok:
            c.name = nick_str

        await self._send_snac(0x0013, CLI_ADDEND_CMD)
        return ok


    async def move_contact(self, uin: str, new_group_id: int) -> bool:
        """
        Перемещает контакт из одной группы в другую через SSI.

        Последовательность:
          1. CLI_ADDSTART
          2. CLI_ROSTERDELETE — удаляем запись контакта из старой группы
          3. CLI_ROSTERUPDATE — обновляем старую группу (убираем item_id)
          4. CLI_ROSTERADD    — добавляем запись контакта в новую группу
          5. CLI_ROSTERUPDATE — обновляем новую группу (добавляем item_id)
          6. CLI_ADDEND
        """
        c = self.contacts.get(uin)
        if not c:
            log.warning(f"move_contact: {uin} not in roster")
            return False
        if c.group_id == new_group_id:
            return True

        old_group_id = c.group_id
        old_item_id  = c.item_id

        loop = asyncio.get_event_loop()

        new_item_id = self._rand_item_id()
        fut: asyncio.Future = loop.create_future()
        self._pending_ssi_ack[old_item_id] = fut

        await self._send_snac(0x0013, CLI_ADDSTART_CMD)

        old_entry = self._pack_contact_entry(uin, old_group_id, old_item_id, include_nick=False)
        await self._send_snac(0x0013, CLI_ROSTERDELETE_CMD, old_entry)

        try:
            ok = await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            self._pending_ssi_ack.pop(old_item_id, None)
            log.warning(f"move_contact {uin}: timeout on delete")
            await self._send_snac(0x0013, CLI_ADDEND_CMD)
            return False

        if ok:
            old_group = self.groups.get(old_group_id)
            old_group_name = old_group.name if old_group else f"Group{old_group_id}"
            remaining_old = [ct.item_id for ct in self.contacts.values()
                             if ct.group_id == old_group_id and ct.uin != uin]
            await self._send_snac(0x0013, CLI_ROSTERUPDATE_CMD,
                                  self._pack_group_entry(old_group_id, old_group_name, remaining_old))

            fut2: asyncio.Future = loop.create_future()
            self._pending_ssi_ack[new_item_id] = fut2
            nick_str = c.name or uin
            new_entry = self._pack_contact_entry(uin, new_group_id, new_item_id, nick_str)
            await self._send_snac(0x0013, CLI_ROSTERADD_CMD, new_entry)

            try:
                ok2 = await asyncio.wait_for(fut2, timeout=10.0)
            except asyncio.TimeoutError:
                self._pending_ssi_ack.pop(new_item_id, None)
                log.warning(f"move_contact {uin}: timeout on add")
                await self._send_snac(0x0013, CLI_ADDEND_CMD)
                return False

            if ok2:
                new_group = self.groups.get(new_group_id)
                new_group_name = new_group.name if new_group else f"Group{new_group_id}"
                existing_new = [ct.item_id for ct in self.contacts.values()
                                if ct.group_id == new_group_id]
                existing_new.append(new_item_id)
                await self._send_snac(0x0013, CLI_ROSTERUPDATE_CMD,
                                      self._pack_group_entry(new_group_id, new_group_name, existing_new))

                c.group_id = new_group_id
                c.item_id  = new_item_id
                log.info(f"move_contact: {uin} → group {new_group_id}, new item_id={new_item_id:#06x}")

        await self._send_snac(0x0013, CLI_ADDEND_CMD)
        return ok and ok2

    def _next_meta_seq(self) -> int:
        self._meta_seq = (self._meta_seq + 1) & 0xFFFF
        return self._meta_seq

    def _rand_item_id(self) -> int:
        import random
        return random.randint(0x0001, 0x7FFF)


    async def request_xstatus(self, to_uin: str):
        """
        Отправляет xTraz-запрос xStatus контакту.

        Отправляются оба варианта запроса:
          - not_duck (вариант A): строгий по XtrazSM.b(), l2=0, с part_b маркером
          - duck (вариант B): упрощённый по Jimm, l2=l1=ts, xml прямо в TLV 0x2711
        Разные клиенты отвечают на разные форматы.
        """
        ts      = int(time.time() * 1000) & 0xFFFFFFFF
        counter = int(time.time()) & 0xFFFF

        payload_a = self._build_xtraz_request_payload(to_uin, counter, ts, 0)
        await self._send_snac(0x0004, 0x0006, payload_a)
        log.debug(f"xTraz request (A) sent to {to_uin}")

        await asyncio.sleep(0.05)
        payload_b = self._build_xtraz_request_payload_duck(to_uin, counter, ts, ts)
        await self._send_snac(0x0004, 0x0006, payload_b)
        log.debug(f"xTraz request (B) sent to {to_uin}")

    def _build_xtraz_request_payload(self, to_uin: str, counter: int,
                                      l1: int, l2: int) -> bytes:
        """
        Строит payload SNAC 4/6 для xTraz-запроса.

        Точная реализация XtrazSM.b(String s, int i, long l1, long l2, String s1):

        XML строится как в XtrazSM.a(String s, int ID):
          "<N><QUERY>" + MangleXml("<Q><PluginID>srvMng</PluginID></Q>") +
          "</QUERY><NOTIFY>" + MangleXml("<srv>...<senderId>OUR_UIN</senderId>...</srv>") +
          "</NOTIFY></N>"

        Структура пакета (b()):
          DWORD_LE l1, DWORD_LE l2            ← cookie (8 байт)
          BE 0x0002                            ← channel
          BYTE uin_len, uin[]
          [TLV 0x0005 = a(abyte0, j, 55+k, l1, l2, 1)]:
            BE 0x0005, BE 36+j_inner
            BE 0x0000                          ← rendezvous type
            DWORD_LE l1, DWORD_LE l2
            GUID[16] = 09461349 4C7F11D1 82224445 53540000
            BE 0x000A, BE 0x0002, BE 0x0001    ← TLV
            BE 0x000F, BE 0x0000               ← TLV
          [TLV 0x2711 = a(abyte0, j, i=counter, k=0, i1=256, j1=k)]:
            BE 0x2711, BE 51+k
            LE 27, BYTE 8, 16×0x00
            DWORD_LE 3, DWORD_LE 0             ← 0 в запросе (не 4!)
            LE counter, LE 14, LE counter
            12×0x00
            BYTE 26, BYTE 0
            LE 0 (k=0), BE 256
          [method a(abyte0, j) — LE 1, BYTE 0]
          [method b(abyte0, j) — маркер + Script Plug-in + ...]
          LE(len+4), LE(0), LE(len), LE(0)
          xml_bytes
          DWORD BE 0x00030000                  ← трейлер
        """
        xml_str = (
            "<N><QUERY>"
            + _xml_escape("<Q><PluginID>srvMng</PluginID></Q>")
            + "</QUERY><NOTIFY>"
            + _xml_escape(
                "<srv><id>cAwaySrv</id>"
                "<req><id>AwayStat</id>"
                f"<trans>1</trans>"
                f"<senderId>{self.uin}</senderId>"
                "</req></srv>"
            )
            + "</NOTIFY></N>"
        )
        xml_b = xml_str.encode("ascii")
        k = len(xml_b)

        uin_b = to_uin.encode("ascii")

        tlv2711_body = bytearray()
        tlv2711_body += struct.pack("<H", 27)
        tlv2711_body += b"\x08"
        tlv2711_body += b"\x00" * 16
        tlv2711_body += struct.pack("<I", 3)
        tlv2711_body += struct.pack("<I", 0)
        tlv2711_body += struct.pack("<H", counter)
        tlv2711_body += struct.pack("<H", 14)
        tlv2711_body += struct.pack("<H", counter)
        tlv2711_body += b"\x00" * 12
        tlv2711_body += b"\x1a\x00"
        tlv2711_body += struct.pack("<H", 0)
        tlv2711_body += struct.pack("!H", 256)

        assert len(tlv2711_body) == 51, f"TLV 2711 body должен быть 51, получили {len(tlv2711_body)}"
        tlv2711 = struct.pack("!HH", 10001, 51 + k) + bytes(tlv2711_body)

        part_a_small = struct.pack("<H", 1) + b"\x00"

        part_b = bytearray()
        part_b += struct.pack("<H", 79)
        part_b += struct.pack("<I", 0x3b60b3ef)
        part_b += struct.pack("<I", 0xd82a6c45)
        part_b += struct.pack("<I", 0xa4e09c5a)
        part_b += struct.pack("<I", 0x5e67e865)
        part_b += struct.pack("<H", 8)
        part_b += struct.pack("<I", 42)
        part_b += b"Script Plug-in: Remote Notification Arrive"
        part_b += struct.pack("!I", 256)
        part_b += struct.pack("!I", 0)
        part_b += struct.pack("!I", 0)
        part_b += struct.pack("!H", 0)
        part_b += b"\x00"

        xml_payload = bytearray()
        xml_payload += struct.pack("<H", k + 4)
        xml_payload += struct.pack("<H", 0)
        xml_payload += struct.pack("<H", k)
        xml_payload += struct.pack("<H", 0)
        xml_payload += xml_b

        trailer = struct.pack("!I", 0x00030000)

        tlv5_body = bytearray()
        tlv5_body += struct.pack("!H", 0x0000)
        tlv5_body += struct.pack("<I", l1)
        tlv5_body += struct.pack("<I", l2)
        tlv5_body += struct.pack("!I", 0x09461349)
        tlv5_body += struct.pack("!I", 0x4C7F11D1)
        tlv5_body += struct.pack("!I", 0x82224445)
        tlv5_body += struct.pack("!I", 0x53540000)
        tlv5_body += struct.pack("!HHH", 0x000A, 0x0002, 0x0001)
        tlv5_body += struct.pack("!HH", 0x000F, 0x0000)
        tlv5_body += tlv2711
        tlv5_body += part_a_small
        tlv5_body += part_b
        tlv5_body += xml_payload
        tlv5_body += trailer

        tlv5 = struct.pack("!HH", 0x0005, len(tlv5_body)) + bytes(tlv5_body)

        payload = bytearray()
        payload += struct.pack("<I", l1)
        payload += struct.pack("<I", l2)
        payload += struct.pack("!H", 0x0002)
        payload += struct.pack("B", len(uin_b))
        payload += uin_b
        payload += tlv5

        return bytes(payload)

    def _build_xtraz_request_payload_duck(self, to_uin: str, counter: int,
                                           l1: int, l2: int) -> bytes:
        """
        Строит payload SNAC 4/6 для xTraz-запроса — duck/Jimm вариант.

        Упрощённая структура (без part_a_small, part_b, трейлера):
          DWORD_LE l1, DWORD_LE l2 (= l1 = ts)
          BE 0x0002
          BYTE uin_len, uin[]
          TLV 0x0005:
            BE 0x0000 + DWORD_LE l1 + DWORD_LE l2 + CAP_AIM_SERVERRELAY
            TLV 0x000A(0x0001) + TLV 0x000F
            TLV 0x2711:
              LE 27, BYTE 8, 16×0
              DWORD_LE 3, DWORD_LE 0
              LE counter, LE 14, LE counter, 12×0
              BYTE 26, BYTE 0, LE 0, BE 256
              LE(j+4), LE(0), LE(j), LE(0), xml_bytes
        """
        xml_str = (
            "<N><QUERY>"
            + _xml_escape("<Q><PluginID>srvMng</PluginID></Q>")
            + "</QUERY><NOTIFY>"
            + _xml_escape(
                "<srv><id>cAwaySrv</id>"
                "<req><id>AwayStat</id>"
                f"<trans>1</trans>"
                f"<senderId>{self.uin}</senderId>"
                "</req></srv>"
            )
            + "</NOTIFY></N>"
        )
        xml_b = xml_str.encode("ascii")
        j     = len(xml_b)
        uin_b = to_uin.encode("ascii")

        tlv2711_body = bytearray()
        tlv2711_body += struct.pack("<H", 27)
        tlv2711_body += b"\x08"
        tlv2711_body += b"\x00" * 16
        tlv2711_body += struct.pack("<I", 3)
        tlv2711_body += struct.pack("<I", 0)
        tlv2711_body += struct.pack("<H", counter)
        tlv2711_body += struct.pack("<H", 14)
        tlv2711_body += struct.pack("<H", counter)
        tlv2711_body += b"\x00" * 12
        tlv2711_body += b"\x1a\x00"
        tlv2711_body += struct.pack("<H", 0)
        tlv2711_body += struct.pack("!H", 256)
        tlv2711_body += struct.pack("<H", j + 4)
        tlv2711_body += struct.pack("<H", 0)
        tlv2711_body += struct.pack("<H", j)
        tlv2711_body += struct.pack("<H", 0)
        tlv2711_body += xml_b

        tlv2711 = struct.pack("!HH", 10001, len(tlv2711_body)) + bytes(tlv2711_body)

        tlv5_body = bytearray()
        tlv5_body += struct.pack("!H", 0x0000)
        tlv5_body += struct.pack("<I", l1)
        tlv5_body += struct.pack("<I", l2)
        tlv5_body += CAP_AIM_SERVERRELAY
        tlv5_body += struct.pack("!HHH", 0x000A, 0x0002, 0x0001)
        tlv5_body += struct.pack("!HH", 0x000F, 0x0000)
        tlv5_body += tlv2711

        tlv5 = struct.pack("!HH", 0x0005, len(tlv5_body)) + bytes(tlv5_body)

        payload = bytearray()
        payload += struct.pack("<I", l1)
        payload += struct.pack("<I", l2)
        payload += struct.pack("!H", 0x0002)
        payload += struct.pack("B", len(uin_b))
        payload += uin_b
        payload += tlv5

        return bytes(payload)


    async def _send_xtraz_response(self, to_uin: str, title: str, desc: str):
        """
        Отправляет xTraz-ответ двумя вариантами:
          - QIP-вариант (SNAC 4/11, utf-8): всегда
          - Jasmine-вариант (SNAC 4/11, cp1251): только для не-QIP клиентов

        Разные клиенты понимают разные форматы пакета, поэтому отправляются оба.
        """
        if not title and not desc:
            title = " "
            desc  = " "

        client = self._client_cache.get(to_uin, "Unknown")
        is_qip = client.lower().startswith("qip")

        try:
            try:
                await self._send_xtraz_qip_variant(to_uin, title, desc)
            except Exception as e:
                log.warning(f"xTraz QIP variant failed for {to_uin}: {e}")

            if not is_qip:
                await asyncio.sleep(0.05)
                try:
                    await self._send_xtraz_jasmine_variant(to_uin, title, desc)
                except Exception as e:
                    log.warning(f"xTraz Jasmine variant failed for {to_uin}: {e}")

            log.info(f"xTraz response sent to {to_uin} (client={client}): '{title}'")
        except Exception as e:
            log.error(f"_send_xtraz_response error for {to_uin}: {e}", exc_info=True)

    async def _send_xtraz_qip_variant(self, to_uin: str, title: str, desc: str):
        """
        QIP-вариант xTraz-ответа (SNAC 4/11).

        XML кодируется в utf-8, обёртывается в writeUTF (BE(len) + bytes).
        Magic-байты маркера в LE, put_le(79) перед magic.
        Завершается BE(0x0005) вместо трейлера 0x00030000.
        """
        xml_inner = (
            _xml_escape("<ret event='OnRemoteNotification'>") +
            _xml_escape(
                "<srv><id>cAwaySrv</id><val srv_id='cAwaySrv'>"
                "<Root><CASXtraSetAwayMessage></CASXtraSetAwayMessage>"
                f"<uin>{self.uin}</uin><index>1</index>"
                f"<title>{_xml_escape(title)}</title>"
                f"<desc>{_xml_escape(desc)}</desc>"
                "</Root></val></srv></ret>"
            )
        )
        xml_raw   = f"<NR><RES>{xml_inner}</RES></NR>"
        xml_bytes = xml_raw.encode("utf-8")

        xml_len       = len(xml_bytes)
        writeutf_bytes = struct.pack("!H", xml_len) + xml_bytes
        j = len(writeutf_bytes)

        uin_b   = to_uin.encode("ascii")
        ts      = int(time.time() * 1000) & 0xFFFFFFFF
        counter = int(time.time()) & 0xFFFF

        buf = bytearray(len(uin_b) + 180 + j)
        pos = 0

        def put_be(off, val):      struct.pack_into("!H", buf, off, val); return off + 2
        def put_le(off, val):      struct.pack_into("<H", buf, off, val); return off + 2
        def put_dword_le(off, val):struct.pack_into("<I", buf, off, val); return off + 4
        def put_byte(off, val):    struct.pack_into("B",  buf, off, val); return off + 1

        pos = put_dword_le(pos, ts)
        pos = put_dword_le(pos, ts)
        pos = put_be(pos, 0x0002)
        pos = put_byte(pos, len(uin_b))
        buf[pos:pos+len(uin_b)] = uin_b; pos += len(uin_b)
        pos = put_be(pos, 0x0003)
        pos = put_le(pos, 27)
        pos = put_byte(pos, 0x08)
        pos += 16
        pos = put_dword_le(pos, 3)
        pos = put_dword_le(pos, 4)
        pos = put_le(pos, counter)
        pos = put_le(pos, 14)
        pos = put_le(pos, counter)
        pos += 12
        pos = put_byte(pos, 26)
        pos = put_byte(pos, 0)
        pos = put_le(pos, 0)
        pos = put_be(pos, 0)
        pos = put_le(pos, 0x0001)
        pos = put_byte(pos, 0x00)

        pos = put_le(pos, 79)
        magic = bytes.fromhex("efb3603b456c2ad85a9ce0a465e8675e")
        buf[pos:pos+16] = magic; pos += 16
        pos = put_le(pos, 8)
        pos = put_dword_le(pos, 42)
        script_str = b"Script Plug-in: Remote Notification Arrive"
        buf[pos:pos+42] = script_str; pos += 42
        pos = put_dword_le(pos, 256)
        pos = put_dword_le(pos, 0)
        pos = put_dword_le(pos, 0)
        pos = put_be(pos, 0)
        pos = put_byte(pos, 0)
        pos = put_be(pos, 0x0005)

        pos = put_le(pos, j + 4)
        pos = put_le(pos, j)
        buf[pos:pos+j] = writeutf_bytes; pos += j

        payload = bytes(buf[:pos])
        await self._send_flap(2, _make_snac(0x0004, 0x000B, flags=0, reqid=0, payload=payload))
        log.debug(f"xTraz QIP variant sent to {to_uin}")

    async def _send_xtraz_jasmine_variant(self, to_uin: str, title: str, desc: str):
        """
        Jasmine/Jimm-вариант xTraz-ответа (SNAC 4/11).

        XML кодируется в cp1251, маркер использует put_dword_le(8) вместо put_le(8),
        завершается put_dword_le(j) вместо put_le(j+4)/put_le(j).
        """
        safe_title = _xml_escape(title)
        safe_desc  = _xml_escape(desc)

        inner_xml = (
            "<ret event='OnRemoteNotification'>"
            "<srv><id>cAwaySrv</id><val srv_id='cAwaySrv'>"
            "<Root><CASXtraSetAwayMessage></CASXtraSetAwayMessage>"
            f"<uin>{self.uin}</uin><index>1</index>"
            f"<title>{safe_title}</title>"
            f"<desc>{safe_desc}</desc>"
            "</Root></val></srv></ret>"
        )
        xml_raw   = f"<NR><RES>{_xml_escape(inner_xml)}</RES></NR>"
        xml_bytes = xml_raw.encode("cp1251")

        j              = len(xml_bytes) + 2
        writeutf_bytes = struct.pack("!H", len(xml_bytes)) + xml_bytes

        uin_b   = to_uin.encode("ascii")
        ts      = int(time.time() * 1000) & 0xFFFFFFFF
        counter = int(time.time()) & 0xFFFF

        buf = bytearray(len(uin_b) + 200 + j)
        pos = 0

        def put_be(off, val):      struct.pack_into("!H", buf, off, val); return off + 2
        def put_le(off, val):      struct.pack_into("<H", buf, off, val); return off + 2
        def put_dword_le(off, val):struct.pack_into("<I", buf, off, val); return off + 4
        def put_byte(off, val):    struct.pack_into("B",  buf, off, val); return off + 1

        pos = put_dword_le(pos, ts)
        pos = put_dword_le(pos, ts)
        pos = put_be(pos, 0x0002)
        pos = put_byte(pos, len(uin_b))
        buf[pos:pos+len(uin_b)] = uin_b; pos += len(uin_b)
        pos = put_be(pos, 0x0003)
        pos = put_le(pos, 27)
        pos = put_byte(pos, 0x08)
        pos += 16
        pos = put_dword_le(pos, 3)
        pos = put_dword_le(pos, 4)
        pos = put_le(pos, counter)
        pos = put_le(pos, 14)
        pos = put_le(pos, counter)
        pos += 12
        pos = put_byte(pos, 26)
        pos = put_byte(pos, 0)
        pos = put_le(pos, 0)
        pos = put_be(pos, 0)
        pos = put_le(pos, 0x0001)
        pos = put_byte(pos, 0x00)

        pos = put_le(pos, 79)
        magic = bytes.fromhex("efb3603b456c2ad85a9ce0a465e8675e")
        buf[pos:pos+16] = magic; pos += 16
        pos = put_dword_le(pos, 8)
        pos = put_dword_le(pos, 42)
        script_str = b"Script Plug-in: Remote Notification Arrive"
        buf[pos:pos+42] = script_str; pos += 42
        pos = put_dword_le(pos, 256)
        pos = put_dword_le(pos, 0)
        pos = put_dword_le(pos, 0)
        pos = put_be(pos, 0)
        pos = put_byte(pos, 0)
        pos = put_be(pos, 0x0005)

        pos = put_dword_le(pos, j)
        buf[pos:pos+j] = writeutf_bytes; pos += j

        payload = bytes(buf[:pos])
        await self._send_flap(2, _make_snac(0x0004, 0x000B, flags=0, reqid=0, payload=payload))
        log.debug(f"xTraz Jasmine variant sent to {to_uin}")

    async def stop(self):
        self._intentional_stop = True
        self._stop_requested = True
        self._running = False
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass

    async def run(self):
        retry_delay = 5
        while not self._stop_requested:
            self._running = True
            keepalive_task = None
            normal_exit = False

            try:
                await self._connect()
                recon = await self._login_stage1()
            except AuthError as e:
                log.error(f"Auth error: {e}")
                self._running = False
                await self._disconnect()
                await self._fire(self.on_error, e)
                for task in list(self._message_tasks):
                    task.cancel()
                if self._message_tasks:
                    await asyncio.gather(*self._message_tasks, return_exceptions=True)
                return
            except asyncio.CancelledError:
                self._running = False
                await self._disconnect()
                return
            except Exception as e:
                log.error(f"Connect error: {e}", exc_info=True)
                self._running = False
                self._stop_requested = True
                await self._disconnect()
                await self._fire(self.on_error, e)
                await self._fire(self.on_disconnected)
                break

            try:
                await self._reconnect_with_cookie(recon)
                await self._initialize()
                keepalive_task = asyncio.create_task(self._keepalive())
                await self._fire(self.on_connected)
                log.info("ICQ client ready")
                await self._message_loop()
            except asyncio.CancelledError:
                log.error("Session terminated (CancelledError) — stopping without reconnect")
                self._stop_requested = True
                await self._fire(self.on_error, ConnectionError("Сессия прервана: UIN используется на другом клиенте"))
            except Exception as e:
                log.error(f"Client error: {e}", exc_info=True)
                self._stop_requested = True
                if not self._intentional_stop:
                    await self._fire(self.on_error, e)
            finally:
                self._running = False
                if keepalive_task and not keepalive_task.done():
                    keepalive_task.cancel()
                    try:
                        await keepalive_task
                    except Exception:
                        pass
                await self._disconnect()
                if not self._intentional_stop:
                    await self._fire(self.on_disconnected)
            break


    async def _send_flap(self, channel: int, payload: bytes = b""):
        if not self._writer or self._writer.is_closing():
            raise ConnectionError("Not connected")
        self._seq = (self._seq + 1) & 0xFFFF
        self._writer.write(_pack_flap(channel, self._seq, payload))
        await self._writer.drain()

    async def _recv_flap(self, timeout: float = None) -> Tuple[int, int, bytes]:
        try:
            coro = self._reader.readexactly(6)
            hdr  = await (asyncio.wait_for(coro, timeout) if timeout else coro)
            _, ch, seq, size = struct.unpack("!BBHH", hdr)
            body = await self._reader.readexactly(size) if size else b""
            return ch, seq, body
        except asyncio.IncompleteReadError:
            raise ConnectionError("EOF from server")
        except asyncio.TimeoutError:
            raise ConnectionError("Timeout")

    async def _send_snac(self, fam: int, sub: int,
                         payload: bytes = b"", reqid: int = None):
        if reqid is None:
            reqid = int(time.time()) & 0xFFFFFFFF
        await self._send_flap(2, _make_snac(fam, sub, reqid=reqid, payload=payload))


    async def _connect(self):
        self._reader, self._writer = await asyncio.open_connection(self.server, self.port)
        self._seq = int(time.time()) & 0xFFFF
        log.info(f"TCP connected to {self.server}:{self.port}")

    async def _disconnect(self):
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None

    async def _login_stage1(self) -> str:
        await self._recv_flap(timeout=10.0)
        payload = (b"\x00\x00\x00\x01"
                   + _make_tlv(1, self.uin.encode())
                   + _make_tlv(2, _xor_password(self.password)))
        await self._send_flap(1, payload)
        _, _, body = await self._recv_flap(timeout=10.0)
        tlvs = _parse_tlvs(body)
        if 0x0008 in tlvs:
            err_code = struct.unpack_from("!H", tlvs[0x0008])[0] if len(tlvs[0x0008]) >= 2 else 0
            raise AuthError(f"Auth failed: server error code {err_code:#06x}")
        self._cookie = tlvs.get(6)
        if not self._cookie:
            raise AuthError("Auth failed: no cookie")
        recon = tlvs.get(5, b"").decode(errors="ignore")
        log.info(f"Auth OK, BOS={recon}")
        return recon

    async def _reconnect_with_cookie(self, recon: str):
        host, port = self.server, self.port
        if recon and ":" in recon:
            h, p = recon.split(":")
            host, port = h, int(p)
        await self._disconnect()
        await asyncio.sleep(0.2)
        self._reader, self._writer = await asyncio.open_connection(host, port)
        try:
            await self._recv_flap(timeout=10.0)
        except Exception:
            pass
        await self._send_flap(1, b"\x00\x00\x00\x01" + _make_tlv(6, self._cookie))
        await self._recv_flap(timeout=20.0)
        log.info("BOS auth OK")

    async def _initialize(self):
        families = [(0x0001,0x0003),(0x0022,0x000B),(0x0004,0x0001),
                    (0x0013,0x0004),(0x0002,0x0001),(0x0003,0x0001),
                    (0x0015,0x0001),(0x0006,0x0001),(0x0009,0x0001),
                    (0x000a,0x0001),(0x000b,0x0001)]
        await self._send_snac(0x0001, 0x0017,
            b"".join(struct.pack("!HH", f, v) for f, v in families))
        await asyncio.sleep(0.2)
        for fam, sub, pl in [
            (0x0001,0x000E,b""),
            (0x0013,0x0002,struct.pack("!IH",0x000B0002,0x000F)),
            (0x0002,0x0002,b""),
            (0x0003,0x0002,struct.pack("!IH",0x00050002,0x0003)),
            (0x0004,0x0004,b""),
            (0x0009,0x0002,b""),
        ]:
            await self._send_snac(fam, sub, pl)
        await asyncio.sleep(0.2)
        await self._send_snac(0x0013, 0x0004)
        await asyncio.sleep(0.5)
        await self._send_snac(0x0013, 0x0007, b"\x00\x00\x00\x07")
        await asyncio.sleep(0.2)
        await self._send_snac(0x0004, 0x0002, bytes([
            0x00,0x00,0x00,0x00,0x00,0x0B,
            0x1F,0x40,0x03,0xE7,0x03,0xE7,
            0x00,0x00,0x00,0x00]))
        await asyncio.sleep(0.2)
        await self._send_cli_setuserinfo()
        await asyncio.sleep(0.3)
        await self._apply_status()
        await asyncio.sleep(0.2)
        await self._send_dc_info()
        await asyncio.sleep(0.2)
        await self._send_snac(0x0001, 0x0002, CLI_READY_DATA)
        log.info("Session initialized")
        await asyncio.sleep(0.3)
        await self.request_offline_messages()

    async def _send_cli_setuserinfo(self):
        caps = [CAP_TYPING, CAP_XTRAZ, CAP_RTF,
                CAP_AIM_SERVERRELAY, CAP_UTF8]
        if self._my_xstatus_guid:
            caps.append(self._my_xstatus_guid)
        payload = (_make_tlv(0x0005, b"".join(caps))
                   + _make_tlv(0x0006, struct.pack("!I", int(self._my_status)))
                   + _make_tlv(0x0001, b"ICQ Client")
                   + _make_tlv(0x0008, struct.pack("!I", 0)))
        if self._my_status_msg:
            payload += _make_tlv(0x0002, self._my_status_msg.encode("utf-16be"))
        await self._send_snac(0x0002, 0x0004, payload)

    async def _apply_status(self):
        chain = _make_tlv(0x0006, struct.pack("!I", int(self._my_status)))
        if self._my_status_msg:
            chain = _make_tlv(0x0002, self._my_status_msg.encode("utf-16be")) + chain
        dc = bytearray()
        dc += struct.pack("!II", 0, 0)
        dc += b"\x02"
        dc += struct.pack("!H", 8)
        dc += struct.pack("!IIIII", 0, 0x0E, 0x0F, 0, 0)
        dc += struct.pack("!IH", 0, 0)
        chain += _make_tlv(0x000C, bytes(dc))
        chain += _make_tlv(0x0008, struct.pack("!I", 0))
        await self._send_snac(0x0001, 0x001E, chain)
        log.debug(f"Status: {self._my_status.label}")

    async def _send_dc_info(self):
        dc = bytearray()
        dc += struct.pack("!II", 0, 0)
        dc += b"\x02"
        dc += struct.pack("!H", 8)
        dc += struct.pack("!IIIII", 0, 0x0E, 0x0F, 0, 0)
        dc += struct.pack("!IH", 0, 0)
        await self._send_snac(0x0001, 0x001E, bytes(dc))


    async def _keepalive(self):
        while self._running:
            try:
                await asyncio.sleep(45)
                if not self._running: break
                await self._send_flap(5)
                log.debug("Keepalive sent")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Keepalive error: {e}"); break

    async def _message_loop(self):
        while self._running:
            try:
                ch, _, body = await self._recv_flap()
                if ch == 2:
                    await self._dispatch(body)
                elif ch == 4:
                    log.warning("Server disconnect (ch4)")
                    raise ConnectionError("Сервер разорвал соединение (возможно, вход с другого клиента)")
            except ConnectionError as e:
                if self._intentional_stop:
                    log.info("Connection closed (intentional disconnect)")
                    return
                log.error(f"Connection lost: {e}"); raise
            except Exception as e:
                log.error(f"Loop error: {e}", exc_info=True); raise


    async def _dispatch(self, data: bytes):
        if len(data) < 4:
            return
        fam, sub = struct.unpack_from("!HH", data, 0)
        if   fam == 0x0013 and sub == 0x0006: await self._handle_ssi(data[10:])
        elif fam == 0x0013 and sub == 0x000F: await self._handle_ssi_update(data)
        elif fam == 0x0013 and sub == 0x000E: await self._handle_ssi_ack(data)
        elif fam == 0x0013 and sub == SSI_AUTH_REQ:       await self._handle_ssi_auth_request(data)
        elif fam == 0x0013 and sub == SSI_AUTH_REPLY:     await self._handle_ssi_auth_reply(data)
        elif fam == 0x0013 and sub == SSI_YOU_WERE_ADDED: await self._handle_ssi_you_were_added(data)
        elif fam == 0x0015 and sub == 0x0003: await self._handle_icq_meta(data)
        elif fam == 0x0003 and sub == 0x000B: await self._handle_buddy_online(data)
        elif fam == 0x0003 and sub == 0x000C: await self._handle_buddy_offline(data)
        elif fam == 0x0004 and sub == 0x0007: await self._handle_message(data)
        elif fam == 0x0004 and sub == 0x000B: await self._handle_server_relay(data)
        elif fam == 0x0004 and sub == 0x0014: await self._handle_typing(data)
        else: log.debug(f"unhandled SNAC {fam:#06x}/{sub:#06x} ({len(data)}b): {data[:20].hex()}")


    async def _handle_ssi(self, data: bytes):
        groups, contacts = _parse_ssi(data)
        for g in groups:
            self.groups[g.group_id] = g
        for c in contacts:
            if c.uin in self.contacts:
                c.status = self.contacts[c.uin].status
            self.contacts[c.uin] = c
        log.info(f"Roster: {len(groups)} групп, {len(contacts)} контактов")
        await self._fire(self.on_roster,
                         list(self.groups.values()),
                         list(self.contacts.values()))


    async def _handle_ssi_update(self, data: bytes):
        """
        SNAC 0x13/0x0F: сервер сообщает об изменении SSI-записи.

        iserverd посылает этот пакет через send_item_auth_update с нестандартной
        преамбулой из 8 байт перед SSI-записью:
          WORD(0x0006) WORD(0x0001) WORD(0x0002) WORD(0x0004)
        за которой идёт стандартная SSI-запись:
          WORD(name_len) + name + WORD(gid) + WORD(iid) + WORD(type) + WORD(tlv_len) + tlvs

        Парсим обоими способами (со смещением и без), чтобы обработать и
        этот нестандартный формат, и любой стандартный 13/0F если он придёт.
        """
        try:
            log.debug(f"SSI update (13/0F) {len(data)}b: {data[:48].hex()}")
            body = data[10:]  # пропускаем SNAC header

            def _parse_ssi_entries(buf, start):
                """Разбирает SSI-записи начиная с offset start, возвращает список (name, tlv_data)."""
                pos = start
                result = []
                while pos + 10 <= len(buf):
                    name_len = struct.unpack_from("!H", buf, pos)[0]
                    # Санитарная проверка: имя UIN — только цифры, длина 5–12
                    if name_len < 5 or name_len > 64 or pos + 2 + name_len + 8 > len(buf):
                        break
                    name      = buf[pos+2:pos+2+name_len].decode("utf-8", errors="ignore")
                    pos2      = pos + 2 + name_len
                    _gid      = struct.unpack_from("!H", buf, pos2)[0]
                    _iid      = struct.unpack_from("!H", buf, pos2+2)[0]
                    item_type = struct.unpack_from("!H", buf, pos2+4)[0]
                    tlv_len   = struct.unpack_from("!H", buf, pos2+6)[0]
                    end       = pos2 + 8 + tlv_len
                    if end > len(buf):
                        break
                    tlv_data  = buf[pos2+8:end]
                    result.append((name, item_type, tlv_data))
                    pos = end
                return result

            # Попробуем стандартный offset=0, потом со смещением +8 (iserverd auth_update)
            entries = _parse_ssi_entries(body, 0)
            if not entries:
                entries = _parse_ssi_entries(body, 8)

            updated_uins = []
            for name, item_type, tlv_data in entries:
                if item_type == 0x0000 and name in self.contacts:
                    tlvs        = _parse_tlvs(tlv_data)
                    old_pending = self.contacts[name].pending_auth
                    new_pending = (0x0066 in tlvs)
                    if old_pending != new_pending:
                        self.contacts[name].pending_auth = new_pending
                        log.info(f"SSI 13/0F: {name} pending_auth {old_pending}→{new_pending}")
                        updated_uins.append(name)

            for uin in updated_uins:
                c = self.contacts.get(uin)
                if c:
                    await self._fire(self.on_contact_status, c)
        except Exception as e:
            log.warning(f"_handle_ssi_update error: {e}", exc_info=True)

    async def _handle_ssi_ack(self, data: bytes):
        """
        Разбирает SNAC 0x13/0x0E (SRV_UPDATEACK).
        Структура: SNAC(10) + BE(result:2) per entry.
        Резолвит future по item_id из _pending_ssi_ack.

        Коды результата:
          0x0000 — успех
          0x000E — SSI_UPDATE_AUTH_REQUIRED: цель требует авторизацию
          другие — ошибка добавления
        """
        SSI_UPDATE_AUTH_REQUIRED = 0x000E
        try:
            pos = 10
            if pos + 2 > len(data):
                return
            result = struct.unpack_from("!H", data, pos)[0]
            if result == 0x0000:
                value = True
            elif result == SSI_UPDATE_AUTH_REQUIRED:
                value = "auth_required"
            else:
                value = False
            log.debug(f"SSI ack: result={result:#06x} → {value!r}")
            for item_id, fut in list(self._pending_ssi_ack.items()):
                if not fut.done():
                    fut.set_result(value)
                del self._pending_ssi_ack[item_id]
                break
        except Exception as e:
            log.error(f"ssi_ack parse error: {e}", exc_info=True)


    async def _process_offline_message(self, payload: bytes):
        """
        Разбирает тело одного оффлайн-сообщения (subtype=0x0041) из payload SNAC 15/03.
        Структура payload (после uin+subtype+seq в TLV):
        LE(from_uin:4)       ← отправитель
        LE(year:2) BYTE(month) BYTE(day) BYTE(hour) BYTE(min)
        LE(msg_type:2)
        LE(msg_len:2) + msg_bytes (с нуль-терминатором)
        """
        try:
            if len(payload) < 8:
                return

            pos = 0

            # from_uin — отправитель
            if pos + 4 > len(payload):
                return
            from_uin = str(struct.unpack_from("<I", payload, pos)[0])
            pos += 4

            # Дата/время
            if pos + 8 > len(payload):
                return
            year     = struct.unpack_from("<H", payload, pos)[0]
            month    = payload[pos + 2]
            day      = payload[pos + 3]
            hour     = payload[pos + 4]
            minute   = payload[pos + 5]
            pos += 6  # мы прочитали 6 байт (год 2 + месяц 1 + день 1 + час 1 + минута 1)

            # msg_type (2 байта)
            if pos + 2 > len(payload):
                return
            # msg_type = struct.unpack_from("<H", payload, pos)[0]  # не используется
            pos += 2

            # msg_len и сам текст
            if pos + 2 > len(payload):
                return
            msg_len = struct.unpack_from("<H", payload, pos)[0]
            pos += 2

            raw = payload[pos:pos + msg_len]
            # Убираем нуль-терминатор, если есть
            if raw and raw[-1:] == b"\x00":
                raw = raw[:-1]

            text = _decode_text(raw) if raw else ""

            import calendar
            try:
                ts = float(calendar.timegm((year, month, day, hour, minute, 0, 0, 0, 0)))
            except Exception:
                ts = time.time()

            msg = Message(sender_uin=from_uin, text=text,
                        timestamp=ts, is_outgoing=False)
            if not text.strip():
                log.debug(f"Offline message from {from_uin} has empty content, skipping")
                return
            log.info(f"Offline message from {from_uin}: {text[:60]!r}")
            await self._fire(self.on_offline_message, msg)

        except Exception as e:
            log.error(f"_process_offline_message error: {e}", exc_info=True)


    async def _handle_ssi_auth_request(self, data: bytes):
        """
        SNAC 0x13/0x18: входящий запрос авторизации от другого пользователя.

        Структура пакета (iserverd, ssi_send_auth_req):
          [0..9]   SNAC header
          [10..17] 8 байт преамбулы: WORD(0x0006) WORD(0x0001) WORD(0x0002) WORD(0x0002)
          [18]     BYTE(uin_len)
          [19..]   uin (ASCII цифры)
          [19+L]   WORD(reason_len) + reason bytes
          [...]    WORD(chs_flg)
          [если chs_flg != 0] WORD(unk) + WORD(charset_len) + charset
        """
        try:
            log.debug(f"SSI auth_req raw ({len(data)}b): {data.hex()}")

            # SNAC header = 10 байт, преамбула сервера = 8 байт
            pos = 18  # 10 + 8
            if pos >= len(data):
                log.warning("auth_req: пакет слишком короткий")
                return

            uin_len = data[pos]; pos += 1
            if pos + uin_len > len(data):
                log.warning("auth_req: UIN выходит за границы пакета")
                return
            uin = data[pos:pos+uin_len].decode("ascii", errors="ignore"); pos += uin_len

            if not uin.isdigit():
                log.warning(f"auth_req: невалидный UIN {uin!r}, попробуем сдвиг")
                # Fallback: преамбула может отсутствовать (нестандартный клиент)
                pos = 10
                uin_len = data[pos]; pos += 1
                uin = data[pos:pos+uin_len].decode("ascii", errors="ignore"); pos += uin_len
                if not uin.isdigit():
                    log.warning(f"auth_req: UIN не найден, пакет: {data.hex()}")
                    return

            # reason: WORD(len) + bytes
            if pos + 2 > len(data):
                message = ""
            else:
                reason_len = struct.unpack_from("!H", data, pos)[0]; pos += 2
                raw_reason = data[pos:pos+reason_len]; pos += reason_len

                # charset: WORD(chs_flg), если != 0 то WORD(unk) + WORD(cs_len) + charset
                charset = "utf-8"
                if pos + 2 <= len(data):
                    chs_flg = struct.unpack_from("!H", data, pos)[0]; pos += 2
                    if chs_flg and pos + 4 <= len(data):
                        pos += 2  # unk_flg
                        cs_len = struct.unpack_from("!H", data, pos)[0]; pos += 2
                        if pos + cs_len <= len(data) and cs_len > 0:
                            charset = data[pos:pos+cs_len].decode("ascii", errors="ignore")

                try:
                    message = raw_reason.decode(charset).rstrip("\x00")
                except Exception:
                    message = raw_reason.decode("latin-1", errors="replace").rstrip("\x00")

            log.info(f"Auth request from {uin}: {message!r}")
            await self._fire(self.on_auth_request, uin, message)

        except Exception as e:
            log.error(f"_handle_ssi_auth_request error: {e}", exc_info=True)


    async def _handle_ssi_auth_reply(self, data: bytes):
        """
        SNAC 0x13/0x1B: ответ сервера на наш запрос авторизации.

        Реальная структура (подтверждена hex-дампом):
          [0..9]   SNAC header (fam, sub, flags, reqid)
          [10..17] 8 байт SSI-контекста (4 × WORD)
          [18]     BYTE(uin_len)
          [19..]   uin_bytes (ASCII цифры)
          [19+uin_len]  BYTE: 0x00=denied, 0x01=granted
          [20+uin_len]  WORD(msg_len) + msg  (опционально)
        """
        try:
            log.warning(f"SSI auth_reply raw ({len(data)}b): {data.hex()}")

            # Ищем блок BYTE(uin_len)+uin в диапазоне offset 10..25
            def _find_uin(start_pos, limit):
                for pos in range(start_pos, min(limit, len(data))):
                    uin_len = data[pos]
                    if uin_len < 5 or uin_len > 12:
                        continue
                    end_pos = pos + 1 + uin_len
                    if end_pos >= len(data):
                        continue
                    uin_bytes = data[pos+1:end_pos]
                    if all(48 <= b <= 57 for b in uin_bytes):
                        return pos, uin_bytes.decode("ascii")
                return None, None

            pos_uin, uin = _find_uin(10, 26)
            if uin is None:
                log.warning(f"auth_reply: UIN не найден: {data.hex()}")
                return

            pos_state = pos_uin + 1 + len(uin)
            if pos_state >= len(data):
                log.warning("auth_reply: нет auth_state байта")
                return

            auth_state = data[pos_state]
            granted = (auth_state == 0x01)

            message = ""
            p = pos_state + 1
            if p + 2 <= len(data):
                msg_len = struct.unpack_from("!H", data, p)[0]; p += 2
                if 0 < msg_len <= len(data) - p:
                    raw = data[p:p+msg_len]
                    try:    message = raw.decode("utf-8").rstrip("\x00")
                    except Exception: message = raw.decode("latin-1").rstrip("\x00")

            log.info(f"Auth reply: uin={uin} state={auth_state:#04x} "
                     f"({'GRANTED' if granted else 'DENIED'}) msg={message!r}")

            if granted and uin in self.contacts:
                # Сервер сам снимает TLV 0x0066, добавляет контакт в online_contacts
                # и начинает слать присутствие (send_item_auth_update + db_contact_insert
                # в grant_ssi_authorization на сервере) — нам ничего делать не нужно.
                self.contacts[uin].pending_auth = False
            await self._fire(self.on_auth_reply, uin, granted, message)
        except Exception as e:
            log.error(f"_handle_ssi_auth_reply error: {e}", exc_info=True)

    async def _handle_ssi_you_were_added(self, data: bytes):
        """
        Сервер прислал SNAC 0x13/0x1C — кто-то добавил нас к себе.

        Структура (после SNAC-заголовка 10 байт):
          BYTE(uin_len) + uin_ascii

        Вызывает on_you_were_added(uin: str).
        """
        try:
            pos = 10
            if pos >= len(data):
                return
            uin_len = data[pos]; pos += 1
            uin = data[pos:pos + uin_len].decode("ascii", errors="ignore")
            log.info(f"You were added by {uin}")
            await self._fire(self.on_you_were_added, uin)
        except Exception as e:
            log.error(f"_handle_ssi_you_were_added error: {e}", exc_info=True)


    async def _handle_icq_meta(self, data: bytes):
        """
        Разбирает SNAC 0x15/0x03 (SRV_FROMICQSRV).
        Оффлайн-сообщения (0x0041, 0x0042) обёрнуты в TLV(0x0001), но имеют
        другую структуру внутри.
        """
        try:
            pos = 10
            if pos + 4 > len(data):
                return

            tlv_type = struct.unpack_from("!H", data, pos)[0]
            tlv_len  = struct.unpack_from("!H", data, pos + 2)[0]
            pos += 4
            if tlv_type != 0x0001 or pos + tlv_len > len(data):
                return

            inner = data[pos:pos + tlv_len]
            if len(inner) < 10:
                return

            # subtype находится на позиции 6 (после remaining_size(2) + uin(4))
            subtype = struct.unpack_from("<H", inner, 6)[0]

            # ---------- Оффлайн-сообщения (без 0x07DA и без success) ----------
            if subtype == OFFLINE_MSG_RESPONSE:          # 0x0041
                seq_id = struct.unpack_from("<H", inner, 8)[0]
                payload = inner[10:]
                log.debug(f"Offline message: seq={seq_id}")
                await self._process_offline_message(payload)
                return

            if subtype == OFFLINE_MSG_EOF:               # 0x0042
                log.info("Offline queue end — sending ACK")
                ack_payload = _make_offline_req_snac(self.uin, META_ACK_OFFLINE_MSG)
                await self._send_snac(0x0015, 0x0002, ack_payload)
                return

            # ---------- Обычные meta-ответы (анкета, поиск, ACK) ----------
            # Структура: [remaining(2)][uin(4)][0x07DA(2)][seq(2)][subtype(2)][success(1)][data]
            if len(inner) < 13:
                return

            seq_id  = struct.unpack_from("<H", inner, 8)[0]
            subtype = struct.unpack_from("<H", inner, 10)[0]
            success = inner[12] if len(inner) > 12 else 0
            payload = inner[13:]

            log.debug(f"ICQ meta TLV: seq={seq_id} subtype={subtype:#06x} success={success:#04x}")
            await self._dispatch_meta_reply(seq_id, subtype, success, payload)

        except Exception as e:
            log.error(f"icq_meta parse error: {e}", exc_info=True)

    async def _dispatch_meta_reply(self, seq_id: int, subtype: int,
                                   success: int, payload: bytes):
        """Диспетчеризация meta-ответов (анкета, сохранение, поиск)."""
        # 1. Проверяем pending_save по seq_id
        if seq_id in self._pending_save:
            save_subtypes = (0x0C3F, 0x00A0, 0x00DC, 0x00F0, 0x00FA, META_INFO_PASS_ACK)
            if subtype in save_subtypes:
                fut = self._pending_save.pop(seq_id, None)
                if fut and not fut.done():
                    fut.set_result(success == 0x0A)
                return

        # 2. Проверяем pending_info по seq_id
        entry = self._pending_info.get(seq_id)
        if entry is None and self._pending_info:
            info_subtypes = {SRV_META_GENERAL_TYPE, SRV_META_MORE_TYPE,
                             SRV_META_WORK_TYPE, SRV_META_ABOUT_TYPE, SRV_META_END_TYPE}
            if subtype in info_subtypes:
                fallback_seq, entry = next(iter(self._pending_info.items()))
                log.debug(f"info reply: seq mismatch (got {seq_id}, using {fallback_seq})")
                seq_id = fallback_seq

        if entry:
            await self._process_info_reply(seq_id, subtype, success, payload, entry)
            return

        # 3. Проверяем pending_search по seq_id
        if seq_id in self._pending_search:
            await self._process_search_reply(seq_id, subtype, success, payload)
            return

        log.debug(f"ICQ meta seq={seq_id}: no pending request found")

    async def _process_info_reply(self, seq_id: int, subtype: int,
                                  success: int, payload: bytes,
                                  entry: tuple):
        """Разбирает один пакет ответа на запрос анкеты."""
        uin, info, expected, is_own = entry

        if success != 0x0A and subtype != SRV_META_END_TYPE:
            log.warning(f"info reply: subtype={subtype:#x} success={success:#x} — skip")
            return

        pos = 0

        if subtype == SRV_META_GENERAL_TYPE:
            # nick, first, last, email, city, state, phone, fax, address, cell
            str_fields = ["nick","first_name","last_name","email","city",
                          "state","phone","fax","address","cell_phone"]
            for f in str_fields:
                val, pos = _read_asciiz_le(payload, pos)
                setattr(info, f, val)
            # Далее по структуре mr_send_home_info (sn15_ext_messages.cpp):
            # zip(str), country(u16), gmt_offset(s8), auth(u8), webaware(u8), iphide(u8), e1publ(u8)
            _, pos = _read_asciiz_le(payload, pos)  # zip
            pos += 2                                  # country (u16)
            pos += 1                                  # gmt_offset (s8)
            if pos < len(payload):
                auth_byte = payload[pos]; pos += 1
                # сервер: auth=0 → требовать авторизацию, auth=1 → не требовать
                info.auth_required = (auth_byte == 0)
            expected.discard(SRV_META_GENERAL_TYPE)

        elif subtype == SRV_META_MORE_TYPE:
            # age(u16), gender(u8), hpage(str), byear(u16), bmonth(u8), bday(u8),
            # lang1(u8), lang2(u8), lang3(u8) — auth в этом пакете не передаётся
            if pos + 2 <= len(payload):
                info.age = struct.unpack_from("<H", payload, pos)[0]; pos += 2
            if pos < len(payload):
                g = payload[pos]; pos += 1
                info.gender = {1: "F", 2: "M"}.get(g, "")
            hp, pos = _read_asciiz_le(payload, pos)
            info.home_page = hp
            if pos + 4 <= len(payload):
                year  = struct.unpack_from("<H", payload, pos)[0]; pos += 2
                month = payload[pos]; pos += 1
                day   = payload[pos]; pos += 1
                if year:
                    info.birthday = f"{day:02d}.{month:02d}.{year}"
            expected.discard(SRV_META_MORE_TYPE)

        elif subtype == SRV_META_WORK_TYPE:
            wfields = ["work_city","work_state","work_phone","work_fax","work_addr"]
            for f in wfields:
                val, pos = _read_asciiz_le(payload, pos)
                setattr(info, f, val)
            _, pos = _read_asciiz_le(payload, pos)
            pos += 2
            wn, pos = _read_asciiz_le(payload, pos)
            info.work_name = wn
            wd, pos = _read_asciiz_le(payload, pos)
            info.work_dep  = wd
            wp, pos = _read_asciiz_le(payload, pos)
            info.work_pos  = wp
            expected.discard(SRV_META_WORK_TYPE)

        elif subtype == SRV_META_ABOUT_TYPE:
            info.about, _ = _read_asciiz_le(payload, pos)
            expected.discard(SRV_META_ABOUT_TYPE)

        elif subtype == SRV_META_END_TYPE:
            expected.discard(SRV_META_END_TYPE)

        self._pending_info[seq_id] = (uin, info, expected, is_own)

        if not expected or subtype == SRV_META_END_TYPE:
            del self._pending_info[seq_id]
            log.info(f"UserInfo complete for {uin}: nick={info.nick!r}")
            if is_own:
                self.my_info = info
                if info.nick:
                    self.my_nick = info.nick
                    log.info(f"Own nick saved: {self.my_nick!r}")
                await self._fire(self.on_my_info, info)
            else:
                await self._fire(self.on_user_info, info)

    async def _process_search_reply(self, seq_id: int, subtype: int,
                                    success: int, payload: bytes):
        """Разбирает один пакет результата поиска."""
        SRV_SEARCH_FOUND = 0x01A4
        SRV_SEARCH_LAST  = 0x01AE

        if success != 0x0A:
            results = self._pending_search.pop(seq_id, [])
            fut = self._search_futures.pop(seq_id, None)
            if fut and not fut.done():
                fut.set_result(results)
            return

        result = _parse_search_result(payload)
        if result:
            self._pending_search[seq_id].append(result)
            await self._fire(self.on_search_result, result)
            log.debug(f"Search result: {result.uin} {result.nick!r}")

        if subtype == SRV_SEARCH_LAST:
            results = self._pending_search.pop(seq_id, [])
            fut = self._search_futures.pop(seq_id, None)
            if fut and not fut.done():
                fut.set_result(results)


    async def _handle_buddy_online(self, data: bytes):
        try:
            pos = 10
            uin_len = data[pos]; pos += 1
            uin = data[pos:pos+uin_len].decode("ascii", errors="ignore"); pos += uin_len
            pos += 2
            tlv_count = struct.unpack_from("!H", data, pos)[0]; pos += 2

            status       = Status.ONLINE
            caps_old     = b""
            caps_new     = b""
            xstatus_name = ""
            xstatus_msg  = ""
            signon_time  = 0
            online_secs  = 0
            idle_secs    = 0

            for _ in range(tlv_count):
                if pos + 4 > len(data):
                    break
                tlv_type = struct.unpack_from("!H", data, pos)[0]
                tlv_len  = struct.unpack_from("!H", data, pos+2)[0]
                pos += 4
                tlv_data = data[pos:pos+tlv_len]; pos += tlv_len

                if tlv_type == 0x0006:
                    if len(tlv_data) >= 4:
                        flags = struct.unpack_from("!I", tlv_data, 0)[0]
                        status = Status.from_flags(flags)
                elif tlv_type == 0x000D:
                    caps_old = tlv_data
                elif tlv_type == 0x0019:
                    caps_new = tlv_data
                elif tlv_type == 0x001D:
                    xstatus_name, xstatus_msg = _parse_tlv001d(tlv_data)
                elif tlv_type == 0x0003:
                    if len(tlv_data) >= 4:
                        signon_time = struct.unpack_from("!I", tlv_data, 0)[0]
                elif tlv_type == 0x000F:
                    if len(tlv_data) >= 4:
                        online_secs = struct.unpack_from("!I", tlv_data, 0)[0]
                elif tlv_type == 0x0004:
                    if len(tlv_data) >= 2:
                        idle_secs = struct.unpack_from("!H", tlv_data, 0)[0] * 60

            caps   = caps_old + caps_new
            client = _detect_client(caps)
            if client != "Unknown" or uin not in self._client_cache:
                self._client_cache[uin] = client

            xs_from_caps = _detect_xstatus_from_caps(caps)
            if xs_from_caps:
                xstatus_name = xs_from_caps

            if uin in self.contacts:
                c = self.contacts[uin]
            else:
                c = Contact(uin=uin, name=uin, group_id=0, item_id=0)
                self.contacts[uin] = c

            was_online = c.is_online
            c.status      = status
            c.client      = self._client_cache.get(uin, "Unknown")
            c.xstatus     = xstatus_name
            c.xstatus_msg = xstatus_msg
            c.signon_time = signon_time
            c.online_secs = online_secs
            c.idle_secs   = idle_secs

            log.info(
                f"[ONLINE] {uin} ({c.display_name}) — {status.label}"
                + (f" | {client}" if client != "Unknown" else "")
                + (f" | xStatus: {xstatus_name}" if xstatus_name else "")
            )

            await self._fire(self.on_contact_status, c)
            if not was_online:
                await self._fire(self.on_contact_online, c)

            if xstatus_name:
                task = asyncio.create_task(self._request_xstatus_safe(uin))
                self._message_tasks.add(task)
                task.add_done_callback(self._message_tasks.discard)

        except Exception as e:
            log.error(f"buddy_online parse error: {e}", exc_info=True)

    async def _request_xstatus_safe(self, uin: str):
        await asyncio.sleep(0.3)
        try:
            await self.request_xstatus(uin)
        except Exception as e:
            log.warning(f"xTraz request failed for {uin}: {e}")


    async def _handle_buddy_offline(self, data: bytes):
        try:
            pos = 10
            uin_len = data[pos]; pos += 1
            uin = data[pos:pos+uin_len].decode("ascii", errors="ignore")
            if uin in self.contacts:
                c = self.contacts[uin]
                c.status      = Status.OFFLINE
                c.xstatus     = ""
                c.xstatus_msg = ""
                c.client      = "Unknown"
                log.info(f"[OFFLINE] {uin} ({c.display_name})")
                await self._fire(self.on_contact_offline, c)
            else:
                log.debug(f"[OFFLINE] unknown uin {uin}")
        except Exception as e:
            log.error(f"buddy_offline parse error: {e}", exc_info=True)


    async def _handle_message(self, data: bytes):
        """
        Обрабатывает SNAC 0x0004/0x0007 (SRV_RECVMSG).

        Порядок проверки для channel 2:
          A) not_duck: _extract_xtraz_xml_from_2711(raw2711) → str-парсеры
          B) duck:     raw2711 напрямую → bytes-парсеры
          C) duck relay-xml из полного пакета → bytes-парсеры
          Первый вернувший результат (ответ или запрос) побеждает.
        """
        try:
            pos_ch = 10 + 8
            channel = struct.unpack_from("!H", data, pos_ch)[0]

            if channel == 2:
                raw2711 = self._extract_raw_2711(data)
                if raw2711 is not None:
                    sender = self._extract_sender(data)

                    result = None

                    xml_text = _extract_xtraz_xml_from_2711(raw2711)
                    if xml_text:
                        log.debug(f"[msg A] xml_text from {sender}: {xml_text[:120]}")
                        result = _parse_xtraz_response(xml_text)

                    if result is None:
                        log.debug(f"[msg B] raw2711 from {sender}: {raw2711[:80]}")
                        result = _parse_xtraz_response_bytes(raw2711)

                    if result is None:
                        raw_duck = self._extract_relay_xml_duck(data)
                        if raw_duck is not None:
                            log.debug(f"[msg C] raw_duck from {sender}: {raw_duck[:80]}")
                            result = _parse_xtraz_response_bytes(raw_duck)

                    if result is not None:
                        title, desc = result
                        if sender:
                            c = self.contacts.get(sender)
                            if c:
                                new_msg = (f"{title} — {desc}"
                                           if title and desc
                                           else (title or desc))
                                if c.xstatus_msg == new_msg:
                                    log.debug(f"[xTraz resp 4/7] {sender}: duplicate, skip")
                                    return
                                c.xstatus_msg = new_msg
                                log.info(f"[xTraz resp 4/7] {sender}: '{title}' / '{desc}'")
                                await self._fire(self.on_xstatus_updated, c)
                        return

                    xtraz_sender = None
                    if xml_text:
                        xtraz_sender = _parse_xtraz_request(xml_text)
                    if not xtraz_sender:
                        xtraz_sender = _parse_xtraz_request_bytes(raw2711)
                    if not xtraz_sender:
                        raw_duck = self._extract_relay_xml_duck(data)
                        if raw_duck is not None:
                            xtraz_sender = _parse_xtraz_request_bytes(raw_duck)

                    if xtraz_sender:
                        log.info(f"xTraz request (4/7) from {xtraz_sender}")
                        await self._send_xtraz_response(
                            xtraz_sender,
                            self._my_xstatus_title,
                            self._my_xstatus_desc
                        )
                        return

            pos = 10 + 8 + 2
            uin_len = data[pos]; pos += 1
            sender  = data[pos:pos+uin_len].decode("ascii", errors="ignore"); pos += uin_len
            pos += 4

            text = None
            if channel == 1:
                text = _extract_ch1_text(data, pos)
            elif channel == 2:
                text = _extract_ch2_text(data)

            if not text:
                return

            msg = Message(sender_uin=sender, text=text)
            log.info(f"← {sender}: {text[:100]}")
            task = asyncio.create_task(self._fire(self.on_message, msg))
            self._message_tasks.add(task)
            task.add_done_callback(self._message_tasks.discard)

        except Exception as e:
            log.error(f"message parse error: {e}", exc_info=True)


    async def _handle_server_relay(self, data: bytes):
        """
        Обрабатывает SNAC 0x0004/0x000B (CLI_ACKMSG).

        Применяются оба метода извлечения XML последовательно:
          A) not_duck: строгий разбор по ActionListener.java → str-парсеры
          B) duck: маркерный подход (duck-смещения) → bytes-парсеры
          Первый вернувший результат побеждает.
        """
        try:
            sender = self._extract_relay_sender(data)
            if not sender:
                return

            result = None

            xml_text = _extract_xtraz_xml_from_relay(data)
            if xml_text:
                log.debug(f"[relay A] xml from {sender}: {xml_text[:120]}")
                result = _parse_xtraz_response(xml_text)

            if result is None:
                raw_duck = self._extract_relay_xml_duck(data)
                if raw_duck is not None:
                    log.debug(f"[relay B] raw_duck from {sender}: {raw_duck[:80]}")
                    result = _parse_xtraz_response_bytes(raw_duck)

            if result is not None:
                title, desc = result
                c = self.contacts.get(sender)
                if c:
                    new_msg = (f"{title} — {desc}"
                               if title and desc
                               else (title or desc))
                    if c.xstatus_msg == new_msg:
                        log.debug(f"[xTraz resp 4/11] {sender}: duplicate, skip")
                        return
                    c.xstatus_msg = new_msg
                    log.info(f"[xTraz resp 4/11] {sender}: '{title}' / '{desc}'")
                    await self._fire(self.on_xstatus_updated, c)
                else:
                    log.debug(f"[xTraz resp 4/11] unknown {sender}: '{title}'/'{desc}'")
                return

            if not xml_text and not self._extract_relay_xml_duck(data):
                log.debug(f"[relay] no xml from {sender}")
                return

            xtraz_sender = _parse_xtraz_request(xml_text) if xml_text else None

            if not xtraz_sender:
                raw_duck = self._extract_relay_xml_duck(data)
                if raw_duck is not None:
                    xtraz_sender = _parse_xtraz_request_bytes(raw_duck)

            if xtraz_sender:
                log.info(f"xTraz request (4/11) from {xtraz_sender}")
                await self._send_xtraz_response(
                    xtraz_sender,
                    self._my_xstatus_title,
                    self._my_xstatus_desc
                )

        except Exception as e:
            log.error(f"server_relay parse error: {e}", exc_info=True)


    def _extract_relay_sender(self, data: bytes) -> Optional[str]:
        """UIN из SNAC 4/11: SNAC(10) + cookie(8) + channel(2) + uin_len(1) + uin"""
        try:
            pos = 10 + 8 + 2
            uin_len = data[pos]; pos += 1
            return data[pos:pos+uin_len].decode("ascii", errors="ignore")
        except Exception:
            return None

    def _extract_relay_xml_duck(self, data: bytes) -> Optional[bytes]:
        """
        Извлекает XML (bytes) из SNAC 4/11 по маркерному подходу (duck-вариант).

        Ищет 'Script Plug-in: Remote Notification Arrive' и разбирает смещения
        согласно структуре: marker + 12+2+1+2 + 2+2 → BE xml_len → xml.
        Возвращает raw bytes (не строку), чтобы их мог обработать _parse_xtraz_response_bytes.
        """
        try:
            marker = b"Script Plug-in: Remote Notification Arrive"
            idx = data.find(marker)
            if idx < 0:
                return None

            pos = idx + len(marker)
            pos += 12 + 2 + 1 + 2
            pos += 2 + 2

            if pos + 2 > len(data):
                return None

            xml_len = struct.unpack_from("!H", data, pos)[0]; pos += 2
            if pos + xml_len > len(data):
                xml_raw = data[pos:]
            else:
                xml_raw = data[pos:pos+xml_len]

            if not xml_raw:
                return None

            return xml_raw
        except Exception:
            return None

    def _extract_sender(self, data: bytes) -> Optional[str]:
        """UIN из SNAC 4/7: SNAC(10) + cookie(8) + channel(2) + uin_len(1) + uin"""
        try:
            pos = 10 + 8 + 2
            uin_len = data[pos]; pos += 1
            return data[pos:pos+uin_len].decode("ascii", errors="ignore")
        except Exception:
            return None

    def _extract_raw_2711(self, data: bytes) -> Optional[bytes]:
        """
        Извлекает содержимое TLV 0x2711 из SNAC 4/7 channel 2.
        """
        try:
            pos = 10 + 8
            channel = struct.unpack_from("!H", data, pos)[0]; pos += 2
            if channel != 2:
                return None

            uin_len = data[pos]; pos += 1 + uin_len
            pos += 4

            while pos + 4 <= len(data):
                t = struct.unpack_from("!H", data, pos)[0]
                l = struct.unpack_from("!H", data, pos+2)[0]
                pos += 4
                if pos + l > len(data):
                    break
                v = data[pos:pos+l]; pos += l
                if t != 0x0005:
                    continue

                inner_pos = 26
                while inner_pos + 4 <= len(v):
                    it = struct.unpack_from("!H", v, inner_pos)[0]
                    il = struct.unpack_from("!H", v, inner_pos+2)[0]
                    inner_pos += 4
                    if inner_pos + il > len(v):
                        break
                    iv = v[inner_pos:inner_pos+il]; inner_pos += il
                    if it == 0x2711:
                        return iv
                break
        except Exception:
            pass
        return None


    async def _handle_typing(self, data: bytes):
        try:
            pos = 20
            uin_len = data[pos]; pos += 1
            uin = data[pos:pos+uin_len].decode("ascii", errors="ignore"); pos += uin_len
            flag = struct.unpack_from("!H", data, pos)[0]
            is_typing = (flag == 0x0002)
            log.debug(f"Typing from {uin}: {'начал' if is_typing else 'закончил'}")
            await self._fire(self.on_typing, uin, is_typing)
        except Exception as e:
            log.error(f"typing parse error: {e}", exc_info=True)


    async def _fire(self, cb: Optional[Callable], *args):
        if cb is None:
            return
        try:
            if asyncio.iscoroutinefunction(cb):
                await cb(*args)
            else:
                cb(*args)
        except Exception as e:
            log.error(f"Callback {cb} error: {e}", exc_info=True)