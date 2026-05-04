#!/usr/bin/env python3
import struct
import time
import asyncio
import logging
import re

SERVER   = "195.66.114.37"
PORT     = 5190
UIN      = ""
PASSWORD = ""

# ── Constants ─────────────────────────────────────────────────────────────────

XSTATUS_GUIDS = {
    "journal":  "0072d9084ad143dd91996f026966026f",
    "angry":    "01d8d7eeac3b492aa58dd3d877e66b92",
    "ppc":      "101117c9a3b040f981ac49e159fbd5d4",
    "cinema":   "107a9a1812324da4b6cd0879db780f09",
    "phone":    "1292e5501b644f66b206b29af378e48d",
    "browsing": "12d07e3ef885489e8e97a72a6551e58d",
    "mobile":   "160c60bbdd4443f39140050f00e6c009",
    "wc":       "16f5b76fa9d240358cc5c084703c98fa",
    "coffee":   "1b78ae31fa0b4d3893d1997eeeafb218",
    "sick":     "1f7a4071bf3b4e60bc324c5787b04cf1",
    "picnic":   "2ce0e4e57c6443709c3a7a1ce878a7dc",
    "smoking":  "3fb0bd36af3b4a609eefcf190f6a5a7e",
    "thinking": "3fb0bd36af3b4a609eefcf190f6a5a7f",
    "business": "488e14898aca4a0882aa77ce7a165208",
    "duck":     "5a581ea1e580430ca06f612298b7e4c7",
    "studying": "609d52f8a29a49a6b2a02524c5e9d260",
    "unknown1": "631436ff3f8a40d0a5cb7b66e051b364",
    "typing":   "634f6bd8add24aa1aab9115bc26d05a1",
    "shopping": "63627337a03f49ff80e5f709cde0a4ee",
    "music":    "61bee0dd8bdd475d8dee5f4baacf19a7",
    "zzz":      "6443c6af22604517b58cd7df8e290352",
    "fun":      "6f4930984f7c4affa27634a03bceaea7",
    "sleeping": "785e8c4840d34c65886f04cf3f3f43df",
    "tv":       "80537de2a4674a76b3546dfd075f5ec6",
    "tired":    "83c9b78e77e74378b2c5fb6cfcc35bec",
    "beer":     "8c50dbae81ed4786acca16cc3213c7b7",
    "surfing":  "a6ed557e6bf744d4a5d4d2e7d95ce81f",
    "pro7":     "b70867f538254327a1ffcf4cc1939797",
    "working":  "ba74db3e9e24434b87b62f6b8dfee50f",
    "love2":    "cd5643a2c94c4724b52cdc0124a1d0cd",
    "gaming":   "d4a611d08f014ec09223c5b6bec6ccf0",
    "google":   "d4e2b0ba334e4fa598d0117dbf4d3cc8",
    "love":     "ddcf0ea971954048a9c6413206d6f280",
    "party":    "e601e41c33734bd1bc06811d6c323d81",
    "sex":      "e601e41c33734bd1bc06811d6c323d82",
    "meeting":  "f18ab52edc57491d99dc6444502457af",
    "eating":   "f8e8d7b282c4414290f810c6ce0a89a6",
    "none":     None,
}

# ── QIP 2005a Capabilities ─────────────────────────────────────────────────────
CAP_QIP2005         = bytes.fromhex("563FC8090B6F41514950203230303561")
CAP_TYPING          = bytes.fromhex("563FC8090B6F41BD9F79422609DFA2F3")
CAP_RTF             = bytes.fromhex("97B12751243C4334AD22D6ABF73F1492")
CAP_AIM_SERVERRELAY = bytes.fromhex("094613494C7F11D18222444553540000")
CAP_UTF8            = bytes.fromhex("0946134E4C7F11D18222444553540000")
CAP_XTRAZ           = bytes.fromhex("1A093C6CD7FD4EC59D51A6474E34F5A0")
CAP_DIRECT          = bytes.fromhex("094613444C7F11D18222444553540000")
CAP_AIMFILE         = bytes.fromhex("094613434C7F11D18222444553540000")

# ── Known client capabilities ─────────────────────────────────────────────────
CAP_JIMM    = bytes.fromhex("97B12751243C4334AD22D6ABF73F1409")
CAP_MIRANDA = bytes.fromhex("4D6972616E64614D61696E0000000000")
CAP_ICQ6    = bytes.fromhex("3B7248ED5EDE4D6993F729D5BDF8A27F")
CAP_ICQ7    = bytes.fromhex("3FF19BEB53714657BCDEA39142E55D99")

CLIENT_CAPS = {
    CAP_QIP2005: "QIP 2005a",
    CAP_JIMM:    "Jimm",
    CAP_MIRANDA: "Miranda",
    CAP_ICQ6:    "ICQ 6",
    CAP_ICQ7:    "ICQ 7",
}

STATUS_FLAGS = {
    "online": 0x00000000,
    "away":   0x00000001,
    "dnd":    0x00000002,
    "free":   0x00000020,
}

# CLI_READY payload
CLI_READY_DATA = bytes([
    0x00, 0x01, 0x00, 0x04, 0x01, 0x10, 0x16, 0x4f,
    0x00, 0x02, 0x00, 0x01, 0x01, 0x10, 0x16, 0x4f,
    0x00, 0x03, 0x00, 0x01, 0x01, 0x10, 0x16, 0x4f,
    0x00, 0x04, 0x00, 0x01, 0x01, 0x10, 0x16, 0x4f,
    0x00, 0x06, 0x00, 0x01, 0x01, 0x10, 0x16, 0x4f,
    0x00, 0x09, 0x00, 0x01, 0x01, 0x10, 0x16, 0x4f,
    0x00, 0x0a, 0x00, 0x01, 0x01, 0x10, 0x16, 0x4f,
    0x00, 0x0b, 0x00, 0x01, 0x01, 0x10, 0x16, 0x4f,
])

ICQ_MAX_MSG_BYTES = 1000

# ── Message sanitization ──────────────────────────────────────────────────────

_CP1251_TRANSLITERATION: dict[str, str] = {
    "\u2014": "--",   "\u2013": "-",    "\u2018": "'",    "\u2019": "'",
    "\u201c": '"',    "\u201d": '"',    "\u2026": "...",  "\u00ab": "<<",
    "\u00bb": ">>",   "\u2022": "*",    "\u00b7": ".",    "\u2212": "-",
    "\u00d7": "x",    "\u00f7": "/",    "\u2260": "!=",   "\u2264": "<=",
    "\u2265": ">=",   "\u221e": "inf",  "\u20ac": "EUR",  "\u00a9": "(c)",
    "\u00ae": "(r)",  "\u2122": "(tm)",
}


def sanitize_for_icq(text: str, max_bytes: int = ICQ_MAX_MSG_BYTES) -> str:
    if not text:
        return ""
    for src, dst in _CP1251_TRANSLITERATION.items():
        text = text.replace(src, dst)
    cleaned_chars = []
    for ch in text:
        try:
            ch.encode("cp1251")
            cleaned_chars.append(ch)
        except (UnicodeEncodeError, UnicodeDecodeError):
            if ch in ("\n", "\r", "\t"):
                cleaned_chars.append(ch)
    text = "".join(cleaned_chars)
    text = re.sub(r" {2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    if not text:
        return ""
    encoded = text.encode("cp1251", errors="ignore")
    if len(encoded) <= max_bytes:
        return text
    truncated = encoded[:max_bytes].decode("cp1251", errors="ignore")
    suffix = " [...]"
    suffix_bytes = len(suffix.encode("cp1251"))
    if max_bytes > suffix_bytes:
        truncated = encoded[:max_bytes - suffix_bytes].decode("cp1251", errors="ignore")
        truncated = truncated.rstrip() + suffix
    return truncated


# ── Client detection helpers ──────────────────────────────────────────────────

def detect_client_by_caps(caps_blob: bytes) -> str:
    if not caps_blob:
        return "Unknown"
    caps = [
        caps_blob[i:i+16]
        for i in range(0, len(caps_blob), 16)
        if len(caps_blob[i:i+16]) == 16
    ]
    for cap in caps:
        if cap in CLIENT_CAPS:
            return CLIENT_CAPS[cap]
    return "Unknown"


def parse_userinfo_tlvs(data: bytes) -> dict[int, bytes]:
    tlvs = {}
    p = 0
    while p + 4 <= len(data):
        t, l = struct.unpack_from("!HH", data, p)
        if p + 4 + l > len(data):
            break
        tlvs[t] = data[p + 4:p + 4 + l]
        p += 4 + l
    return tlvs


# ── xTraz XML helpers ─────────────────────────────────────────────────────────

def mangle_xml(text: str) -> str:
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


def demangle_xml(text: str) -> str:
    return (text
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&"))


def parse_xtraz_request(data: bytes) -> str | None:
    try:
        text = data.decode("utf-8", errors="ignore")
        text = demangle_xml(text)
        if "<QUERY>" not in text or "</QUERY>" not in text:
            return None
        if "<NOTIFY>" not in text or "</NOTIFY>" not in text:
            return None
        plugin_match = re.search(r"<PluginID>([^<]+)</PluginID>", text, re.I)
        if not plugin_match or plugin_match.group(1).strip().lower() != "srvmng":
            return None
        sender_match = re.search(r"<senderId>([^<]+)</senderId>", text)
        if not sender_match:
            return None
        return sender_match.group(1).strip()
    except Exception:
        return None


# ── Low-level helpers ─────────────────────────────────────────────────────────

def xor_password(password: str) -> bytes:
    key = [0xF3, 0x26, 0x81, 0xC4, 0x39, 0x86, 0xDB, 0x92,
           0x71, 0xA3, 0xB9, 0xE6, 0x53, 0x7A, 0x95, 0x7C]
    return bytes(key[i % len(key)] ^ ord(ch) for i, ch in enumerate(password))


def make_tlv(t: int, v: bytes) -> bytes:
    return struct.pack("!HH", t, len(v)) + v


def make_snac(fam: int, sub: int, flags: int = 0,
              reqid: int = None, payload: bytes = b"") -> bytes:
    if reqid is None:
        reqid = int(time.time()) & 0xFFFFFFFF
    return struct.pack("!HHHI", fam, sub, flags, reqid) + payload


def pack_flap(channel: int, seq: int, payload: bytes) -> bytes:
    return struct.pack("!BBHH", 0x2A, channel, seq, len(payload)) + payload


# ── Packet parsing ────────────────────────────────────────────────────────────

def _tlvs_be(data: bytes) -> dict[int, bytes]:
    out, p = {}, 0
    while p + 4 <= len(data):
        t, l = struct.unpack_from(">HH", data, p)
        if p + 4 + l > len(data):
            break
        out[t] = data[p + 4: p + 4 + l]
        p += 4 + l
    return out


def _decode_icq_text(raw: bytes) -> str:
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
        pass
    return clean.decode("cp1251", errors="replace")


def _find_text_le(data: bytes) -> str | None:
    p = 0
    while p + 4 <= len(data):
        t = struct.unpack_from("<H", data, p)[0]
        l = struct.unpack_from("<H", data, p + 2)[0]
        if p + 4 + l > len(data):
            p += 1
            continue
        if t == 0x0001 and l > 0:
            text = _decode_icq_text(data[p + 4: p + 4 + l])
            if text:
                return text
        p += 4 + l
    return None


def parse_icq_im_packet(data: bytes) -> tuple[str, str] | tuple[None, None]:
    if len(data) < 30:
        return None, None
    family, subtype = struct.unpack_from(">HH", data, 0)
    if family != 0x0004 or subtype != 0x0007:
        return None, None
    pos = 10
    pos += 8
    channel = struct.unpack_from(">H", data, pos)[0]
    pos += 2
    uin_len = data[pos]; pos += 1
    uin = data[pos: pos + uin_len].decode("ascii", errors="ignore")
    pos += uin_len
    pos += 4
    if channel != 2:
        return None, None
    outer = _tlvs_be(data[pos:])
    raw5  = outer.get(0x0005)
    if not raw5 or len(raw5) < 26:
        return None, None
    req_type = struct.unpack_from(">H", raw5, 0)[0]
    if req_type != 0:
        return None, None
    sub     = _tlvs_be(raw5[26:])
    raw2711 = sub.get(0x2711)
    if not raw2711 or len(raw2711) < 36:
        return None, None
    if raw2711[4:20] != b"\x00" * 16:
        return None, None
    text = _find_text_le(raw2711[36:])
    if not text:
        return None, None
    return uin, text


def _extract_text_ch1(snac_body: bytes, offset: int) -> str | None:
    while offset + 4 <= len(snac_body):
        tlv_type, tlv_len = struct.unpack_from("!HH", snac_body, offset)
        offset += 4
        if offset + tlv_len > len(snac_body):
            break
        value = snac_body[offset: offset + tlv_len]
        offset += tlv_len
        if tlv_type != 0x0002 or len(value) < 4:
            continue
        inner_off = 0
        while inner_off + 4 <= len(value):
            it, il = struct.unpack_from("!HH", value, inner_off)
            inner_off += 4
            if inner_off + il > len(value):
                break
            iv = value[inner_off: inner_off + il]
            inner_off += il
            if it == 0x0101 and len(iv) >= 4:
                charset   = struct.unpack_from("!H", iv, 0)[0]
                text_data = iv[4:]
                if charset == 2:
                    return text_data.decode("utf-16-be", errors="replace").strip("\x00").strip()
                text = _decode_icq_text(text_data)
                return text or None
    return None


# ── Bot ───────────────────────────────────────────────────────────────────────

class AsyncICQEchoBot:
    def __init__(self, server: str, port: int, uin: str, password: str,
                 max_concurrent: int = 3):
        self.server   = server
        self.port     = port
        self.uin      = uin
        self.password = password

        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.seq     = 0
        self.cookie  = None
        self.running = True
        self._stop_requested = False

        self.status_flags   = STATUS_FLAGS["free"]
        self.status_message = ""

        self.current_xstatus = ""
        self.xstatus_guid    = b""
        self.xstatus_message = ""

        self.xstatus_title = ""
        self.xstatus_message_xtraz = ""

        self.command_handler = None
        self.semaphore       = asyncio.Semaphore(max_concurrent)
        self.active_tasks: set[asyncio.Task] = set()

        self._last_recv_time = time.time()
        self.client_cache: dict[str, str] = {}

    # ── Transport ─────────────────────────────────────────────────────────────

    async def _send_flap(self, channel: int, payload: bytes = b""):
        if not self.writer or self.writer.is_closing():
            raise ConnectionError("Connection closed")
        self.seq = (self.seq + 1) & 0xFFFF
        self.writer.write(pack_flap(channel, self.seq, payload))
        await self.writer.drain()

    async def _recv_flap(self, timeout: float = None) -> tuple[int, int, bytes]:
        try:
            if timeout is not None:
                hdr = await asyncio.wait_for(self.reader.readexactly(6), timeout)
            else:
                hdr = await self.reader.readexactly(6)
            _, ch, seq, size = struct.unpack("!BBHH", hdr)
            body = await self.reader.readexactly(size) if size else b""
            self._last_recv_time = time.time()
            return ch, seq, body
        except asyncio.IncompleteReadError:
            raise ConnectionError("Connection closed by server (EOF)")
        except asyncio.TimeoutError:
            raise ConnectionError("Connection timed out")
        except (ConnectionError, BrokenPipeError, OSError) as e:
            raise ConnectionError(f"Connection lost: {e}")

    # ── Connection lifecycle ──────────────────────────────────────────────────

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.server, self.port)
        self.seq = int(time.time()) & 0xFFFF
        self._last_recv_time = time.time()
        logging.info(f"Connected to {self.server}:{self.port}")

    async def disconnect(self):
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None

    async def _login_stage1(self) -> str:
        await self._recv_flap(timeout=10.0)
        auth_payload = (b"\x00\x00\x00\x01"
                        + make_tlv(1, self.uin.encode())
                        + make_tlv(2, xor_password(self.password)))
        await self._send_flap(1, auth_payload)
        _, _, body = await self._recv_flap(timeout=10.0)
        tlvs = _tlvs_be(body)
        recon_server = tlvs.get(5, b"").decode(errors="ignore")
        self.cookie  = tlvs.get(6)
        logging.info(f"Got cookie, recon={recon_server}")
        return recon_server

    async def _reconnect_with_cookie(self, recon_server: str):
        host, port = self.server, self.port
        if recon_server and ":" in recon_server:
            host, port_s = recon_server.split(":")
            port = int(port_s)
        await self.disconnect()
        await asyncio.sleep(0.2)
        self.reader, self.writer = await asyncio.open_connection(host, port)
        self._last_recv_time = time.time()
        try:
            await self._recv_flap(timeout=4.0)
        except Exception:
            pass
        await self._send_flap(1, b"\x00\x00\x00\x01" + make_tlv(6, self.cookie))
        await self._recv_flap(timeout=5.0)
        logging.info("Cookie auth completed")

    # ── SNAC senders ─────────────────────────────────────────────────────────

    async def _send_snac(self, fam: int, sub: int, payload: bytes = b""):
        await self._send_flap(2, make_snac(fam, sub, payload=payload))

    async def _send_cli_families(self):
        families = [
            (0x0001, 0x0003),
            (0x0022, 0x000B),
            (0x0004, 0x0001),
            (0x0013, 0x0004),
            (0x0002, 0x0001),
            (0x0003, 0x0001),
            (0x0015, 0x0001),
            (0x0006, 0x0001),
            (0x0009, 0x0001),
            (0x000a, 0x0001),
            (0x000b, 0x0001),
        ]
        payload = b"".join(struct.pack("!HH", fam, ver) for fam, ver in families)
        await self._send_snac(0x0001, 0x0017, payload)

    async def _send_cli_setuserinfo(self):
        full_caps = [
            CAP_QIP2005,
            CAP_TYPING,
            CAP_XTRAZ,
            CAP_RTF,
            CAP_AIM_SERVERRELAY,
            CAP_UTF8,
        ]
        if self.xstatus_guid:
            full_caps.append(self.xstatus_guid)

        caps_payload = b"".join(full_caps)

        payload  = make_tlv(0x0005, caps_payload)
        payload += make_tlv(0x0006, struct.pack("!I", self.status_flags))
        if self.status_message:
            payload += make_tlv(0x0002, self.status_message.encode("utf-16be"))
        payload += make_tlv(0x0001, b"ICQ Client")
        payload += make_tlv(0x0008, struct.pack("!I", 0x00000000))
        await self._send_snac(0x0002, 0x0004, payload)

    async def _send_cli_seticbm(self):
        await self._send_snac(0x0004, 0x0002,
                              bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x0B,
                                     0x1F, 0x40, 0x03, 0xE7, 0x03, 0xE7,
                                     0x00, 0x00, 0x00, 0x00]))

    async def _send_cli_setdcinfo(self):
        dc = bytearray()
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!I", 0x00000000)
        dc += b"\x02"
        dc += struct.pack("!H", 0x0008)
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!I", 0x0000000E)
        dc += struct.pack("!I", 0x0000000F)
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!H", 0x0000)
        await self._send_snac(0x0001, 0x001E, bytes(dc))

    async def _initialize_client(self):
        await self._send_cli_families()
        await asyncio.sleep(0.2)
        for fam, sub, payload in [
            (0x0001, 0x000E, b""),
            (0x0013, 0x0002, struct.pack("!IH", 0x000B0002, 0x000F)),
            (0x0002, 0x0002, b""),
            (0x0003, 0x0002, struct.pack("!IH", 0x00050002, 0x0003)),
            (0x0004, 0x0004, b""),
            (0x0009, 0x0002, b""),
        ]:
            await self._send_snac(fam, sub, payload)
        await asyncio.sleep(0.2)
        await self._send_snac(0x0013, 0x0004)
        await asyncio.sleep(0.5)
        await self._send_snac(0x0013, 0x0007, b"\x00\x00\x00\x07")
        await asyncio.sleep(0.2)
        await self._send_cli_seticbm()
        await asyncio.sleep(0.2)
        await self._send_cli_setuserinfo()
        await asyncio.sleep(0.3)
        await self.apply_status()
        await asyncio.sleep(0.2)
        await self._send_cli_setdcinfo()
        await asyncio.sleep(0.2)
        await self._send_snac(0x0001, 0x0002, CLI_READY_DATA)
        logging.info("Client fully initialized (QIP 2005a profile)")

    # ── Status / XStatus ──────────────────────────────────────────────────────

    def _build_status_tlv_chain(self) -> bytes:
        chain = b""
        if self.status_message:
            chain += make_tlv(0x0002, self.status_message.encode("utf-16be"))
        chain += make_tlv(0x0006, struct.pack("!I", self.status_flags & 0xFFFFFFFF))

        dc = bytearray()
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!I", 0x00000000)
        dc += b"\x02"
        dc += struct.pack("!H", 0x0008)
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!I", 0x0000000E)
        dc += struct.pack("!I", 0x0000000F)
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!I", 0x00000000)
        dc += struct.pack("!H", 0x0000)
        chain += make_tlv(0x000C, bytes(dc))

        chain += make_tlv(0x0008, struct.pack("!I", 0x00000000))
        return chain

    async def apply_status(self):
        try:
            await self._send_snac(0x0001, 0x001E, self._build_status_tlv_chain())
            name = {v: k for k, v in STATUS_FLAGS.items()}.get(self.status_flags, "unknown")
            logging.info(f"Status set to {name} (0x{self.status_flags:08x})")
        except Exception as e:
            logging.error(f"Failed to apply status: {e}")

    async def set_status_by_name(self, name: str, message: str = ""):
        nm = name.lower()
        if nm not in STATUS_FLAGS:
            raise ValueError(f"Unknown status: {name}")
        self.status_flags   = STATUS_FLAGS[nm]
        self.status_message = message
        await self.apply_status()

    async def update_xstatus(self):
        await self._send_cli_setuserinfo()
        await asyncio.sleep(0.15)
        tlv_chain = make_tlv(0x0006, struct.pack("!I", self.status_flags))
        if self.current_xstatus and self.current_xstatus in XSTATUS_GUIDS:
            mood_index = list(XSTATUS_GUIDS.keys()).index(self.current_xstatus)
            mood_str   = f"icqmood{mood_index}".encode("ascii")
            inner = b""
            if self.xstatus_message:
                inner += make_tlv(0x0002, self.xstatus_message.encode("utf-8"))
            inner     += make_tlv(0x000E, mood_str)
            tlv_chain += make_tlv(0x001D, inner)
        await self._send_snac(0x0001, 0x001E, tlv_chain)
        logging.info(f"XStatus updated: {self.current_xstatus}")

    async def set_xstatus(self, xstatus_name: str):
        name = xstatus_name.lower()
        if name not in XSTATUS_GUIDS:
            raise ValueError(f"Unknown XStatus: {name}. Available: {', '.join(XSTATUS_GUIDS)}")
        if name == "none":
            self.current_xstatus = ""
            self.xstatus_guid    = b""
        else:
            self.current_xstatus = name
            self.xstatus_guid    = bytes.fromhex(XSTATUS_GUIDS[name])
        await self.update_xstatus()
        logging.info(f"XStatus set to {name}")

    # ── SNAC handler for client detection ─────────────────────────────────────

    async def _handle_snac(self, data: bytes):
        if len(data) < 10:
            return

        fam, sub = struct.unpack_from("!HH", data, 0)

        if fam == 0x0003 and sub == 0x000B:
            try:
                pos = 10
                uin_len = data[pos]
                pos += 1
                uin = data[pos:pos + uin_len].decode("ascii", errors="ignore")
                pos += uin_len
                warning_level = struct.unpack_from("!H", data, pos)[0]
                pos += 2
                tlv_count = struct.unpack_from("!H", data, pos)[0]
                pos += 2
                tlvs = parse_userinfo_tlvs(data[pos:])
                caps = tlvs.get(0x000D)
                client = detect_client_by_caps(caps)

                # Сохраняем только если определили (не "Unknown") или ещё нет записи
                if client != "Unknown" or uin not in self.client_cache:
                    self.client_cache[uin] = client
                    logging.info(f"[CLIENT] {uin}: {client}")
            except Exception as e:
                logging.error(f"USER_ONLINE parse error: {e}")

    # ── xTraz methods ─────────────────────────────────────────────────────────

    async def _send_xtraz_qip_variant(self, to_uin: str, title: str, desc: str):
        xml_inner = (mangle_xml("<ret event='OnRemoteNotification'>") +
                    mangle_xml(
                        "<srv><id>cAwaySrv</id><val srv_id='cAwaySrv'>"
                        "<Root><CASXtraSetAwayMessage></CASXtraSetAwayMessage>"
                        f"<uin>{self.uin}</uin><index>1</index>"
                        f"<title>{mangle_xml(title)}</title>"
                        f"<desc>{mangle_xml(desc)}</desc>"
                        "</Root></val></srv></ret>"
                    ))
        xml_raw = f"<NR><RES>{xml_inner}</RES></NR>"
        xml_bytes = xml_raw.encode("utf-8")

        xml_len = len(xml_bytes)
        writeutf_bytes = struct.pack("!H", xml_len) + xml_bytes
        j = len(writeutf_bytes)

        uin_b = to_uin.encode("ascii")
        ts = int(time.time() * 1000) & 0xFFFFFFFF
        counter = int(time.time()) & 0xFFFF

        buf = bytearray(len(uin_b) + 180 + j)
        pos = 0

        def put_be(off, val): struct.pack_into("!H", buf, off, val); return off + 2
        def put_le(off, val): struct.pack_into("<H", buf, off, val); return off + 2
        def put_dword_le(off, val): struct.pack_into("<I", buf, off, val); return off + 4
        def put_byte(off, val): struct.pack_into("B", buf, off, val); return off + 1

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
        buf[pos:pos+j] = writeutf_bytes
        pos += j

        payload = bytes(buf[:pos])
        await self._send_flap(2, make_snac(0x0004, 0x000B, flags=0, reqid=0, payload=payload))
        logging.debug(f"xTraz QIP variant sent to {to_uin}")

    async def _send_xtraz_jasmine_variant(self, to_uin: str, title: str, desc: str):
        safe_title = mangle_xml(title)
        safe_desc  = mangle_xml(desc)

        inner_xml = (
            "<ret event='OnRemoteNotification'>"
            "<srv><id>cAwaySrv</id><val srv_id='cAwaySrv'>"
            "<Root><CASXtraSetAwayMessage></CASXtraSetAwayMessage>"
            f"<uin>{self.uin}</uin><index>1</index>"
            f"<title>{safe_title}</title>"
            f"<desc>{safe_desc}</desc>"
            "</Root></val></srv></ret>"
        )
        xml_raw   = f"<NR><RES>{mangle_xml(inner_xml)}</RES></NR>"
        xml_bytes = xml_raw.encode("cp1251")
        
        j = len(xml_bytes) + 2
        writeutf_bytes = struct.pack("!H", len(xml_bytes)) + xml_bytes

        uin_b = to_uin.encode("ascii")
        ts = int(time.time() * 1000) & 0xFFFFFFFF
        counter = int(time.time()) & 0xFFFF

        buf = bytearray(len(uin_b) + 200 + j)
        pos = 0

        def put_be(off, val): struct.pack_into("!H", buf, off, val); return off + 2
        def put_le(off, val): struct.pack_into("<H", buf, off, val); return off + 2
        def put_dword_le(off, val): struct.pack_into("<I", buf, off, val); return off + 4
        def put_byte(off, val): struct.pack_into("B", buf, off, val); return off + 1

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
        buf[pos:pos+j] = writeutf_bytes
        pos += j

        payload = bytes(buf[:pos])
        await self._send_flap(2, make_snac(0x0004, 0x000B, flags=0, reqid=0, payload=payload))
        logging.debug(f"xTraz Jasmine variant sent to {to_uin}")

    async def _send_xtraz_response(self, to_uin: str, title: str, desc: str):
        """Отправляет xTraz-ответ: QIP-вариант всегда, Jasmine-вариант — только не-QIP клиентам."""
        if not title and not desc:
            title = " "
            desc = " "

        client = self.client_cache.get(to_uin, "Unknown")
        is_qip = client.lower().startswith("qip")

        try:
            # QIP-вариант отправляем всегда
            try:
                await self._send_xtraz_qip_variant(to_uin, title, desc)
            except Exception as e:
                logging.warning(f"Failed to send QIP xTraz variant: {e}")

            # Jasmine-вариант отправляем только НЕ-QIP клиентам
            if not is_qip:
                await asyncio.sleep(0.05)
                try:
                    await self._send_xtraz_jasmine_variant(to_uin, title, desc)
                except Exception as e:
                    logging.warning(f"Failed to send Jasmine xTraz variant: {e}")

            logging.info(f"xTraz response sent to {to_uin} (client={client}): '{title}'")

        except Exception as e:
            logging.error(f"Critical error in _send_xtraz_response: {e}", exc_info=True)

    def set_xtraz_text(self, title: str, desc: str):
        self.xstatus_title = title
        self.xstatus_message_xtraz = desc
        logging.info(f"xTraz text updated: title='{title}', desc='{desc}'")

    # ── Sending messages ──────────────────────────────────────────────────────

    async def _send_message(self, to_uin: str, text: str):
        try:
            safe_text = sanitize_for_icq(text)
            safe_text = re.sub(r'\?{2,}', '', safe_text)
            safe_text = re.sub(r' {2,}', ' ', safe_text)
            safe_text = re.sub(r'\n{3,}', '\n\n', safe_text).strip()
            if not safe_text:
                logging.warning(f"Message to {to_uin} is empty after sanitization, skipping")
                return

            text_encoded = safe_text.encode("cp1251", errors="ignore")
            if len(text_encoded) > ICQ_MAX_MSG_BYTES:
                text_encoded = text_encoded[:ICQ_MAX_MSG_BYTES]

            msg_tlv = struct.pack("!HHI", 0x0101, len(text_encoded) + 4, 0) + text_encoded
            payload = (struct.pack("!Q", int(time.time()))
                       + struct.pack("!H", 1)
                       + struct.pack("!B", len(to_uin)) + to_uin.encode("ascii")
                       + struct.pack("!HH", 0x0002, len(msg_tlv)) + msg_tlv)
            await self._send_flap(2, make_snac(0x0004, 0x0006, payload=payload))
            logging.info(f"Sent to {to_uin} ({len(text_encoded)}b): {safe_text[:100]}")

        except ConnectionError as e:
            logging.error(f"Connection error while sending to {to_uin}: {e}")
            raise
        except Exception as e:
            logging.error(f"Failed to send message to {to_uin}: {e}", exc_info=True)

    # ── Incoming message handling ─────────────────────────────────────────────

    async def _handle_incoming_message(self, snac_body: bytes):
        if len(snac_body) < 10:
            return
        try:
            fam, sub = struct.unpack_from("!HH", snac_body, 0)

            if fam == 0x0004 and sub == 0x0007:
                sender_uin = parse_xtraz_request(snac_body)
                if sender_uin:
                    logging.info(f"xTraz request from {sender_uin}")
                    await self._send_xtraz_response(
                        sender_uin,
                        self.xstatus_title,
                        self.xstatus_message_xtraz
                    )
                    return

            if fam != 0x0004 or sub != 0x0007:
                return
            pos = 10
            pos += 8
            channel = struct.unpack_from("!H", snac_body, pos)[0]
            pos += 2
            uin_len = snac_body[pos]; pos += 1
            sender_uin = snac_body[pos: pos + uin_len].decode("ascii", errors="ignore")
            pos += uin_len
            pos += 4
            text = None
            if channel == 1:
                text = _extract_text_ch1(snac_body, pos)
            elif channel == 2:
                _, text = parse_icq_im_packet(snac_body)
            if not text:
                return
            logging.info(f"Message from {sender_uin}: {text}")
            task = asyncio.create_task(self._process_message(sender_uin, text))
            self.active_tasks.add(task)
            task.add_done_callback(self.active_tasks.discard)
        except Exception as e:
            logging.error(f"Message handling error: {e}", exc_info=True)

    async def _process_message(self, sender_uin: str, message_text: str):
        try:
            await asyncio.wait_for(self.semaphore.acquire(), timeout=0.1)
        except asyncio.TimeoutError:
            logging.info(f"System busy, rejecting message from {sender_uin}")
            await self._send_message(sender_uin, "Sistema peregruzhena, poprobujte pozhe.")
            return
        try:
            logging.info(f"Processing message from {sender_uin}: {message_text[:100]}")
            if self.command_handler:
                response = await self.command_handler.handle_message_async(
                    self, sender_uin, message_text)
                if response:
                    await self._send_message(sender_uin, response)
            else:
                logging.warning("No command_handler set, ignoring message")
        except ConnectionError as e:
            logging.error(f"Connection lost while processing message from {sender_uin}: {e}")
        except Exception as e:
            logging.error(f"Error processing message from {sender_uin}: {e}", exc_info=True)
        finally:
            self.semaphore.release()

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _keepalive_task(self):
        while self.running:
            try:
                await asyncio.sleep(45)
                if not self.running:
                    break
                await self._send_flap(5)
                logging.debug("Keepalive sent")
            except ConnectionError as e:
                logging.error(f"Keepalive: connection dead: {e}")
                if self.writer:
                    self.writer.transport.abort()
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Keepalive error: {e}")

    async def _message_loop(self):
        while self.running:
            try:
                ch, _, body = await self._recv_flap()
                if ch == 2:
                    await self._handle_snac(body)
                    await self._handle_incoming_message(body)
                elif ch == 4:
                    logging.warning("Server sent disconnect (channel 4)")
                    break
            except ConnectionError as e:
                logging.error(f"Connection lost in message loop: {e}")
                break
            except Exception as e:
                logging.error(f"Unexpected error in message loop: {e}")
                break

    async def stop(self):
        self._stop_requested = True
        self.running = False

    async def run(self):
        keepalive_task = None
        retry_delay    = 5
        max_delay      = 300

        while not self._stop_requested:
            self.running = True
            try:
                await self.connect()
                recon_server = await self._login_stage1()
                await self._reconnect_with_cookie(recon_server)
                await self._initialize_client()

                retry_delay    = 5
                keepalive_task = asyncio.create_task(self._keepalive_task())
                logging.info(f"ICQ bot ready as QIP 2005a (max_concurrent={self.semaphore._value})")
                await self._message_loop()

            except asyncio.CancelledError:
                self._stop_requested = True
                break

            except Exception as e:
                logging.error(f"Bot error: {e}")

            finally:
                self.running = False
                if keepalive_task and not keepalive_task.done():
                    keepalive_task.cancel()
                    try:
                        await keepalive_task
                    except Exception:
                        pass
                keepalive_task = None
                await self.disconnect()

            if self._stop_requested:
                break

            logging.info(f"Reconnecting in {retry_delay}s...")
            try:
                await asyncio.sleep(retry_delay)
            except asyncio.CancelledError:
                break
            retry_delay = min(retry_delay * 2, max_delay)

        for task in list(self.active_tasks):
            task.cancel()
        if self.active_tasks:
            await asyncio.gather(*self.active_tasks, return_exceptions=True)
        logging.info("Bot stopped")