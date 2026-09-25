import json
import os
import re
import html
import asyncio
import threading
from datetime import datetime, timedelta

from dsk.api import DeepSeekAPI, DeepSeekError, AuthenticationError, RateLimitError


def clean_response(text: str) -> str:
    text = html.unescape(text)
    text = re.sub(r'\s*\[citation:\d+\]', '', text)
    text = re.sub(r'<details>.*?</details>', '', text, flags=re.DOTALL)
    text = re.sub(r'\n\s*\n+', '\n', text)
    text = re.sub(r'Response ID: [a-f0-9-]+', '', text)
    text = re.sub(r'Request ID: [a-f0-9-]+', '', text)
    return text.strip()


def remove_markdown(text: str) -> str:

    def format_codeblock(m):
        lang = m.group(1).strip() if m.group(1) else ''
        code = m.group(2).strip()
        indented = '\n'.join('  ' + line for line in code.split('\n'))
        header = f'[{lang}]:\n' if lang else '[код]:\n'
        return header + indented

    text = re.sub(r'```(\w*)\n?([\s\S]*?)```', format_codeblock, text)
    text = re.sub(r'^#{1,6}\s+(.+)$', r'>> \1', text, flags=re.MULTILINE)

    text = re.sub(r'\*\*\*(.*?)\*\*\*', r'\1', text)
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'__(.*?)__', r'\1', text)
    text = re.sub(r'\*(.*?)\*', r'\1', text)
    text = re.sub(r'_(.*?)_', r'\1', text)
    text = re.sub(r'~~(.*?)~~', r'\1', text)

    text = re.sub(r'`([^`]+)`', r'[\1]', text)

    text = re.sub(r'!\[([^\]]*)\]\([^)]+\)', r'[\1]', text)
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'\1 (\2)', text)

    text = re.sub(r'^[ \t]*[*\-+]\s+', '- ', text, flags=re.MULTILINE)
    text = re.sub(r'^[\-\*_]{3,}$', '---', text, flags=re.MULTILINE)
    text = re.sub(r'<[^>]+>', '', text)

    text = re.sub(r'\n{2,}', '\n', text)

    return text.strip()


def fix_punctuation(text: str) -> str:
    replacements = {
        '—': '-', '–': '-', '―': '-',
        '«': '"', '»': '"', '„': '"', '\u201c': '"', '\u201d': '"',
        '\u2018': "'", '\u2019': "'",
        '…': '...',
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


_EMOJI_TO_SMILEY = {
    '😇': 'O:-)',
    '😊': ':-)', '🙂': ':-)',
    '😄': ':-D', '😃': ':-D', '😁': ':-D', '😆': ':-D',
    '😂': '*ROFL*', '🤣': '*ROFL*',
    '😉': ';-)',
    '😍': '*IN LOVE*',
    '😘': ':-*', '😗': ':-*', '😙': '*KISSING*',
    '😋': ':-P', '😛': ':-P',
    '😜': ';D',
    '🤔': '*SCRATCH*',
    '😐': ':-|', '😑': ':-|', '🙄': ':-|',
    '😏': ':-!',
    '😒': ':-/', '😕': ':-\\', '😷': ':-/',
    '😔': ':-(', '😞': ':-(', '😟': ':-(', '💔': ':-(',
    '😴': '*TIRED*', '🥱': '*TIRED*',
    '😎': '8-)', '🤓': '8-)',
    '😮': '=-O', '😯': '=-O', '😲': '=-O', '😱': '=-O',
    '😳': ':-[', '🙈': '*PARDON*',
    '😢': ":'(", '😭': ":'(",
    '😡': '>:o', '😠': '>:o', '🤬': '>:o',
    '🤐': ':-X',
    '😈': ']:->', '👿': ']:->',
    '❤️': '@}->--', '🌹': '@}->--',
    '👍': '*THUMBS UP*',
    '👏': '*BRAVO*',
    '🙌': '*YAHOO*',
    '👋': '*HI*',
    '🤷': '*DONT_KNOW*',
    '💃': '*DANCE*', '🕺': '*DANCE*',
    '🤦': '*WALL*',
    '✍️': '*WRITE*',
    '🍺': '*DRINK*', '🍷': '*DRINK*', '🥂': '*DRINK*',
    '🆘': '*HELP*',
    '👌': '*OK*',
    '🤘': '\\m/',
    '😵': '%)',
    '🙅': '*NO*',
    '🤪': '*CRAZY*',
}


def emoji_to_simple(text: str) -> str:
    for emoji, smile in _EMOJI_TO_SMILEY.items():
        text = text.replace(emoji, smile)
    return text


def remove_unhandled_emoji(text: str) -> str:
    emoji_pattern = re.compile(
        "[\U0001F300-\U0001F9FF\U0001FA00-\U0001FAFF\U00002600-\U000027BF"
        "\U0000FE00-\U0000FE0F\U0001F1E0-\U0001F1FF\U00002702-\U000027B0"
        "\U000024C2-\U0001F251]+",
        flags=re.UNICODE
    )
    return emoji_pattern.sub('', text)


def strip_non_bmp(text: str) -> str:
    return ''.join(c for c in text if ord(c) <= 0xFFFF)


def format_response(text: str) -> str:
    text = clean_response(text)
    text = remove_markdown(text)
    text = fix_punctuation(text)
    text = emoji_to_simple(text)
    text = remove_unhandled_emoji(text)
    text = strip_non_bmp(text)
    return text


class DeepSeekHandler:
    def __init__(self, token: str, search_enabled: bool = True):
        self.api = DeepSeekAPI(token)
        self.search_enabled = search_enabled
        self.context_expiry = timedelta(minutes=30)
        self.context_file = os.path.join("db", "deepseek_contexts.json")
        self.api_lock = threading.Lock()
        self.ctx_lock = threading.Lock()
        self.contexts = self._load()

    def _load(self) -> dict:
        try:
            with open(self.context_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    def _save(self):
        os.makedirs(os.path.dirname(self.context_file), exist_ok=True)
        with open(self.context_file, 'w', encoding='utf-8') as f:
            json.dump(self.contexts, f, ensure_ascii=False, indent=2)

    def _get_context(self, user_id: str):
        with self.ctx_lock:
            ctx = self.contexts.get(user_id)
            if not ctx:
                return None
            if datetime.now() - datetime.fromisoformat(ctx['updated']) > self.context_expiry:
                self.contexts.pop(user_id, None)
                self._save()
                return None
            return dict(ctx)

    def _set_context(self, user_id: str, chat_session_id: str, last_message_id):
        with self.ctx_lock:
            self.contexts[user_id] = {
                'chat_session_id': chat_session_id,
                'last_message_id': last_message_id,
                'updated': datetime.now().isoformat(),
            }
            self._save()

    def clear_context(self, user_id: str):
        with self.ctx_lock:
            if self.contexts.pop(user_id, None) is not None:
                self._save()

    def _complete(self, user_id: str, message: str) -> str:
        with self.api_lock:
            ctx = self._get_context(user_id)
            if ctx:
                chat_session_id, parent_id = ctx['chat_session_id'], ctx['last_message_id']
            else:
                chat_session_id, parent_id = self.api.create_chat_session(), None

            text = ""
            last_id = parent_id
            for chunk in self.api.chat_completion(
                chat_session_id=chat_session_id,
                prompt=message,
                parent_message_id=parent_id,
                thinking_enabled=False,
                search_enabled=self.search_enabled,
            ):
                if chunk['type'] == 'meta' and chunk.get('response_message_id'):
                    last_id = chunk['response_message_id']
                elif chunk['type'] in ('text', 'content') and chunk.get('content'):
                    text += chunk['content']

            self._set_context(user_id, chat_session_id, last_id)
            return text

    async def process_message(self, user_id: str, message: str) -> str:
        for attempt in range(2):
            try:
                text = await asyncio.to_thread(self._complete, user_id, message)
            except AuthenticationError:
                return "API error 401"
            except RateLimitError:
                return "API error 429"
            except DeepSeekError as e:
                if attempt == 0:
                    self.clear_context(user_id)
                    continue
                return f"Error: {e}"
            except Exception as e:
                return f"Error: {e}"

            if text.strip():
                return format_response(text)
            if attempt == 0:
                self.clear_context(user_id)

        return "No response from AI"
