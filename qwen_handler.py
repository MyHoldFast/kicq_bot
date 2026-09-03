import json
import os
import re
import html
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import List, Dict
from dataclasses import dataclass


def clean_qwen_response(text: str) -> str:
    text = html.unescape(text)
    text = re.sub(r'<details>.*?</details>', '', text, flags=re.DOTALL)
    text = re.sub(r'<!--\s*qwen_metadata:.*?-->', '', text, flags=re.DOTALL)
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
    text = clean_qwen_response(text)
    text = remove_markdown(text)
    text = fix_punctuation(text)
    text = emoji_to_simple(text)
    text = remove_unhandled_emoji(text)
    text = strip_non_bmp(text)
    return text


@dataclass
class ChatMessage:
    role: str
    content: str
    timestamp: datetime

    def to_dict(self):
        return {
            'role': self.role,
            'content': self.content,
            'timestamp': self.timestamp.isoformat()
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            role=data['role'],
            content=data['content'],
            timestamp=datetime.fromisoformat(data['timestamp'])
        )


class QwenHandler:
    def __init__(self, api_url: str = "https://qwen.aikit.club/v1/chat/completions",
                 api_key: str = None, model: str = "qwen3.7-max"):
        self.api_url = api_url
        self.api_key = api_key
        self.model = model
        self.context_expiry = timedelta(minutes=30)
        self.context_dir = "qwen_contexts"

        if not os.path.exists(self.context_dir):
            os.makedirs(self.context_dir)

    def _get_context_file(self, user_id: str) -> str:
        return os.path.join(self.context_dir, f"{user_id}.json")

    def _clean_expired_contexts(self, user_id: str):
        context_file = self._get_context_file(user_id)
        if not os.path.exists(context_file):
            return
        try:
            with open(context_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            updated_time = datetime.fromisoformat(data.get('updated', ''))
            if datetime.now() - updated_time > self.context_expiry:
                os.remove(context_file)
                logging.info(f"Cleaned expired context for user {user_id}")
        except Exception as e:
            logging.error(f"Error cleaning context for {user_id}: {e}")

    def _load_messages(self, user_id: str) -> List[ChatMessage]:
        context_file = self._get_context_file(user_id)
        if not os.path.exists(context_file):
            return []
        try:
            with open(context_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return [ChatMessage.from_dict(m) for m in data.get('messages', [])]
        except Exception as e:
            logging.error(f"Error loading context for {user_id}: {e}")
            return []

    def _save_user_context(self, user_id: str, messages: List[ChatMessage]):
        context_file = self._get_context_file(user_id)
        data = {
            'user_id': user_id,
            'updated': datetime.now().isoformat(),
            'messages': [msg.to_dict() for msg in messages]
        }
        try:
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"Failed to save context for {user_id}: {e}")

    def _get_user_context(self, user_id: str) -> List[Dict[str, str]]:
        self._clean_expired_contexts(user_id)
        messages = self._load_messages(user_id)[-10:]
        return [{"role": msg.role, "content": msg.content} for msg in messages]

    def _add_to_context(self, user_id: str, role: str, content: str):
        messages = self._load_messages(user_id)
        messages.append(ChatMessage(role=role, content=content, timestamp=datetime.now()))
        self._save_user_context(user_id, messages[-20:])

    def clear_context(self, user_id: str):
        context_file = self._get_context_file(user_id)
        if os.path.exists(context_file):
            try:
                os.remove(context_file)
                logging.info(f"Context cleared for user {user_id}")
            except Exception as e:
                logging.error(f"Failed to clear context for {user_id}: {e}")

    async def process_message(self, user_id: str, message: str) -> str:
        if not self.api_key:
            return "API key not configured"

        self._add_to_context(user_id, "user", message)
        messages = self._get_user_context(user_id)

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        json_data = {
            "model": self.model,
            "messages": messages,
            "stream": False,
        }

        max_retries = 10
        retry_delay = 2

        for attempt in range(1, max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.api_url,
                        headers=headers,
                        json=json_data,
                        timeout=180
                    ) as response:

                        if response.status == 200:
                            result = await response.json()
                            choices = result.get("choices")
                            if choices:
                                response_text = choices[0]["message"].get("content", "")
                                self._add_to_context(user_id, "assistant", response_text)
                                return format_response(response_text)
                            return "No response from AI"

                        if response.status == 400:
                            error_text = await response.text()
                            if "The chat is in progress" in error_text and attempt < max_retries:
                                logging.info(f"Chat in progress for user {user_id}, retrying...")
                                await asyncio.sleep(retry_delay)
                                continue
                            return f"API error {response.status}"

                        return f"API error {response.status}"

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < max_retries:
                    logging.warning(f"Request failed ({e}), retrying...")
                    await asyncio.sleep(retry_delay)
                    continue
                return f"Network error: {str(e)}"

            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                return f"Error: {str(e)}"

        return "Maximum retry attempts reached. Please try again later."