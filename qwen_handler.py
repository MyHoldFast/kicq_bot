import json
import os
import re
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import List, Dict
from dataclasses import dataclass


def clean_for_win1251(text: str) -> str:
    """
    Очищает текст для windows-1251:
    1. Заменяет известные символы на ASCII-аналоги
    2. Оставшиеся неподходящие символы заменяет на '?'
    3. Сохраняет структуру с переносами строк
    """
    # Карта замен для частых символов
    replacements = {
        '—': '-', '–': '-', '―': '-',
        '«': '"', '»': '"', '„': '"', '"': '"', '"': '"',
        '…': '...',
        '°': '', '℃': 'C', '℉': 'F',
        '→': '->', '←': '<-',
        '×': 'x', '÷': '/',
        '€': 'EUR', '₽': 'RUB',
        '\u00A0': ' ',  # неразрывный пробел
        '\u200B': '',   # zero-width space
        '\u200C': '',   # zero-width non-joiner
        '\u200D': '',   # zero-width joiner
        '\uFEFF': '',   # BOM
    }
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    # Разбиваем на строки, обрабатываем каждую отдельно
    lines = text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        # Удаляем лишние пробелы в начале и конце строки
        line = line.strip()
        if not line:
            cleaned_lines.append('')  # сохраняем пустые строки для абзацев
            continue
            
        # Кодируем в cp1251, неподходящие символы заменяем на '?'
        try:
            line = line.encode("cp1251", errors="replace").decode("cp1251")
        except Exception as e:
            logging.error(f"Encoding error: {e}")
            # Fallback — оставляем только ASCII + кириллицу
            line = ''.join(c if ord(c) < 128 or 0x0400 <= ord(c) <= 0x04FF else '?' for c in line)
        
        # Убираем множественные пробелы внутри строки
        line = re.sub(r'[ \t]+', ' ', line)
        
        # Убираем пробелы перед знаками препинания
        line = re.sub(r'\s+([,.:;!?)])', r'\1', line)
        
        # Открывающая скобка — пробел перед ней
        line = re.sub(r'\s*\(\s*', ' (', line)
        
        cleaned_lines.append(line)
    
    # Собираем обратно, убираем более двух пустых строк подряд
    result_lines = []
    empty_count = 0
    
    for line in cleaned_lines:
        if line == '':
            empty_count += 1
            if empty_count <= 2:  # не больше двух пустых строк подряд
                result_lines.append(line)
        else:
            empty_count = 0
            result_lines.append(line)
    
    # Убираем пустые строки в начале и конце
    while result_lines and result_lines[0] == '':
        result_lines.pop(0)
    while result_lines and result_lines[-1] == '':
        result_lines.pop()
    
    return '\n'.join(result_lines)


def fix_punctuation(text: str) -> str:
    """Заменяет типографские символы на ASCII-аналоги."""
    text = text.replace('—', '-')
    text = text.replace('–', '-')
    text = text.replace('«', '"')
    text = text.replace('»', '"')
    text = text.replace('„', '"')
    text = text.replace('"', '"')
    text = text.replace('"', '"')
    text = text.replace(''', "'")
    text = text.replace(''', "'")
    text = text.replace('…', '...')
    return text


def emoji_to_simple(text: str) -> str:
    """Заменяет эмодзи на текстовые смайлики."""
    emoji_map = {
        '😊': ':)', '🙂': ':)', '😄': ':-D', '😃': ':-D', '😁': ':-D',
        '😆': ':-D', '😂': '^_^', '😉': ';)', '😍': ':*', '😘': ':*',
        '😋': ':P', '😛': ':P', '😜': ';P', '🤔': ':|', '😐': ':|',
        '😏': ';)', '😒': ':/', '🙄': ':|', '😔': ':(', '😴': '|-)',
        '😷': ':-/', '😎': '8)', '🤓': '8-|', '😕': ':/', '😟': ':(',
        '😮': ':O', '😯': ':O', '😲': ':O', '😳': 'O_O', '😢': ":'(",
        '😭': ":'(", '😱': ':O', '😡': '>:(', '😠': '>:(', '🤬': '>:(',
        '💀': 'x_x', '💩': ':P', '👻': 'oO', '👽': 'ET', '🤖': '0_0',
        '❤️': '<3', '💔': '</3', '💯': '100%',
    }
    
    for emoji, smile in emoji_map.items():
        text = text.replace(emoji, smile)
    
    return text


def format_response(text: str) -> str:
    """
    Полное форматирование ответа от Qwen для отправки в ICQ.
    Порядок обработки важен!
    """
    text = clean_qwen_response(text)
    text = remove_markdown(text)
    text = fix_punctuation(text)
    text = emoji_to_simple(text)
    text = clean_for_win1251(text)  # ← Финальная очистка для win1251
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


def clean_qwen_response(text: str) -> str:
    """Удаляет служебные теги Qwen и лишние пустые строки."""
    text = re.sub(r'<details>.*?</details>', '', text, flags=re.DOTALL)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'Response ID: [a-f0-9-]+', '', text)
    text = re.sub(r'Request ID: [a-f0-9-]+', '', text)
    return text.strip()


def remove_markdown(text: str) -> str:
    """Удаляет Markdown-разметку из текста."""
    text = re.sub(r'^#{1,3}\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'__(.*?)__', r'\1', text)
    text = re.sub(r'\*(?!\*)(.*?)\*(?!\*)', r'\1', text)
    text = re.sub(r'_(?!_)(.*?)_(?!_)', r'\1', text)
    text = re.sub(r'~~(.*?)~~', r'\1', text)
    text = re.sub(r'`([^`]+)`', r'\1', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    text = re.sub(r'!\[([^\]]*)\]\([^)]+\)', '', text)
    text = re.sub(r'```[\s\S]*?```', '', text)
    text = re.sub(r'^[\-\*_]{3,}$', '', text, flags=re.MULTILINE)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


class QwenHandler:
    def __init__(self, api_url: str = "https://qwen.aikit.club/v1/chat/completions", 
                 api_key: str = None, model: str = "qwen3-max"):
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
        if os.path.exists(context_file):
            try:
                with open(context_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if 'messages' in data and 'updated' in data:
                    updated_time = datetime.fromisoformat(data['updated'])
                    now = datetime.now()
                    
                    if now - updated_time > self.context_expiry:
                        os.remove(context_file)
                        logging.info(f"Cleaned expired context for user {user_id}")
                        return True
            except Exception as e:
                logging.error(f"Error cleaning context for {user_id}: {e}")
        return False
    
    def _get_user_context(self, user_id: str) -> List[Dict[str, str]]:
        self._clean_expired_contexts(user_id)
        
        context_file = self._get_context_file(user_id)
        if not os.path.exists(context_file):
            return []
        
        try:
            with open(context_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'messages' in data:
                messages = [ChatMessage.from_dict(m) for m in data['messages']]
                return [{"role": msg.role, "content": msg.content} for msg in messages[-10:]]
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
    
    def _add_to_context(self, user_id: str, role: str, content: str):
        messages = []
        
        context_file = self._get_context_file(user_id)
        if os.path.exists(context_file):
            try:
                with open(context_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if 'messages' in data:
                    messages = [ChatMessage.from_dict(m) for m in data['messages']]
            except:
                pass
        
        messages.append(ChatMessage(
            role=role,
            content=content,
            timestamp=datetime.now()
        ))
        
        if len(messages) > 20:
            messages = messages[-20:]
        
        self._save_user_context(user_id, messages)
    
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
        attempt = 0
        
        while attempt < max_retries:
            attempt += 1
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
                            
                            if "choices" in result and len(result["choices"]) > 0:
                                assistant_message = result["choices"][0]["message"]
                                response_text = assistant_message.get("content", "")
                                
                                self._add_to_context(user_id, "assistant", response_text)
                                response_text = format_response(response_text)
                                
                                return response_text
                            else:
                                return "No response from AI"
                        
                        elif response.status == 400:
                            error_text = await response.text()
                            
                            if "The chat is in progress" in error_text:
                                if attempt < max_retries:
                                    logging.info(f"Chat in progress for user {user_id}, retrying...")
                                    await asyncio.sleep(retry_delay)
                                    continue
                                else:
                                    return "AI is busy. Please try again in a moment."
                            
                            return f"API error {response.status}"
                        
                        else:
                            return f"API error {response.status}"
                            
            except aiohttp.ClientError as e:
                if attempt < max_retries:
                    logging.warning(f"Network error, retrying...")
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    return f"Network error: {str(e)}"
                    
            except asyncio.TimeoutError:
                if attempt < max_retries:
                    logging.warning(f"Timeout, retrying...")
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    return "Request timeout"
                    
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                return f"Error: {str(e)}"
        
        return "Maximum retry attempts reached. Please try again later."
    