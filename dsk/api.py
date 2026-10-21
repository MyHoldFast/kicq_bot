import json
from pathlib import Path
from typing import Optional, Dict, Any, Generator

from curl_cffi import requests

from .pow import DeepSeekPOW


class DeepSeekError(Exception):
    pass


class AuthenticationError(DeepSeekError):
    pass


class RateLimitError(DeepSeekError):
    pass


class NetworkError(DeepSeekError):
    pass


class CloudflareError(DeepSeekError):
    pass


class APIError(DeepSeekError):
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class DeepSeekAPI:
    BASE_URL = "https://chat.deepseek.com/api/v0"

    def __init__(self, auth_token: str, cookies_path: Optional[str] = None):
        if not auth_token or not isinstance(auth_token, str):
            raise AuthenticationError("Invalid auth token provided")

        self.auth_token = auth_token
        self.pow_solver = DeepSeekPOW()
        self.cookies: Dict[str, str] = {}

        path = Path(cookies_path) if cookies_path else Path(__file__).parent / 'cookies.json'
        if path.exists():
            try:
                self.cookies = json.loads(path.read_text()).get('cookies', {})
            except json.JSONDecodeError:
                self.cookies = {}

    def _get_headers(self, pow_response: Optional[str] = None) -> Dict[str, str]:
        headers = {
            'accept': '*/*',
            'accept-language': 'en,fr-FR;q=0.9,fr;q=0.8,es-ES;q=0.7,es;q=0.6,en-US;q=0.5,am;q=0.4,de;q=0.3',
            'authorization': f'Bearer {self.auth_token}',
            'content-type': 'application/json',
            'origin': 'https://chat.deepseek.com',
            'referer': 'https://chat.deepseek.com/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'x-app-version': '20241129.1',
            'x-client-locale': 'en_US',
            'x-client-platform': 'web',
            'x-client-version': '1.0.0-always',
        }
        if pow_response:
            headers['x-ds-pow-response'] = pow_response
        return headers

    def _make_request(self, method: str, endpoint: str, json_data: Dict[str, Any]) -> Any:
        try:
            response = requests.request(
                method=method,
                url=f"{self.BASE_URL}{endpoint}",
                headers=self._get_headers(),
                json=json_data,
                cookies=self.cookies,
                impersonate='chrome120',
                timeout=None
            )
        except requests.exceptions.RequestException as e:
            raise NetworkError(str(e))

        if "<!DOCTYPE html>" in response.text and "Just a moment" in response.text:
            raise CloudflareError("Blocked by Cloudflare challenge")

        if response.status_code == 401:
            raise AuthenticationError("Invalid or expired authentication token")
        if response.status_code == 429:
            raise RateLimitError("API rate limit exceeded")
        if response.status_code >= 400:
            raise APIError(f"API request failed: {response.text}", response.status_code)

        try:
            payload = response.json()
        except json.JSONDecodeError:
            raise APIError("Invalid JSON response from server")
        if not isinstance(payload, dict):
            raise APIError(f"Unexpected response: {response.text[:200]}")

        data = payload.get('data') if isinstance(payload.get('data'), dict) else {}
        code = payload.get('code') or 0
        biz_code = data.get('biz_code') or 0
        msg = payload.get('msg') or data.get('biz_msg') or ''
        if code in (40001, 40002, 40003) or biz_code in (40001, 40002, 40003):
            raise AuthenticationError(msg or "Invalid or expired authentication token")
        if code or biz_code:
            raise APIError(f"API error {code or biz_code}: {msg}", response.status_code)

        biz_data = data.get('biz_data')
        if not isinstance(biz_data, dict):
            raise APIError(f"Unexpected response: {response.text[:200]}")
        return biz_data

    def _get_pow_challenge(self) -> Dict[str, Any]:
        biz = self._make_request('POST', '/chat/create_pow_challenge', {'target_path': '/api/v0/chat/completion'})
        if not isinstance(biz.get('challenge'), dict):
            raise APIError(f"Invalid challenge response: {json.dumps(biz)[:200]}")
        return biz['challenge']

    def create_chat_session(self) -> str:
        biz = self._make_request('POST', '/chat_session/create', {'character_id': None})
        session = biz.get('chat_session') if isinstance(biz.get('chat_session'), dict) else biz
        if not session.get('id'):
            raise APIError(f"Invalid session creation response: {json.dumps(biz)[:200]}")
        return session['id']

    def delete_chat_session(self, chat_session_id: str) -> str:
        self._make_request('POST', '/chat_session/delete', {'chat_session_id': chat_session_id})
        return chat_session_id

    def chat_completion(self,
                         chat_session_id: str,
                         prompt: str,
                         parent_message_id: Optional[str] = None,
                         thinking_enabled: bool = True,
                         search_enabled: bool = False) -> Generator[Dict[str, Any], None, None]:
        if not prompt or not isinstance(prompt, str):
            raise ValueError("Prompt must be a non-empty string")
        if not chat_session_id or not isinstance(chat_session_id, str):
            raise ValueError("Chat session ID must be a non-empty string")

        json_data = {
            'chat_session_id': chat_session_id,
            'parent_message_id': parent_message_id,
            'prompt': prompt,
            'ref_file_ids': [],
            'thinking_enabled': thinking_enabled,
            'search_enabled': search_enabled,
        }

        headers = self._get_headers(self.pow_solver.solve_challenge(self._get_pow_challenge()))

        try:
            response = requests.post(
                f"{self.BASE_URL}/chat/completion",
                headers=headers,
                json=json_data,
                cookies=self.cookies,
                impersonate='chrome120',
                stream=True,
                timeout=None
            )
        except requests.exceptions.RequestException as e:
            raise NetworkError(str(e))

        if response.status_code != 200:
            error_text = next(response.iter_lines(), b'').decode('utf-8', 'ignore')
            if response.status_code == 401:
                raise AuthenticationError("Invalid or expired authentication token")
            if response.status_code == 429:
                raise RateLimitError("API rate limit exceeded")
            raise APIError(f"API request failed: {error_text}", response.status_code)

        state = {'path': None, 'fragments': []}
        for line in response.iter_lines():
            if line and line.startswith(b'{'):
                try:
                    err = json.loads(line)
                except json.JSONDecodeError:
                    err = None
                if isinstance(err, dict) and err.get('code'):
                    raise APIError(f"API error {err['code']}: {err.get('msg', '')}")
            if not line or not line.startswith(b'data: '):
                continue
            try:
                data = json.loads(line[6:])
            except json.JSONDecodeError:
                continue
            if not isinstance(data, dict):
                continue

            if 'response_message_id' in data:
                yield {'content': '', 'type': 'meta', 'finish_reason': None,
                       'response_message_id': data['response_message_id']}
                continue

            if 'p' in data:
                state['path'] = data['p']
            if 'v' not in data:
                continue

            finished = False
            for event in self._apply(state, state['path'] or '', data.get('o'), data['v']):
                if event['type'] == 'finish':
                    finished = True
                else:
                    yield event
            if finished:
                break

    def _apply(self, state: Dict[str, Any], path: str, op: Optional[str], value: Any) -> list:
        events = []

        if op == 'BATCH' and isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and 'p' in item and 'v' in item:
                    sub = f"{path}/{item['p']}" if path else item['p']
                    events += self._apply(state, sub, item.get('o'), item['v'])
            return events

        if not path and isinstance(value, dict) and isinstance(value.get('response'), dict):
            resp = value['response']
            if resp.get('message_id') is not None:
                events.append({'content': '', 'type': 'meta', 'finish_reason': None,
                               'response_message_id': resp['message_id']})
            if resp.get('content'):
                events.append({'content': resp['content'], 'type': 'content', 'finish_reason': None})
            events += self._add_fragments(state, resp.get('fragments') or [])
            return events

        if path == 'response/fragments' and isinstance(value, list):
            return self._add_fragments(state, value)

        if path == 'response/content' and isinstance(value, str):
            return [{'content': value, 'type': 'content', 'finish_reason': None}]

        if path == 'response/thinking_content' and isinstance(value, str):
            return [{'content': value, 'type': 'thinking', 'finish_reason': None}]

        if path.startswith('response/fragments/') and path.endswith('/content') and isinstance(value, str):
            idx = path[len('response/fragments/'):-len('/content')]
            try:
                ftype = state['fragments'][int(idx)]
            except (ValueError, IndexError):
                return events
            return self._fragment_event(ftype, value)

        if not path and isinstance(value, str) and state['fragments']:
            return self._fragment_event(state['fragments'][-1], value)

        if path == 'response/status' and value == 'FINISHED':
            return [{'type': 'finish'}]

        return events

    def _add_fragments(self, state: Dict[str, Any], fragments: list) -> list:
        events = []
        for fragment in fragments:
            if not isinstance(fragment, dict):
                continue
            ftype = fragment.get('type', '')
            state['fragments'].append(ftype)
            if fragment.get('content'):
                events += self._fragment_event(ftype, fragment['content'])
        return events

    @staticmethod
    def _fragment_event(ftype: str, content: str) -> list:
        if ftype == 'RESPONSE':
            return [{'content': content, 'type': 'content', 'finish_reason': None}]
        if ftype == 'THINK':
            return [{'content': content, 'type': 'thinking', 'finish_reason': None}]
        return []