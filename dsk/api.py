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
            return response.json()
        except json.JSONDecodeError:
            raise APIError("Invalid JSON response from server")

    def _get_pow_challenge(self) -> Dict[str, Any]:
        response = self._make_request('POST', '/chat/create_pow_challenge', {'target_path': '/api/v0/chat/completion'})
        try:
            return response['data']['biz_data']['challenge']
        except KeyError:
            raise APIError("Invalid challenge response format from server")

    def create_chat_session(self) -> str:
        response = self._make_request('POST', '/chat_session/create', {'character_id': None})
        try:
            return response['data']['biz_data']['id']
        except KeyError:
            raise APIError("Invalid session creation response format from server")

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
