import itertools
import json
import os.path
import random
import re
import time
import requests
import base64
import hashlib
import urllib.parse

from streamonitor.bot import RoomIdBot
from streamonitor.downloaders.hls import getVideoNativeHLS
from streamonitor.enums import Status, Gender, COUNTRIES


class StripChat(RoomIdBot):
    site = 'StripChat'
    siteslug = 'SC'

    bulk_update = True
    _static_data = None
    _main_js_data = None
    _doppio_js_data = None
    _mouflon_cache_filename = 'stripchat_mouflon_keys.json'
    _mouflon_keys: dict = None
    _cached_keys: dict[str, bytes] = None
    _PRIVATE_STATUSES = frozenset(["private", "groupShow", "p2p", "virtualPrivate", "p2pVoice"])
    _OFFLINE_STATUSES = frozenset(["off", "idle"])

    _GENDER_MAP = {
        'female': Gender.FEMALE,
        'male': Gender.MALE,
        'maleFemale': Gender.BOTH
    }

    # Load cached keys from disk if available (fallback if Doppio fetch fails)
    if os.path.exists(_mouflon_cache_filename):
        with open(_mouflon_cache_filename) as f:
            try:
                if not isinstance(_mouflon_keys, dict):
                    _mouflon_keys = {}
                _mouflon_keys.update(json.load(f))
                print('Loaded StripChat mouflon key cache')
            except Exception as e:
                print('Error loading mouflon key cache:', e)

    def __init__(self, username, room_id=None):
        if StripChat._static_data is None:
            StripChat._static_data = {}
            try:
                self.getInitialData()
            except Exception as e:
                print('Error initializing StripChat static data:', e)

        super().__init__(username, room_id)
        self._id = None
        self.vr = False
        self.getVideo = lambda _, url, filename: getVideoNativeHLS(self, url, filename, StripChat.m3u_decoder)

    @classmethod
    def getInitialData(cls):
        session = requests.Session()
        r = session.get('https://stripchat.com/api/front/v3/config/static', headers=cls.headers)
        if r.status_code != 200:
            raise Exception("Failed to fetch static data from StripChat")
        StripChat._static_data = r.json().get('static')

        # --- bloomfi3ld: Fetch Doppio.js for mouflon key auto-extraction ---
        try:
            mmp_origin = StripChat._static_data['features']['MMPExternalSourceOrigin']
            mmp_version = StripChat._static_data['featuresV2']['playerModuleExternalLoading']['mmpVersion']
            # mmp_version may already contain 'v' prefix (e.g. 'v2.3.1')
            if mmp_version.startswith('v'):
                mmp_base = f"{mmp_origin}/{mmp_version}"
            else:
                mmp_base = f"{mmp_origin}/v{mmp_version}"
            print(f'[SC] MMP Base: {mmp_base}')

            r = session.get(f"{mmp_base}/main.js", headers=cls.headers)
            if r.status_code != 200:
                raise Exception(f"main.js fetch failed: {r.status_code}")
            StripChat._main_js_data = r.content.decode('utf-8')

            doppio_js_names = re.findall(r'require[(]"./(Doppio.*?[.]js)"[)]', StripChat._main_js_data)
            if not doppio_js_names:
                # Fallback: try alternate quote patterns
                doppio_js_names = re.findall(r'["\']\./(Doppio[^"\']*\.js)["\']', StripChat._main_js_data)
            if not doppio_js_names:
                raise Exception("Could not find Doppio.js reference in main.js")

            doppio_js_name = doppio_js_names[0]
            print(f'[SC] Found Doppio.js: {doppio_js_name}')

            r = session.get(f"{mmp_base}/{doppio_js_name}", headers=cls.headers)
            if r.status_code != 200:
                raise Exception(f"Doppio.js fetch failed: {r.status_code}")
            StripChat._doppio_js_data = r.content.decode('utf-8')

            # Extract keys and persist to cache
            StripChat._populateMouflonKeysFromDoppio()
            if StripChat._mouflon_keys:
                print(f'[SC] Extracted {len(StripChat._mouflon_keys)} mouflon key(s) from Doppio.js')
                try:
                    with open(cls._mouflon_cache_filename, 'w') as f:
                        json.dump(StripChat._mouflon_keys, f)
                    print(f'[SC] Saved mouflon keys to {cls._mouflon_cache_filename}')
                except Exception as e:
                    print(f'[SC] Failed to save mouflon key cache: {e}')
            else:
                print('[SC] WARNING: No mouflon keys extracted from Doppio.js')
        except Exception as e:
            print(f'[SC] Doppio key extraction failed: {e}')
            if cls._mouflon_keys:
                print(f'[SC] Falling back to {len(cls._mouflon_keys)} cached key(s)')
            else:
                print('[SC] No cached keys available either — downloads will likely fail')

    @classmethod
    def _populateMouflonKeysFromDoppio(cls):
        """Extracts pkey:decode_key pairs from Doppio.js using a generic regex.
        Enables dynamic pkey discovery without relying on fixed values."""
        try:
            if not cls._doppio_js_data:
                return
            if cls._mouflon_keys is None:
                cls._mouflon_keys = {}
            pattern = r"\b[A-Za-z0-9]{12,}:[A-Za-z0-9]{12,}\b"
            matches = re.findall(pattern, cls._doppio_js_data)
            for m in matches:
                left, right = m.split(":", 1)
                if left and right and left not in cls._mouflon_keys:
                    cls._mouflon_keys[left] = right
        except Exception:
            pass

    @classmethod
    def m3u_decoder(cls, content):
        _mouflon_file_attr = "#EXT-X-MOUFLON:FILE:"
        _mouflon_filename = 'media.mp4'

        def _decode(encrypted_b64: str, key: str) -> str:
            if cls._cached_keys is None:
                cls._cached_keys = {}
            hash_bytes = cls._cached_keys[key] if key in cls._cached_keys \
                else cls._cached_keys.setdefault(key, hashlib.sha256(key.encode("utf-8")).digest())
            encrypted_data = base64.b64decode(encrypted_b64 + "==")
            return bytes(a ^ b for (a, b) in zip(encrypted_data, itertools.cycle(hash_bytes))).decode("utf-8")

        # Extract Mouflon from M3U8; if pkey is missing or unmapped, choose one detected from Doppio
        psch, pkey = StripChat._getMouflonFromM3U(content)
        if not psch and pkey:
            psch = 'v1'
        # Ensure we have a valid pkey that exists in the detected mapping
        cls._populateMouflonKeysFromDoppio()
        if not pkey or (cls._mouflon_keys and pkey not in cls._mouflon_keys):
            try:
                candidates = list(cls._mouflon_keys.keys()) if cls._mouflon_keys else []
                chosen = None
                for c in candidates:
                    if c.lower().startswith('zokee'):
                        chosen = c
                        break
                if not chosen and candidates:
                    chosen = candidates[0]
                pkey = chosen
            except Exception:
                pkey = None

        def _append_params(url: str) -> str:
            try:
                p = urllib.parse.urlsplit(url)
                if not ('doppiocdn.com' in p.netloc or 'doppiocdn.net' in p.netloc or 'doppiocdn.org' in p.netloc):
                    return url
                q = urllib.parse.parse_qs(p.query, keep_blank_values=True)
                changed = False
                if psch and 'psch' not in q:
                    q['psch'] = [psch]
                    changed = True
                if pkey and 'pkey' not in q:
                    q['pkey'] = [pkey]
                    changed = True
                if not changed:
                    return url
                new_q = urllib.parse.urlencode({k: v[0] for k, v in q.items()})
                return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, new_q, p.fragment))
            except Exception:
                return url

        decoded = ''
        lines = content.splitlines()
        last_decoded_file = None
        for line in lines:
            if line.startswith(_mouflon_file_attr):
                if pkey:
                    pdkey = cls.getMouflonDecKey(pkey)
                    if pdkey:
                        last_decoded_file = _decode(line[len(_mouflon_file_attr):], pdkey)
                    else:
                        last_decoded_file = None
                else:
                    last_decoded_file = None
            elif line.endswith(_mouflon_filename) and last_decoded_file:
                replaced = line.replace(_mouflon_filename, last_decoded_file)
                decoded += _append_params(replaced) + '\n'
                last_decoded_file = None
            elif line.startswith('#EXT-X-MAP:'):
                m = re.search(r'URI="([^"]+)"', line)
                if m:
                    new_uri = _append_params(m.group(1))
                    line = re.sub(r'URI="([^"]+)"', f'URI="{new_uri}"', line)
                decoded += line + '\n'
            elif line.startswith('#EXT-X-PART:'):
                m = re.search(r'URI="([^"]+)"', line)
                if m:
                    new_uri = _append_params(m.group(1))
                    line = re.sub(r'URI="([^"]+)"', f'URI="{new_uri}"', line)
                decoded += line + '\n'
            elif line.startswith('http://') or line.startswith('https://'):
                decoded += _append_params(line) + '\n'
            else:
                decoded += line + '\n'
        return decoded

    @classmethod
    def getMouflonDecKey(cls, pkey):
        if cls._mouflon_keys is None:
            cls._mouflon_keys = {}
        if pkey in cls._mouflon_keys:
            return cls._mouflon_keys[pkey]
        # Try populating from Doppio if not present yet
        cls._populateMouflonKeysFromDoppio()
        if pkey in cls._mouflon_keys:
            return cls._mouflon_keys[pkey]
        # Fallback to specific pattern if the generic did not find it
        if cls._doppio_js_data:
            match = re.findall(f'"{pkey}:(.*?)"', cls._doppio_js_data)
            if match:
                cls._mouflon_keys[pkey] = match[0]
                return match[0]
        # As a last resort, return a decode key for a detected pkey
        candidates = list(cls._mouflon_keys.keys()) if cls._mouflon_keys else []
        for c in candidates:
            if c.lower().startswith('zokee'):
                return cls._mouflon_keys[c]
        if candidates:
            return cls._mouflon_keys[candidates[0]]
        return None

    @staticmethod
    def _getMouflonFromM3U(m3u8_doc):
        if '#EXT-X-MOUFLON:' in m3u8_doc:
            _mouflon_start = m3u8_doc.find('#EXT-X-MOUFLON:')
            if _mouflon_start >= 0:
                _mouflon = m3u8_doc[_mouflon_start:m3u8_doc.find('\n', _mouflon_start)].strip().split(':')
                if len(_mouflon) >= 4:
                    psch = _mouflon[2]
                    pkey = _mouflon[3]
                    return psch, pkey
        return None, None

    def getWebsiteURL(self):
        return "https://stripchat.com/" + self.username

    def getVideoUrl(self):
        return self.getWantedResolutionPlaylist(None)

    def getPlaylistVariants(self, url):
        url = "https://edge-hls.{host}/hls/{id}{vr}/master/{id}{vr}{auto}.m3u8".format(
                host='doppiocdn.' + random.choice(['org', 'com', 'net']),
                id=self.room_id,
                vr='_vr' if self.vr else '',
                auto='_auto' if not self.vr else ''
            )
        headers = dict(self.headers)
        headers.setdefault('Referer', self.getWebsiteURL())
        headers.setdefault('Origin', 'https://stripchat.com')
        result = self.session.get(url, headers=headers, cookies=self.cookies)
        m3u8_doc = result.content.decode("utf-8")

        psch, pkey = StripChat._getMouflonFromM3U(m3u8_doc)
        if not psch and pkey:
            psch = 'v1'

        # Ensure pkey maps to a decode key; override with detected if unmapped/missing
        StripChat._populateMouflonKeysFromDoppio()
        if not pkey or (StripChat._mouflon_keys and pkey not in StripChat._mouflon_keys):
            candidates = list(StripChat._mouflon_keys.keys()) if StripChat._mouflon_keys else []
            chosen = None
            for c in candidates:
                if c.lower().startswith('zokee'):
                    chosen = c
                    break
            if not chosen and candidates:
                chosen = candidates[0]
            pkey = chosen

        pdkey = StripChat.getMouflonDecKey(pkey) if pkey else None
        if pdkey is None:
            self.log('Failed to get mouflon decryption key')
            return []

        variants = super().getPlaylistVariants(m3u_data=m3u8_doc)
        if psch and pkey:
            return [
                variant | {'url': f'{variant["url"]}{"&" if "?" in variant["url"] else "?"}psch={psch}&pkey={pkey}'}
                for variant in variants
            ]
        return variants

    @staticmethod
    def uniq(length=16):
        chars = ''.join(chr(i) for i in range(ord('a'), ord('z')+1))
        chars += ''.join(chr(i) for i in range(ord('0'), ord('9')+1))
        return ''.join(random.choice(chars) for _ in range(length))

    def _getStatusData(self, username):
        r = self.session.get(
            f'https://stripchat.com/api/front/v2/models/username/{username}/cam?uniq={StripChat.uniq()}',
            headers=self.headers
        )

        try:
            data = r.json()
        except requests.exceptions.JSONDecodeError:
            self.log('Failed to parse JSON response')
            return None
        return data

    def _update_lastInfo(self, data):
        if data is None:
            return None
        if 'cam' not in data:
            if 'error' in data:
                error = data['error']
                if error == 'Not Found':
                    return Status.NOTEXIST
                self.logger.warn(f'Status returned error: {error}')
            return Status.UNKNOWN

        self.lastInfo = {'model': data['user']['user']}
        if isinstance(data['cam'], dict):
            self.lastInfo |= data['cam']
        return None

    def getRoomIdFromUsername(self, username):
        if username == self.username and self.room_id is not None:
            return self.room_id

        data = self._getStatusData(username)
        if username == self.username:
            self._update_lastInfo(data)

        if 'user' not in data:
            return None
        if 'user' not in data['user']:
            return None
        if 'id' not in data['user']['user']:
            return None

        return str(data['user']['user']['id'])

    def getStatus(self):
        data = self._getStatusData(self.username)
        if data is None:
            return Status.UNKNOWN

        error = self._update_lastInfo(data)
        if error:
            return error

        if 'user' in data and 'user' in data['user']:
            model_data = data['user']['user']
            if model_data.get('gender'):
                self.gender = StripChat._GENDER_MAP.get(model_data.get('gender'))

            if model_data.get('country'):
                self.country = model_data.get('country', '').upper()
            elif model_data.get('languages'):
                for lang in model_data['languages']:
                    if lang.upper() in COUNTRIES:
                        self.country = lang.upper()
                        break

        status = self.lastInfo['model'].get('status')
        if status == "public" and self.lastInfo["isCamAvailable"] and self.lastInfo["isCamActive"]:
            return Status.PUBLIC
        if status in self._PRIVATE_STATUSES:
            return Status.PRIVATE
        if status in self._OFFLINE_STATUSES:
            return Status.OFFLINE
        if self.lastInfo['model'].get('isDeleted') is True:
            return Status.NOTEXIST
        if data['user'].get('isGeoBanned') is True:
            return Status.RESTRICTED
        self.logger.warn(f'Got unknown status: {status}')
        return Status.UNKNOWN

    @classmethod
    def getStatusBulk(cls, streamers):
        model_ids = {}
        for streamer in streamers:
            if not isinstance(streamer, StripChat):
                continue
            if streamer.room_id:
                model_ids[streamer.room_id] = streamer

        base_url = 'https://stripchat.com/api/front/models/list?'
        batch_num = 100
        data_map = {}
        model_id_list = list(model_ids)
        for _batch_ids in [model_id_list[i:i+batch_num] for i in range(0, len(model_id_list), batch_num)]:
            session = requests.Session()
            session.headers.update(cls.headers)
            r = session.get(base_url + '&'.join(f'modelIds[]={model_id}' for model_id in _batch_ids), timeout=10)

            try:
                data = r.json()
            except requests.exceptions.JSONDecodeError:
                print('Failed to parse JSON response')
                return
            data_map |= {str(model['id']): model for model in data.get('models', [])}

        for model_id, streamer in model_ids.items():
            model_data = data_map.get(model_id)
            if not model_data:
                streamer.setStatus(Status.UNKNOWN)
                continue
            if model_data.get('gender'):
                streamer.gender = cls._GENDER_MAP.get(model_data.get('gender'))
            if model_data.get('country'):
                streamer.country = model_data.get('country', '').upper()
            status = model_data.get('status')
            if status == "public" and model_data.get("isOnline"):
                streamer.setStatus(Status.PUBLIC)
            elif status in cls._PRIVATE_STATUSES:
                streamer.setStatus(Status.PRIVATE)
            elif status in cls._OFFLINE_STATUSES:
                streamer.setStatus(Status.OFFLINE)
            else:
                print(f'[{streamer.siteslug}] {streamer.username}: Bulk update got unknown status: {status}')
                streamer.setStatus(Status.UNKNOWN)
