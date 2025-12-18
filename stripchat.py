import itertools
import json
import os.path
import random
import re
import requests
import base64
import hashlib

from streamonitor.bot import RoomIdBot, LOADED_SITES
from streamonitor.downloaders.hls import getVideoNativeHLS
from streamonitor.enums import Status


class StripChat(RoomIdBot):
    site = 'StripChat'
    siteslug = 'SC'

    bulk_update = True
    _static_data = None
    _main_js_data = None
    _doppio_js_data = None
    _mouflon_cache_filename = 'stripchat_mouflon_keys.json'
    _mouflon_keys: dict = {}
    _cached_keys: dict = {}
    _PRIVATE_STATUSES = frozenset(["private", "groupShow", "p2p", "virtualPrivate", "p2pVoice"])
    _OFFLINE_STATUSES = frozenset(["off", "idle"])

    def __init__(self, username, room_id=None):
        if StripChat._static_data is None:
            try:
                self.getInitialData()
            except Exception as e:
                print(f'Error initializing StripChat: {e}')
                StripChat._static_data = {}

        super().__init__(username, room_id)
        self._id = None
        self.vr = False
        self.getVideo = lambda _, url, filename: getVideoNativeHLS(self, url, filename, StripChat.m3u_decoder)

    @classmethod
    def getInitialData(cls):
        session = requests.Session()
        
        # Add comprehensive browser-like headers
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://stripchat.com/',
            'Origin': 'https://stripchat.com',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
        })
        
        r = session.get('https://hu.stripchat.com/api/front/v3/config/static', timeout=10)
        if r.status_code != 200:
            raise Exception(f"Failed to fetch static data: {r.status_code} - {r.text[:200]}")
        
        cls._static_data = r.json().get('static')

        mmp_origin = cls._static_data['features']['MMPExternalSourceOrigin']
        mmp_version = cls._static_data['featuresV2']['playerModuleExternalLoading']['mmpVersion']
        mmp_base = f"{mmp_origin}/{mmp_version}"

        r = session.get(f"{mmp_base}/main.js", timeout=10)
        if r.status_code != 200:
            raise Exception(f"Failed to fetch main.js: {r.status_code}")
        cls._main_js_data = r.text

        # Find doppio
        doppio_match = re.search(r'([0-9]+):"Doppio"', cls._main_js_data)
        if not doppio_match:
            raise Exception("Could not find Doppio index")
        
        doppio_js_index = doppio_match.group(1)
        hash_match = re.search(f'{doppio_js_index}:\\\\"([a-zA-Z0-9]{{20}})\\\\"', cls._main_js_data)
        if not hash_match:
            raise Exception("Could not find Doppio hash")
        
        doppio_js_hash = hash_match.group(1)

        r = session.get(f"{mmp_base}/chunk-Doppio-{doppio_js_hash}.js", timeout=10)
        if r.status_code != 200:
            raise Exception(f"Failed to fetch doppio.js: {r.status_code}")
        cls._doppio_js_data = r.text

    @classmethod
    def m3u_decoder(cls, content):
        _mouflon_file_attr = "#EXT-X-MOUFLON:FILE:"
        _mouflon_filename = 'media.mp4'

        def _decode(encrypted_b64: str, key: str) -> str:
            if key not in cls._cached_keys:
                cls._cached_keys[key] = hashlib.sha256(key.encode("utf-8")).digest()
            hash_bytes = cls._cached_keys[key]
            encrypted_data = base64.b64decode(encrypted_b64 + "==")
            return bytes(a ^ b for (a, b) in zip(encrypted_data, itertools.cycle(hash_bytes))).decode("utf-8")

        psch, pkey, pdkey = cls._getMouflonFromM3U(content)
        if not pdkey:
            return content

        decoded = ''
        lines = content.splitlines()
        last_decoded_file = None
        for line in lines:
            if line.startswith(_mouflon_file_attr):
                last_decoded_file = _decode(line[len(_mouflon_file_attr):], pdkey)
            elif line.endswith(_mouflon_filename) and last_decoded_file:
                decoded += (line.replace(_mouflon_filename, last_decoded_file)) + '\n'
                last_decoded_file = None
            else:
                decoded += line + '\n'
        return decoded

    @classmethod
    def getMouflonDecKey(cls, pkey):
        if not cls._doppio_js_data:
            print("Warning: Doppio JS data not loaded")
            return None
            
        if pkey in cls._mouflon_keys:
            return cls._mouflon_keys[pkey]
        
        pattern = f'"{pkey}:(.*?)"'
        matches = re.findall(pattern, cls._doppio_js_data)
        if matches:
            pdk = matches[0]
            cls._mouflon_keys[pkey] = pdk
            try:
                with open(cls._mouflon_cache_filename, 'w') as f:
                    json.dump(cls._mouflon_keys, f)
            except:
                pass
            return pdk
        return None

    @staticmethod
    def _getMouflonFromM3U(m3u8_doc):
        _needle = '#EXT-X-MOUFLON:'
        idx = m3u8_doc.find(_needle)
        
        while idx != -1:
            line_end = m3u8_doc.find('\n', idx)
            if line_end == -1:
                line_end = len(m3u8_doc)
            
            line = m3u8_doc[idx:line_end]
            parts = line.split(':')
            
            if len(parts) >= 4:
                psch = parts[2]
                pkey = parts[3]
                pdkey = StripChat.getMouflonDecKey(pkey)
                if pdkey:
                    return psch, pkey, pdkey
            
            idx = m3u8_doc.find(_needle, idx + len(_needle))
        
        return None, None, None

    def getWebsiteURL(self):
        return f"https://stripchat.com/{self.username}"

    def getVideoUrl(self):
        return self.getWantedResolutionPlaylist(None)

    def getPlaylistVariants(self, url):
        if not self.lastInfo or "streamName" not in self.lastInfo:
            return []
            
        url = "https://edge-hls.{host}/hls/{id}{vr}/master/{id}{vr}{auto}.m3u8".format(
            host='doppiocdn.' + random.choice(['org', 'com', 'net']),
            id=self.lastInfo["streamName"],
            vr='_vr' if self.vr else '',
            auto='_auto' if not self.vr else ''
        )
        
        try:
            result = self.session.get(url, headers=self.headers, cookies=self.cookies, timeout=5)
            m3u8_doc = result.text
        except:
            return []
        
        psch, pkey, pdkey = self._getMouflonFromM3U(m3u8_doc)
        if pdkey is None:
            self.log(f'Failed to get mouflon decryption key')
            return []
        
        variants = super().getPlaylistVariants(m3u_data=m3u8_doc)
        return [dict(variant, url=f'{variant["url"]}{"&" if "?" in variant["url"] else "?"}psch={psch}&pkey={pkey}')
                for variant in variants]

    @staticmethod
    def uniq(length=16):
        return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(length))

    def _getStatusData(self, username):
        try:
            r = self.session.get(
                f'https://stripchat.com/api/front/v2/models/username/{username}/cam?uniq={StripChat.uniq()}',
                headers=self.headers,
                timeout=5
            )
            return r.json()
        except:
            return None

    def _update_lastInfo(self, data):
        if data is None:
            return Status.UNKNOWN
        if 'cam' not in data:
            if data.get('error') == 'Not Found':
                return Status.NOTEXIST
            return Status.UNKNOWN

        self.lastInfo = {'model': data['user']['user']}
        if isinstance(data['cam'], dict):
            self.lastInfo.update(data['cam'])
        return None

    def getRoomIdFromUsername(self, username):
        if username == self.username and self.room_id:
            return self.room_id

        data = self._getStatusData(username)
        if not data or 'user' not in data:
            return None
        
        user_id = data.get('user', {}).get('user', {}).get('id')
        return str(user_id) if user_id else None

    def getStatus(self):
        data = self._getStatusData(self.username)
        error = self._update_lastInfo(data)
        if error:
            return error

        status = self.lastInfo['model'].get('status')
        if status == "public" and self.lastInfo.get("isCamAvailable") and self.lastInfo.get("isCamActive"):
            return Status.PUBLIC
        if status in self._PRIVATE_STATUSES:
            return Status.PRIVATE
        if status in self._OFFLINE_STATUSES:
            return Status.OFFLINE
        if self.lastInfo['model'].get('isDeleted'):
            return Status.NOTEXIST
        if data.get('user', {}).get('isGeoBanned'):
            return Status.RESTRICTED
        return Status.UNKNOWN

    @classmethod
    def getStatusBulk(cls, streamers):
        model_ids = {}
        for streamer in streamers:
            if isinstance(streamer, StripChat) and streamer.room_id:
                model_ids[streamer.room_id] = streamer

        if not model_ids:
            return

        url = 'https://hu.stripchat.com/api/front/models/list?' + '&'.join(f'modelIds[]={mid}' for mid in model_ids)
        
        try:
            session = requests.Session()
            session.headers.update(cls.headers)
            r = session.get(url, timeout=5)
            data = r.json()
        except:
            return

        data_map = {str(m['id']): m for m in data.get('models', [])}

        for model_id, streamer in model_ids.items():
            model_data = data_map.get(model_id)
            if not model_data:
                streamer.setStatus(Status.UNKNOWN)
                continue
            
            status = model_data.get('status')
            if status == "public" and model_data.get("isOnline"):
                streamer.setStatus(Status.PUBLIC)
            elif status in cls._PRIVATE_STATUSES:
                streamer.setStatus(Status.PRIVATE)
            elif status in cls._OFFLINE_STATUSES:
                streamer.setStatus(Status.OFFLINE)
            else:
                streamer.setStatus(Status.UNKNOWN)

LOADED_SITES.add(StripChat)
