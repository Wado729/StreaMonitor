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
    _mouflon_keys: dict = None
    _cached_keys: dict[str, bytes] = None
    _PRIVATE_STATUSES = frozenset(["private", "groupShow", "p2p", "virtualPrivate", "p2pVoice"])
    _OFFLINE_STATUSES = frozenset(["off", "idle"])

    _GENDER_MAP = {
        'female': Gender.FEMALE,
        'male': Gender.MALE,
        'maleFemale': Gender.BOTH
    }

    # ── Mouflon Key Configuration ──────────────────────────────────────────
    #
    # StripChat encrypts HLS segment URLs using a pkey/pdkey scheme (v2 PSCH).
    # Keys are NOT extractable from JS source code — they only exist as runtime
    # variables inside webpack closure scope during stream initialization.
    #
    # To extract fresh keys, use Chrome/Firefox DevTools:
    #   1. Open a StripChat stream and press F12
    #   2. Sources tab → find chunk file under img.doppiocdn.com
    #   3. Pretty-print, search for _onPlaylistLoadingStateChanged
    #   4. Set breakpoint on that function
    #   5. When breakpoint hits, check Scope → Closure for 16-char alphanumeric values
    #   6. The pkey appears in the m3u8 playlist; the pdkey is in the closure variables
    #
    # See: https://github.com/aitschti/plugin.video.sc19/issues/19
    #
    # Configure your keys in 'mouflon_keys.json' next to this script:
    #   { "pkey": "YourPkeyHere1234", "pdkey": "YourPdkeyHere567" }
    #
    # Or as a dict of multiple pkey→pdkey mappings:
    #   { "Pkey1here1234567": "Pdkey1here123456", "Pkey2here1234567": "Pdkey2here123456" }

    _MOUFLON_CONFIG_FILENAME = 'mouflon_keys.json'

    # Hardcoded fallback keys — these WILL eventually stop working when SC rotates.
    # Last updated: Feb 9, 2026. If downloads fail, extract fresh keys via DevTools.
    _FALLBACK_KEYS = {
        'Ook7quaiNgiyuhai': '$iPRUU0AnxoOSif9',   # current active key as of Feb 2026
        'Zeechoej4aleeshi': 'ubahjae7goPoodi6',   # deprecated ~Jan 9 2026
    }

    _keys_loaded = False
    _keys_warned = False

    @classmethod
    def _loadMouflonKeys(cls):
        """Load mouflon pkey/pdkey from config file, with fallback to hardcoded keys."""
        if cls._keys_loaded:
            return
        cls._keys_loaded = True

        if cls._mouflon_keys is None:
            cls._mouflon_keys = {}

        # 1. Try loading from config file
        config_path = cls._MOUFLON_CONFIG_FILENAME
        if os.path.exists(config_path):
            try:
                with open(config_path) as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    # Format A: { "pkey": "...", "pdkey": "..." }
                    if 'pkey' in data and 'pdkey' in data:
                        cls._mouflon_keys[data['pkey']] = data['pdkey']
                        print(f'[SC] Loaded mouflon key from {config_path}: pkey={data["pkey"][:4]}...')
                    else:
                        # Format B: { "pkey1": "pdkey1", "pkey2": "pdkey2", ... }
                        for k, v in data.items():
                            if isinstance(k, str) and isinstance(v, str) and len(k) >= 12 and len(v) >= 12:
                                cls._mouflon_keys[k] = v
                        if cls._mouflon_keys:
                            print(f'[SC] Loaded {len(cls._mouflon_keys)} mouflon key(s) from {config_path}')
            except Exception as e:
                print(f'[SC] Error loading {config_path}: {e}')

        # 2. Add fallback keys (lower priority — config keys checked first)
        for k, v in cls._FALLBACK_KEYS.items():
            if k not in cls._mouflon_keys:
                cls._mouflon_keys[k] = v

        if not cls._mouflon_keys:
            cls._printKeyHelp()

    @classmethod
    def _printKeyHelp(cls):
        """Print instructions for obtaining mouflon keys."""
        if cls._keys_warned:
            return
        cls._keys_warned = True
        print('')
        print('=' * 72)
        print('[SC] ERROR: No mouflon decryption keys configured!')
        print('')
        print('  StripChat encrypts stream URLs. Keys must be extracted manually')
        print('  using browser DevTools (they cannot be auto-extracted from JS).')
        print('')
        print('  Quick fix: Create "mouflon_keys.json" with:')
        print('    { "pkey": "YOUR_PKEY_HERE", "pdkey": "YOUR_PDKEY_HERE" }')
        print('')
        print('  To extract keys:')
        print('    1. Open a StripChat stream in Chrome/Firefox, press F12')
        print('    2. Sources tab -> find chunk file under img.doppiocdn.com')
        print('    3. Pretty-print -> search "_onPlaylistLoadingStateChanged"')
        print('    4. Set breakpoint -> when hit, check Scope -> Closure')
        print('    5. Look for 16-char alphanumeric pkey/pdkey values')
        print('')
        print('  Full guide: https://github.com/aitschti/plugin.video.sc19/issues/19')
        print('=' * 72)
        print('')

    def __init__(self, username, room_id=None):
        if StripChat._static_data is None:
            StripChat._static_data = {}
            try:
                self.getInitialData()
            except Exception as e:
                print('Error initializing StripChat static data:', e)

        # Load mouflon keys on first instantiation
        StripChat._loadMouflonKeys()

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

    @classmethod
    def getMouflonDecKey(cls, pkey):
        """Look up the pdkey for a given pkey from configured keys."""
        cls._loadMouflonKeys()

        if not pkey:
            return None

        # Direct lookup
        if pkey in cls._mouflon_keys:
            return cls._mouflon_keys[pkey]

        # If the playlist pkey doesn't match any configured key, try using
        # any available key — SC may have rotated which pkey is active
        if cls._mouflon_keys:
            # Prefer non-fallback keys (user-configured are added first)
            for k, v in cls._mouflon_keys.items():
                if k not in cls._FALLBACK_KEYS:
                    print(f'[SC] WARNING: Playlist pkey "{pkey[:4]}..." not in config, trying configured key "{k[:4]}..."')
                    return v
            # Last resort: try a fallback key
            first_key = next(iter(cls._mouflon_keys))
            print(f'[SC] WARNING: Using fallback key "{first_key[:4]}..." -- this may be outdated!')
            print(f'[SC] If downloads fail, extract fresh keys via DevTools (see mouflon_keys.json)')
            return cls._mouflon_keys[first_key]

        cls._printKeyHelp()
        return None

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

        # Extract psch/pkey from playlist
        psch, pkey = StripChat._getMouflonFromM3U(content)
        if not psch and pkey:
            psch = 'v1'

        # Get pdkey for the pkey found in the playlist
        pdkey = cls.getMouflonDecKey(pkey) if pkey else None

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
                if pkey and pdkey:
                    try:
                        last_decoded_file = _decode(line[len(_mouflon_file_attr):], pdkey)
                    except Exception as e:
                        print(f'[SC] Mouflon decode error: {e} -- keys may be wrong/expired')
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

        # Resolve the pkey to use — prefer playlist's pkey if we have its pdkey,
        # otherwise fall back to whatever key we have configured
        pdkey = StripChat.getMouflonDecKey(pkey) if pkey else None
        if pdkey is None and pkey:
            # Playlist has a pkey we don't recognize; try any configured key
            if StripChat._mouflon_keys:
                alt_pkey = next(iter(StripChat._mouflon_keys))
                pdkey = StripChat._mouflon_keys[alt_pkey]
                pkey = alt_pkey
                print(f'[SC] Overriding playlist pkey with configured key "{pkey[:4]}..."')

        if pdkey is None:
            self.log('Failed to get mouflon decryption key -- see mouflon_keys.json instructions above')
            StripChat._printKeyHelp()
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
