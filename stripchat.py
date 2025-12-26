import re
import time
import requests
import base64
import hashlib
import random
import itertools
import json
import os
from functools import lru_cache
from typing import Optional, Tuple, List, Dict

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from streamonitor.bot import Bot
from streamonitor.downloaders.hls import getVideoNativeHLS
from streamonitor.enums import Status
from streamonitor.utils.CloudflareDetection import looks_like_cf_html


class StripChat(Bot):
    site = 'StripChat'
    siteslug = 'SC'

    _static_data = None
    _main_js_data = None
    _doppio_js_data = None
    _mouflon_keys: dict = None
    _mouflon_pkey: str = None
    _mouflon_pdkey: str = None
    _cached_keys: dict[str, bytes] = None
    _session = None

    # Pre-compiled regex patterns
    _DOPPIO_INDEX_PATTERN = re.compile(r'([0-9]+):"Doppio"')
    _DOPPIO_REQUIRE_PATTERN = re.compile(r'require\(["\']\./(Doppio[^"\']+\.js)["\']\)')
    # New webpack chunk pattern: looks for DoppioWrapper being loaded from chunk
    # Pattern: Promise.all([n.e(149),n.e(184)]).then(n.bind(n,4184))).DoppioWrapper
    _DOPPIO_CHUNK_PATTERN = re.compile(r'n\.e\((\d+)\)\]\)\.then\(n\.bind\(n,\d+\)\)\)\.DoppioWrapper')
    # Chunk hash mapping pattern: n.u=e=>"chunk-"+{149:"hash",184:"hash",...}[e]+".js"
    _CHUNK_HASH_PATTERN = re.compile(r'n\.u=e=>"chunk-"\+\{([^}]+)\}\[e\]\+"\.js"')
    
    # Constants
    _MOUFLON_NEEDLE = "#EXT-X-MOUFLON:"
    _MOUFLON_FILE_ATTR = "#EXT-X-MOUFLON:FILE:"
    _MOUFLON_FILENAME = "media.mp4"
    _CDN_DOMAINS = ("org", "com", "net")
    _CHARSET = "abcdefghijklmnopqrstuvwxyz0123456789"
    
    # Hardcoded fallback mouflon keys (verified working as of v2.1.1)
    # These are used when dynamic extraction fails due to obfuscation changes
    _FALLBACK_PKEY = "Zeechoej4aleeshi"
    _FALLBACK_PDKEY = "ubahjae7goPoodi6"
    
    # Status sets for O(1) lookup
    # groupShow (ticket shows) - stream IS accessible, blur is only on the website player
    _PRIVATE_STATUSES = frozenset(["private", "p2p", "virtualPrivate", "p2pVoice"])
    _TICKET_SHOW_STATUSES = frozenset(["groupShow"])  # Recordable!
    _OFFLINE_STATUSES = frozenset(["off", "idle"])

    def __init__(self, username):
        if StripChat._static_data is None:
            StripChat._static_data = {}
            try:
                StripChat.getInitialData()
            except Exception as e:
                StripChat._static_data = None
                raise e
        
        # Fast wait with timeout
        end_time = time.time() + 15
        while StripChat._static_data == {} and time.time() < end_time:
            time.sleep(0.05)
        
        if StripChat._static_data == {}:
            raise TimeoutError("Static data initialization timeout")
        
        super().__init__(username)
        self.vr = False
        self.getVideo = lambda _, url, filename: getVideoNativeHLS(self, url, filename, StripChat.m3u_decoder)

    def get_site_color(self):
        """Return the color scheme for this site"""
        return ("green", [])

    @classmethod
    def _get_session(cls):
        """Optimized session with connection pooling"""
        if cls._session is None:
            cls._session = requests.Session()
            
            retry = Retry(
                total=2,
                backoff_factor=0.1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            
            adapter = HTTPAdapter(
                max_retries=retry,
                pool_connections=15,
                pool_maxsize=30,
                pool_block=False
            )
            
            cls._session.mount("http://", adapter)
            cls._session.mount("https://", adapter)
            cls._session.headers.update({
                'Connection': 'keep-alive',
                'Accept-Encoding': 'gzip, deflate',
            })
            
            # Proxy support
            if http_proxy := os.getenv('HTTP_PROXY'):
                cls._session.proxies['http'] = http_proxy
            if https_proxy := os.getenv('HTTPS_PROXY'):
                cls._session.proxies['https'] = https_proxy
        
        return cls._session

    @classmethod
    def getInitialData(cls):
        """Fetch static configuration and parse mouflon keys from Doppio JS."""
        s = cls._get_session()
        
        r = s.get('https://hu.stripchat.com/api/front/v3/config/static', headers=cls.headers, timeout=5)
        if r.status_code != 200:
            raise Exception("Failed to fetch static data from StripChat")
        StripChat._static_data = r.json().get('static')

        mmp_origin = StripChat._static_data['features']['MMPExternalSourceOrigin']
        mmp_version = StripChat._static_data['featuresV2']['playerModuleExternalLoading']['mmpVersion']
        # mmpVersion may already include 'v' prefix (e.g., 'v2.1.1'), so check before adding
        if mmp_version.startswith('v'):
            mmp_base = f"{mmp_origin}/{mmp_version}"
        else:
            mmp_base = f"{mmp_origin}/v{mmp_version}"

        r = s.get(f"{mmp_base}/main.js", headers=cls.headers, timeout=5)
        if r.status_code != 200:
            raise Exception("Failed to fetch main.js from StripChat")
        StripChat._main_js_data = r.text

        # Find Doppio JS file
        doppio_js_name = None
        
        # Try direct require pattern first (legacy)
        if match := cls._DOPPIO_REQUIRE_PATTERN.search(StripChat._main_js_data):
            doppio_js_name = match[1]
        # Try new webpack chunk pattern: n.e(184)...DoppioWrapper
        elif match := cls._DOPPIO_CHUNK_PATTERN.search(StripChat._main_js_data):
            chunk_id = match[1]
            # Find the chunk hash mapping
            if hash_match := cls._CHUNK_HASH_PATTERN.search(StripChat._main_js_data):
                chunk_mapping = hash_match[1]
                # Parse the mapping to find the hash for our chunk_id
                # Format: 149:"hash1",184:"hash2",...
                for mapping in chunk_mapping.split(','):
                    if ':' in mapping:
                        cid, chash = mapping.split(':', 1)
                        if cid.strip() == chunk_id:
                            # Remove quotes from hash
                            chash = chash.strip().strip('"')
                            doppio_js_name = f"chunk-{chash}.js"
                            break
        elif match := cls._DOPPIO_INDEX_PATTERN.search(StripChat._main_js_data):
            idx = match[1]
            # Look for hash in various formats
            for pattern in [
                rf'{idx}:\\"([a-zA-Z0-9]{{20}})\\"',
                rf'{idx}:"([a-zA-Z0-9]{{20}})"',
                rf'"{idx}":"([a-zA-Z0-9]{{20}})"',
            ]:
                if hash_match := re.search(pattern, StripChat._main_js_data):
                    doppio_js_name = f"chunk-Doppio-{hash_match[1]}.js"
                    break
        
        if not doppio_js_name:
            raise Exception("Could not find Doppio JS file in main.js")

        r = s.get(f"{mmp_base}/{doppio_js_name}", headers=cls.headers, timeout=5)
        if r.status_code != 200:
            raise Exception("Failed to fetch doppio.js from StripChat")
        StripChat._doppio_js_data = r.text
        
        # Parse mouflon keys
        cls._parseMouflonKeys()
        
        if cls._mouflon_keys:
            cls._mouflon_pkey = next(iter(cls._mouflon_keys.keys()))
            cls._mouflon_pdkey = cls._mouflon_keys[cls._mouflon_pkey]
        else:
            # Use hardcoded fallback keys
            print("[StripChat] Using hardcoded fallback mouflon keys")
            cls._mouflon_pkey = cls._FALLBACK_PKEY
            cls._mouflon_pdkey = cls._FALLBACK_PDKEY
            cls._mouflon_keys = {cls._FALLBACK_PKEY: cls._FALLBACK_PDKEY}

    @classmethod
    def _parseMouflonKeys(cls):
        """
        Extract mouflon pkey/pdkey from Doppio JS.
        
        The keys are built from a specific formula in the obfuscated JS:
        - Base36 conversions of fixed numbers
        - Character shifts
        - IIFEs that decode numeric arguments
        
        Current known formula (v2.0.10):
        pkey = "Zeechoej4aleeshi"
        pdkey = "ubahjae7goPoodi6"
        """
        if cls._mouflon_keys is None:
            cls._mouflon_keys = {}
        
        js_data = cls._doppio_js_data
        if not js_data:
            return
        
        try:
            # Try to extract keys from the ns expression
            keys = cls._extractNsKeys(js_data)
            if keys:
                cls._mouflon_keys[keys[0]] = keys[1]
                return
            
            # Fallback to legacy patterns
            cls._parseLegacyMouflonKeys()
            
        except Exception as e:
            print(f"[StripChat] Warning: Failed to parse mouflon keys: {e}")
            cls._parseLegacyMouflonKeys()

    @classmethod
    def _extractNsKeys(cls, js_data):
        """
        Extract pkey/pdkey from the Jn/ss/ns expression in Doppio JS.
        
        v2.1.3 key construction (const Jn=):
        - pkey: IIFE(45,196,195,...) + 36918.toString(36) = "Zeechoej4aleeshi"
        - pdkey: 0xaf004b1e62348.toString(36) + shifted(32) + 24.toString(36) + IIFE_first4 = "ubahjae7goPoodi6"
        
        The keys are built from:
        1. Fixed numbers converted to base36
        2. Character shifts (-39)
        3. IIFE patterns with specific offsets
        """
        
        def to_base36(n):
            chars = '0123456789abcdefghijklmnopqrstuvwxyz'
            if n == 0:
                return '0'
            result = ''
            while n > 0:
                result = chars[n % 36] + result
                n //= 36
            return result
        
        def shift_chars(s, offset):
            return ''.join(chr(ord(c) + offset) for c in s)
        
        def decode_iife_v213(args, offset=38):
            """Decode v2.1.3 IIFE: first arg is offset, reverse remaining, then (a - first - offset) - i"""
            first = args[0]
            remaining = args[1:][::-1]
            return ''.join(chr((a - first - offset) - i) for i, a in enumerate(remaining))
        
        def decode_iife_v213_pdkey(args, offset=39):
            """Decode v2.1.3 pdkey IIFE: (a - first - offset) - i"""
            first = args[0]
            remaining = args[1:][::-1]
            return ''.join(chr((a - first - offset) - i) for i, a in enumerate(remaining))
        
        # Check for v2.1.3 'const Jn=' pattern
        if 'const Jn=' in js_data:
            try:
                start = js_data.find('const Jn=')
                if start != -1:
                    chunk = js_data[start:start+3000]
                    
                    # Find all IIFEs in the chunk - they appear as }(num,num,num,...)
                    all_iifes = re.findall(r'\}\((\d+(?:,\d+)+)\)', chunk)
                    iifes = []
                    for iife_str in all_iifes:
                        args = [int(x) for x in iife_str.split(',')]
                        iifes.append(args)
                    
                    # First IIFE (14 args starting with ~45) is for pkey
                    pkey_part1 = ''
                    for args in iifes:
                        if len(args) >= 10 and len(args) <= 16 and 40 <= args[0] <= 50:
                            pkey_part1 = decode_iife_v213(args, 38)
                            break
                    
                    # Find 36918.toString(36) for "shi"
                    if '36918' in chunk:
                        pkey_part2 = to_base36(36918)
                    else:
                        pkey_part2 = ''
                    
                    # Find the large hex number for pdkey: 0xaf004b1e62348 or decimal equivalent
                    pdkey_part1 = ''
                    hex_match = re.search(r'0x([0-9a-fA-F]+)', chunk)
                    if hex_match:
                        hex_val = int(hex_match.group(1), 16)
                        pdkey_part1 = to_base36(hex_val)
                    else:
                        # Try decimal: look for large number > 100000000000
                        for m in re.finditer(r'\b(\d{12,16})\b', chunk):
                            n = int(m.group(1))
                            if n > 100000000000:
                                pdkey_part1 = to_base36(n)
                                break
                    
                    # 32 shifted by -39 for 'P'
                    pdkey_part2 = shift_chars(to_base36(32), -39) if '32' in chunk else ''
                    
                    # 24.toString(36) for 'o'
                    pdkey_part3 = to_base36(24) if '24' in chunk else ''
                    
                    # Second IIFE (19-20 args starting with ~42) is for pdkey 'odi6' part
                    pdkey_part4 = ''
                    for args in iifes:
                        if len(args) >= 18 and len(args) <= 22 and 40 <= args[0] <= 45:
                            pdkey_part4 = decode_iife_v213_pdkey(args, 39)[:4]  # Only first 4 chars
                            break
                    
                    pkey = pkey_part1 + pkey_part2
                    pdkey = pdkey_part1 + pdkey_part2 + pdkey_part3 + pdkey_part4
                    
                    # Both keys should be 16 characters
                    if len(pkey) == 16 and len(pdkey) == 16:
                        print(f"[StripChat] Extracted v2.1.3 keys: pkey={pkey}, pdkey={pdkey}")
                        return (pkey, pdkey)
                    elif len(pkey) >= 12 and len(pdkey) >= 12:
                        print(f"[StripChat] Partially extracted keys: pkey={pkey}({len(pkey)}), pdkey={pdkey}({len(pdkey)})")
                        return (pkey, pdkey)
                        
            except Exception as e:
                print(f"[StripChat] v2.1.3 key extraction failed: {e}")
        
        # Helper for older patterns
        def decode_iife(args, offset):
            """Decode IIFE: reverse args[1:], then (a - args[0] - offset) - i"""
            first = args[0]
            remaining = args[1:][::-1]
            return ''.join(chr((a - first - offset) - i) for i, a in enumerate(remaining))
        
        # Check for the 'const ss=' pattern (v2.1.1+)
        if 'const ss=' in js_data:
            try:
                # Find the ss= area
                start = js_data.find('const ss=(')
                if start == -1:
                    start = js_data.find('const ss=')
                
                if start != -1:
                    chunk = js_data[start:start+10000]
                    
                    # Extract all numbers in toString(36) calls
                    # Format 1: 16..toString(36)
                    # Format 2: 1128328536208[Jt(0,0,0,561)](36)
                    numbers = {}
                    for m in re.finditer(r'(\d+)\.\.toString\(36\)', chunk):
                        numbers[int(m.group(1))] = to_base36(int(m.group(1)))
                    for m in re.finditer(r'(\d+)\[[A-Za-z]+\([^)]+\)\]\(36\)', chunk):
                        numbers[int(m.group(1))] = to_base36(int(m.group(1)))
                    
                    # Find IIFE patterns: }(num,num,num,...)
                    iifes = []
                    for m in re.finditer(r'\}\((\d+(?:,\d+)+)\)', chunk[:5000]):
                        args = [int(x) for x in m.group(1).split(',')]
                        if 2 <= len(args) <= 15:  # Reasonable IIFE size
                            iifes.append(args)
                    
                    # Build pkey using known pattern
                    # 16 >> -13 = 'Z'
                    p1 = shift_chars(to_base36(16), -13) if 16 in numbers else ''
                    
                    # Large number for 'eechoej4'
                    p2 = ''
                    for n in numbers:
                        if n > 1000000000000:  # Large number
                            p2 = numbers[n]
                            break
                    
                    # IIFE for 'ale' (offset 11, 3-4 args)
                    p3 = ''
                    for args in iifes:
                        if len(args) == 4 and 30 <= args[0] <= 40:
                            decoded = decode_iife(args, 11)
                            if decoded.isalpha() and decoded.islower():
                                p3 = decoded
                                break
                    
                    # 690102 = 'eshi'
                    p4 = numbers.get(690102, '')
                    
                    # Build pdkey
                    # 39286 = 'uba'
                    p5 = numbers.get(39286, '')
                    
                    # IIFE for 'hjae' (offset 10, 5 args)
                    p6 = ''
                    for args in iifes:
                        if len(args) == 5 and 60 <= args[0] <= 65:
                            decoded = decode_iife(args, 10)
                            if decoded.isalpha() and decoded.islower():
                                p6 = decoded
                                break
                    
                    # 9672 = '7go'
                    p7 = numbers.get(9672, '')
                    
                    # 32 >> -39 = 'P'
                    p8 = shift_chars(to_base36(32), -39) if 32 in numbers else ''
                    
                    # 888 = 'oo'
                    p9 = numbers.get(888, '')
                    
                    # IIFE for 'di' (offset 39, 3 args)
                    p10 = ''
                    for args in iifes:
                        if len(args) == 3 and 40 <= args[0] <= 50:
                            decoded = decode_iife(args, 39)
                            if decoded.isalpha() and decoded.islower():
                                p10 = decoded
                                break
                    
                    # 6 = '6'
                    p11 = numbers.get(6, '')
                    
                    pkey = p1 + p2 + p3 + p4
                    pdkey = p5 + p6 + p7 + p8 + p9 + p10 + p11
                    
                    if len(pkey) >= 12 and len(pdkey) >= 12:
                        print(f"[StripChat] Extracted keys: pkey={pkey}, pdkey={pdkey}")
                        return (pkey, pdkey)
                    
            except Exception as e:
                print(f"[StripChat] v2.1.1 key extraction failed: {e}")
        
        # Try legacy 'const ns=' pattern
        if 'const ns=' in js_data:
            try:
                # Find the two IIFEs with their arguments
                iife1_match = re.search(r'\}\((\d+(?:,\d+){8,12})\)', js_data)
                iife2_match = re.search(r'\}\((\d{2},\d{3},\d{3})\)', js_data)
                
                if iife1_match and iife2_match:
                    args1 = [int(x) for x in iife1_match.group(1).split(',')]
                    n1 = args1[0]
                    remaining1 = args1[1:][::-1]
                    p4 = ''.join(chr((a - n1 - 26) - i) for i, a in enumerate(remaining1))
                    
                    args2 = [int(x) for x in iife2_match.group(1).split(',')]
                    o2 = args2[0]
                    remaining2 = args2[1:][::-1]
                    p8 = ''.join(chr(((a - o2) - 56) - i) for i, a in enumerate(remaining2))
                    
                    p1 = ''.join(chr(ord(c) - 13) for c in to_base36(16))
                    p2 = to_base36(0x531f77594da7d).lower()
                    p3 = to_base36(18676).lower()
                    p5 = to_base36(662856).lower()
                    p6 = ''.join(chr(ord(c) - 39) for c in to_base36(32).lower())
                    p7 = to_base36(31981).lower()
                    
                    key_string = p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8
                    
                    if ':' in key_string:
                        pkey, pdkey = key_string.split(':', 1)
                        if len(pkey) >= 8 and len(pdkey) >= 8:
                            return (pkey, pdkey)
            except Exception as e:
                print(f"[StripChat] Legacy key extraction failed: {e}")
        
        return None

    @classmethod
    def _parseLegacyMouflonKeys(cls):
        """Fallback: Parse legacy format 'pkey:pdkey' from Doppio JS."""
        if cls._doppio_js_data:
            legacy_pattern = r'"(\w{8,24}):(\w{8,24})"'
            matches = re.findall(legacy_pattern, cls._doppio_js_data)
            for pkey, pdkey in matches:
                cls._mouflon_keys[pkey] = pdkey

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

        psch, pkey, pdkey = StripChat._getMouflonFromM3U(content)

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
        if cls._mouflon_keys is None:
            cls._mouflon_keys = {}
        
        # Check if we already have the key cached
        if pkey in cls._mouflon_keys:
            return cls._mouflon_keys[pkey]
        
        # Try legacy format: "pkey:pdkey"
        if cls._doppio_js_data:
            _pdks = re.findall(f'"{pkey}:(.*?)"', cls._doppio_js_data)
            if len(_pdks) > 0:
                return cls._mouflon_keys.setdefault(pkey, _pdks[0])
        
        # If not found, try to re-parse the keys with the specific pkey
        # This handles cases where the pkey wasn't in the initial parse
        if cls._doppio_js_data and cls._ln_array:
            # Search for the pkey in the string array
            if pkey in cls._ln_array:
                pkey_idx = cls._ln_array.index(pkey)
                # The pdkey is often adjacent or at a related offset
                # Try common patterns
                for offset in [1, -1, 2, -2]:
                    pdkey_idx = pkey_idx + offset
                    if 0 <= pdkey_idx < len(cls._ln_array):
                        pdkey = cls._ln_array[pdkey_idx]
                        if pdkey and pdkey.isalnum() and len(pdkey) >= 8:
                            return cls._mouflon_keys.setdefault(pkey, pdkey)
        
        return None

    @classmethod
    def _getMouflonFromM3U(cls, m3u8_doc):
        """
        Extract psch, pkey, pdkey from m3u8 MOUFLON tags.
        Uses cached class variables for pkey/pdkey.
        """
        # Return cached keys directly - they were extracted once at startup
        if cls._mouflon_pkey and cls._mouflon_pdkey:
            return 'v1', cls._mouflon_pkey, cls._mouflon_pdkey
        
        # Keys missing - try to re-extract
        if cls._doppio_js_data:
            print("[StripChat] Keys missing in _getMouflonFromM3U, attempting re-extraction...")
            cls._parseMouflonKeys()
            if cls._mouflon_keys:
                cls._mouflon_pkey = next(iter(cls._mouflon_keys.keys()))
                cls._mouflon_pdkey = cls._mouflon_keys[cls._mouflon_pkey]
                print(f"[StripChat] Re-extracted: pkey={cls._mouflon_pkey}, pdkey={cls._mouflon_pdkey}")
                return 'v1', cls._mouflon_pkey, cls._mouflon_pdkey
        
        # Fallback to hardcoded keys if all else fails
        print("[StripChat] Using hardcoded fallback keys in _getMouflonFromM3U")
        cls._mouflon_pkey = cls._FALLBACK_PKEY
        cls._mouflon_pdkey = cls._FALLBACK_PDKEY
        cls._mouflon_keys = {cls._FALLBACK_PKEY: cls._FALLBACK_PDKEY}
        return 'v1', cls._mouflon_pkey, cls._mouflon_pdkey

    @staticmethod
    def uniq():
        """Generate a random unique string for API requests."""
        chars = ''.join(chr(i) for i in range(ord('a'), ord('z') + 1))
        chars += ''.join(chr(i) for i in range(ord('0'), ord('9') + 1))
        return ''.join(random.choice(chars) for _ in range(16))

    @staticmethod
    def normalizeInfo(raw: dict) -> dict:
        """
        Normalize JSON so lastInfo is always a dict.
        Keep top-level where possible; flatten only obvious wrappers like `item` or single-element lists.
        """
        if not raw:
            return {}
        # If it's a list, use first element (common wrapping)
        if isinstance(raw, list):
            return raw[0] if raw else {}
        # If it's a dict with 'item' containing the real object
        if isinstance(raw, dict) and "item" in raw and isinstance(raw["item"], dict):
            return raw["item"]
        # If it's a dict with a single top wrapper like {"data": {...}} try common keys but avoid dropping cam/user
        if isinstance(raw, dict) and len(raw) == 1:
            sole_key = next(iter(raw))
            if sole_key in ("data", "result", "response") and isinstance(raw[sole_key], dict):
                return raw[sole_key]
        # Otherwise return as-is (we'll handle nested shapes in getters)
        return raw

    def _get_by_path(self, data: dict, path: list):
        """
        Safely get nested value by path list from dict-like structures.
        Returns None if any step is missing or type mismatch.
        """
        cur = data
        for p in path:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return None
        return cur

    def _recursive_find(self, data, key):
        """
        Depth-first search for first occurrence of `key` in nested dict/list objects.
        Returns the value if found, otherwise None.
        """
        if isinstance(data, dict):
            if key in data:
                return data[key]
            for v in data.values():
                found = self._recursive_find(v, key)
                if found is not None:
                    return found
        elif isinstance(data, list):
            for item in data:
                found = self._recursive_find(item, key)
                if found is not None:
                    return found
        return None

    def _first_in_paths(self, paths: list):
        """
        Try a list of paths (each path is a list of keys). Return first non-None value.
        """
        for p in paths:
            val = self._get_by_path(self.lastInfo, p)
            if val is not None:
                return val
        return None
    

    def getWebsiteURL(self):
        return "https://stripchat.com/" + self.username

    def getVideoUrl(self):
        return self.getWantedResolutionPlaylist(None)

    def getStreamName(self) -> str:
        """
        Robust streamName detector: checks multiple common locations and finally
        performs a recursive search for the key if not found.
        Raises KeyError if nothing found.
        """
        if not self.lastInfo:
            raise KeyError("lastInfo is empty, call getStatus() first")

        # Common paths to check in order
        paths = [
            ["streamName"],
            ["cam", "streamName"],
            ["user", "streamName"],
            ["user", "user", "streamName"],
            ["model", "streamName"],
            ["user", "user", "userStreamName"],
            ["cam", "userStreamName"],
        ]
        val = self._first_in_paths(paths)
        if val:
            return str(val)

        # Last resort: recursive find
        val = self._recursive_find(self.lastInfo, "streamName")
        if val:
            return str(val)

        raise KeyError(f"No streamName in lastInfo: keys={list(self.lastInfo.keys())}")

    def getStatusField(self):
        """
        Robust status detector. Tries known paths and falls back to recursive search.
        Returns the raw status value or None.
        """
        if not self.lastInfo:
            return None

        # Explicit candidate paths (ordered)
        paths = [
            ["status"],
            ["cam", "streamStatus"],
            ["cam", "status"],
            ["model", "status"],
            ["user", "status"],
            ["user", "user", "status"],
            ["user", "user", "state"],
            ["user", "user", "broadcastStatus"],
        ]
        status = self._first_in_paths(paths)
        if status is not None:
            return status

        # Recursive fallback — but prefer strings that match expected status tokens
        found = self._recursive_find(self.lastInfo, "status")
        if isinstance(found, str):
            return found
        return None

    def getIsLive(self) -> bool:
        """
        Robust isLive detector. Checks multiple locations and returns boolean.
        """
        if not self.lastInfo:
            return False

        # Try direct root flag
        val = self._get_by_path(self.lastInfo, ["isLive"])
        if val is not None:
            return bool(val)

        # Common locations
        paths = [
            ["cam", "isCamActive"],
            ["cam", "isCamAvailable"],
            ["cam", "isLive"],
            ["model", "isLive"],
            ["user", "isLive"],
            ["user", "user", "isLive"],
            ["user", "user", "isCamActive"],
            ["broadcastSettings", "isLive"],
            ["cam", "broadcastSettings", "isCamActive"],
            ["cam", "broadcastSettings", "isLive"],
        ]
        val = self._first_in_paths(paths)
        if val is not None:
            return bool(val)

        # Recursive fallback for any key named isLive or isCamActive
        for k in ("isLive", "isCamActive", "isCamAvailable"):
            found = self._recursive_find(self.lastInfo, k)
            if found is not None:
                return bool(found)

        return False

    def getIsMobile(self) -> bool:
        """
        Robust isMobile detector. Checks multiple locations and returns boolean.
        """
        if not self.lastInfo:
            return False

        # Direct
        val = self._get_by_path(self.lastInfo, ["isMobile"])
        if val is not None:
            return bool(val)

        # Common paths
        paths = [
            ["model", "isMobile"],
            ["user", "isMobile"],
            ["user", "user", "isMobile"],
            ["broadcastSettings", "isMobile"],
            ["cam", "broadcastSettings", "isMobile"],
            ["cam", "isMobile"],
        ]
        val = self._first_in_paths(paths)
        if val is not None:
            return bool(val)

        # Recursive fallback
        found = self._recursive_find(self.lastInfo, "isMobile")
        if found is not None:
            return bool(found)

        return False

    def getIsGeoBanned(self) -> bool:
        """Check if user is geo-banned from viewing this model."""
        if not self.lastInfo:
            return False

        paths = [
            ["isGeoBanned"],
            ["user", "isGeoBanned"],
            ["user", "user", "isGeoBanned"],
        ]
        val = self._first_in_paths(paths)
        if val is not None:
            return bool(val)

        found = self._recursive_find(self.lastInfo, "isGeoBanned")
        return bool(found) if found is not None else False

    def getIsDeleted(self) -> bool:
        """Check if the model account has been deleted."""
        if not self.lastInfo:
            return False

        # Try common paths where isDeleted might be found
        paths = [
            ["isDeleted"],
            ["user", "isDeleted"],
            ["user", "user", "isDeleted"],
            ["model", "isDeleted"],
        ]
        val = self._first_in_paths(paths)
        if val is not None:
            return bool(val)

        # Recursive fallback
        found = self._recursive_find(self.lastInfo, "isDeleted")
        return bool(found) if found is not None else False

    def getStatus(self):
        """Check the current status of the model's stream."""
        url = f'https://stripchat.com/api/front/v2/models/username/{self.username}/cam?uniq={StripChat.uniq()}'
        r = self.session.get(url, headers=self.headers, bucket='api')

        ct = (r.headers.get("content-type") or "").lower()
        body = r.text or ""

        # Handle HTTP errors
        if r.status_code == 404:
            return Status.NOTEXIST
        if r.status_code == 403:
            if looks_like_cf_html(body):
                self.logger.error(f'Cloudflare challenge (403) for {self.username}')
                return Status.CLOUDFLARE
            return Status.RESTRICTED
        if r.status_code == 429:
            self.logger.error(f'Rate limited (429) for {self.username}')
            return Status.RATELIMIT
        if r.status_code >= 500:
            if looks_like_cf_html(body):
                self.logger.error(f'Cloudflare challenge ({r.status_code}) for {self.username}')
                return Status.CLOUDFLARE
            self.logger.error(f'Server error {r.status_code} for {self.username}')
            return Status.UNKNOWN

        # Validate JSON response
        if "application/json" not in ct or not body.strip():
            self.logger.warning(f'Non-JSON response for {self.username}')
            return Status.UNKNOWN

        try:
            raw = r.json()
        except Exception as e:
            self.logger.error(f'Failed to parse JSON for {self.username}: {e}')
            return Status.UNKNOWN

        # Normalize and store info
        self.lastInfo = self.normalizeInfo(raw)

        # Check for geo-ban
        if self.getIsGeoBanned():
            return Status.RESTRICTED

        # Check if model account has been deleted
        if self.getIsDeleted():
            self.logger.warning(f'⚠️ Model account {self.username} has been deleted - this model will be auto-deregistered')
            return Status.DELETED

        # Determine status - check isLive first as it's more reliable
        is_live = self.getIsLive()
        is_cam_available = self._get_by_path(self.lastInfo, ["isCamAvailable"]) or \
                          self._get_by_path(self.lastInfo, ["cam", "isCamAvailable"]) or False
        
        # If not live, check if camera is available (model is online but no stream yet)
        if not is_live:
            if is_cam_available:
                # Model is connected and ready but hasn't started streaming
                return Status.ONLINE
            # Not live and no camera available = offline
            return Status.OFFLINE
        
        # If live, check the status field
        status = self.getStatusField()
        if status == "public":
            return Status.PUBLIC
        # Ticket/group shows - HLS stream is fully accessible (unblurred)!
        if status in self._TICKET_SHOW_STATUSES:
            self.logger.info(f"Ticket show detected - recording unblurred stream")
            return Status.PUBLIC
        if status in self._PRIVATE_STATUSES:
            return Status.PRIVATE
        
        # Edge case: is_live=true but status is unclear - default to private to be safe
        if is_live and status is None:
            return Status.PRIVATE
        
        # Status is set to something unexpected
        if status in ["off", "idle"]:
            return Status.OFFLINE
        
        # Unknown status - log the actual data for debugging
        self.logger.warning(f"Unknown status '{status}' for {self.username} - lastInfo keys: {list(self.lastInfo.keys())}")
        self.logger.debug(f"Full response for {self.username}: {str(self.lastInfo)[:500]}")
        return Status.UNKNOWN

    def isMobile(self):
        """Check if the current broadcast is from a mobile device."""
        return self.getIsMobile()

    def getPlaylistVariants(self, url):
        """Fetch HLS playlist variants with mouflon encryption keys."""
        s = self._get_session()
        stream_name = self.getStreamName()
        
        # Check if stream is origin-only (not yet replicated to edge CDN)
        # This is a temporary state - retry a few times with delay
        max_origin_retries = 5
        origin_retry_delay = 3  # seconds
        
        for retry in range(max_origin_retries):
            origin_only = self._get_by_path(self.lastInfo, ["cam", "broadcastSettings", "originOnly"])
            if not origin_only:
                break
            
            if retry < max_origin_retries - 1:
                self.logger.info(f"Stream is origin-only (not on edge CDN yet), waiting {origin_retry_delay}s... (attempt {retry + 1}/{max_origin_retries})")
                time.sleep(origin_retry_delay)
                # Re-fetch status to check if originOnly changed
                try:
                    self.getStatus()
                except Exception as e:
                    self.logger.warning(f"Failed to re-fetch status: {e}")
                    break
            else:
                self.logger.warning(f"Stream still origin-only after {max_origin_retries} attempts - skipping for now")
                return []
        
        # Build playlist URL - try multiple CDN hosts
        cdn_hosts = ['doppiocdn.org', 'doppiocdn.com', 'doppiocdn.net', 'doppiocdn.live']
        random.shuffle(cdn_hosts)
        
        vr_suffix = '_vr' if self.vr else ''
        auto_suffix = '_auto' if not self.vr else ''
        
        result = None
        playlist_url = None
        
        for host in cdn_hosts:
            playlist_url = f"https://edge-hls.{host}/hls/{stream_name}{vr_suffix}/master/{stream_name}{vr_suffix}{auto_suffix}.m3u8"
            self.debug(f"Fetching playlist from: {playlist_url}")
            
            try:
                result = s.get(playlist_url, headers=self.headers, cookies=self.cookies, timeout=10)
                self.debug(f"Playlist response from {host}: {result.status_code}")
                
                if result.status_code == 200:
                    break
                elif result.status_code == 404:
                    self.logger.warning(f"Playlist not found on {host}, trying next CDN...")
                    result = None
                else:
                    self.logger.warning(f"Unexpected status {result.status_code} from {host}")
                    result = None
            except Exception as e:
                self.logger.warning(f"Failed to fetch from {host}: {e}")
                result = None
        
        if not result or result.status_code != 200:
            self.logger.error(f"Failed to fetch playlist from any CDN host")
            return []
            
        m3u8_doc = result.text
        self.debug(f"M3U8 content (first 300 chars): {m3u8_doc[:300]}")
        
        psch, pkey, pdkey = StripChat._getMouflonFromM3U(m3u8_doc)
        self.debug(f"Extracted key psch={psch}, pkey={pkey}, pdkey={pdkey}")
        
        if not pkey:
            self.logger.error("No mouflon pkey available - keys not extracted at startup?")
            self.debug(f"Class state: _mouflon_pkey={StripChat._mouflon_pkey}, _mouflon_pdkey={StripChat._mouflon_pdkey}")
            return []
        
        variants = super().getPlaylistVariants(m3u_data=m3u8_doc)
        self.debug(f"Parsed {len(variants) if variants else 0} variants from playlist")
        
        if not variants:
            self.logger.error("No variants found in playlist")
            return []
        
        # Add authentication keys to variant URLs and rewrite to use direct CDN
        # The master playlist returns media-hls.doppiocdn.X/b-hls-XX/... which is blocked (403)
        # We need to rewrite to b-hls-XX.doppiocdn.live/hls/... which works
        result = []
        for variant in variants:
            url = variant['url']
            
            # Rewrite media-hls URLs to direct b-hls CDN
            # From: https://media-hls.doppiocdn.com/b-hls-25/189420462/189420462.m3u8
            # To:   https://b-hls-25.doppiocdn.live/hls/189420462/189420462.m3u8
            import re
            match = re.match(r'https://media-hls\.doppiocdn\.\w+/(b-hls-\d+)/(\d+)/(.+)', url)
            if match:
                b_hls_server = match.group(1)  # e.g., b-hls-25
                stream_id = match.group(2)      # e.g., 189420462
                filename = match.group(3)       # e.g., 189420462.m3u8?...
                
                # Strip any existing query params from filename for reconstruction
                if '?' in filename:
                    filename_base = filename.split('?')[0]
                else:
                    filename_base = filename
                
                # Construct the direct CDN URL with all keys
                url = f"https://{b_hls_server}.doppiocdn.live/hls/{stream_id}/{filename_base}?psch={psch}&pkey={pkey}&pdkey={pdkey}"
                self.debug(f"Rewrote variant URL to: {url[:60]}...")
            else:
                # URL doesn't match expected pattern - just add keys if missing
                if 'pkey=' not in url or 'pdkey=' not in url:
                    sep = '&' if '?' in url else '?'
                    url = f"{url}{sep}psch={psch}&pkey={pkey}&pdkey={pdkey}"
            
            result.append(variant | {"url": url})
        
        return result

Bot.loaded_sites.add(StripChat)