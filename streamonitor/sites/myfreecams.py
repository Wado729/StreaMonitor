import json
import random
import threading
import time
import urllib.parse

import requests
import websocket

from parameters import HTTP_USER_AGENT
from streamonitor.bot import Bot
from streamonitor.enums import Status


_SERVER_CONFIG_URL = 'https://www.myfreecams.com/_js/serverconfig.js'
_WEBSOCKET_ORIGIN = 'https://m.myfreecams.com'
_WEBSOCKET_HANDSHAKE = 'fcsws_20180422\n\x00'
_LOGIN_FRAME = '1 0 0 20080910 0 guest:guest\n\x00'
_NULL_FRAME = '0 0 0 0 0\n\x00'
_VIDEO_OFFLINE = {90, 127}
_MAX_FRAME_SIZE = 8 * 1024 * 1024


class _MFCRealtimeClient:
    def __init__(self, session):
        self.session = session
        self.session.headers.update({'User-Agent': HTTP_USER_AGENT})
        self.socket = None
        self.buffer = bytearray()
        self.query_id = 0
        self.server_config = None
        self.server_config_expires = 0
        self.lock = threading.Lock()

    def lookup(self, username):
        with self.lock:
            for attempt in range(2):
                try:
                    config = self._get_server_config()
                    self._connect(config)
                    return self._lookup_model(username), config
                except (
                    requests.RequestException,
                    websocket.WebSocketException,
                    OSError,
                    ValueError,
                ) as error:
                    self._reset()
                    if attempt == 1:
                        raise RuntimeError(f'MFC realtime lookup failed: {error}') from error

    def _get_server_config(self):
        if self.server_config and time.monotonic() < self.server_config_expires:
            return self.server_config

        response = self.session.get(_SERVER_CONFIG_URL, timeout=10)
        response.raise_for_status()
        config = response.json()
        if not isinstance(config, dict):
            raise ValueError('MFC server config is not a JSON object')
        servers = config.get('websocket_servers')
        if not isinstance(servers, dict):
            raise ValueError('MFC server config has no websocket server list')
        if not any(protocol == 'rfc6455' for protocol in servers.values()):
            raise ValueError('MFC server config has no RFC6455 websocket servers')

        self.server_config = config
        self.server_config_expires = time.monotonic() + 600
        return config

    def _connect(self, config):
        if self.socket is not None:
            return

        servers = [
            name
            for name, protocol in config['websocket_servers'].items()
            if protocol == 'rfc6455'
        ]
        server = random.choice(servers)
        self.socket = websocket.create_connection(
            f'wss://{server}.myfreecams.com/fcsl',
            timeout=10,
            origin=_WEBSOCKET_ORIGIN,
        )
        self.socket.send(_WEBSOCKET_HANDSHAKE)
        self.socket.send(_LOGIN_FRAME)

    def _lookup_model(self, username):
        self.query_id += 1
        query_id = self.query_id
        query = urllib.parse.quote(username, safe='')
        self.socket.send(_NULL_FRAME)
        self.socket.send(f'10 0 0 {query_id} 0 {query}\n\x00')

        while True:
            frame_type, arg1, payload = self._read_frame()
            if frame_type != 10 or arg1 != query_id:
                continue
            if payload.decode('utf-8', 'replace') == username:
                return None

            model = json.loads(payload.decode('utf-8'))
            if not isinstance(model, dict):
                raise ValueError('MFC lookup returned an invalid model record')
            return model

    def _read_frame(self):
        while True:
            if len(self.buffer) >= 6:
                prefix = bytes(self.buffer[:6])
                if not prefix.isdigit():
                    raise ValueError('MFC sent an invalid frame length')
                body_length = int(prefix)
                if body_length > _MAX_FRAME_SIZE:
                    raise ValueError('MFC frame exceeds the maximum allowed size')
                if len(self.buffer) >= 6 + body_length:
                    body = bytes(self.buffer[6:6 + body_length])
                    del self.buffer[:6 + body_length]
                    parts = body.split(b' ', 5)
                    if len(parts) < 5:
                        raise ValueError('MFC sent a malformed frame')
                    frame_type = int(parts[0])
                    arg1 = int(parts[3])
                    payload = b''
                    if len(parts) == 6:
                        payload = urllib.parse.unquote_to_bytes(parts[5].decode('ascii'))
                    return frame_type, arg1, payload

            message = self.socket.recv()
            chunk = message if isinstance(message, bytes) else message.encode('utf-8')
            self.buffer.extend(chunk)
            if len(self.buffer) > _MAX_FRAME_SIZE + 6:
                raise ValueError('MFC websocket buffer exceeds the maximum allowed size')

    def _reset(self):
        if self.socket is not None:
            self.socket.close()
        self.socket = None
        self.buffer.clear()


class MyFreeCams(Bot):
    site = 'MyFreeCams'
    siteslug = 'MFC'

    _realtime_client = None
    _realtime_client_lock = threading.Lock()

    def __init__(self, username):
        super().__init__(username)
        self.attrs = {}
        self.videoUrl = None

    def getWebsiteURL(self):
        return "https://www.myfreecams.com/#" + self.username

    def getVideoUrl(self, refresh=False):
        if not refresh:
            return self.videoUrl

        server = self.attrs.get('video_server')
        server_type = self.attrs.get('video_server_type')
        model_id = self.attrs.get('model_id')
        if not server or not model_id or server_type not in ('h5video', 'wzobs'):
            return None

        mid = 100000000 + int(model_id)
        prefix = 'a_' if server_type == 'wzobs' else ''
        playlist_url = (
            f'https://{server}.myfreecams.com/NxServer/'
            f'ngrp:mfc_{prefix}{mid}.f4v_mobile/playlist.m3u8'
        )
        response = self.session.get(playlist_url, timeout=10)
        if response.status_code != 200:
            return None
        return self.getWantedResolutionPlaylist(playlist_url, response.text)

    def _getRealtimeClient(self):
        client_class = type(self)
        with client_class._realtime_client_lock:
            if client_class._realtime_client is None:
                client_class._realtime_client = _MFCRealtimeClient(self.session)
            return client_class._realtime_client

    @staticmethod
    def _getVideoServer(config, camserv):
        server_id = str(camserv)
        for server_type, config_key in (
            ('h5video', 'h5video_servers'),
            ('wzobs', 'wzobs_servers'),
        ):
            servers = config.get(config_key) or {}
            server = servers.get(server_id)
            if server:
                return server, server_type
        return None, None

    def getStatus(self):
        self.attrs = {}
        self.videoUrl = None
        model, config = self._getRealtimeClient().lookup(self.username)
        if model is None:
            return Status.NOTEXIST

        try:
            model_id = int(model.get('uid'))
        except (TypeError, ValueError):
            return Status.NOTEXIST
        if model_id == 0:
            return Status.NOTEXIST

        model_meta = model.get('m') or {}
        if not isinstance(model_meta, dict):
            model_meta = {}
        try:
            raw_video_status = model.get('vs')
            if raw_video_status is None:
                raw_video_status = model_meta.get('vs')
            video_status = int(raw_video_status)
        except (TypeError, ValueError):
            return Status.UNKNOWN

        if video_status in _VIDEO_OFFLINE or video_status == 12:
            return Status.OFFLINE
        if video_status in (13, 91):
            return Status.PRIVATE
        if video_status != 0:
            return Status.UNKNOWN

        user_data = model.get('u') or {}
        if not isinstance(user_data, dict):
            return Status.UNKNOWN
        camserv = user_data.get('camserv')
        if not camserv:
            self.logger.warning('MFC reports %s online without a camera server', self.username)
            return Status.UNKNOWN

        video_server, server_type = self._getVideoServer(config, camserv)
        if not video_server:
            self.logger.warning(
                'MFC does not provide an HLS server for %s (camera server %s)',
                self.username,
                camserv,
            )
            return Status.UNKNOWN

        self.attrs = {
            'model_id': model_id,
            'video_server': video_server,
            'video_server_type': server_type,
        }
        self.videoUrl = self.getVideoUrl(refresh=True)
        return Status.PUBLIC
