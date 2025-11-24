'''
Function:
    Implementation of BaseMusicClient
Author:
    Zhenchao Jin
WeChat Official Account (微信公众号):
    Charles的皮卡丘
'''
import os
import copy
import pickle
import requests
from freeproxy import freeproxy
from fake_useragent import UserAgent
from pathvalidate import sanitize_filepath
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..utils import LoggerHandle, legalizestring, touchdir, usedownloadheaderscookies, usesearchheaderscookies
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn, MofNCompleteColumn


'''BaseMusicClient'''
class BaseMusicClient():
    source = 'BaseMusicClient'
    def __init__(self, search_size_per_source: int = 5, auto_set_proxies: bool = False, random_update_ua: bool = False, max_retries: int = 5, maintain_session: bool = False, 
                 logger_handle: LoggerHandle = None, disable_print: bool = False, work_dir: str = 'musicdl_outputs', proxy_sources: list = None, default_search_cookies: dict = None,
                 default_download_cookies: dict = None):
        # set up work dir
        touchdir(work_dir)
        # set attributes
        self.search_size_per_source = search_size_per_source
        self.auto_set_proxies = auto_set_proxies
        self.random_update_ua = random_update_ua
        self.max_retries = max_retries
        self.maintain_session = maintain_session
        self.logger_handle = logger_handle if logger_handle else LoggerHandle()
        self.disable_print = disable_print
        self.work_dir = work_dir
        self.proxy_sources = proxy_sources
        self.default_search_cookies = default_search_cookies or {}
        self.default_download_cookies = default_download_cookies or {}
        self.default_cookies = default_search_cookies
        # init requests.Session
        self.default_search_headers = {'User-Agent': UserAgent().random}
        self.default_download_headers = {'User-Agent': UserAgent().random}
        self.default_headers = self.default_search_headers
        self._initsession()
        # proxied_session_client
        self.proxied_session_client = freeproxy.ProxiedSessionClient(
            proxy_sources=['QiyunipProxiedSession'] if proxy_sources is None else proxy_sources, 
            disable_print=True
        ) if auto_set_proxies else None
    '''_initsession'''
    def _initsession(self):
        self.session = requests.Session()
        self.session.headers = self.default_headers
    '''_constructsearchurls'''
    def _constructsearchurls(self, keyword: str, rule: dict = None, request_overrides: dict = None):
        raise NotImplementedError('not to be implemented')
    @staticmethod
    def _extract_name_from_data(data):
        if data is None:
            return None
        if isinstance(data, str):
            cleaned = data.strip()
            if cleaned and cleaned.upper() != 'NULL':
                return cleaned
            return None
        if isinstance(data, dict):
            for key in ('album_artist', 'artist', 'artists', 'name', 'title', 'singer', 'singers'):
                candidate = BaseMusicClient._extract_name_from_data(data.get(key))
                if candidate:
                    return candidate
            for key, value in data.items():
                if key.lower().endswith('name'):
                    candidate = BaseMusicClient._extract_name_from_data(value)
                    if candidate:
                        return candidate
            return None
        if isinstance(data, (list, tuple, set)):
            for item in data:
                candidate = BaseMusicClient._extract_name_from_data(item)
                if candidate:
                    return candidate
            return None
        for attr in ('album_artist', 'artist', 'artists', 'name', 'title', 'singer', 'singers'):
            if hasattr(data, attr):
                attr_value = getattr(data, attr)
                if attr_value is data:
                    continue
                candidate = BaseMusicClient._extract_name_from_data(attr_value)
                if candidate:
                    return candidate
        return None
    @staticmethod
    def _strip_featured_artist(name: str):
        cleaned = name.strip()
        lowered = cleaned.lower()
        feature_tokens = [' feat.', ' featuring', ' ft.', ' ft ', ' with ', ' x ', ' × ', ' presents ', ' pres. ']
        for token in feature_tokens:
            idx = lowered.find(token)
            if idx != -1:
                cleaned = cleaned[:idx]
                lowered = cleaned.lower()
        for separator in [',', '，', '、']:
            if separator in cleaned:
                cleaned = cleaned.split(separator)[0]
                break
        return cleaned.strip(' -&')
    def _resolve_artist_name(self, song_info: dict = None, keyword: str = '', preferred_artist: str = ''):
        song_info = song_info or {}
        keyword = keyword or ''
        raw_data = song_info.get('raw_data') or {}
        search_result = raw_data.get('search_result')
        candidates = [preferred_artist, song_info.get('album_artist'), raw_data.get('album_artist')]
        if isinstance(search_result, dict):
            candidates.extend([
                search_result.get('album'), search_result.get('al'), search_result.get('artists'), search_result.get('artist'),
                search_result.get('ar'), search_result.get('singers'), search_result.get('singer'),
            ])
        elif search_result is not None:
            for attr in ('album', 'artist', 'artists', 'primaryArtist'):
                candidates.append(getattr(search_result, attr, None))
        candidates.extend([
            song_info.get('singers'), song_info.get('artist'), song_info.get('artists'), song_info.get('singer'),
        ])
        if keyword:
            candidates.append(keyword)
        for candidate in candidates:
            name = self._extract_name_from_data(candidate)
            if not name:
                continue
            stripped = self._strip_featured_artist(name)
            stripped = stripped.strip()
            if stripped:
                return stripped
        return 'Unknown Artist'
    def _resolve_album_name(self, song_info: dict = None, keyword: str = ''):
        song_info = song_info or {}
        raw_data = song_info.get('raw_data') or {}
        search_result = raw_data.get('search_result')
        candidates = [
            song_info.get('album'), song_info.get('album_name'), song_info.get('albumTitle'), song_info.get('record'),
        ]
        if isinstance(search_result, dict):
            candidates.extend([search_result.get('album'), search_result.get('al'), search_result.get('album_name')])
        elif search_result is not None:
            candidates.append(getattr(search_result, 'album', None))
        download_result = raw_data.get('download_result')
        if isinstance(download_result, dict):
            candidates.append(download_result.get('album'))
        candidates.append(keyword)
        for candidate in candidates:
            name = self._extract_name_from_data(candidate)
            if name:
                return name
        return 'Unknown Album'
    @staticmethod
    def _normalizetracknumber(value):
        if value is None:
            return None
        if isinstance(value, (list, tuple, set)):
            for item in value:
                normalized = BaseMusicClient._normalizetracknumber(item)
                if normalized is not None:
                    return normalized
            return None
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped or stripped.upper() == 'NULL':
                return None
            for sep in ('/', '-', '.', ' '):
                if sep in stripped:
                    stripped = stripped.split(sep)[0]
                    break
            if stripped.isdigit():
                candidate = int(stripped)
                return candidate if candidate > 0 else None
            try:
                candidate = int(float(stripped))
                return candidate if candidate > 0 else None
            except (ValueError, TypeError):
                return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            try:
                candidate = int(value)
            except (ValueError, TypeError):
                return None
            return candidate if candidate > 0 else None
        if hasattr(value, '__int__'):
            try:
                candidate = int(value)
                return candidate if candidate > 0 else None
            except Exception:
                return None
        return None
    '''_constructuniqueworkdir'''
    def _constructuniqueworkdir(self, song_info: dict = None, keyword: str = '', album_artist: str = ''):
        artist_raw = self._resolve_artist_name(song_info=song_info, keyword=keyword, preferred_artist=album_artist)
        album_raw = self._resolve_album_name(song_info=song_info, keyword=keyword)
        artist_dir = legalizestring(artist_raw, replace_null_string='Unknown Artist')
        album_dir = legalizestring(album_raw, replace_null_string='Unknown Album')
        work_dir = os.path.join(self.work_dir, artist_dir, album_dir)
        touchdir(work_dir)
        return work_dir
    '''_removeduplicates'''
    def _removeduplicates(self, song_infos: list = None):
        unique_song_infos, identifiers = [], set()
        for song_info in song_infos:
            if song_info['identifier'] in identifiers:
                continue
            identifiers.add(song_info['identifier'])
            unique_song_infos.append(song_info)
        return unique_song_infos
    '''_search'''
    @usesearchheaderscookies
    def _search(self, keyword: str = '', search_url: str = '', request_overrides: dict = None, song_infos: list = [], progress: Progress = None, progress_id: int = 0):
        raise NotImplementedError('not be implemented')
    '''search'''
    @usesearchheaderscookies
    def search(self, keyword: str, num_threadings=5, request_overrides: dict = None, rule: dict = None):
        # init
        rule, request_overrides = rule or {}, request_overrides or {}
        # logging
        self.logger_handle.info(f'Start to search music files using {self.source}.', disable_print=self.disable_print)
        # construct search urls
        search_urls = self._constructsearchurls(keyword=keyword, rule=rule, request_overrides=request_overrides)
        # multi threadings for searching music files
        with Progress(TextColumn("{task.description}"), BarColumn(bar_width=None), MofNCompleteColumn(), TimeRemainingColumn()) as progress:
            progress_id = progress.add_task(f"{self.source}.search >>> completed (0/{len(search_urls)})", total=len(search_urls))
            song_infos, submitted_tasks = [], []
            with ThreadPoolExecutor(max_workers=num_threadings) as pool:
                for search_url in search_urls:
                    submitted_tasks.append(pool.submit(
                        self._search, keyword, search_url, request_overrides, song_infos, progress, progress_id
                    ))
                for _ in as_completed(submitted_tasks):
                    num_searched_urls = int(progress.tasks[progress_id].completed)
                    progress.update(progress_id, description=f"{self.source}.search >>> completed ({num_searched_urls}/{len(search_urls)})")
        song_infos = self._removeduplicates(song_infos=song_infos)
        album_artist_map = {}
        for song_info in song_infos:
            album_name = song_info.get('album') if isinstance(song_info, dict) else None
            if isinstance(album_name, str):
                album_key = album_name.strip().lower()
            else:
                album_key = None
            resolved_artist = album_artist_map.get(album_key) if album_key else None
            if not resolved_artist:
                resolved_artist = self._resolve_artist_name(song_info=song_info, keyword=keyword)
                if album_key:
                    album_artist_map[album_key] = resolved_artist
            song_info['work_dir'] = self._constructuniqueworkdir(song_info=song_info, keyword=keyword, album_artist=resolved_artist)
        # logging
        if len(song_infos) > 0:
            work_dir = song_infos[0]['work_dir']
            touchdir(work_dir)
            self._savetopkl(song_infos, os.path.join(work_dir, 'search_results.pkl'))
        else:
            work_dir = self.work_dir
        self.logger_handle.info(f'Finished searching music files using {self.source}. Search results have been saved to {work_dir}, valid items: {len(song_infos)}.', disable_print=self.disable_print)
        # return
        return song_infos
    '''_download'''
    @usedownloadheaderscookies
    def _download(self, song_info: dict, request_overrides: dict = None, downloaded_song_infos: list = [], progress: Progress = None, 
                  song_progress_id: int = 0, songs_progress_id: int = 0):
        request_overrides = request_overrides or {}
        try:
            touchdir(song_info['work_dir'])
            with self.get(song_info['download_url'], stream=True, **request_overrides) as resp:
                resp.raise_for_status()
                total_size, chunk_size, downloaded_size = int(resp.headers.get('content-length', 0)), song_info.get('chunk_size', 1024), 0
                progress.update(song_progress_id, total=total_size)
                track_prefix = ''
                track_number = self._normalizetracknumber(song_info.get('track_number'))
                if track_number is None:
                    # also consider disc information appearing under misc keys
                    track_number = self._normalizetracknumber(song_info.get('trackNumber'))
                if track_number is not None:
                    track_prefix = f"{track_number:02d} - "
                file_base = f"{track_prefix}{song_info['song_name']}"
                save_path = os.path.join(song_info['work_dir'], f"{file_base}.{song_info['ext']}")
                same_name_file_idx = 1
                while os.path.exists(save_path):
                    save_path = os.path.join(song_info['work_dir'], f"{file_base}_{same_name_file_idx}.{song_info['ext']}")
                    same_name_file_idx += 1
                with open(save_path, "wb") as fp:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if not chunk: continue
                        fp.write(chunk)
                        downloaded_size = downloaded_size + len(chunk)
                        if total_size > 0:
                            downloading_text = "%0.2fMB/%0.2fMB" % (downloaded_size / 1024 / 1024, total_size / 1024 / 1024)
                        else:
                            progress.update(song_progress_id, total=downloaded_size)
                            downloading_text = "%0.2fMB/%0.2fMB" % (downloaded_size / 1024 / 1024, downloaded_size / 1024 / 1024)
                        progress.advance(song_progress_id, len(chunk))
                        progress.update(song_progress_id, description=f"{self.source}.download >>> {song_info['song_name']} (Downloading: {downloading_text})")
                progress.advance(songs_progress_id, 1)
                progress.update(song_progress_id, description=f"{self.source}.download >>> {song_info['song_name']} (Success)")
                downloaded_song_info = copy.deepcopy(song_info)
                downloaded_song_info['save_path'] = save_path
                downloaded_song_infos.append(downloaded_song_info)
        except Exception as err:
            progress.update(song_progress_id, description=f"{self.source}.download >>> {song_info['song_name']} (Error: {err})")
        return downloaded_song_infos
    '''download'''
    @usedownloadheaderscookies
    def download(self, song_infos: list, num_threadings=5, request_overrides: dict = None):
        # init
        request_overrides = request_overrides or {}
        # logging
        self.logger_handle.info(f'Start to download music files using {self.source}.', disable_print=self.disable_print)
        # multi threadings for downloading music files
        columns = [
            SpinnerColumn(), TextColumn("{task.description}"), BarColumn(bar_width=None), TaskProgressColumn(),
            DownloadColumn(), TransferSpeedColumn(), TimeRemainingColumn(),
        ]
        with Progress(*columns, refresh_per_second=20, expand=True) as progress:
            songs_progress_id = progress.add_task(f"{self.source}.download >>> completed (0/{len(song_infos)})", total=len(song_infos))
            song_progress_ids, downloaded_song_infos, submitted_tasks = [], [], []
            for _, song_info in enumerate(song_infos):
                desc = f"{self.source}.download >>> {song_info['song_name']} (Preparing)"
                song_progress_ids.append(progress.add_task(desc, total=None))
            with ThreadPoolExecutor(max_workers=num_threadings) as pool:
                for song_progress_id, song_info in zip(song_progress_ids, song_infos):
                    submitted_tasks.append(pool.submit(
                        self._download, song_info, request_overrides, downloaded_song_infos, progress, song_progress_id, songs_progress_id
                    ))
                for _ in as_completed(submitted_tasks):
                    num_downloaded_songs = int(progress.tasks[songs_progress_id].completed)
                    progress.update(songs_progress_id, description=f"{self.source}.download >>> completed ({num_downloaded_songs}/{len(song_infos)})")
        # logging
        if len(downloaded_song_infos) > 0:
            work_dir = downloaded_song_infos[0]['work_dir']
            touchdir(work_dir)
            self._savetopkl(downloaded_song_infos, os.path.join(work_dir, 'download_results.pkl'))
        else:
            work_dir = self.work_dir
        self.logger_handle.info(f'Finished downloading music files using {self.source}. Download results have been saved to {work_dir}, valid downloads: {len(downloaded_song_infos)}.', disable_print=self.disable_print)
        # return
        return downloaded_song_infos
    '''get'''
    def get(self, url, **kwargs):
        if 'cookies' not in kwargs: kwargs['cookies'] = self.default_cookies
        resp = None
        for _ in range(self.max_retries):
            if not self.maintain_session:
                self._initsession()
                if self.random_update_ua: self.session.headers.update({'User-Agent': UserAgent().random})
            if self.auto_set_proxies:
                try:
                    self.session.proxies = self.proxied_session_client.getrandomproxy()
                except Exception as err:
                    self.logger_handle.error(f'{self.source}.get >>> {url} (Error: {err})', disable_print=self.disable_print)
                    self.session.proxies = {}
            else:
                self.session.proxies = {}
            try:
                resp = self.session.get(url, **kwargs)
            except Exception as err:
                self.logger_handle.error(f'{self.source}.get >>> {url} (Error: {err})', disable_print=self.disable_print)
                continue
            if resp.status_code != 200: continue
            return resp
        return resp
    '''post'''
    def post(self, url, **kwargs):
        if 'cookies' not in kwargs: kwargs['cookies'] = self.default_cookies
        resp = None
        for _ in range(self.max_retries):
            if not self.maintain_session:
                self._initsession()
                if self.random_update_ua: self.session.headers.update({'User-Agent': UserAgent().random})
            if self.auto_set_proxies:
                try:
                    self.session.proxies = self.proxied_session_client.getrandomproxy()
                except Exception as err:
                    self.logger_handle.error(f'{self.source}.post >>> {url} (Error: {err})', disable_print=self.disable_print)
                    self.session.proxies = {}
            else:
                self.session.proxies = {}
            try:
                resp = self.session.post(url, **kwargs)
            except Exception as err:
                self.logger_handle.error(f'{self.source}.post >>> {url} (Error: {err})', disable_print=self.disable_print)
                continue
            if resp.status_code != 200: continue
            return resp
        return resp
    '''_savetopkl'''
    def _savetopkl(self, data, file_path, auto_sanitize=True):
        if auto_sanitize: file_path = sanitize_filepath(file_path)
        with open(file_path, 'wb') as fp:
            pickle.dump(data, fp)