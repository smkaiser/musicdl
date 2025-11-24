'''
Function:
    Implementation of TIDALMusicClient: https://tidal.com/
Author:
    Zhenchao Jin
WeChat Official Account (微信公众号):
    Charles的皮卡丘
'''
import os
import re
import copy
import aigpy
import base64
import tempfile
import json_repair
from xml.etree import ElementTree
from collections.abc import Iterable
from urllib.parse import urlencode, urljoin, urlparse
from .base import BaseMusicClient
from rich.progress import Progress
from ..utils import legalizestring, byte2mb, resp2json, isvalidresp, seconds2hms, touchdir, replacefile, usesearchheaderscookies, usedownloadheaderscookies, AudioLinkTester
from ..utils.tidalutils import (
    TIDALTvSession, SearchResult, StreamRespond, StreamUrl, Manifest, Period, AdaptationSet, Representation, SegmentTemplate, SegmentList, SegmentTimelineEntry,
    decryptfile, decryptsecuritytoken, pyavready, ffmpegready, remuxflacstream, setmetadata, Track
)


'''TIDALMusicClient'''
class TIDALMusicClient(BaseMusicClient):
    source = 'TIDALMusicClient'

    def __init__(self, **kwargs):
        super(TIDALMusicClient, self).__init__(**kwargs)
        self.tidal_session = TIDALTvSession(headers={}, cookies=self.default_cookies)

        cached_session = False
        try:
            cached_session = self.tidal_session.loadfromcache()
        except Exception:
            cached_session = False

        if cached_session:
            try:
                self.tidal_session.refresh()
            except Exception:
                self.logger_handle.info(
                    f'{self.source} cached session refresh failed; requesting a new device login.',
                    disable_print=self.disable_print,
                )
                self.tidal_session.auth()
        else:
            self.logger_handle.info(
                f'{self.source} cache not found; requesting a new device login.',
                disable_print=self.disable_print,
            )
            self.tidal_session.auth()

        self.tidal_session.cache()
        self._setauthheaders()
        self._initsession()
    '''_setauthheaders'''
    def _setauthheaders(self):
        self.default_search_headers = self.tidal_session.auth_headers
        self.default_download_headers = self.tidal_session.auth_headers
        self.default_headers = self.default_search_headers
    '''_saferequestget'''
    def _saferequestget(self, url, **kwargs):
        resp = self.get(url, **kwargs)
        if resp.status_code in [401, 403]:
            self.tidal_session.refresh()
            self._setauthheaders()
            self._initsession()
            resp = self.get(url, **kwargs)
        return resp
    '''_parsedashmanifest'''
    def _parsedashmanifest(self, xml):
        # getbaseurl
        def _getbaseurl(element: ElementTree.Element, inherited: str):
            base_url = inherited
            base_el = element.find('BaseURL')
            if base_el is not None and base_el.text:
                candidate = base_el.text.strip()
                if candidate:
                    base_url = urljoin(inherited, candidate)
            return base_url
        # _parsesegmenttemplate
        def _parsesegmenttemplate(element: ElementTree.Element):
            template = SegmentTemplate(
                media=element.get('media'), initialization=element.get('initialization'), start_number=int(element.get('startNumber') or 1),
                timescale=int(element.get('timescale') or 1), presentation_time_offset=int(element.get('presentationTimeOffset') or 0),
            )
            timeline_el = element.find('SegmentTimeline')
            if timeline_el is not None:
                for s_el in timeline_el.findall('S'):
                    duration = int(s_el.get('d'))
                    repeat = int(s_el.get('r') or 0)
                    start_time = int(s_el.get('t')) if s_el.get('t') else None
                    template.timeline.append(SegmentTimelineEntry(start_time=start_time, duration=duration, repeat=repeat))
            return template
        # _parsesegmentlist
        def _parsesegmentlist(element: ElementTree.Element):
            init_el = element.find('Initialization')
            initialization = init_el.get('sourceURL') if init_el is not None else None
            media_segments = []
            for seg_el in element.findall('SegmentURL'):
                media = seg_el.get('media')
                if media: media_segments.append(media)
            return SegmentList(initialization=initialization, media_segments=media_segments)
        # _parserepresentation
        def _parserepresentation(element: ElementTree.Element, parent_base: str):
            base_url = _getbaseurl(element, parent_base)
            template = element.find('SegmentTemplate')
            seg_template = _parsesegmenttemplate(template) if template is not None else None
            seg_list_el = element.find('SegmentList')
            seg_list = _parsesegmentlist(seg_list_el) if seg_list_el is not None else None
            return Representation(
                id=element.get('id'), bandwidth=element.get('bandwidth'), codec=element.get('codecs'), base_url=base_url, segment_template=seg_template,
                segment_list=seg_list,
            )
        # _parseadaptation
        def _parseadaptation(element: ElementTree.Element, parent_base: str):
            base_url = _getbaseurl(element, parent_base)
            adaptation = AdaptationSet(content_type=element.get('contentType'), base_url=base_url)
            for rep_el in element.findall('Representation'):
                adaptation.representations.append(_parserepresentation(rep_el, base_url))
            return adaptation
        # _parseperiod
        def _parseperiod(element: ElementTree.Element, parent_base: str):
            base_url = _getbaseurl(element, parent_base)
            period = Period(base_url=base_url)
            for adaptation_el in element.findall('AdaptationSet'):
                period.adaptation_sets.append(_parseadaptation(adaptation_el, base_url))
            return period
        # convert to string text
        if isinstance(xml, bytes):
            xml_text = xml.decode("utf-8")
        else:
            xml_text = str(xml)
        # parse
        xml_text = re.sub(r'xmlns="[^"]+"', '', xml_text, count=1)
        root = ElementTree.fromstring(xml_text)
        manifest_base = _getbaseurl(root, '')
        manifest = Manifest(base_url=manifest_base)
        for period_el in root.findall('Period'):
            manifest.periods.append(_parseperiod(period_el, manifest_base))
        # return
        return manifest
    '''_parsempd'''
    def _parsempd(self, xml: bytes):
        manifest = self._parsedashmanifest(xml)
        for period in manifest.periods:
            for adaptation in period.adaptation_sets:
                if adaptation.content_type == 'audio':
                    for representation in adaptation.representations:
                        if representation.segments:
                            return manifest
    '''_parsemanifest'''
    def _parsemanifest(self, stream_resp: StreamRespond):
        # vnd.tidal.bt
        if "vnd.tidal.bt" in stream_resp.manifestMimeType:
            manifest = json_repair.loads(base64.b64decode(stream_resp.manifest).decode('utf-8'))
            stream_url = StreamUrl()
            stream_url.trackid = stream_resp.trackid
            stream_url.soundQuality = stream_resp.audioQuality
            stream_url.codec = manifest['codecs']
            stream_url.encryptionKey = manifest['keyId'] if 'keyId' in manifest else ""
            stream_url.url = manifest['urls'][0]
            stream_url.urls = [stream_url.url]
            return stream_url
        # dash+xml
        elif "dash+xml" in stream_resp.manifestMimeType:
            xml_bytes = base64.b64decode(stream_resp.manifest)
            manifest = self._parsempd(xml_bytes)
            if not manifest: return
            stream_url = StreamUrl()
            stream_url.trackid = stream_resp.trackid
            stream_url.soundQuality = stream_resp.audioQuality
            audio_reps = []
            for period in manifest.periods:
                for adaptation in period.adaptation_sets:
                    if adaptation.content_type == 'audio':
                        audio_reps.extend(adaptation.representations)
            if not audio_reps: return
            representation = next((rep for rep in audio_reps if rep.segments), audio_reps[0])
            codec = (representation.codec or '').upper()
            if codec.startswith('MP4A'): codec = 'AAC'
            stream_url.codec = codec
            stream_url.encryptionKey = ""
            stream_url.urls = representation.segments
            if len(stream_url.urls) > 0:
                stream_url.url = stream_url.urls[0]
            return stream_url
    '''_guessextension'''
    def _guessextension(self, stream_url: StreamUrl):
        url = (stream_url.url or '').lower()
        codec = (stream_url.codec or '').lower()
        if '.flac' in url: return '.flac'
        if '.mp4' in url:
            if 'ac4' in codec or 'mha1' in codec: return '.mp4'
            elif 'flac' in codec: return '.flac'
            return '.m4a'
        return '.m4a'
    '''_guessstreamextension'''
    def _guessstreamextension(self, stream_url: StreamUrl):
        candidates = []
        if stream_url.url: candidates.append(stream_url.url)
        if stream_url.urls: candidates.extend(stream_url.urls)
        for candidate in candidates:
            if not candidate: continue
            lowered: str = candidate.split("?")[0].lower()
            for ext in (".flac", ".mp4", ".m4a", ".m4b", ".mp3", ".ogg", ".aac"):
                if lowered.endswith(ext): return ext
        codec = (stream_url.codec or "").lower()
        if "flac" in codec:
            return ".flac"
        if "mp4" in codec or "m4a" in codec or "aac" in codec:
            return ".m4a"
        return ".m4a"
    '''_constructsearchurls'''
    def _constructsearchurls(self, keyword: str, rule: dict = None, request_overrides: dict = None):
        # init
        rule, request_overrides = rule or {}, request_overrides or {}
        # search rules
        default_rule = {'countryCode': self.tidal_session.storage.country_code, 'limit': 10, 'offset': 0, 'query': keyword, 'includeContributors': 'truee'}
        default_rule.update(rule)
        # construct search urls based on search rules
        base_url = 'https://api.tidal.com/v1/search?'
        search_urls, page_size, count = [], 10, 0
        while self.search_size_per_source > count:
            page_rule = copy.deepcopy(default_rule)
            page_rule['offset'] = count
            search_urls.append(base_url + urlencode(page_rule))
            count += page_size
        # return
        return search_urls
    '''_download'''
    @usedownloadheaderscookies
    def _download(self, song_info: dict, request_overrides: dict = None, downloaded_song_infos: list = [], progress: Progress = None, 
                  song_progress_id: int = 0, songs_progress_id: int = 0):
        # init
        request_overrides = request_overrides or {}
        # success
        try:
            touchdir(song_info['work_dir'])
            # parse basic information
            stream_url: StreamUrl = song_info['download_url']
            download_ext, final_ext = self._guessstreamextension(stream_url=stream_url), song_info['ext']
            if (final_ext != ".flac") or (download_ext == ".flac"):
                remux_required = False
            else:
                remux_required = "flac" in (stream_url.codec or "").lower()
            if remux_required and (not ffmpegready() and not pyavready()):
                final_ext, remux_required = download_ext, False
            chunk_size = 1048576
            progress.update(song_progress_id, total=1)
            progress.update(song_progress_id, description=f"{self.source}.download >>> {song_info['song_name']} (Downloading")
            # download music file
            with tempfile.TemporaryDirectory(prefix="musicdl-TIDALMusicClient-track-") as tmpdir:
                download_part = os.path.join(
                    tmpdir, f"download{download_ext}.part" if download_ext else "download.part"
                )
                tool = aigpy.download.DownloadTool(download_part, stream_url.urls)
                tool.setUserProgress(None)
                tool.setPartSize(chunk_size)
                check, err = tool.start(showProgress=False)
                assert check
                decrypted_target = os.path.join(
                    tmpdir, f"decrypted{download_ext}" if download_ext else "decrypted"
                )
                if aigpy.string.isNull(stream_url.encryptionKey):
                    replacefile(download_part, decrypted_target)
                    decrypted_path = decrypted_target
                else:
                    key, nonce = decryptsecuritytoken(stream_url.encryptionKey)
                    decryptfile(download_part, decrypted_target, key, nonce)
                    os.remove(download_part)
                    decrypted_path = decrypted_target
                if remux_required:
                    remux_target = os.path.join(tmpdir, "remux.flac")
                    processed_path, backend_used = remuxflacstream(decrypted_path, remux_target)
                    if processed_path != decrypted_path:
                        if os.path.exists(decrypted_path): os.remove(decrypted_path)
                        decrypted_path = processed_path
                    else:
                        final_ext = download_ext
                        decrypted_path = decrypted_path
                save_path, same_name_file_idx = os.path.join(song_info['work_dir'], f"{song_info['song_name']}{final_ext}"), 1
                while os.path.exists(save_path):
                    save_path = os.path.join(song_info['work_dir'], f"{song_info['song_name']}_{same_name_file_idx}{final_ext}")
                    same_name_file_idx += 1
                replacefile(decrypted_path, save_path)
                setmetadata(track=song_info['raw_data']['search_result'], filepath=save_path, stream=stream_url)
            # update progress
            progress.advance(song_progress_id, 1)
            progress.advance(songs_progress_id, 1)
            progress.update(song_progress_id, description=f"{self.source}.download >>> {song_info['song_name']} (Success)")
            downloaded_song_info = copy.deepcopy(song_info)
            downloaded_song_info['save_path'] = save_path
            downloaded_song_info['ext'] = final_ext
            downloaded_song_infos.append(downloaded_song_info)
        # failure
        except Exception as err:
            progress.update(song_progress_id, description=f"{self.source}.download >>> {song_info['song_name']} (Error: {err})")
        # return
        return downloaded_song_infos
    '''_search'''
    @usesearchheaderscookies
    def _search(self, keyword: str = '', search_url: str = '', request_overrides: dict = None, song_infos: list = [], progress: Progress = None, progress_id: int = 0):
        # init
        request_overrides = request_overrides or {}
        # successful
        try:
            # --search results
            resp = self._saferequestget(search_url, **request_overrides)
            resp.raise_for_status()
            search_results = aigpy.model.dictToModel(resp2json(resp=resp), SearchResult()).tracks.items
            for search_result in search_results:
                if search_result.id is None:
                    continue
                song_info = self._build_song_info(track=search_result, request_overrides=request_overrides)
                if song_info:
                    song_infos.append(song_info)
            # --update progress
            progress.advance(progress_id, 1)
            progress.update(progress_id, description=f"{self.source}.search >>> {search_url} (Success)")
        # failure
        except Exception as err:
            progress.update(progress_id, description=f"{self.source}.search >>> {search_url} (Error: {err})")
        # return
        return song_infos
    '''_primarystreamurl'''
    def _primarystreamurl(self, stream_url: StreamUrl):
        if not isinstance(stream_url, StreamUrl):
            return None
        candidates = []
        if getattr(stream_url, 'url', None):
            candidates.append(stream_url.url)
        urls = getattr(stream_url, 'urls', None)
        if isinstance(urls, (list, tuple)):
            candidates.extend([candidate for candidate in urls if candidate])
        elif urls:
            candidates.append(urls)
        for candidate in candidates:
            if candidate:
                return candidate
        return None
    '''_fetchlyrics'''
    def _fetchlyrics(self, track_id, request_overrides: dict = None):
        lyric_result, lyric = {}, 'NULL'
        overrides = copy.deepcopy(request_overrides or {})
        overrides['params'] = {'countryCode': self.tidal_session.storage.country_code, 'include': 'lyrics'}
        try:
            resp = self._saferequestget(f'https://openapi.tidal.com/v2/tracks/{track_id}', **overrides)
        except Exception:
            return lyric_result, lyric
        if isvalidresp(resp):
            try:
                lyric_result = resp2json(resp)
                lyric = lyric_result.get('included', [{}])[0].get('attributes', {}).get('lrcText', 'NULL')
            except Exception:
                lyric_result, lyric = {}, 'NULL'
        return lyric_result, lyric
    '''_getstreamfortrack'''
    def _getstreamfortrack(self, track_id, request_overrides: dict = None):
        qualities = [
            ('hi_res_lossless', 'HI_RES_LOSSLESS'),
            ('high_lossless', 'LOSSLESS'),
            ('low_320k', 'HIGH'),
            ('low_96k', 'LOW'),
        ]
        base_url = f'https://tidal.com/v1/tracks/{track_id}/playbackinfo'
        for _, quality in qualities:
            overrides = copy.deepcopy(request_overrides or {})
            overrides['params'] = {"playbackmode": "STREAM", "audioquality": quality, "assetpresentation": "FULL"}
            resp = self._saferequestget(base_url, **overrides)
            if not isvalidresp(resp):
                continue
            download_result = aigpy.model.dictToModel(resp2json(resp=resp), StreamRespond())
            if ("vnd.tidal.bt" not in download_result.manifestMimeType) and ("dash+xml" not in download_result.manifestMimeType):
                continue
            try:
                stream_url = self._parsemanifest(stream_resp=download_result)
            except Exception:
                stream_url = None
            if not stream_url:
                continue
            primary_url = self._primarystreamurl(stream_url)
            if not primary_url:
                continue
            download_url_status = AudioLinkTester(headers=self.default_download_headers, cookies=self.default_download_cookies).test(primary_url, request_overrides)
            if download_url_status.get('ok'):
                return download_result, stream_url, download_url_status
        return None, None, {'ok': False, 'reason': 'No playable stream found'}
    '''_extractartistnames'''
    def _extractartistnames(self, track: Track):
        names = []
        artists_attr = getattr(track, 'artists', None)
        if isinstance(artists_attr, Iterable) and not isinstance(artists_attr, (str, bytes)):
            for artist in artists_attr:
                name = getattr(artist, 'name', None)
                if name:
                    names.append(name)
        primary_artist = getattr(track, 'artist', None)
        if not names and primary_artist is not None:
            name = getattr(primary_artist, 'name', None)
            if name:
                names.append(name)
        return names
    '''_build_song_info'''
    def _build_song_info(self, track: Track, request_overrides: dict = None):
        request_overrides = request_overrides or {}
        try:
            download_result, stream_url, download_url_status = self._getstreamfortrack(track_id=track.id, request_overrides=request_overrides)
            if not stream_url or not download_url_status.get('ok'):
                return None
            ext = self._guessextension(stream_url=stream_url)
            duration = seconds2hms(track.duration)
            lyric_result, lyric = self._fetchlyrics(track_id=track.id, request_overrides=request_overrides)
            singer_names = self._extractartistnames(track)
            album = getattr(track, 'album', None)
            album_title = getattr(album, 'title', 'NULL') if album else 'NULL'
            track_number_candidates = [
                getattr(track, 'trackNumber', None),
                getattr(track, 'number', None),
                getattr(track, 'trackNumberOnAlbum', None),
                getattr(track, 'sequence', None),
                getattr(track, 'trackNumberOnPlaylist', None),
            ]
            track_number = None
            for candidate in track_number_candidates:
                track_number = self._normalizetracknumber(candidate)
                if track_number is not None:
                    break
            song_info = dict(
                source=self.source,
                raw_data=dict(search_result=track, download_result=download_result, lyric_result=lyric_result),
                download_url_status=download_url_status,
                download_url=stream_url,
                ext=ext,
                file_size=byte2mb(download_url_status.get('clen')),
                lyric=lyric,
                duration=duration,
                song_name=legalizestring(getattr(track, 'title', 'NULL'), replace_null_string='NULL'),
                singers=legalizestring(', '.join(singer_names), replace_null_string='NULL'),
                album=legalizestring(album_title, replace_null_string='NULL'),
                identifier=track.id,
                track_number=track_number,
            )
            return song_info
        except Exception as err:
            self.logger_handle.error(f"{self.source}._build_song_info >>> {getattr(track, 'id', 'UNKNOWN')} (Error: {err})", disable_print=self.disable_print)
            return None
    '''_fetchtrackmetadata'''
    def _fetchtrackmetadata(self, track_id: str, request_overrides: dict = None):
        overrides = copy.deepcopy(request_overrides or {})
        overrides['params'] = {'countryCode': self.tidal_session.storage.country_code}
        try:
            resp = self._saferequestget(f'https://api.tidal.com/v1/tracks/{track_id}', **overrides)
        except Exception as err:
            self.logger_handle.error(f"{self.source}._fetchtrackmetadata >>> {track_id} (Error: {err})", disable_print=self.disable_print)
            return None
        if not isvalidresp(resp):
            return None
        try:
            return aigpy.model.dictToModel(resp2json(resp=resp), Track())
        except Exception as err:
            self.logger_handle.error(f"{self.source}._fetchtrackmetadata >>> {track_id} (Parse Error: {err})", disable_print=self.disable_print)
            return None
    '''_fetchalbumtracks'''
    def _fetchalbumtracks(self, album_id: str, request_overrides: dict = None):
        tracks = []
        offset, limit = 0, 100
        while True:
            overrides = copy.deepcopy(request_overrides or {})
            overrides['params'] = {'countryCode': self.tidal_session.storage.country_code, 'limit': limit, 'offset': offset}
            resp = self._saferequestget(f'https://api.tidal.com/v1/albums/{album_id}/tracks', **overrides)
            if not isvalidresp(resp):
                break
            data = resp2json(resp=resp)
            items = data.get('items') or []
            if not isinstance(items, list) or len(items) == 0:
                break
            for item in items:
                track_data = item.get('track') if isinstance(item, dict) and 'track' in item else item
                try:
                    track_model = aigpy.model.dictToModel(track_data, Track())
                except Exception:
                    continue
                if getattr(track_model, 'id', None) is None:
                    continue
                tracks.append(track_model)
            offset += len(items)
            total = data.get('totalNumberOfItems')
            if len(items) < limit or (isinstance(total, int) and offset >= total):
                break
        return tracks
    '''_fetchplaylisttracks'''
    def _fetchplaylisttracks(self, playlist_id: str, request_overrides: dict = None):
        tracks = []
        offset, limit = 0, 100
        while True:
            overrides = copy.deepcopy(request_overrides or {})
            overrides['params'] = {'countryCode': self.tidal_session.storage.country_code, 'limit': limit, 'offset': offset}
            resp = self._saferequestget(f'https://api.tidal.com/v1/playlists/{playlist_id}/tracks', **overrides)
            if not isvalidresp(resp):
                break
            data = resp2json(resp=resp)
            items = data.get('items') or []
            if not isinstance(items, list) or len(items) == 0:
                break
            for item in items:
                track_data = item.get('track') if isinstance(item, dict) and 'track' in item else item
                try:
                    track_model = aigpy.model.dictToModel(track_data, Track())
                except Exception:
                    continue
                if getattr(track_model, 'id', None) is None:
                    continue
                tracks.append(track_model)
            offset += len(items)
            total = data.get('totalNumberOfItems')
            if len(items) < limit or (isinstance(total, int) and offset >= total):
                break
        return tracks
    '''_buildsonginfosfromtracks'''
    def _buildsonginfosfromtracks(self, tracks: list, request_overrides: dict = None):
        song_infos = []
        for track in tracks:
            if track is None or getattr(track, 'id', None) is None:
                continue
            song_info = self._build_song_info(track=track, request_overrides=request_overrides)
            if song_info:
                song_infos.append(song_info)
        return self._removeduplicates(song_infos=song_infos)
    '''_parsetidalresource'''
    def _parsetidalresource(self, tidal_url: str):
        parsed = urlparse(tidal_url)
        segments = [segment for segment in parsed.path.split('/') if segment]
        if segments and segments[0].lower() == 'browse':
            segments = segments[1:]
        resource_type, resource_id = None, None
        for idx, segment in enumerate(segments):
            lowered = segment.lower()
            if lowered in ('track', 'album', 'playlist'):
                if idx + 1 >= len(segments):
                    break
                resource_type = lowered
                resource_id = segments[idx + 1]
                break
        if not resource_type or not resource_id:
            raise ValueError(f'Unsupported TIDAL url: {tidal_url}')
        return resource_type, resource_id
    '''parse_url'''
    def parse_url(self, tidal_url: str, request_overrides: dict = None):
        request_overrides = request_overrides or {}
        resource_type, resource_id = self._parsetidalresource(tidal_url)
        tracks = []
        if resource_type == 'track':
            track = self._fetchtrackmetadata(resource_id, request_overrides=request_overrides)
            if track:
                tracks.append(track)
        elif resource_type == 'album':
            tracks = self._fetchalbumtracks(resource_id, request_overrides=request_overrides)
        elif resource_type == 'playlist':
            tracks = self._fetchplaylisttracks(resource_id, request_overrides=request_overrides)
        if not tracks:
            return []
        song_infos = self._buildsonginfosfromtracks(tracks=tracks, request_overrides=request_overrides)
        if not song_infos:
            return []
        keyword = f"tidal_{resource_type}_{resource_id}"
        assigned_work_dirs = []
        album_artist_map = {}
        for song_info in song_infos:
            album_name = song_info.get('album') if isinstance(song_info, dict) else None
            album_key = album_name.strip().lower() if isinstance(album_name, str) else None
            resolved_artist = album_artist_map.get(album_key) if album_key else None
            if not resolved_artist:
                resolved_artist = self._resolve_artist_name(song_info=song_info, keyword=keyword)
                if album_key:
                    album_artist_map[album_key] = resolved_artist
            work_dir = self._constructuniqueworkdir(song_info=song_info, keyword=keyword, album_artist=resolved_artist)
            song_info['work_dir'] = work_dir
            assigned_work_dirs.append(work_dir)
        try:
            results_dir = assigned_work_dirs[0] if assigned_work_dirs else self.work_dir
            touchdir(results_dir)
            self._savetopkl(song_infos, os.path.join(results_dir, 'parse_results.pkl'))
        except Exception:
            pass
        self.logger_handle.info(f'Parsed {len(song_infos)} tracks from {resource_type} {resource_id}.', disable_print=self.disable_print)
        return song_infos