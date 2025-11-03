import asyncio
import logging
from pyrogram import utils, raw
from pyrogram import errors as py_errors
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth
from typing import Dict, Union
from Backend.logger import LOGGER
from Backend.helper.exceptions import FIleNotFound
from Backend.helper.pyro import get_file_ids
from Backend.pyrofork.bot import work_loads
from pyrogram import Client, utils, raw
from fastapi import HTTPException


class ByteStreamer:
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.__cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, chat_id: int, message_id: int) -> FileId:
        if message_id not in self.__cached_file_ids:
            file_id = await get_file_ids(self.client, int(chat_id), int(message_id))
            if not file_id:
                LOGGER.info('Message with ID %s not found!', message_id)
                raise FIleNotFound
            self.__cached_file_ids[message_id] = file_id
        return self.__cached_file_ids[message_id]

    async def yield_file(media_session, location, file_size, chunk_size=64 * 1024):
    offset = 0
    max_retries = 3

    while offset < file_size:
        limit = min(chunk_size, file_size - offset)
        attempt = 0

        while True:
            try:
                # Try to fetch the chunk from Telegram
                r = await media_session.send(
                    raw.functions.upload.GetFile(location=location, offset=offset, limit=limit)
                )
                data = r.bytes  # adjust depending on returned object
                if not data:
                    # nothing returned — stop streaming
                    return
                yield data
                offset += len(data)
                break  # chunk succeeded -> break retry loop

            except py_errors.exceptions.service_unavailable_503.Timeout as e:
                attempt += 1
                log.warning("GetFile Timeout (attempt %s/%s) offset=%s: %s", attempt, max_retries, offset, e)
                if attempt >= max_retries:
                    # Give a neat HTTP 503 so the client knows it was a server-side transient failure
                    raise HTTPException(status_code=503, detail="Telegram upload.GetFile timeout, try again later.")
                # exponential backoff
                await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
                continue

            except py_errors.RPCError as e:
                # Non-timeout RPC error from Telegram — log & return a 502/503
                log.exception("Telegram RPCError during GetFile: %s", e)
                raise HTTPException(status_code=502, detail="Telegram API error while streaming file.")

            except asyncio.CancelledError:
                # Client closed connection; stop without raising
                log.info("Streaming cancelled by client at offset=%s", offset)
                return

            except Exception:
                # Catch-all to avoid unhandled exceptions leaving the generator
                log.exception("Unhandled error while streaming file at offset=%s", offset)
                raise HTTPException(status_code=500, detail="Internal server error while streaming.")

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        media_session = client.media_sessions.get(file_id.dc_id, None)
        if media_session is None:
            if file_id.dc_id != await client.storage.dc_id():
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await Auth(client, file_id.dc_id, await client.storage.test_mode()).create(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()
                for _ in range(6):
                    exported_auth = await client.invoke(raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id))
                    try:
                        
                        await media_session.send(raw.functions.auth.ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes))
                        break
                    except AuthBytesInvalid:
                        LOGGER.debug(f"Invalid authorization bytes for DC {file_id.dc_id}, retrying...")
                    except OSError:
                        LOGGER.debug(f"Connection error, retrying...")
                        await asyncio.sleep(2)
                else:
                    await media_session.stop()
                    LOGGER.debug(f"Failed to establish media session for DC {file_id.dc_id} after multiple retries")
                    return None 
            else:
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()
            LOGGER.debug(f"Created media session for DC {file_id.dc_id}")
            client.media_sessions[file_id.dc_id] = media_session
        else:
            LOGGER.debug(f"Using cached media session for DC {file_id.dc_id}")
        return media_session


    @staticmethod
    async def get_location(file_id: FileId) -> Union[raw.types.InputPhotoFileLocation, raw.types.InputDocumentFileLocation, raw.types.InputPeerPhotoFileLocation]:
        file_type = file_id.file_type
        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash)
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(channel_id=utils.get_channel_id(
                        file_id.chat_id), access_hash=file_id.chat_access_hash)
            location = raw.types.InputPeerPhotoFileLocation(peer=peer,
                                                            volume_id=file_id.volume_id,
                                                            local_id=file_id.local_id,
                                                            big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG)
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(id=file_id.media_id,
                                                        access_hash=file_id.access_hash,
                                                        file_reference=file_id.file_reference,
                                                        thumb_size=file_id.thumbnail_size)
        else:
            location = raw.types.InputDocumentFileLocation(id=file_id.media_id,
                                                           access_hash=file_id.access_hash,
                                                           file_reference=file_id.file_reference,
                                                           thumb_size=file_id.thumbnail_size)
        return location

    async def clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.clean_timer)
            self.__cached_file_ids.clear()
            LOGGER.debug("Cleaned the cache")
