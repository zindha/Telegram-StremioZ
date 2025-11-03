import asyncio
import logging
from typing import Dict, Union
from fastapi import HTTPException
from pyrogram import Client, utils, raw
from pyrogram.errors import AuthBytesInvalid, RPCError
from pyrogram import errors as py_errors
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth

from Backend.logger import LOGGER
from Backend.helper.exceptions import FIleNotFound
from Backend.helper.pyro import get_file_ids
from Backend.pyrofork.bot import work_loads


class ByteStreamer:
    def __init__(self, client: Client):
        self.client: Client = client
        self.clean_timer = 30 * 60  # 30 minutes
        self.__cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def clean_cache(self) -> None:
        """Periodically clears cached file IDs."""
        while True:
            await asyncio.sleep(self.clean_timer)
            self.__cached_file_ids.clear()
            LOGGER.debug("Cleaned the cache")

    async def get_file_properties(self, chat_id: int, message_id: int) -> FileId:
        """Fetch and cache file metadata from Telegram."""
        if message_id not in self.__cached_file_ids:
            file_id = await get_file_ids(self.client, int(chat_id), int(message_id))
            if not file_id:
                LOGGER.warning("Message with ID %s not found!", message_id)
                raise FIleNotFound
            self.__cached_file_ids[message_id] = file_id
        return self.__cached_file_ids[message_id]

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """Create or reuse a Telegram media session."""
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
                    exported_auth = await client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                    )
                    try:
                        await media_session.send(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, bytes=exported_auth.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        LOGGER.debug(f"Invalid authorization bytes for DC {file_id.dc_id}, retrying...")
                    except OSError:
                        LOGGER.debug(f"Connection error, retrying...")
                        await asyncio.sleep(2)
                else:
                    await media_session.stop()
                    LOGGER.error(f"Failed to establish media session for DC {file_id.dc_id}")
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

            LOGGER.info(f"Created media session for DC {file_id.dc_id}")
            client.media_sessions[file_id.dc_id] = media_session
        else:
            LOGGER.debug(f"Using cached media session for DC {file_id.dc_id}")
        return media_session

    @staticmethod
    async def get_location(
        file_id: FileId,
    ) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        """Get the Telegram file location for a given FileId."""
        file_type = file_id.file_type
        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )
            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        return location

    async def yield_file(
        self,
        file_id,
        index,
        offset,
        first_part_cut,
        last_part_cut,
        part_count,
        chunk_size=64 * 1024,
    ):
        """Stream Telegram media in chunks with optional range and part control."""
        try:
            media_session = await self.generate_media_session(self.client, file_id)
            location = await self.get_location(file_id)

            file_size = getattr(file_id, "file_size", None)
            if not file_size:
                raise HTTPException(status_code=404, detail="Unable to fetch file size from Telegram")

            start_offset = offset + first_part_cut
            end_offset = file_size if part_count == index + 1 else start_offset + chunk_size - last_part_cut

            current_offset = start_offset
            max_retries = 3

            MAX_TELEGRAM_CHUNK = 512 * 1024  # 512 KB hard Telegram limit
            
            while current_offset < end_offset:
                limit = min(MAX_TELEGRAM_CHUNK, end_offset - current_offset)
                attempt = 0

                while True:
                    try:
                        result = await media_session.send(
                            raw.functions.upload.GetFile(
                                location=location, offset=current_offset, limit=limit
                            )
                        )

                        data = getattr(result, "bytes", None)
                        if not data:
                            return

                        yield data
                        current_offset += len(data)
                        break  # Success, go to next chunk

                    except py_errors.exceptions.service_unavailable_503.Timeout as e:
                        attempt += 1
                        LOGGER.warning(f"Timeout ({attempt}/{max_retries}) at offset={current_offset}: {e}")
                        if attempt >= max_retries:
                            raise HTTPException(
                                status_code=503,
                                detail="Telegram timeout while downloading. Try again later.",
                            )
                        await asyncio.sleep(0.5 * (2 ** (attempt - 1)))

                    except RPCError as e:
                        LOGGER.error(f"Telegram RPCError: {e}")
                        raise HTTPException(
                            status_code=502, detail="Telegram API error during stream."
                        )

                    except asyncio.CancelledError:
                        LOGGER.info(f"Streaming cancelled at offset={current_offset}")
                        return

                    except Exception as e:
                        LOGGER.exception(f"Unhandled streaming error: {e}")
                        raise HTTPException(
                            status_code=500, detail="Internal streaming error from Telegram."
                        )

        except Exception as e:
            LOGGER.exception(f"Error in yield_file: {e}")
            raise HTTPException(status_code=500, detail=str(e))
