"""QQ channel implementation using botpy SDK."""

import asyncio
from collections import deque
from typing import TYPE_CHECKING

from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import QQConfig

try:
    import botpy
    from botpy.message import C2CMessage

    QQ_AVAILABLE = True
except ImportError:
    QQ_AVAILABLE = False
    botpy = None
    C2CMessage = None

if TYPE_CHECKING:
    from botpy.message import C2CMessage


def _make_bot_class(channel: "QQChannel") -> "type[botpy.Client]":
    """Create a botpy Client subclass bound to the given channel."""
    intents = botpy.Intents(public_messages=True, direct_message=True)

    class _Bot(botpy.Client):
        def __init__(self):
            super().__init__(intents=intents)

        async def on_ready(self):
            logger.info("QQ bot ready: {}", self.robot.name)

        async def on_c2c_message_create(self, message: "C2CMessage"):
            await channel._on_message(message)

        async def on_direct_message_create(self, message):
            await channel._on_message(message)

        async def on_at_message_create(self, message):
            logger.info("Received @ message (group): {}", message)
            await channel._on_group_message(message)

    return _Bot


class QQChannel(BaseChannel):
    """QQ channel using botpy SDK with WebSocket connection."""

    name = "qq"

    def __init__(self, config: QQConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: QQConfig = config
        self._client: "botpy.Client | None" = None
        self._processed_ids: deque = deque(maxlen=1000)

    async def start(self) -> None:
        """Start the QQ bot."""
        if not QQ_AVAILABLE:
            logger.error("QQ SDK not installed. Run: pip install qq-botpy")
            return

        if not self.config.app_id or not self.config.secret:
            logger.error("QQ app_id and secret not configured")
            return

        self._running = True
        BotClass = _make_bot_class(self)
        self._client = BotClass()

        logger.info("QQ bot started (C2C private message + group messages)")
        await self._run_bot()

    async def _run_bot(self) -> None:
        """Run the bot connection with auto-reconnect."""
        while self._running:
            try:
                await self._client.start(appid=self.config.app_id, secret=self.config.secret)
            except Exception as e:
                logger.warning("QQ bot error: {}", e)
            if self._running:
                logger.info("Reconnecting QQ bot in 5 seconds...")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop the QQ bot."""
        self._running = False
        if self._client:
            try:
                await self._client.close()
            except Exception:
                pass
        logger.info("QQ bot stopped")

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through QQ."""
        if not self._client:
            logger.warning("QQ client not initialized")
            return
        
        try:
            # 检查是否是群聊消息（通过metadata判断）
            metadata = msg.metadata or {}
            msg_type = metadata.get("type", "private")
            
            logger.info("Sending QQ message (type: {}): {}", msg_type, msg.content[:100] + "..." if len(msg.content) > 100 else msg.content)
            
            if msg_type == "group":
                # 发送群聊消息
                group_id = metadata.get("group_id", msg.chat_id)
                logger.info("Sending to group: {}", group_id)
                
                # 使用post_group_message发送群聊消息
                result = await self._client.api.post_group_message(
                    group_openid=group_id,
                    msg_type=0,  # 0表示文本消息
                    content=msg.content,
                )
                logger.info("Group message sent successfully: {}", result)
            else:
                # 发送私聊消息
                logger.info("Sending to user: {}", msg.chat_id)
                
                # 使用post_c2c_message发送私聊消息
                result = await self._client.api.post_c2c_message(
                    openid=msg.chat_id,
                    msg_type=0,  # 0表示文本消息
                    content=msg.content,
                )
                logger.info("Private message sent successfully: {}", result)
        except Exception as e:
            logger.error("Error sending QQ message: {}", e)
            logger.exception("Detailed error:")

    async def _on_message(self, data: "C2CMessage") -> None:
        """Handle incoming private message from QQ."""
        try:
            # Dedup by message ID
            if data.id in self._processed_ids:
                return
            self._processed_ids.append(data.id)

            author = data.author
            user_id = str(getattr(author, 'id', None) or getattr(author, 'user_openid', 'unknown'))
            content = (data.content or "").strip()
            if not content:
                return

            await self._handle_message(
                sender_id=user_id,
                chat_id=user_id,
                content=content,
                metadata={
                    "message_id": data.id,
                    "type": "private",
                    "sender_id": user_id
                },
            )
        except Exception:
            logger.exception("Error handling QQ message")

    async def _on_group_message(self, data) -> None:
        """Handle incoming group message from QQ."""
        try:
            # Dedup by message ID
            if data.id in self._processed_ids:
                return
            self._processed_ids.append(data.id)

            author = data.author
            # 群聊消息中，用户ID是 member_openid
            user_id = str(getattr(author, 'member_openid', None) or getattr(author, 'user_openid', 'unknown'))
            content = (data.content or "").strip()
            if not content:
                return

            # 获取群组ID - 群聊消息中有 group_openid 属性
            group_id = str(getattr(data, 'group_openid', 'unknown'))
            
            # 打印调试信息
            logger.info("Received group message:")
            logger.info("  Group ID: {}", group_id)
            logger.info("  Sender ID: {}", user_id)
            logger.info("  Content: {}", content[:100] + "..." if len(content) > 100 else content)
            
            await self._handle_message(
                sender_id=user_id,
                chat_id=group_id,  # 使用群组ID作为chat_id
                content=content,
                metadata={
                    "message_id": data.id,
                    "type": "group",
                    "group_id": group_id,
                    "sender_id": user_id
                },
            )
        except Exception:
            logger.exception("Error handling QQ group message")
