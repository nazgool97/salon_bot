"""Example Aiogram handler that opens the Telegram Mini App."""

from __future__ import annotations

import os
import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, WebAppInfo

router = Router()
logger = logging.getLogger(__name__)

WEBAPP_URL = os.getenv("TWA_WEBAPP_URL", "")


@router.message(Command("webapp"))
async def open_webapp(message: Message) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Open booking",
                    web_app=WebAppInfo(url=f"{WEBAPP_URL}?entry=booking"),
                )
            ]
        ]
    )
    await message.answer("Open the booking mini app", reply_markup=keyboard)
