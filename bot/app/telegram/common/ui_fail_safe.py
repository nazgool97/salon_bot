from __future__ import annotations

import contextlib
import logging
import inspect
from typing import Any, TypeVar
from collections.abc import Callable
from typing import cast
from aiogram.types import Message, CallbackQuery

logger = logging.getLogger(__name__)

__all__ = ["safe_text", "safe_edit", "try_or_fallback"]


__all__.append("SafeUIMiddleware")


async def handle_callback_error(
    target: Message | CallbackQuery | Any,
    lang: str,
    e: Exception,
    *,
    fallback_text: str | None = None,
) -> None:
    """Unified error handling for handlers.

    Logs the exception and notifies the user using the safest available channel
    (edit current message where possible, otherwise answer the callback/message).

    Args:
        target: CallbackQuery or Message (or wrapper) that originated the error.
        lang: locale code for translation lookup.
        e: the exception instance.
        fallback_text: optional explicit text to show instead of the generic error key.
    """
    with contextlib.suppress(Exception):
        logger.exception("Handler exception: %s", e)

    try:
        from bot.app.translations import t

        text = fallback_text or t("error", lang)
    except Exception:
        text = fallback_text or "Ошибка"

    # Try safe_edit which prefers editing the existing message or sending a reply
    try:
        await safe_edit(target, text)
        return
    except Exception:
        # safe_edit already logs; fallthrough to direct answers
        pass

    try:
        # If target is a CallbackQuery, try to answer it (toast/alert)
        if isinstance(target, CallbackQuery):
            await target.answer(text, show_alert=True)
            return
        # If it's a Message, reply to it
        if isinstance(target, Message):
            await target.reply(text)
            return
        # Generic fallback: call answer() if available
        if hasattr(target, "answer"):
            res = target.answer(text)
            if inspect.isawaitable(res):
                await cast(Any, res)
            return
    except Exception:
        logger.error("Failed to notify user about error", exc_info=True)
    # Nothing else to do
    return


def normalize_msg(obj: Message | CallbackQuery | Any) -> Message | CallbackQuery:
    """Normalize various aiogram message-like objects to either Message or CallbackQuery.

    - If a CallbackQuery is provided and it contains an accessible Message, return that Message.
    - If a CallbackQuery is provided but its .message is inaccessible or missing, return the CallbackQuery itself.
    - If a Message is provided, return it.
    - For other objects, prefer returning their `.message` if it's a Message, otherwise return the object as-is.

    The goal is to produce a value that static checkers will accept where
    `Message | CallbackQuery` is required while preserving runtime safety.
    """
    try:
        # Avoid top-level import ambiguity in type checkers
        from aiogram.types import Message as AiMessage, CallbackQuery as AiCallback

        if isinstance(obj, AiCallback):
            # Prefer the embedded Message when it's a real Message instance
            msg = getattr(obj, "message", None)
            if isinstance(msg, AiMessage):
                return msg
            return obj
        if isinstance(obj, AiMessage):
            return obj
        # Fallback: maybe it's a wrapper with .message attribute
        msg = getattr(obj, "message", None)
        if isinstance(msg, AiMessage):
            return msg
    except Exception:
        # liberal fallback — return whatever was passed
        pass
    return obj


T = TypeVar("T")


def safe_text(txt: str) -> str:
    """Обрезает текст до безопасной длины для Telegram.

    Args:
        txt: Входной текст.

    Returns:
        Обрезанный текст (максимум 4096 символов).
    """
    max_length = 4096
    if len(txt) > max_length:
        logger.warning("Текст обрезан до %d символов: %s...", max_length, txt[:50])
        return txt[:max_length]
    return txt


async def safe_edit(
    message: Message | CallbackQuery | Any,
    text: str,
    *,
    fallback_text: str | None = None,
    **kwargs: Any,
) -> bool:
    """Безопасно редактирует или отправляет сообщение в Telegram.

    Args:
        message: Объект сообщения Telegram.
        text: Текст для отправки/редактирования.
        fallback_text: Резервный текст, если редактирование и отправка не удались.
        **kwargs: Дополнительные параметры для edit_text или answer.

    Returns:
        True, если операция успешна, иначе False.
    """

    text = safe_text(text)
    fallback_text = safe_text(fallback_text) if fallback_text else None

    # Normalize the incoming object so static checkers see a narrow union
    norm = normalize_msg(message)

    # Try to edit the target message. If editing is not possible or fails
    # (except for the special 'message is not modified' case), we'll try to
    # send a new message as a fallback below.
    try:
        target_msg = getattr(norm, "message", None) if isinstance(norm, CallbackQuery) else norm

        if isinstance(target_msg, Message) and hasattr(target_msg, "edit_text"):
            await target_msg.edit_text(text, **kwargs)
            logger.debug("Сообщение успешно отредактировано: %s", text[:50])
            return True
    except Exception as e:
        serr = str(e).lower()
        # Telegram raises a BadRequest when the new message is identical to the
        # current one. This is not an actionable error for our flows — treat it
        # as a no-op and return success so callers don't try redundant fallbacks.
        if "message is not modified" in serr:
            logger.debug("Ignored 'message is not modified' while editing message")
            return True
        logger.debug("Ошибка редактирования сообщения: %s", e)

    # If we reached here, editing was not performed or failed. Try sending as
    # a new message. Prefer calling answer() on the underlying Message
    # (for CallbackQuery) or on Message itself.
    try:
        if isinstance(norm, CallbackQuery):
            msg_obj = getattr(norm, "message", None)
            if msg_obj is not None and hasattr(msg_obj, "answer"):
                res = msg_obj.answer(text, **kwargs)
                if inspect.isawaitable(res):
                    await cast(Any, res)
            else:
                # As a last resort, answer the callback (toast/alert)
                res = norm.answer(text, **kwargs)
                if inspect.isawaitable(res):
                    await cast(Any, res)
        elif isinstance(norm, Message):
            res = norm.answer(text, **kwargs)
            if inspect.isawaitable(res):
                await cast(Any, res)
        else:
            # Fallback for other types that may implement answer()
            if hasattr(norm, "answer"):
                res = norm.answer(text, **kwargs)
                if inspect.isawaitable(res):
                    await cast(Any, res)
            else:
                raise RuntimeError("No suitable answer method on message object")
        logger.debug("Сообщение успешно отправлено: %s", text[:50])
        return True
    except Exception as e:
        logger.debug("Ошибка отправки сообщения: %s", e)
        if fallback_text and fallback_text != text:
            try:
                if isinstance(norm, CallbackQuery):
                    msg_obj = getattr(norm, "message", None)
                    if msg_obj is not None and hasattr(msg_obj, "answer"):
                        res = msg_obj.answer(fallback_text, **kwargs)
                        if inspect.isawaitable(res):
                            await cast(Any, res)
                    else:
                        res = norm.answer(fallback_text, **kwargs)
                        if inspect.isawaitable(res):
                            await cast(Any, res)
                elif isinstance(norm, Message) or hasattr(norm, "answer"):
                    res = norm.answer(fallback_text, **kwargs)
                    if inspect.isawaitable(res):
                        await cast(Any, res)
                else:
                    raise
                logger.debug("Резервное сообщение отправлено: %s", fallback_text[:50])
                return True
            except Exception as e:
                logger.error("Ошибка отправки резервного сообщения: %s", e)
    logger.error("Не удалось отредактировать или отправить сообщение: %s", text[:50])
    return False


async def try_or_fallback(
    func: Callable[..., T], fallback: Callable[..., T], *args: Any, **kwargs: Any
) -> T | None:
    """Выполняет (в том числе асинхронную) функцию с резервным вариантом при ошибке.

    Поддерживает как синхронные, так и асинхронные функции — если результат
    выполнения является awaitable, он будет ожидаться.
    """
    try:
        result = func(*args, **kwargs)
        # Если func возвращает awaitable — дождёмся результата
        if inspect.isawaitable(result):
            result = await result
        logger.debug("Функция %s успешно выполнена", getattr(func, "__name__", str(func)))
        return result
    except Exception as e:
        logger.debug("Ошибка выполнения функции %s: %s", getattr(func, "__name__", str(func)), e)
        try:
            fb_res = fallback()
            if inspect.isawaitable(fb_res):
                fb_res = await fb_res
            logger.debug(
                "Резервная функция %s выполнена", getattr(fallback, "__name__", str(fallback))
            )
            return fb_res
        except Exception as e:
            logger.error(
                "Ошибка выполнения резервной функции %s: %s",
                getattr(fallback, "__name__", str(fallback)),
                e,
            )
            return None


class SafeUIMiddleware:
    """Middleware that centralizes UI safety checks and error handling for handlers.

    Behavior:
    - If the incoming event contains a `from_user` object but it lacks an `id`,
      it will notify the caller via `safe_edit` and skip handler execution.
    - Wraps handler execution in try/except and forwards exceptions to
      `handle_callback_error` so UI fallbacks are consistent across handlers.

    Register on routers like:
        router.message.middleware(SafeUIMiddleware())
        router.callback_query.middleware(SafeUIMiddleware())
    """

    async def __call__(
        self, handler: Callable[..., Any], event: Any, data: dict[str, Any] | Any
    ) -> Any:
        # Use local handle_callback_error to avoid circular imports
        _hcb = handle_callback_error

        target = event
        # Try to obtain a user id if present
        user_id = None
        try:
            user = getattr(event, "from_user", None)
            if user is None and hasattr(event, "message"):
                msg = event.message
                user = getattr(msg, "from_user", None)
            user_id = getattr(user, "id", None) if user is not None else None
        except Exception:
            user_id = None

        # If user object exists but has no id, inform and return without calling handler
        if getattr(event, "from_user", None) is not None and not user_id:
            with contextlib.suppress(Exception):
                await safe_edit(target, "Error: missing user id")
            return None

        # Execute handler and forward any exceptions to the centralized handler
        try:
            return await handler(event, data)
        except Exception as e:
            try:
                lang = data.get("locale") if isinstance(data, dict) else None
                if _hcb is not None:
                    await _hcb(target, lang or "en", e)
                    return None
            except Exception:
                # Re-raise if even error handling fails so it surfaces to outer handlers
                raise


def safe_handler(require_from_user: bool = True, fallback_text: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to centralize handler-level UI safety and error handling.

    - Verifies `.from_user.id` presence when `require_from_user` is True; if
      missing, it will attempt a safe notification and return early.
    - Catches exceptions raised by the handler and forwards them to
      `handle_callback_error` to present a consistent UI fallback.

    Usage:
        @router.callback_query(...)
        @safe_handler()
        async def handler(cb: CallbackQuery, ...):
            ...
    """
    import functools
    import inspect

    def _find_target(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        for v in list(args) + list(kwargs.values()):
            try:
                if hasattr(v, "from_user") or hasattr(v, "message"):
                    return v
            except Exception:
                continue
        return None

    def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def _wrapped(*args: Any, **kwargs: Any) -> Any:
                target = _find_target(args, kwargs)
                # Check presence of from_user.id when required
                if require_from_user:
                    user_id = None
                    try:
                        if target is not None and hasattr(target, "from_user"):
                            user = target.from_user
                            user_id = getattr(user, "id", None)
                        if (not user_id) and hasattr(target, "message"):
                            m = target.message
                            if hasattr(m, "from_user"):
                                user = m.from_user
                                user_id = getattr(user, "id", None)
                    except Exception:
                        user_id = None

                    if not user_id:
                        try:
                            txt = fallback_text or "Error"
                            await safe_edit(target or "", txt)
                        except Exception:
                            pass
                        return None

                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    try:
                        # prefer using the provided handler above; fall back to a
                        # generic target if none found
                        lang = kwargs.get("locale") or (args[-1] if args else None) or "en"
                        await handle_callback_error(
                            target or (args[0] if args else None),
                            lang,
                            e,
                            fallback_text=fallback_text,
                        )
                        return None
                    except Exception:
                        raise

            return _wrapped
        else:

            @functools.wraps(fn)
            def _wrapped_sync(*args: Any, **kwargs: Any) -> Any:
                return fn(*args, **kwargs)

            return _wrapped_sync

    return _decorator
