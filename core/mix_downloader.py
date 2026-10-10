from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiofiles

from core import item_reasons
from core.downloader_base import BaseDownloader, DownloadResult
from core.user_modes.base_strategy import (
    _HARD_PAGE_FAILURE_CAUSES,
    BaseUserModeStrategy,
    PageRequestFailedError,
    fetch_page_folding_bridge_failure,
    hard_page_failure_message,
    page_failure_advice,
)
from utils.logger import setup_logger
from utils.naming import compact_legacy_label, episode_number_width

logger = setup_logger("MixDownloader")

_SCOPE_LABEL = "合集"
# v1 captured network arrival order without checking the collection DOM.
_MIX_CACHE_VERSION = 2


def derive_mix_collection_dir(mix_detail: Optional[Dict[str, Any]], mix_id: Any) -> str:
    """Folder name for a 合集's own directory.

    Prefer the mix's display name (``mix_name`` → ``title``); fall back to the
    ``mix_id`` so downloads never collapse into a bare shared ``mix`` dir.
    Shared by the fresh-download (:class:`MixDownloader`) and retry
    (:mod:`core.retry_executor`) paths so a retried item lands in the SAME
    ``<author>/mix/<collection>/`` folder as its originally-downloaded peers.
    """
    name = ""
    if isinstance(mix_detail, dict):
        name = (mix_detail.get("mix_name") or mix_detail.get("title") or "").strip()
    return name or str(mix_id)


def derive_legacy_mix_output_root(
    mix_detail: Optional[Dict[str, Any]], mix_id: Any
) -> str:
    """Root directory used by the original standalone collection downloader."""
    name = derive_mix_collection_dir(mix_detail, mix_id)
    compact_name = compact_legacy_label(name)
    if compact_name and compact_name != str(mix_id):
        return f"mix_{compact_name}_{mix_id}"
    return f"mix_{mix_id}"


class MixDownloader(BaseDownloader):
    async def download(self, parsed_url: Dict[str, Any]) -> DownloadResult:
        result = DownloadResult()
        self._browser_mix_fallback_used = False
        self._browser_mix_detail: Optional[Dict[str, Any]] = None
        self._mix_collection_complete = False
        self._mix_cache_hit = False

        mix_id = parsed_url.get("mix_id")
        if not mix_id:
            logger.error("No mix_id found in parsed URL")
            return result

        cached = await self._load_mix_cache(str(mix_id))
        if cached is not None:
            aweme_list = cached["items"]
            result.incomplete_reason = None
            cached_detail = cached.get("mix_detail")
            if isinstance(cached_detail, dict):
                self._browser_mix_detail = cached_detail
            self._browser_mix_fallback_used = True
            self._mix_collection_complete = True
            self._mix_cache_hit = True
            self._progress_update_step("读取合集缓存", f"已加载 {len(aweme_list)} 条")
        else:
            aweme_list, result.incomplete_reason = await self._collect_mix_aweme_list(str(mix_id))

        result.total = len(aweme_list)
        self._progress_set_item_total(result.total, "合集作品待下载")
        self._progress_update_step("下载合集", f"mix_id={mix_id}，待处理 {result.total} 条")

        mix_detail = self._browser_mix_detail
        if mix_detail is None and not self._browser_mix_fallback_used:
            mix_detail = await self._get_mix_detail(str(mix_id))
        if mix_detail is None:
            mix_detail = self._mix_detail_from_items(aweme_list)
        author_name = (
            (mix_detail.get("author") or {}).get("nickname")
            if isinstance(mix_detail, dict)
            else None
        ) or self._author_name_from_items(aweme_list) or "mix"

        if self._mix_collection_complete and not self._mix_cache_hit and aweme_list:
            await self._save_mix_cache(str(mix_id), aweme_list, mix_detail)

        # The sequence is assigned before workers start, so concurrent
        # completion order can never change filenames. Layout and ordering are
        # independent: page order is the default even for the legacy folders.
        legacy_layout = bool(self.config.get("legacy_layout", False))
        mix_order = str(self.config.get("mix_order") or "page").strip().lower()
        if mix_order not in {"page", "create_time_asc", "create_time_desc"}:
            logger.warning("Invalid mix_order value: %s, using page", mix_order)
            mix_order = "page"
        aweme_list = [dict(item) for item in aweme_list]
        if mix_order != "page":
            aweme_list.sort(
                key=lambda item: int(item.get("create_time") or 0),
                reverse=mix_order == "create_time_desc",
            )
        logger.info("合集排序：mix_id=%s mix_order=%s items=%s", mix_id, mix_order, len(aweme_list))
        episode_width = episode_number_width(len(aweme_list))
        for episode_number, item in enumerate(aweme_list, start=1):
            item["_episode_number"] = episode_number
            item["_episode_width"] = episode_width

        # Give each 合集 its own folder: <author>/mix/<合集名>/... .
        collection_dir = None if legacy_layout else derive_mix_collection_dir(mix_detail, mix_id)
        output_root = (
            derive_legacy_mix_output_root(mix_detail, mix_id) if legacy_layout else None
        )

        async def _process_aweme(item: Dict[str, Any]):
            aweme_id = item.get("aweme_id")
            if not aweme_id:
                self._progress_advance_item(
                    "failed", "missing_aweme_id", item_reasons.FAIL_MISSING_ID
                )
                return {"status": "failed", "aweme_id": None}

            if not await self._should_download(str(aweme_id)):
                saved = await self._collect_comments_for_existing_aweme(
                    item,
                    author_name,
                    mode="mix",
                    collection_dir=collection_dir,
                    output_root=output_root,
                )
                status = "success" if saved else "skipped"
                self._progress_advance_item(status, str(aweme_id))
                return {"status": status, "aweme_id": aweme_id}

            success = await self._download_aweme_assets(
                item,
                author_name,
                mode="mix",
                collection_dir=collection_dir,
                output_root=output_root,
            )
            status = "success" if success else "failed"
            self._progress_advance_item(status, str(aweme_id))
            return {"status": status, "aweme_id": aweme_id}

        download_results = await self.queue_manager.download_batch(_process_aweme, aweme_list)
        # download_batch 按 items 顺序回结果，异常条目只剩异常对象，靠 zip 找回 id。
        for item, entry in zip(aweme_list, download_results):
            status = entry.get("status") if isinstance(entry, dict) else None
            if status == "success":
                result.success += 1
            elif status == "skipped":
                result.skipped += 1
            else:
                result.failed += 1
                if not isinstance(entry, dict):
                    self._settle_crashed_item(item, entry)
        return result

    async def _collect_mix_aweme_list(
        self, mix_id: str
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """返回 ``(作品列表, 没取全的原因)``。

        空页分三档,与 ``BaseUserModeStrategy._raise_if_page_request_failed``
        同一套判据:请求失败 / 接口报错是硬失败——第一页就失败没有成果可保,
        抛 ``PageRequestFailedError`` 按失败结案;后面的页失败则保住已拿到的
        条目并记原因。``"aweme_list": null`` 只软停(见 docs/spec/gotchas.md)。
        以前一律 ``break``,``mix/aweme/`` 被 Argus 403 时任务就成了
        「全部成功 · 0 项」,用户看不到任何原因。
        """
        fetch_mix_aweme = getattr(self.api_client, "get_mix_aweme", None)
        if not callable(fetch_mix_aweme):
            logger.error("API client has no get_mix_aweme implementation")
            return [], None

        aweme_list: List[Dict[str, Any]] = []
        has_more = True
        cursor = 0
        page_index = 0
        number_limit = int(self.config.get("number", {}).get("mix", 0) or 0)

        while has_more:
            await self.rate_limiter.acquire()
            page_index += 1
            raw_page = await fetch_page_folding_bridge_failure(
                fetch_mix_aweme, mix_id, cursor=cursor, count=20
            )
            page = BaseUserModeStrategy._normalize_page_data(raw_page)
            items = page.get("items", [])
            if not items:
                browser_result = await self._try_browser_fallback(
                    page,
                    mix_id=mix_id,
                    page_index=page_index,
                    collected=len(aweme_list),
                    number_limit=number_limit,
                )
                if browser_result is not None:
                    return browser_result
                incomplete_reason = self._stop_reason_for_empty_page(
                    page, page_index=page_index, collected=len(aweme_list)
                )
                return aweme_list, incomplete_reason

            for item in items:
                aweme = self._extract_aweme_from_item(item)
                if aweme:
                    aweme_list.append(aweme)

            if number_limit > 0 and len(aweme_list) >= number_limit:
                aweme_list = aweme_list[:number_limit]
                break

            has_more = bool(page.get("has_more", False))
            if not has_more:
                self._mix_collection_complete = True
            next_cursor = int(page.get("max_cursor", 0) or 0)
            if has_more and next_cursor == cursor:
                logger.warning(
                    "Mix pagination cursor did not advance (%s), stop to avoid loop",
                    cursor,
                )
                break
            cursor = next_cursor

        return aweme_list, None

    async def _try_browser_fallback(
        self,
        page: Dict[str, Any],
        *,
        mix_id: str,
        page_index: int,
        collected: int,
        number_limit: int,
    ) -> Optional[Tuple[List[Dict[str, Any]], Optional[str]]]:
        """Use the collection page only for a deterministic CLI Argus rejection."""
        failure = page.get("raw")
        if (
            page_index != 1
            or collected > 0
            or not BaseUserModeStrategy._page_rejected(page)
            or bool(getattr(failure, "via_bridge", False))
        ):
            return None

        browser_cfg = self.config.get("browser_fallback", {}) or {}
        if not browser_cfg.get("enabled", True):
            return None
        collector = getattr(self.api_client, "collect_mix_aweme_via_browser", None)
        if not callable(collector):
            return None

        self._progress_update_step("浏览器采集合集", f"mix_id={mix_id}")
        logger.warning("mix/aweme 被 Argus 拒绝，启动真实浏览器采集合集：mix_id=%s", mix_id)
        try:
            payload = await collector(
                mix_id,
                max_items=number_limit,
                headless=bool(browser_cfg.get("headless", False)),
                max_scrolls=int(browser_cfg.get("max_scrolls", 240) or 240),
                idle_rounds=int(browser_cfg.get("idle_rounds", 8) or 8),
                wait_timeout_seconds=int(browser_cfg.get("wait_timeout_seconds", 600) or 600),
            )
        except Exception as exc:
            logger.warning("合集浏览器兜底失败：%s", exc)
            self._progress_update_step("浏览器采集合集", f"启动失败：{exc}")
            raise PageRequestFailedError(f"合集浏览器采集失败：{exc}") from exc

        if not isinstance(payload, dict):
            logger.warning("合集浏览器兜底返回了无效结果：%s", type(payload).__name__)
            raise PageRequestFailedError("合集浏览器采集失败：返回了无效结果")
        raw_items = payload.get("items")
        if not isinstance(raw_items, list):
            raise PageRequestFailedError("合集浏览器采集失败：未返回作品列表")

        items: List[Dict[str, Any]] = []
        seen_ids = set()
        for item in raw_items:
            aweme = self._extract_aweme_from_item(item)
            if not aweme:
                continue
            aweme_id = str(aweme.get("aweme_id") or "")
            if aweme_id and aweme_id not in seen_ids:
                seen_ids.add(aweme_id)
                items.append(aweme)
        if number_limit > 0:
            items = items[:number_limit]

        complete = bool(payload.get("complete"))
        if not items and not complete:
            reason = str(payload.get("reason") or "没有捕获到合集接口响应")
            logger.warning("合集浏览器兜底未成功：%s", reason)
            self._progress_update_step("浏览器采集合集", f"未成功：{reason}")
            raise PageRequestFailedError(f"合集浏览器采集失败：{reason}")

        detail = payload.get("mix_detail")
        if isinstance(detail, dict):
            self._browser_mix_detail = detail
        self._browser_mix_fallback_used = True
        self._mix_collection_complete = bool(payload.get("terminal")) or (
            complete and number_limit <= 0
        )

        incomplete_reason = None
        if not complete and not (number_limit > 0 and len(items) >= number_limit):
            detail_reason = str(payload.get("reason") or "未确认合集已取完")
            incomplete_reason = f"合集浏览器采集：{detail_reason}；内容可能不完整"
        self._progress_update_step(
            "浏览器采集合集",
            f"已获取 {len(items)} 条" + ("" if incomplete_reason is None else "，可能不完整"),
        )
        return items, incomplete_reason

    def _mix_cache_settings(self) -> Tuple[bool, int]:
        raw = self.config.get("mix_cache", {}) or {}
        if not isinstance(raw, dict):
            return bool(raw), 86_400
        enabled = raw.get("enabled", True)
        if isinstance(enabled, str):
            enabled = enabled.strip().lower() in {"1", "true", "yes", "on"}
        try:
            ttl_seconds = max(0, int(raw.get("ttl_seconds", 86_400) or 0))
        except (TypeError, ValueError):
            ttl_seconds = 86_400
        return bool(enabled), ttl_seconds

    def _mix_cache_path(self, mix_id: str) -> Path:
        safe_id = "".join(char for char in str(mix_id) if char.isalnum() or char in "_-")
        return self.file_manager.base_path / ".cache" / "mix" / f"{safe_id or 'unknown'}.json"

    async def _load_mix_cache(self, mix_id: str) -> Optional[Dict[str, Any]]:
        enabled, ttl_seconds = self._mix_cache_settings()
        if not enabled:
            return None
        path = self._mix_cache_path(mix_id)
        try:
            stat = await asyncio.to_thread(path.stat)
            if ttl_seconds > 0 and time.time() - stat.st_mtime > ttl_seconds:
                logger.info("Mix cache expired: mix_id=%s path=%s", mix_id, path)
                return None
            async with aiofiles.open(path, "r", encoding="utf-8") as handle:
                payload = json.loads(await handle.read())
        except FileNotFoundError:
            return None
        except (OSError, ValueError, TypeError) as exc:
            logger.warning("Ignoring invalid mix cache %s: %s", path, exc)
            return None

        if (
            not isinstance(payload, dict)
            or payload.get("version") != _MIX_CACHE_VERSION
            or str(payload.get("mix_id") or "") != str(mix_id)
            or payload.get("complete") is not True
            or not isinstance(payload.get("items"), list)
        ):
            logger.warning("Ignoring incomplete or incompatible mix cache: %s", path)
            return None

        items = [
            item
            for item in payload["items"]
            if isinstance(item, dict) and str(item.get("aweme_id") or "")
        ]
        if not items:
            return None
        number_limit = int(self.config.get("number", {}).get("mix", 0) or 0)
        if number_limit > 0:
            items = items[:number_limit]
        logger.warning("命中合集列表缓存：mix_id=%s items=%s path=%s", mix_id, len(items), path)
        return {"items": items, "mix_detail": payload.get("mix_detail")}

    async def _save_mix_cache(
        self,
        mix_id: str,
        items: List[Dict[str, Any]],
        mix_detail: Optional[Dict[str, Any]],
    ) -> None:
        enabled, _ttl_seconds = self._mix_cache_settings()
        if not enabled or not items:
            return
        path = self._mix_cache_path(mix_id)
        temp_path = path.with_name(f".{path.name}.{os.getpid()}.{id(self)}.tmp")
        payload = {
            "version": _MIX_CACHE_VERSION,
            "mix_id": str(mix_id),
            "saved_at": int(time.time()),
            "complete": True,
            "mix_detail": mix_detail if isinstance(mix_detail, dict) else None,
            "items": items,
        }
        try:
            serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
            async with aiofiles.open(temp_path, "w", encoding="utf-8") as handle:
                await handle.write(serialized)
            await asyncio.to_thread(os.replace, temp_path, path)
            logger.warning(
                "已缓存完整合集列表：mix_id=%s items=%s path=%s",
                mix_id,
                len(items),
                path,
            )
        except (OSError, TypeError, ValueError) as exc:
            logger.warning("Save mix cache failed for %s: %s", mix_id, exc)
            try:
                await asyncio.to_thread(temp_path.unlink, missing_ok=True)
            except OSError:
                pass

    @staticmethod
    def _stop_reason_for_empty_page(
        page: Dict[str, Any], *, page_index: int, collected: int
    ) -> Optional[str]:
        cause = BaseUserModeStrategy._empty_page_failure_cause(page)
        if cause is None:
            return None
        detail = f"{_SCOPE_LABEL} 第 {page_index} 页{cause}"
        if cause in _HARD_PAGE_FAILURE_CAUSES and collected <= 0:
            logger.warning("%s page %d %s, aborting", _SCOPE_LABEL, page_index, cause)
            raise PageRequestFailedError(hard_page_failure_message(page, detail))
        logger.warning("%s page %d %s, stopping walk", _SCOPE_LABEL, page_index, cause)
        advice = page_failure_advice(page)
        if advice:
            return f"{detail}：{advice}；内容可能不完整"
        return f"{detail}，内容可能不完整，请稍后重试"

    async def _get_mix_detail(self, mix_id: str) -> Optional[Dict[str, Any]]:
        getter = getattr(self.api_client, "get_mix_detail", None)
        if not callable(getter):
            return None
        try:
            return await getter(mix_id)
        except Exception as exc:
            logger.warning("Get mix detail failed: %s", exc)
            return None

    @staticmethod
    def _mix_detail_from_items(items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        for item in items:
            detail = (
                item.get("mix_info") or item.get("series_info")
                if isinstance(item, dict)
                else None
            )
            if isinstance(detail, dict):
                normalized = dict(detail)
                if not normalized.get("mix_id"):
                    normalized["mix_id"] = normalized.get("series_id") or ""
                if not normalized.get("mix_name"):
                    normalized["mix_name"] = normalized.get("series_name") or ""
                return normalized
        return None

    @staticmethod
    def _author_name_from_items(items: List[Dict[str, Any]]) -> Optional[str]:
        for item in items:
            author = item.get("author") if isinstance(item, dict) else None
            nickname = author.get("nickname") if isinstance(author, dict) else None
            if nickname:
                return str(nickname)
        return None

    @staticmethod
    def _extract_aweme_from_item(item: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None
        if item.get("aweme_id"):
            return item
        for key in ("aweme", "aweme_info", "aweme_detail"):
            value = item.get(key)
            if isinstance(value, dict) and value.get("aweme_id"):
                return value
        return None
