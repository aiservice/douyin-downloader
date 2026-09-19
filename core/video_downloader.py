from typing import Any, Dict, Optional

from core import item_reasons
from core.downloader_base import BaseDownloader, DownloadResult
from utils.logger import setup_logger

logger = setup_logger("VideoDownloader")


class VideoDownloader(BaseDownloader):
    async def download(self, parsed_url: Dict[str, Any]) -> DownloadResult:
        result = DownloadResult()

        aweme_id = parsed_url.get("aweme_id")
        if not aweme_id:
            logger.error("No aweme_id found in parsed URL")
            return result

        result.total = 1
        self._progress_set_item_total(1, "单作品下载")
        self._progress_update_step("下载作品", "单作品资源下载中")

        should_download = await self._should_download(aweme_id)
        if not should_download and not self._comments_config():
            logger.info("Video %s already downloaded, skipping", aweme_id)
            result.skipped += 1
            self._progress_advance_item("skipped", str(aweme_id))
            return result

        await self.rate_limiter.acquire()

        aweme_data = await self._get_aweme_detail(
            str(aweme_id),
            content_type=str(parsed_url.get("type") or "video"),
        )
        if not aweme_data:
            logger.error("Failed to get video detail: %s", aweme_id)
            result.failed += 1
            self._progress_advance_item(
                "failed", str(aweme_id), item_reasons.FAIL_DETAIL_UNAVAILABLE
            )
            return result

        if not should_download:
            if await self._collect_comments_for_existing_video(aweme_data):
                logger.info("Collected comments for already-downloaded video %s", aweme_id)
                result.success += 1
                self._progress_advance_item("success", str(aweme_id))
            else:
                logger.info("Video %s already downloaded, skipping", aweme_id)
                result.skipped += 1
                self._progress_advance_item("skipped", str(aweme_id))
            return result

        success = await self._download_aweme(aweme_data)
        if success:
            result.success += 1
            self._progress_advance_item("success", str(aweme_id))
        else:
            result.failed += 1
            self._progress_advance_item("failed", str(aweme_id))

        return result

    async def _get_aweme_detail(
        self, aweme_id: str, *, content_type: str = "video"
    ) -> Optional[Dict[str, Any]]:
        detail = await self.api_client.get_video_detail(aweme_id)
        if detail:
            return detail

        browser_cfg = self.config.get("browser_fallback", {}) or {}
        if not browser_cfg.get("enabled", True):
            return None
        browser_fetcher = getattr(self.api_client, "get_video_detail_via_browser", None)
        if not callable(browser_fetcher):
            return None

        self._progress_update_step("浏览器回补", "直连接口受限，打开作品页面获取详情")
        logger.warning("Video detail API unavailable, starting browser fallback: %s", aweme_id)
        try:
            return await browser_fetcher(
                aweme_id,
                page_type="note" if content_type in {"gallery", "note"} else "video",
                headless=bool(browser_cfg.get("headless", False)),
                wait_timeout_seconds=int(
                    browser_cfg.get("wait_timeout_seconds", 600) or 600
                ),
            )
        except Exception as exc:
            logger.warning("Video browser fallback failed for %s: %s", aweme_id, exc)
            return None

    async def _collect_comments_for_existing_video(self, aweme_data: Dict[str, Any]) -> bool:
        author = aweme_data.get("author", {}) or {}
        author_name = author.get("nickname", "unknown")
        return await self._collect_comments_for_existing_aweme(aweme_data, author_name)

    async def _download_aweme(self, aweme_data: Dict[str, Any]) -> bool:
        author = aweme_data.get("author", {}) or {}
        author_name = author.get("nickname", "unknown")
        # Cache author on the hosting job so JobRow can display the nickname
        # and `retry_failed_awemes` doesn't need to re-fetch user info.
        self._progress_report_author(
            nickname=author_name if author_name != "unknown" else None,
            sec_uid=author.get("sec_uid"),
        )
        return await self._download_aweme_assets(aweme_data, author_name)
