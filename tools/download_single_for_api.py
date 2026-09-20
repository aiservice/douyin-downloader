#!/usr/bin/env python3
"""Download one Douyin video for an external process and print its exact path.

The normal CLI is optimized for interactive batch work.  This entry point is
intentionally small and machine-facing: one input URL, one output directory,
and one final JSON marker that callers can parse without guessing filenames.
"""

import argparse
import asyncio
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from auth import CookieManager  # noqa: E402
from config import ConfigLoader  # noqa: E402
from control import QueueManager, RateLimiter, RetryHandler  # noqa: E402
from core import DouyinAPIClient, DownloaderFactory, URLParser  # noqa: E402
from storage import Database, FileManager  # noqa: E402
from utils.logger import set_console_log_level  # noqa: E402
from utils.validators import is_short_url, normalize_short_url  # noqa: E402

RESULT_PREFIX = "DOUYIN_DOWNLOAD_RESULT="
_MARKDOWN_URL_PATTERN = re.compile(r"\[[^\]]*\]\((https?://[^)\s]+)\)")
_URL_PATTERN = re.compile(r"https?://[^\s，。；;！？!\[\]()<>\"']+")


def extract_first_url(value: str) -> str:
    """Accept either a plain URL or the share text copied from Douyin."""
    markdown_match = _MARKDOWN_URL_PATTERN.search(value or "")
    if markdown_match:
        return markdown_match.group(1)
    match = _URL_PATTERN.search(value or "")
    if not match:
        raise ValueError("输入内容中没有找到抖音链接")
    return match.group(0).rstrip("、,")


def _primary_video_path(file_context: Dict[str, Any]) -> Path:
    return Path(file_context["save_dir"]) / f"{file_context['file_stem']}.mp4"


def _resolve_database_path(config: ConfigLoader) -> Path:
    database_path = Path(config.get("database_path", "dy_downloader.db") or "dy_downloader.db")
    if database_path.is_absolute():
        return database_path
    return PROJECT_ROOT / database_path


async def _get_video_detail_for_api(
    downloader: Any,
    aweme_id: str,
    config: ConfigLoader,
) -> Dict[str, Any]:
    """Fetch detail while preserving the exact Playwright failure for Java logs."""
    detail = await downloader.api_client.get_video_detail(aweme_id)
    if detail:
        return detail

    browser_cfg = config.get("browser_fallback", {}) or {}
    if not browser_cfg.get("enabled", True):
        raise RuntimeError(
            "直连接口被 Argus 拒绝，且 browser_fallback.enabled=false"
        )

    browser_fetcher = getattr(
        downloader.api_client,
        "get_video_detail_via_browser",
        None,
    )
    if not callable(browser_fetcher):
        raise RuntimeError("当前 Python 下载器不支持 Playwright 浏览器回补")

    headless = bool(browser_cfg.get("headless", False))
    wait_timeout_seconds = int(browser_cfg.get("wait_timeout_seconds", 600) or 600)
    try:
        detail = await browser_fetcher(
            aweme_id,
            page_type="video",
            headless=headless,
            wait_timeout_seconds=wait_timeout_seconds,
        )
    except Exception as exc:
        raise RuntimeError(
            "Playwright 浏览器回补启动或执行失败"
            f"（{type(exc).__name__}: {exc}；headless={headless}）"
        ) from exc

    if not detail:
        mode_hint = (
            "当前为 headless=true，若页面出现验证框请改为 false"
            if headless
            else "浏览器已运行但没有捕获到作品详情，请检查是否出现验证框或登录已失效"
        )
        raise RuntimeError(f"Playwright 浏览器回补未获取到作品详情；{mode_hint}")
    return detail


async def _should_download_for_api(
    downloader: Any,
    aweme_data: Dict[str, Any],
    author_name: str,
    video_path: Path,
) -> bool:
    """Use the newest exact-target check while remaining compatible with v2.0."""
    exact_checker = getattr(downloader, "_should_download_aweme", None)
    if callable(exact_checker):
        return bool(await exact_checker(aweme_data, author_name))

    aweme_id = str(aweme_data.get("aweme_id") or "")
    legacy_checker = getattr(downloader, "_should_download", None)
    if not callable(legacy_checker):
        raise RuntimeError("当前 Python 下载器缺少已存在文件检查方法")
    if not await legacy_checker(aweme_id):
        return False

    # Older downloader versions only recognize filenames containing aweme_id.
    # The API bridge already rendered the exact configured target, so cover
    # idless templates (for example {episode_title}) here as well.
    file_manager = getattr(downloader, "file_manager", None)
    file_exists = getattr(file_manager, "file_exists", None)
    if callable(file_exists) and file_exists(video_path):
        return False
    return True


async def download_single(
    url_text: str,
    output_dir: Path,
    config_path: Path,
) -> Tuple[Path, str, str]:
    """Download one regular video and return (path, aweme_id, status)."""
    input_url = extract_first_url(url_text)
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    config = ConfigLoader(str(config_path))
    # Keep cookies, quality, proxy, naming, and browser-fallback settings from
    # config.yml, but make this invocation deterministic for the Java API.
    config.update(
        path=str(output_dir),
        link=[input_url],
        thread=1,
        video=True,
        cover=False,
        music=False,
        avatar=False,
        json=False,
        comments={"enabled": False},
        transcript={"enabled": False},
        notifications={"enabled": False},
    )

    cookie_manager = CookieManager(str(config_path.parent / ".cookies.json"))
    cookie_manager.set_cookies(config.get_cookies())

    database = None
    if config.get("database"):
        database = Database(db_path=str(_resolve_database_path(config)))
        await database.initialize()

    try:
        async with DouyinAPIClient(
            cookie_manager.get_cookies(),
            proxy=config.get("proxy"),
        ) as api_client:
            resolved_url = input_url
            if is_short_url(resolved_url):
                resolved_url = await api_client.resolve_short_url(
                    normalize_short_url(resolved_url)
                )
                if not resolved_url:
                    raise RuntimeError("抖音短链接解析失败")

            parsed = URLParser.parse(resolved_url)
            if not parsed or parsed.get("type") != "video" or not parsed.get("aweme_id"):
                raise ValueError("Python 下载方式当前只支持单个抖音视频链接")

            file_manager = FileManager(str(output_dir))
            rate_limiter = RateLimiter(max_per_second=float(config.get("rate_limit", 2) or 2))
            retry_handler = RetryHandler(max_retries=config.get("retry_times", 3))
            queue_manager = QueueManager(max_workers=1)
            downloader = DownloaderFactory.create(
                "video",
                config,
                api_client,
                file_manager,
                cookie_manager,
                database,
                rate_limiter,
                retry_handler,
                queue_manager,
            )
            if downloader is None:
                raise RuntimeError("无法创建抖音单视频下载器")

            await rate_limiter.acquire()
            aweme_id = str(parsed["aweme_id"])
            aweme_data = await _get_video_detail_for_api(downloader, aweme_id, config)

            author = aweme_data.get("author", {}) or {}
            author_name = author.get("nickname", "unknown")
            file_context = downloader._build_aweme_file_context(aweme_data, author_name)
            if file_context is None:
                raise RuntimeError(f"无法生成抖音视频保存路径: {aweme_id}")
            video_path = _primary_video_path(file_context)

            should_download = await _should_download_for_api(
                downloader,
                aweme_data,
                author_name,
                video_path,
            )
            status = "skipped"
            if should_download:
                if not await downloader._download_aweme(aweme_data):
                    raise RuntimeError(f"抖音视频下载失败: {aweme_id}")
                status = "downloaded"

            if not file_manager.file_exists(video_path):
                raise RuntimeError(f"下载完成但未找到视频文件: {video_path}")
            return video_path.resolve(), aweme_id, status
    finally:
        if database is not None:
            await database.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download one Douyin video for an API caller")
    parser.add_argument("--url", required=True, help="Douyin URL or copied share text")
    parser.add_argument("--output-dir", required=True, help="Directory for the downloaded video")
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "config.yml"),
        help="Downloader config path (default: project config.yml)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    # The regular CLI intentionally keeps warnings quiet while Rich progress is
    # active.  This machine-facing bridge has no progress UI, so preserve
    # Playwright executable/launch diagnostics in the Java process log.
    set_console_log_level(logging.INFO)
    try:
        path, aweme_id, status = asyncio.run(
            download_single(
                args.url,
                Path(args.output_dir),
                Path(args.config).expanduser().resolve(),
            )
        )
    except Exception as exc:
        print(f"Python 抖音下载失败: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1)

    payload = {
        "path": str(path),
        "awemeId": aweme_id,
        "status": status,
    }
    print(RESULT_PREFIX + json.dumps(payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
