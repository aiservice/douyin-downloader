from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tools.download_single_for_api import (
    _get_video_detail_for_api,
    _primary_video_path,
    _should_download_for_api,
    _single_video_title,
    extract_first_url,
)


def test_extract_first_url_from_share_text():
    text = "3.21 复制打开抖音 https://v.douyin.com/abc123/ 直接观看！"

    assert extract_first_url(text) == "https://v.douyin.com/abc123/"


def test_extract_first_url_from_markdown_link():
    text = "[https://v.douyin.com/abc123/](https://v.douyin.com/abc123/)"

    assert extract_first_url(text) == "https://v.douyin.com/abc123/"


def test_extract_first_url_rejects_text_without_url():
    with pytest.raises(ValueError, match="没有找到"):
        extract_first_url("这段文字里没有链接")


def test_primary_video_path_uses_rendered_context(tmp_path):
    context = {"save_dir": tmp_path / "author", "file_stem": "视频标题"}

    assert _primary_video_path(context) == Path(tmp_path / "author" / "视频标题.mp4")


def test_single_video_title_removes_hashtag_suffix():
    desc = "有外国网友好奇中国人接受了什么教育？   #硬核深度计划 #中国 #权谋"

    assert _single_video_title(desc, "123") == "有外国网友好奇中国人接受了什么教育？"


def test_single_video_title_falls_back_to_aweme_id():
    assert _single_video_title("#只有标签", "123") == "123"


async def test_browser_fallback_error_is_preserved_for_java_log():
    api_client = SimpleNamespace(
        get_video_detail=AsyncMock(return_value=None),
        get_video_detail_via_browser=AsyncMock(
            side_effect=RuntimeError("Executable doesn't exist")
        ),
    )
    downloader = SimpleNamespace(api_client=api_client)
    config = SimpleNamespace(
        get=lambda key, default=None: {
            "browser_fallback": {
                "enabled": True,
                "headless": False,
                "wait_timeout_seconds": 600,
            }
        }.get(key, default)
    )

    with pytest.raises(RuntimeError, match="Executable doesn't exist"):
        await _get_video_detail_for_api(downloader, "123", config)

    api_client.get_video_detail_via_browser.assert_awaited_once_with(
        "123",
        page_type="video",
        headless=False,
        wait_timeout_seconds=600,
    )


async def test_should_download_supports_older_downloader_without_exact_checker(tmp_path):
    video_path = tmp_path / "existing.mp4"
    video_path.write_bytes(b"video")
    file_manager = SimpleNamespace(file_exists=lambda path: path.is_file())
    downloader = SimpleNamespace(
        _should_download=AsyncMock(return_value=True),
        file_manager=file_manager,
    )

    should_download = await _should_download_for_api(
        downloader,
        {"aweme_id": "123"},
        "author",
        video_path,
    )

    assert should_download is False
    downloader._should_download.assert_awaited_once_with("123")
