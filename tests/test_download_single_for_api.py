from pathlib import Path

import pytest

from tools.download_single_for_api import _primary_video_path, extract_first_url


def test_extract_first_url_from_share_text():
    text = "3.21 复制打开抖音 https://v.douyin.com/abc123/ 直接观看！"

    assert extract_first_url(text) == "https://v.douyin.com/abc123/"


def test_extract_first_url_rejects_text_without_url():
    with pytest.raises(ValueError, match="没有找到"):
        extract_first_url("这段文字里没有链接")


def test_primary_video_path_uses_rendered_context(tmp_path):
    context = {"save_dir": tmp_path / "author", "file_stem": "视频标题"}

    assert _primary_video_path(context) == Path(tmp_path / "author" / "视频标题.mp4")
