"""Test SFX parsers + FFmpeg filter builder in handler_video (V2 schema).

V2 schema replaces V1 `sfx_cues` (type enum + mix_db) with:
  - `sfx_list`: [{file, offset_sec, volume}] — puntuales with scene-relative offset
  - `ambient_bed`: {file, volume} — continuous drone for the scene

Files are paths relative to `{channel}/SFX/` in R2 (e.g. "whooshes/x.flac").
"""
import pytest


# ── parse_sfx_list ────────────────────────────────────────────────────────────

def test_parse_sfx_list_empty():
    """Scene sin sfx_list devuelve lista vacia."""
    from handler_video import parse_sfx_list
    scene = {"scene_id": 0, "duration_sec": 8.5}
    assert parse_sfx_list(scene) == []


def test_parse_sfx_list_single():
    """Scene con 1 item se parsea correctamente."""
    from handler_video import parse_sfx_list
    scene = {
        "scene_id": 0,
        "duration_sec": 8.5,
        "sfx_list": [{
            "file": "whooshes/whoosh_short_01_riser_swoosh.flac",
            "offset_sec": 0.3,
            "volume": 0.5,
            "reason": "arrow appears",
        }]
    }
    result = parse_sfx_list(scene)
    assert len(result) == 1
    assert result[0]["file"] == "whooshes/whoosh_short_01_riser_swoosh.flac"
    assert result[0]["offset_sec"] == 0.3
    assert result[0]["volume"] == 0.5
    assert "reason" not in result[0]


def test_parse_sfx_list_multiple():
    """Scene con 3 items mantiene orden."""
    from handler_video import parse_sfx_list
    scene = {
        "scene_id": 1,
        "duration_sec": 10.0,
        "sfx_list": [
            {"file": "whooshes/whoosh_short_01.flac", "offset_sec": 0.1, "volume": 0.5},
            {"file": "impacts/impact_hard_01.flac", "offset_sec": 3.5, "volume": 0.6},
            {"file": "chimes/chime_01_notification.flac", "offset_sec": 7.0, "volume": 0.45},
        ]
    }
    result = parse_sfx_list(scene)
    assert len(result) == 3
    assert result[1]["file"] == "impacts/impact_hard_01.flac"
    assert result[2]["volume"] == 0.45


def test_parse_sfx_list_rejects_missing_field():
    """Item sin 'file' lanza ValueError."""
    from handler_video import parse_sfx_list
    scene = {
        "duration_sec": 5.0,
        "sfx_list": [{"offset_sec": 0.1, "volume": 0.5}],
    }
    with pytest.raises(ValueError, match="missing required field"):
        parse_sfx_list(scene)


def test_parse_sfx_list_rejects_offset_out_of_range():
    """offset_sec > duration_sec lanza ValueError."""
    from handler_video import parse_sfx_list
    scene = {
        "duration_sec": 5.0,
        "sfx_list": [{"file": "x.flac", "offset_sec": 10.0, "volume": 0.5}],
    }
    with pytest.raises(ValueError, match="out of range"):
        parse_sfx_list(scene)


def test_parse_sfx_list_rejects_negative_offset():
    """offset_sec negativo lanza ValueError."""
    from handler_video import parse_sfx_list
    scene = {
        "duration_sec": 5.0,
        "sfx_list": [{"file": "x.flac", "offset_sec": -0.5, "volume": 0.5}],
    }
    with pytest.raises(ValueError, match="out of range"):
        parse_sfx_list(scene)


def test_parse_sfx_list_rejects_volume_out_of_range():
    """volume > 1.0 lanza ValueError."""
    from handler_video import parse_sfx_list
    scene = {
        "duration_sec": 5.0,
        "sfx_list": [{"file": "x.flac", "offset_sec": 0.5, "volume": 1.5}],
    }
    with pytest.raises(ValueError, match="volume.*out of range"):
        parse_sfx_list(scene)


def test_parse_sfx_list_offset_at_boundary():
    """offset_sec == duration_sec es valido (tolerancia 0.1s)."""
    from handler_video import parse_sfx_list
    scene = {
        "duration_sec": 5.0,
        "sfx_list": [{"file": "x.flac", "offset_sec": 5.0, "volume": 0.5}],
    }
    result = parse_sfx_list(scene)
    assert len(result) == 1


# ── parse_ambient_bed ─────────────────────────────────────────────────────────

def test_parse_ambient_bed_none():
    """Scene sin ambient_bed devuelve None."""
    from handler_video import parse_ambient_bed
    scene = {"scene_id": 0, "duration_sec": 5.0}
    assert parse_ambient_bed(scene) is None


def test_parse_ambient_bed_null():
    """ambient_bed == None devuelve None."""
    from handler_video import parse_ambient_bed
    scene = {"scene_id": 0, "duration_sec": 5.0, "ambient_bed": None}
    assert parse_ambient_bed(scene) is None


def test_parse_ambient_bed_valid():
    """ambient_bed valido devuelve dict con file + volume."""
    from handler_video import parse_ambient_bed
    scene = {
        "scene_id": 0,
        "duration_sec": 20.0,
        "ambient_bed": {
            "file": "drones/drone_02_tension.flac",
            "volume": 0.2,
            "reason": "investigation mood",
        }
    }
    result = parse_ambient_bed(scene)
    assert result is not None
    assert result["file"] == "drones/drone_02_tension.flac"
    assert result["volume"] == 0.2
    assert "reason" not in result


def test_parse_ambient_bed_rejects_missing_file():
    """ambient_bed sin 'file' lanza ValueError."""
    from handler_video import parse_ambient_bed
    scene = {"ambient_bed": {"volume": 0.2}}
    with pytest.raises(ValueError, match="missing required field"):
        parse_ambient_bed(scene)


def test_parse_ambient_bed_rejects_invalid_volume():
    """ambient_bed con volume out-of-range lanza ValueError."""
    from handler_video import parse_ambient_bed
    scene = {"ambient_bed": {"file": "drones/x.flac", "volume": 1.5}}
    with pytest.raises(ValueError, match="volume.*out of range"):
        parse_ambient_bed(scene)


# ── build_ffmpeg_audio_filter ─────────────────────────────────────────────────

def test_build_filter_narration_only():
    """Solo narration → single source, acopy a [aout]."""
    from handler_video import build_ffmpeg_audio_filter
    result = build_ffmpeg_audio_filter(narration_idx=1)
    assert "[1:a]volume=1.0[narr]" in result
    assert "[narr]acopy[aout]" in result
    assert "amix" not in result


def test_build_filter_narration_plus_music():
    """Narration + music → amix 2 inputs."""
    from handler_video import build_ffmpeg_audio_filter
    result = build_ffmpeg_audio_filter(narration_idx=1, music_idx=2)
    assert "[1:a]volume=1.0[narr]" in result
    assert "[2:a]volume=0.30[music]" in result
    assert "amix=inputs=2" in result
    assert "[aout]" in result


def test_build_filter_with_sfx_punctual():
    """Narration + music + 2 SFX puntuales → amix 4 inputs con adelay."""
    from handler_video import build_ffmpeg_audio_filter
    sfx = [
        {"idx": 3, "offset_sec_abs": 1.25, "volume": 0.5},
        {"idx": 4, "offset_sec_abs": 45.0, "volume": 0.6},
    ]
    result = build_ffmpeg_audio_filter(
        narration_idx=1, music_idx=2, sfx_inputs=sfx
    )
    assert "adelay=1250|1250" in result
    assert "adelay=45000|45000" in result
    assert "volume=0.5000" in result
    assert "volume=0.6000" in result
    assert "amix=inputs=4" in result


def test_build_filter_with_ambient_bed():
    """Narration + music + 1 bed → amix 3 con aloop + atrim + adelay."""
    from handler_video import build_ffmpeg_audio_filter
    beds = [
        {"idx": 3, "start_sec_abs": 10.0, "duration_sec": 19.22, "volume": 0.2},
    ]
    result = build_ffmpeg_audio_filter(
        narration_idx=1, music_idx=2, bed_inputs=beds
    )
    assert "aloop=loop=-1" in result
    assert "atrim=duration=19.220" in result
    assert "volume=0.2000" in result
    assert "adelay=10000|10000" in result
    assert "amix=inputs=3" in result


def test_build_filter_full_mix():
    """Narration + music + 2 SFX + 1 bed → amix 5."""
    from handler_video import build_ffmpeg_audio_filter
    sfx = [
        {"idx": 3, "offset_sec_abs": 0.5, "volume": 0.5},
        {"idx": 4, "offset_sec_abs": 2.0, "volume": 0.45},
    ]
    beds = [
        {"idx": 5, "start_sec_abs": 0.0, "duration_sec": 30.0, "volume": 0.18},
    ]
    result = build_ffmpeg_audio_filter(
        narration_idx=1, music_idx=2, sfx_inputs=sfx, bed_inputs=beds
    )
    assert "amix=inputs=5" in result
    assert "loudnorm=I=-16" in result
    assert "[aout]" in result


def test_build_filter_empty():
    """Sin ningun input → empty string."""
    from handler_video import build_ffmpeg_audio_filter
    assert build_ffmpeg_audio_filter() == ""


def test_build_filter_volume_linear_not_db():
    """Volume 0-1 linear goes directly to ffmpeg volume (no dB conversion in V2)."""
    from handler_video import build_ffmpeg_audio_filter
    sfx = [{"idx": 2, "offset_sec_abs": 0.1, "volume": 0.5}]
    result = build_ffmpeg_audio_filter(narration_idx=1, sfx_inputs=sfx)
    assert "volume=0.5000" in result
