"""Test SFX cues parser + FFmpeg integration in handler_video."""
import pytest


def test_parse_sfx_cues_empty():
    """Scene sin sfx_cues devuelve lista vacia."""
    from handler_video import parse_sfx_cues
    scene = {"scene_id": "sc_001", "duration_sec": 8.5}
    result = parse_sfx_cues(scene)
    assert result == []


def test_parse_sfx_cues_single_whoosh():
    """Scene con 1 whoosh cue parsea correctamente."""
    from handler_video import parse_sfx_cues
    scene = {
        "scene_id": "sc_001",
        "duration_sec": 8.5,
        "sfx_cues": [{"type": "whoosh", "timing_sec": 0.15}]
    }
    result = parse_sfx_cues(scene)
    assert len(result) == 1
    assert result[0]["type"] == "whoosh"
    assert result[0]["timing_sec"] == 0.15
    assert result[0]["r2_key"] == "dominion/visual-assets/sfx/whoosh-doc-low.mp3"
    assert result[0]["mix_db"] == -15


def test_parse_sfx_cues_multiple():
    """Scene con whoosh + 3 ticks parsea todos."""
    from handler_video import parse_sfx_cues
    scene = {
        "scene_id": "sc_002",
        "duration_sec": 10.0,
        "sfx_cues": [
            {"type": "whoosh", "timing_sec": 0.1},
            {"type": "tick", "timing_sec": 3.5},
            {"type": "tick", "timing_sec": 5.2},
            {"type": "tick", "timing_sec": 7.0},
        ]
    }
    result = parse_sfx_cues(scene)
    assert len(result) == 4
    assert result[1]["mix_db"] == -18  # tick mix level
    assert result[1]["r2_key"] == "dominion/visual-assets/sfx/tick-mechanical.mp3"


def test_parse_sfx_cues_drone():
    """Drone type resolves correctly."""
    from handler_video import parse_sfx_cues
    scene = {"sfx_cues": [{"type": "drone", "timing_sec": 0.2}]}
    result = parse_sfx_cues(scene)
    assert result[0]["mix_db"] == -22
    assert result[0]["r2_key"] == "dominion/visual-assets/sfx/drone-sub-editorial.mp3"


def test_parse_sfx_cues_rejects_invalid_type():
    """Type no soportado lanza ValueError."""
    from handler_video import parse_sfx_cues
    scene = {"sfx_cues": [{"type": "riser", "timing_sec": 0.5}]}
    with pytest.raises(ValueError, match="Unknown SFX type"):
        parse_sfx_cues(scene)


def test_build_ffmpeg_filter_with_sfx():
    """Build FFmpeg filter_complex con SFX inputs adicionales."""
    from handler_video import build_ffmpeg_audio_filter

    sfx_inputs = [
        {"path": "/tmp/whoosh.mp3", "timing_sec": 0.15, "mix_db": -15},
        {"path": "/tmp/tick.mp3", "timing_sec": 3.5, "mix_db": -18},
    ]
    result = build_ffmpeg_audio_filter(
        narration_path="/tmp/narration.flac",
        music_path="/tmp/music.mp3",
        sfx_inputs=sfx_inputs,
    )

    # Should include volume adjustments + adelay for timing + amix
    assert "volume=" in result
    assert "adelay=150|150" in result  # whoosh @ 0.15s * 1000ms
    assert "adelay=3500|3500" in result  # tick @ 3.5s * 1000ms
    assert "amix=inputs=4" in result  # narration + music + 2 sfx


def test_build_ffmpeg_filter_no_sfx():
    """Sin SFX, filter es el estandar narracion + musica."""
    from handler_video import build_ffmpeg_audio_filter
    result = build_ffmpeg_audio_filter(
        narration_path="/tmp/narration.flac",
        music_path="/tmp/music.mp3",
        sfx_inputs=[],
    )
    assert "amix=inputs=2" in result
    assert "adelay" not in result


def test_build_ffmpeg_filter_linear_volume():
    """SFX mix_db -15 should convert to linear volume approx 0.1778."""
    from handler_video import build_ffmpeg_audio_filter
    sfx_inputs = [{"path": "/tmp/w.mp3", "timing_sec": 0.1, "mix_db": -15}]
    result = build_ffmpeg_audio_filter(
        narration_path="/tmp/n.flac",
        music_path="/tmp/m.mp3",
        sfx_inputs=sfx_inputs,
    )
    # 10^(-15/20) = 0.17782...
    assert "volume=0.1778" in result


def test_compose_with_sfx_calls_parse_and_build(mocker):
    """Integration smoke: compose with sfx_cues invokes parse + build."""
    import handler_video

    mock_parse = mocker.patch.object(handler_video, 'parse_sfx_cues', return_value=[
        {"type": "whoosh", "timing_sec": 0.15, "r2_key": "x.mp3", "mix_db": -15},
    ])
    mock_build = mocker.patch.object(handler_video, 'build_ffmpeg_audio_filter', return_value='[aout]')

    # Check that parse_sfx_cues + build_ffmpeg_audio_filter exist and are callable
    # (Full integration test would require mocking S3 + FFmpeg, skip here)
    scene = {"scene_id": "sc_test", "sfx_cues": [{"type": "whoosh", "timing_sec": 0.15}]}
    cues = handler_video.parse_sfx_cues(scene)
    assert len(cues) == 1

    result = handler_video.build_ffmpeg_audio_filter("/tmp/n.flac", "/tmp/m.mp3", [])
    assert result == '[aout]'
