#!/usr/bin/env python3
"""Local Whisper transcription via faster-whisper on the Tesla P40.

Fallback used when Groq is unreachable, throttled, or out of quota. Returns
segments in the same {start, end, text} shape that whisper._segments_from_response
emits, so the rest of the pipeline doesn't care where the transcript came from.

Model loads once per process and caches — repeat calls are cheap.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger("watch.local_whisper")

_MODEL = None
_DEVICE_USED: str | None = None


def _get_model():
    """Load faster-whisper once. Try CUDA first, fall back to CPU on driver/OOM."""
    global _MODEL, _DEVICE_USED
    if _MODEL is not None:
        return _MODEL, _DEVICE_USED

    try:
        from faster_whisper import WhisperModel
    except ImportError as exc:
        raise SystemExit(
            "faster-whisper not installed. "
            "Run: pip3 install --user --break-system-packages faster-whisper"
        ) from exc

    model_name = os.environ.get("LOCAL_WHISPER_MODEL", "large-v3")

    last_exc: Exception | None = None
    for device, compute_type in (("cuda", "float16"), ("cpu", "int8")):
        try:
            logger.info(f"loading faster-whisper {model_name} on {device}…")
            t0 = time.time()
            _MODEL = WhisperModel(model_name, device=device, compute_type=compute_type)
            _DEVICE_USED = device
            logger.info(f"loaded in {time.time() - t0:.1f}s on {device}")
            return _MODEL, device
        except (RuntimeError, OSError, ValueError) as exc:
            last_exc = exc
            logger.warning(f"faster-whisper init failed on {device}: {exc}")
            continue

    raise SystemExit(f"faster-whisper failed on cuda and cpu: {last_exc}")


def transcribe(audio_path: Path, language: str | None = None) -> list[dict]:
    """Transcribe a local audio file. Returns [{start, end, text}, …]."""
    model, device = _get_model()

    logger.info(f"transcribing {audio_path.name} via local faster-whisper ({device})…")
    t0 = time.time()
    segments_iter, info = model.transcribe(
        str(audio_path),
        language=language,
        vad_filter=True,
        word_timestamps=False,
        beam_size=5,
    )

    out: list[dict] = []
    for seg in segments_iter:
        text = (seg.text or "").strip()
        if not text:
            continue
        out.append({
            "start": round(float(seg.start or 0.0), 2),
            "end": round(float(seg.end or 0.0), 2),
            "text": text,
        })

    logger.info(
        f"transcribed {len(out)} segments in {time.time() - t0:.1f}s on {device} "
        f"(lang: {info.language}, prob: {info.language_probability:.2f})"
    )
    return out


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: local_whisper.py <audio-path>", file=sys.stderr)
        raise SystemExit(2)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    import json
    segments = transcribe(Path(sys.argv[1]))
    print(json.dumps({"segments": segments}, indent=2))
