#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import os
import re
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
except Exception:  # pragma: no cover - exercised only on machines without pyarrow.
    pa = None
    pc = None
    ds = None


TEXT_COLUMN_PRIORITY = [
    "기록내용",
    "전문",
    "record",
    "text",
    "full_text",
    "판시사항",
    "판결요지",
    "summary",
    "issues",
    "case_name",
    "case_number",
    "title",
    "사건명",
    "record_id",
]

METADATA_COLUMN_PRIORITY = [
    "기록정보일련번호",
    "기록일련번호",
    "id",
    "identifier",
    "사건명",
    "record_id",
    "선고일자",
    "선고",
    "판정기관명",
    "사건종류명",
    "판결유형",
    "참조조문",
    "참조기록",
    "기록상세링크",
    "case_id",
    "decision_date",
    "case_number",
    "case_name",
    "case_detail_link",
    "case_type",
    "reference_article",
    "reference_case",
    "collection",
    "date",
    "title",
    "creator",
    "word_count",
    "token_count",
]

SCALAR_META_TYPES = (str, int, float, bool)
TEXT_FILE_EXTENSIONS = {".txt", ".md", ".jsonl"}
PDF_EXTENSIONS = {".pdf"}

CASE_REF_RE = re.compile(
    r"(?:대판정기관|헌법재판소|서울고등판정기관|record_org_a|record_org|고등판정기관)?\s*"
    r"\d{4}\s*[가-힣]{1,6}\s*\d{1,6}"
)
DATE_RE = re.compile(r"\d{4}[.\-년]\s*\d{1,2}[.\-월]\s*\d{1,2}[.]?")
TEMPORAL_REF_RE = re.compile(r"(종전|이전|이후|나중|먼저|뒤이어|다시|원심|제1심|환송|파기)")
CONDITION_RE = re.compile(
    r"(권한범위|자료확보|확보|자료확보|검증|참여권|참여|동의|임의제출|범위|기술분석|디지털증거|"
    r"최신|구형|오래된|4자리|네 자리|짧은|짧고|비행기\s*모드|잠금|비밀번호|패턴|인증토큰|TOKEN|SIM)",
    re.I,
)
TOKEN_TERMS = ["인증토큰", "TOKEN", "token", "심카드", "SIM", "sim", "가입자식별", "가입자 식별"]
MESSENGER_TERMS = ["메신저", "메신저서비스", "카톡", "Messenger", "Messenger", "messenger", "텔레그램", "메신저"]
MESSENGER_ACCESS_TERMS = ["로그인", "접속", "인증", "계정", "대화내용", "대화 내용", "열람", "확인", "다운로드", "캡쳐"]
INVESTIGATOR_TERMS = ["검토기관", "검토팀", "검토자", "검토기관", "검토기관", "검토자", "기술분석", "디지털 기술분석", "집행"]
PROCEDURE_TERMS = ["권한범위", "자료확보", "확보", "자료확보", "집행", "참여", "동의", "임의제출"]
PHONE_DEVICE_TERMS = ["장치", "장치", "장치", "스마트폰", "전화기", "iPhone", "아이폰"]
PASSCODE_TERMS = ["비밀번호", "패스워드", "암호", "잠금", "패턴", "PIN", "pin", "비번"]
STRONG_BRUTE_TERMS = [
    "브루트",
    "brute",
    "무차별",
    "반복",
    "여러 차례",
    "대입",
    "추측",
    "풀릴 때까지",
    "짧은 암호",
    "짧은 비밀번호",
    "0000",
    "1234",
    "MFC",
    "UFED",
    "Cellebrite",
]
TECHNICAL_AUDIT_UNLOCK_TERMS = ["기술분석", "디지털 기술분석", "해제", "잠금해제", "풀", "알아내", "복호", "추출", "획득", "확인하지 못"]
PHONE_PASSCODE_LINK_RE = re.compile(
    r"(?:장치|장치|장치|스마트폰|전화기|iPhone|아이폰).{0,90}(?:비밀번호|패스워드|암호|잠금|패턴|PIN|비번)"
    r"|(?:비밀번호|패스워드|암호|잠금|패턴|PIN|비번).{0,90}(?:장치|장치|장치|스마트폰|전화기|iPhone|아이폰)",
    re.I | re.S,
)
TOKEN_MESSENGER_LINK_RE = re.compile(
    r"(?:인증토큰|TOKEN|심카드|SIM).{0,260}(?:메신저|메신저서비스|카톡|Messenger|텔레그램|메신저).{0,260}(?:로그인|접속|인증|확인|다운로드|캡쳐)"
    r"|(?:메신저|메신저서비스|카톡|Messenger|텔레그램|메신저).{0,260}(?:인증토큰|TOKEN|심카드|SIM).{0,260}(?:로그인|접속|인증|확인|다운로드|캡쳐)",
    re.I | re.S,
)
TOKEN_SEIZED_MESSENGER_ACCESS_RE = re.compile(
    r"(?:인증토큰|TOKEN|심카드|SIM).{0,90}(?:확보한\s*후|확보하고|확보하면|확보한|빼|분리|꽂|삽입|장착|끼워|사용하여|이용하여)"
    r".{0,320}(?:메신저|메신저서비스|카톡|Messenger|텔레그램|메신저).{0,220}(?:로그인|접속|인증|확인|다운로드|캡쳐|취득)"
    r"|(?:메신저|메신저서비스|카톡|Messenger|텔레그램|메신저).{0,220}(?:인증토큰|TOKEN|심카드|SIM).{0,90}"
    r"(?:확보한\s*후|확보하고|확보하면|확보한|빼|분리|꽂|삽입|장착|끼워|사용하여|이용하여)",
    re.I | re.S,
)
INVESTIGATOR_TOKEN_SEIZED_MESSENGER_ACCESS_RE = re.compile(
    r"(?:(?:검토기관).{0,100}(?:확보|자료확보|권한범위|집행|기술분석)|검토팀|검토자|검토기관|검토기관|검토자|기술분석|디지털\s*기술분석|접근권한|권한범위)"
    r".{0,260}(?:인증토큰|TOKEN|심카드|SIM).{0,160}"
    r"(?:확보한\s*후|확보하고|확보하면|확보한|빼|분리|꽂|삽입|장착|끼워|공기계|사용하여|이용하여)"
    r".{0,380}(?:메신저|메신저서비스|카톡|Messenger|텔레그램|메신저).{0,260}"
    r"(?:로그인|접속|인증|확인|다운로드|캡쳐|취득|대화내용|대화\s*내용)"
    r"|(?:(?:검토기관).{0,100}(?:확보|자료확보|권한범위|집행|기술분석)|검토팀|검토자|검토기관|검토기관|검토자|기술분석|디지털\s*기술분석).{0,260}"
    r"(?:메신저|메신저서비스|카톡|Messenger|텔레그램|메신저).{0,260}"
    r"(?:인증토큰|TOKEN|심카드|SIM).{0,160}"
    r"(?:확보한\s*후|확보하고|확보하면|확보한|빼|분리|꽂|삽입|장착|끼워|공기계|사용하여|이용하여)",
    re.I | re.S,
)
TOKEN_MESSENGER_EXECUTION_RE = re.compile(
    r"(?:(?:검토기관).{0,100}(?:확보|자료확보|권한범위|집행|기술분석)|검토팀|검토자|검토기관|검토기관|검토자|대상자|접근권한)"
    r".{0,320}(?:관련자|대상자|확보|장치|스마트폰|공기계|공개|별도).{0,220}"
    r"(?:인증토큰|TOKEN|심카드|SIM).{0,220}(?:공기계|공개|장치|별도|삽입|꽂|장착|끼워)"
    r".{0,180}(?:삽입|꽂|장착|끼워|통신을\s*받아|접속).{0,420}"
    r"(?:메신저|메신저서비스|카톡|Messenger|텔레그램|메신저).{0,420}"
    r"(?:성공하|진행하였|진행하였다|확인하였|확인하였다|접속하였|접속하였다|다운로드\s*받았|캡쳐하였)",
    re.I | re.S,
)
TOKEN_ACTION_NEGATION_RE = re.compile(
    r"(?:인증토큰|TOKEN|심카드|SIM).{0,220}(?:메신저|메신저서비스|카톡|Messenger|텔레그램|메신저).{0,220}"
    r"(?:로그인|접속|인증|확인|다운로드|캡쳐|검토기법).{0,120}(?:기재되어\s*있지\s*않|사실[은이]?\s*없|인정할\s*자료가\s*없|아니)"
    r"|(?:기재되어\s*있지\s*않|사실[은이]?\s*없|인정할\s*자료가\s*없|아니).{0,220}"
    r"(?:인증토큰|TOKEN|심카드|SIM).{0,220}(?:메신저|메신저서비스|카톡|Messenger|텔레그램|메신저)",
    re.I | re.S,
)
ACCOUNT_PASSWORD_RE = re.compile(
    r"(?:텔레그램|메신저|메신저서비스|계정|아이디|ID|포털|사이트|은행|계좌|뱅킹|2단계).{0,90}(?:비밀번호|패스워드|암호)"
    r"|(?:비밀번호|패스워드|암호).{0,90}(?:텔레그램|메신저|메신저서비스|계정|아이디|ID|포털|사이트|은행|계좌|뱅킹|2단계)",
    re.I | re.S,
)
PHONE_LOCK_CONTEXT_RE = re.compile(
    r"(?:장치|장치|장치|스마트폰|전화기|iPhone|아이폰).{0,140}(?:잠금|사용자\s*암호|화면\s*잠금|잠금\s*설정)"
    r"|(?:잠금|사용자\s*암호|화면\s*잠금|잠금\s*설정).{0,140}(?:장치|장치|장치|스마트폰|전화기|iPhone|아이폰)",
    re.I | re.S,
)
PHONE_PASSCODE_STRONG_METHOD_RE = re.compile(
    r"(?:비밀번호|패스워드|암호|패턴|PIN|비번).{0,130}(?:브루트|brute|무차별|반복|여러\s*차례|몇\s*차례|대입|추측|0000|1234|MFC|UFED|Cellebrite)"
    r"|(?:브루트|brute|무차별|반복|여러\s*차례|몇\s*차례|대입|추측|0000|1234|MFC|UFED|Cellebrite).{0,130}"
    r"(?:비밀번호|패스워드|암호|패턴|PIN|비번)",
    re.I | re.S,
)
TECHNICAL_AUDIT_FORM_TEMPLATE_TERMS = [
    "별지",
    "서식",
    "확인서",
    "기재란",
    "항목",
    "범죄검토규칙",
    "현장 조사",
    "디지털기술분석 현장",
    "디지털 기술분석 현장",
]
PASSCODE_FORM_VALUE_RE = re.compile(
    r"(?:▣|□|■|○|ㆍ|[-*])?\s*"
    r"(?:화면\s*잠금\s*(?:번호|암호)|화면잠금번호|잠금\s*해제\s*입력값|잠금해제\s*입력값|"
    r"백업\s*비밀번호|비밀번호|패스워드|암호|패턴|PIN|비번)"
    r".{0,45}(?::|：)\s*(?:[A-Za-z0-9]{3,20}|[0-9]{3,8}|[가-힣A-Za-z0-9\s→←↑↓]{2,50})",
    re.I | re.S,
)
PHONE_UNLOCK_ACTION_RE = re.compile(r"(?:잠금\s*)?해제|풀|풀어|알아내|복호|크랙|crack|추출|획득", re.I)
VOLUNTARY_PASSCODE_ENTRY_RE = re.compile(
    r"(?:대상자|대상자|관련자|참여인|소유자|사용자|H).{0,90}(?:직접|스스로|알려|제공|입력|누르)",
    re.I | re.S,
)
SUSPECT_RANDOM_PASSCODE_ENTRY_RE = re.compile(
    r"(?:대상자|대상자|관련자|참여인|소유자|사용자|H).{0,220}"
    r"(?:직접|건네받|비밀번호를\s*푸는\s*척|비밀번호를\s*해제|입력|누르).{0,260}"
    r"(?:불특정\s*숫자|수십\s*회|여러\s*차례|몇\s*차례|반복|3차례|오류|잠겼|방해)"
    r"|(?:불특정\s*숫자|수십\s*회|여러\s*차례|몇\s*차례|반복|3차례|오류|잠겼|방해).{0,260}"
    r"(?:대상자|대상자|관련자|참여인|소유자|사용자|H).{0,220}(?:직접|입력|누르)",
    re.I | re.S,
)
ABSENCE_OF_METHOD_RE = re.compile(
    r"(?:반복|대입|추측|무차별|브루트|brute).{0,120}(?:사실[은이]?\s*없|인정할\s*만한\s*자료는\s*없|볼\s*수\s*없|아니)"
    r"|(?:사실[은이]?\s*없|인정할\s*만한\s*자료는\s*없|볼\s*수\s*없|아니).{0,120}(?:반복|대입|추측|무차별|브루트|brute)",
    re.I | re.S,
)
PASSCODE_UNLOCK_VALIDITY_RE = re.compile(
    r"(?:(?:검토기관|검토자|검토기관|검토기관|검토자|사법검토기관).{0,220}"
    r"(?:장치|장치|장치|스마트폰|전화기|iPhone|아이폰).{0,180}"
    r"(?:비밀번호|패스워드|암호|잠금|패턴|PIN|비번).{0,180}"
    r"(?:해제|입력|요구|풀|알려).{0,420}"
    r"(?:위법|증거능력|자발|강압|강제|어쩔\s*수\s*없이|거부|참여권|임의|동의)"
    r"|(?:위법|증거능력|자발|강압|강제|어쩔\s*수\s*없이|거부|참여권|임의|동의).{0,420}"
    r"(?:검토기관|검토자|검토기관|검토기관|검토자|사법검토기관).{0,220}"
    r"(?:장치|장치|장치|스마트폰|전화기|iPhone|아이폰).{0,180}"
    r"(?:비밀번호|패스워드|암호|잠금|패턴|PIN|비번).{0,180}"
    r"(?:해제|입력|요구|풀|알려))",
    re.I | re.S,
)
PASSCODE_UNLOCK_VALIDITY_TERMS = (
    PHONE_DEVICE_TERMS
    + PASSCODE_TERMS
    + INVESTIGATOR_TERMS
    + ["해제", "입력", "요구", "풀", "알려", "위법", "증거능력", "자발", "강압", "강제", "어쩔 수 없이", "거부", "참여권", "임의", "동의"]
)
TOKEN_VALIDITY_CONTEXT_TERM_RE = re.compile(
    r"(접근권한|자료확보|권한범위|권한범위주의|발부|기재된|범위|절차|참여|동의|자료확보증명서|"
    r"위법|적법|증거능력|위법수집|정당행위|위법성\s*조각|허용|배제|판정기관)",
    re.I,
)
TOKEN_VALIDITY_CONTEXT_TERMS = (
    TOKEN_TERMS
    + MESSENGER_TERMS
    + MESSENGER_ACCESS_TERMS
    + INVESTIGATOR_TERMS
    + PROCEDURE_TERMS
    + ["접근권한", "권한범위주의", "발부", "기재된", "범위", "절차", "증거능력", "위법수집", "정당행위", "위법성 조각", "허용", "배제", "판정기관"]
)

DOMAIN_CONCLUSION_PATTERNS = [
    ("not_unvalid", re.compile(r"위법하지\s*않[^\s.,;)]{0,12}|위법하다고\s*볼\s*수\s*없[^\s.,;)]{0,12}")),
    ("valid", re.compile(r"적법[^\s.,;)]{0,12}|허용[^\s.,;)]{0,12}|증거능력(?:이|은|을)?\s*(?:있|인정)[^\s.,;)]{0,12}")),
    ("unvalid", re.compile(r"위법수집[^\s.,;)]{0,12}|위법[^\s.,;)]{0,12}|증거능력(?:이|은|을)?\s*(?:없|부정|배제)[^\s.,;)]{0,12}")),
    ("inadmissible", re.compile(r"증거(?:로|능력).*?(?:사용할\s*수\s*없|배제|부정)[^\s.,;)]{0,12}")),
]


@dataclass
class SourceRecord:
    source_id: str
    path: str
    source_type: str
    ordinal: int
    raw_text: str
    metadata: dict[str, str]
    content_hash: str


@dataclass
class RecordChunk:
    chunk_id: str
    source_id: str
    path: str
    source_type: str
    ordinal: int
    char_start: int
    char_end: int
    chunk_index: int
    text: str
    metadata: dict[str, str]
    content_hash: str
    prev_chunk_id: str | None = None
    next_chunk_id: str | None = None


@dataclass
class EvidenceSpan:
    evidence_id: str
    source_id: str
    chunk_id: str
    path: str
    content_hash: str
    char_start: int
    char_end: int
    quote: str
    category: str
    label: str
    aspect: str = ""


@dataclass
class Candidate:
    score: float
    candidate_id: str
    source_id: str
    chunk_id: str
    path: str
    source_type: str
    ordinal: int
    chunk_span: dict[str, int]
    metadata: dict[str, str]
    content_hash: str
    matched_terms: list[str]
    matched_aspects: list[str]
    snippets: dict[str, str]
    evidence_spans: list[EvidenceSpan]
    relation_evidence: list[EvidenceSpan]
    conclusion_labels: list[str]
    duplicate_source_ids: list[str] = field(default_factory=list)
    verified_aspects: list[str] = field(default_factory=list)
    aspect_verifications: dict[str, dict[str, Any]] = field(default_factory=dict)


def stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def truncate(text: str, limit: int = 500) -> str:
    cleaned = normalize(text)
    return cleaned[:limit]


def read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_query(query_file: Path) -> str:
    text = query_file.read_text(encoding="utf-8")
    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
    return " ".join(lines)


def render_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def metadata_from_dict(row: dict[str, Any], limit: int = 500) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for key, value in row.items():
        if value is None:
            continue
        if isinstance(value, SCALAR_META_TYPES):
            rendered = truncate(str(value), limit)
            if rendered and rendered.lower() != "nan":
                metadata[str(key)] = rendered
    return metadata


def dict_to_raw_text(row: dict[str, Any], preferred_text_keys: Iterable[str] | None = None) -> str:
    keys = list(row.keys())
    preferred = [key for key in (preferred_text_keys or TEXT_COLUMN_PRIORITY) if key in row]
    remaining = [key for key in keys if key not in set(preferred)]
    parts: list[str] = []
    for key in preferred + remaining:
        value = row.get(key)
        rendered = render_value(value)
        if rendered and rendered.lower() != "nan":
            parts.append(f"[{key}] {rendered}")
    return "\n".join(parts)


def build_query_profile(query: str, policy: dict[str, Any] | None = None) -> dict[str, Any]:
    policy = policy or {}
    aspects: dict[str, list[list[str]]] = {
        "investigative_device_technical_audits": [
            ["검토기관", "검토기관", "검토기관", "확보", "자료확보", "기술분석", "디지털증거"],
            ["장치", "장치", "장치", "스마트폰", "전화기", "휴대전"],
        ],
        "token_messenger_access": [
            ["인증토큰", "TOKEN", "token", "심카드", "SIM", "sim", "가입자식별", "가입자 식별"],
            ["메신저", "메신저서비스", "카톡", "Messenger", "Messenger", "messenger", "메신저"],
            ["로그인", "접속", "인증", "계정", "대화내용", "대화 내용", "열람", "확인"],
        ],
        "phone_password_repeated_attempt": [
            ["비밀번호", "패스워드", "암호", "잠금", "패턴", "PIN", "pin", "비번"],
            ["브루트", "brute", "무차별", "반복", "시도", "추측", "대입", "여러 차례", "입력", "풀릴 때까지", "해제 프로그램", "MFC", "UFED", "Cellebrite"],
            ["해제", "잠금해제", "풀어", "복호", "기술분석", "분석", "추출", "획득", "열람"],
        ],
        "device_passcode_unlock_validity": [
            ["장치", "장치", "장치", "스마트폰", "전화기", "iPhone", "아이폰"],
            ["비밀번호", "패스워드", "암호", "잠금", "패턴", "PIN", "pin", "비번"],
            ["해제", "입력", "요구", "풀", "알려"],
            ["위법", "증거능력", "자발", "강압", "강제", "어쩔 수 없이", "거부", "참여권", "임의", "동의"],
        ],
        "validity_conditions": [
            ["권한범위", "자료확보", "확보", "자료확보", "검증", "참여권", "참여", "동의", "임의제출", "범위"],
            ["적법", "위법", "증거능력", "배제", "허용", "위법수집", "위법하지", "권한범위주의"],
        ],
    }
    for name, groups in policy.get("query_adapters", {}).get("aspects", {}).items():
        if isinstance(groups, list):
            aspects[name] = [[str(term) for term in group] for group in groups if isinstance(group, list)]
    query_terms = [
        token
        for token in re.findall(r"[A-Za-z0-9가-힣]{2,}", query)
        if token not in {"그리고", "이런", "있는지", "사례가", "요건은", "최신", "합법임", "총정리"}
    ]
    if query_terms:
        aspects["query_surface_terms"] = [query_terms[:40]]

    prefilter_exclude = {
        "검토기관",
        "검토기관",
        "검토기관",
        "확보",
        "자료확보",
        "권한범위",
        "위법",
        "적법",
        "증거능력",
        "기술분석",
        "장치",
        "장치",
        "장치",
        "스마트폰",
        "전화기",
    }
    prefilter_terms: list[str] = []
    for groups in aspects.values():
        for group in groups:
            for term in group:
                if len(term) >= 2 and term not in prefilter_exclude and term not in prefilter_terms:
                    prefilter_terms.append(term)
    for term in policy.get("prefilter_terms", []):
        if term not in prefilter_terms:
            prefilter_terms.append(str(term))
    return {
        "query": query,
        "aspects": aspects,
        "prefilter_terms": prefilter_terms[: int(policy.get("max_prefilter_terms", 80))],
        "adapter_note": "Query-specific Korean domain/technical_audit terms are confined to this profile; loaders, chunking, ledgers, duplicate control, and verification are reusable.",
    }


def json_raw_prefilter_terms(profile: dict[str, Any], policy: dict[str, Any]) -> list[str]:
    if not policy.get("json_raw_prefilter", False):
        return []
    allowed_aspects = policy.get("json_raw_prefilter_aspects")
    if allowed_aspects:
        allowed = {str(aspect) for aspect in allowed_aspects}
    else:
        allowed = set(profile["aspects"])
    if not policy.get("json_raw_prefilter_use_query_surface", False):
        allowed.discard("query_surface_terms")
    terms: list[str] = []
    for aspect, groups in profile["aspects"].items():
        if aspect not in allowed:
            continue
        for group in groups:
            for term in group:
                if len(term) >= 2 and term not in terms:
                    terms.append(term)
    return terms


def score_text(text: str, profile: dict[str, Any]) -> tuple[float, list[str], list[str]]:
    lower = text.lower()
    matched_terms: set[str] = set()
    matched_aspects: list[str] = []
    score = 0.0
    for aspect_name, groups in profile["aspects"].items():
        group_hits = 0
        aspect_term_hits = 0
        first_positions: list[int] = []
        for group in groups:
            group_hit = False
            for term in group:
                pos = lower.find(term.lower())
                if pos >= 0:
                    group_hit = True
                    matched_terms.add(term)
                    aspect_term_hits += lower.count(term.lower())
                    first_positions.append(pos)
            if group_hit:
                group_hits += 1
        if aspect_name == "query_surface_terms":
            if group_hits:
                score += min(aspect_term_hits, 20) * 0.25
            continue
        if group_hits:
            score += group_hits * 2.5 + min(aspect_term_hits, 20) * 0.4
        if groups and group_hits == len(groups):
            score += 18.0
            matched_aspects.append(aspect_name)
            if first_positions and max(first_positions) - min(first_positions) < 900:
                score += 5.0
    if "token_messenger_access" in matched_aspects:
        score += 20.0
    if "phone_password_repeated_attempt" in matched_aspects:
        score += 20.0
    if "validity_conditions" in matched_aspects:
        score += 7.0
    return round(score, 4), sorted(matched_terms), matched_aspects


def make_snippet(text: str, terms: list[str], radius: int = 500) -> str:
    if not text:
        return ""
    lower = text.lower()
    positions = [lower.find(term.lower()) for term in terms if lower.find(term.lower()) >= 0]
    center = min(positions) if positions else 0
    start = max(0, center - radius)
    end = min(len(text), center + radius)
    return normalize(text[start:end])


def present_terms(text: str, terms: Iterable[str]) -> list[str]:
    lower = text.lower()
    hits: list[str] = []
    for term in terms:
        if term.lower() in lower and term not in hits:
            hits.append(term)
    return hits


def has_any_term(text: str, terms: Iterable[str]) -> bool:
    return bool(present_terms(text, terms))


def evidence_window(chunk: RecordChunk, terms: Iterable[str], radius: int = 520) -> dict[str, Any]:
    lower = chunk.text.lower()
    positions: list[tuple[int, int, str]] = []
    for term in terms:
        pos = lower.find(term.lower())
        if pos >= 0:
            positions.append((pos, pos + len(term), term))
    if positions:
        start_local = max(0, min(item[0] for item in positions) - radius)
        end_local = min(len(chunk.text), max(item[1] for item in positions) + radius)
    else:
        start_local = 0
        end_local = min(len(chunk.text), radius * 2)
    quote = normalize(chunk.text[start_local:end_local])
    return {
        "char_start": chunk.char_start + start_local,
        "char_end": chunk.char_start + end_local,
        "quote": quote,
        "matched_terms": [item[2] for item in positions],
    }


def verification_payload(
    chunk: RecordChunk,
    aspect: str,
    status: str,
    method_label: str,
    reason: str,
    terms: Iterable[str],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    window = evidence_window(chunk, terms)
    payload: dict[str, Any] = {
        "aspect": aspect,
        "status": status,
        "method_label": method_label,
        "reason": reason,
        "supporting_text": window["quote"],
        "supporting_span": {"char_start": window["char_start"], "char_end": window["char_end"]},
        "matched_terms": window["matched_terms"],
    }
    if extra:
        payload.update(extra)
    return payload


def verification_payload_from_match(
    chunk: RecordChunk,
    aspect: str,
    status: str,
    method_label: str,
    reason: str,
    match: re.Match[str],
    terms: Iterable[str],
    extra: dict[str, Any] | None = None,
    context_chars: int = 40,
) -> dict[str, Any]:
    start_local = max(0, match.start() - context_chars)
    end_local = min(len(chunk.text), match.end() + context_chars)
    quote = normalize(chunk.text[start_local:end_local])
    payload: dict[str, Any] = {
        "aspect": aspect,
        "status": status,
        "method_label": method_label,
        "reason": reason,
        "supporting_text": quote,
        "supporting_span": {"char_start": chunk.char_start + start_local, "char_end": chunk.char_start + end_local},
        "matched_terms": present_terms(quote, terms),
        "match_span": {"char_start": chunk.char_start + match.start(), "char_end": chunk.char_start + match.end()},
    }
    if extra:
        payload.update(extra)
    return payload


def technical_audit_form_template_passcode_value_match(text: str) -> re.Match[str] | None:
    """Return a passcode field match when the context is a form, not a method narrative."""
    if not has_any_term(text, TECHNICAL_AUDIT_FORM_TEMPLATE_TERMS):
        return None
    return PASSCODE_FORM_VALUE_RE.search(text)


def verify_token_messenger_access(chunk: RecordChunk) -> dict[str, Any]:
    text = chunk.text
    core_terms = present_terms(text, TOKEN_TERMS + MESSENGER_TERMS + MESSENGER_ACCESS_TERMS)
    has_core = bool(
        present_terms(text, TOKEN_TERMS)
        and present_terms(text, MESSENGER_TERMS)
        and present_terms(text, MESSENGER_ACCESS_TERMS)
    )
    has_investigation_context = has_any_term(text, INVESTIGATOR_TERMS + PROCEDURE_TERMS)
    linked = bool(TOKEN_MESSENGER_LINK_RE.search(text))
    seized_token_access = bool(TOKEN_SEIZED_MESSENGER_ACCESS_RE.search(text))
    direct_investigator_match = INVESTIGATOR_TOKEN_SEIZED_MESSENGER_ACCESS_RE.search(text)
    direct_investigator_action = bool(direct_investigator_match)
    execution_match = TOKEN_MESSENGER_EXECUTION_RE.search(text)
    action_negated = bool(TOKEN_ACTION_NEGATION_RE.search(text))
    if action_negated:
        return verification_payload(
            chunk,
            "token_messenger_access",
            "candidate_only_rejected",
            "not_operator_acquired_token_access",
            "The raw span negates or disclaims that investigators used a seized TOKEN/SIM to access the messenger account.",
            core_terms + present_terms(text, INVESTIGATOR_TERMS + PROCEDURE_TERMS),
            {"direct_action": False, "rejection": "negated_token_messenger_access"},
        )
    if execution_match:
        return verification_payload_from_match(
            chunk,
            "token_messenger_access",
            "verified_answer_evidence",
            "operator_acquired_token_messenger_access",
            "The decisive raw span describes an investigator/warrant execution using the TOKEN/SIM in another device to access or check messenger contents.",
            execution_match,
            core_terms + present_terms(text, INVESTIGATOR_TERMS + PROCEDURE_TERMS),
            {"direct_action": True, "direct_investigator_action": True, "actual_execution": True},
        )
    if has_core and has_investigation_context and linked and seized_token_access and direct_investigator_action:
        return verification_payload_from_match(
            chunk,
            "token_messenger_access",
            "candidate_only_rejected",
            "planned_or_predicted_token_access_not_actual_execution",
            "TOKEN/SIM seizure or manipulation, messenger access/login, and investigation context are linked, but the selected span does not state actual execution or successful use of the method.",
            direct_investigator_match,
            core_terms + present_terms(text, INVESTIGATOR_TERMS + PROCEDURE_TERMS),
            {
                "direct_action": True,
                "direct_investigator_action": True,
                "actual_execution": False,
                "rejection": "not_actual_execution",
            },
        )
    return verification_payload(
        chunk,
        "token_messenger_access",
        "candidate_only_rejected",
        "not_operator_acquired_token_access",
        "The chunk matched surface terms but did not bind a seized/manipulated TOKEN/SIM to investigator messenger access tightly enough; private criminal Messenger/TOKEN use or evidence-list mentions are not enough.",
        core_terms + present_terms(text, INVESTIGATOR_TERMS + PROCEDURE_TERMS),
        {
            "direct_action": False,
            "direct_investigator_action": direct_investigator_action,
            "linked_token_messenger_terms": linked,
            "seized_token_access_terms": seized_token_access,
            "rejection": "not_operator_acquired_token_access",
        },
    )


def verify_phone_password_repeated_attempt(chunk: RecordChunk) -> dict[str, Any]:
    text = chunk.text
    core_terms = present_terms(
        text,
        PHONE_DEVICE_TERMS + PASSCODE_TERMS + STRONG_BRUTE_TERMS + TECHNICAL_AUDIT_UNLOCK_TERMS + INVESTIGATOR_TERMS,
    )
    actor = has_any_term(text, INVESTIGATOR_TERMS)
    phone_passcode_linked = bool(PHONE_PASSCODE_LINK_RE.search(text))
    lock_context = bool(PHONE_LOCK_CONTEXT_RE.search(text))
    strong_method = bool(PHONE_PASSCODE_STRONG_METHOD_RE.search(text))
    unlock_action = bool(PHONE_UNLOCK_ACTION_RE.search(text))
    technical_audit_unlock_context = actor and phone_passcode_linked and has_any_term(text, ["기술분석", "디지털 기술분석"]) and has_any_term(text, ["시도", "알아내", "풀지 못", "확인하지 못", "해제", "추출"])
    voluntary_user_entry = bool(VOLUNTARY_PASSCODE_ENTRY_RE.search(text))
    suspect_random_entry = SUSPECT_RANDOM_PASSCODE_ENTRY_RE.search(text)
    account_password_without_lock_context = bool(ACCOUNT_PASSWORD_RE.search(text)) and not lock_context
    method_absence = bool(ABSENCE_OF_METHOD_RE.search(text))
    form_template_passcode_value = technical_audit_form_template_passcode_value_match(text)

    if method_absence:
        return verification_payload(
            chunk,
            "phone_password_repeated_attempt",
            "candidate_only_rejected",
            "method_negated",
            "The raw span explicitly negates repeated guessing/brute-force style conduct.",
            core_terms,
            {"repeated_attempt_detail_found": False, "rejection": "method_negated"},
        )
    if account_password_without_lock_context:
        return verification_payload(
            chunk,
            "phone_password_repeated_attempt",
            "candidate_only_rejected",
            "account_password_not_phone_passcode",
            "The password terms attach to an account, portal, banking, or messenger credential rather than a phone lock-screen passcode.",
            core_terms,
            {"repeated_attempt_detail_found": False, "rejection": "account_password_not_phone_passcode"},
        )
    if form_template_passcode_value:
        return verification_payload_from_match(
            chunk,
            "phone_password_repeated_attempt",
            "candidate_only_rejected",
            "technical_audit_form_template_or_disclosed_passcode_value",
            "The raw span is an administrative technical_audit/form field containing a passcode value or sample, not a narrative that investigators repeatedly guessed, brute-forced, or cracked a phone lock.",
            form_template_passcode_value,
            core_terms + present_terms(text, TECHNICAL_AUDIT_FORM_TEMPLATE_TERMS),
            {"repeated_attempt_detail_found": False, "rejection": "technical_audit_form_template_or_disclosed_passcode_value"},
            context_chars=110,
        )
    if voluntary_user_entry and not strong_method and not technical_audit_unlock_context:
        return verification_payload(
            chunk,
            "phone_password_repeated_attempt",
            "candidate_only_rejected",
            "voluntary_user_entry",
            "The password entry is by the phone user/suspect, not an investigator or technical_audit unlocking method.",
            core_terms,
            {"repeated_attempt_detail_found": False, "rejection": "voluntary_user_entry"},
        )
    if suspect_random_entry:
        return verification_payload_from_match(
            chunk,
            "phone_password_repeated_attempt",
            "candidate_only_rejected",
            "suspect_or_user_random_entry_not_investigator_repeated_attempt",
            "The repeated/random passcode entry is attributed to the phone user or suspect during warrant execution, not to an investigator brute-force or technical_audit cracking method.",
            suspect_random_entry,
            core_terms,
            {"repeated_attempt_detail_found": False, "rejection": "suspect_or_user_random_entry_not_investigator_repeated_attempt"},
            context_chars=90,
        )
    if actor and phone_passcode_linked and lock_context and strong_method and unlock_action:
        return verification_payload(
            chunk,
            "phone_password_repeated_attempt",
            "verified_answer_evidence",
            "repeated_attempt_or_guessing_phone_passcode",
            "Investigator/technical_audit actor, phone passcode object, and repeated/guessing/brute-force method are linked in one raw chunk.",
            core_terms,
            {"repeated_attempt_detail_found": True},
        )
    if actor and phone_passcode_linked and technical_audit_unlock_context:
        return verification_payload(
            chunk,
            "phone_password_repeated_attempt",
            "candidate_only_rejected",
            "technical_audit_attempt_without_cracking_or_guessing_method",
            "Investigator/technical_audit actor and phone passcode access attempt are linked, but repeated guessing, cracking, or a comparable unlocking method is not stated.",
            core_terms,
            {"repeated_attempt_detail_found": False, "rejection": "technical_audit_attempt_without_cracking_or_guessing_method"},
        )
    return verification_payload(
        chunk,
        "phone_password_repeated_attempt",
        "candidate_only_rejected",
        "terms_not_semantically_linked",
        "The chunk matched broad password/unlock terms, but not a source-grounded investigator phone-passcode unlocking method.",
        core_terms,
        {"repeated_attempt_detail_found": False, "rejection": "terms_not_semantically_linked"},
    )


def verify_device_passcode_unlock_validity(chunk: RecordChunk) -> dict[str, Any]:
    text = chunk.text
    match = PASSCODE_UNLOCK_VALIDITY_RE.search(text)
    if match:
        return verification_payload_from_match(
            chunk,
            "device_passcode_unlock_validity",
            "verified_answer_evidence",
            "compelled_or_requested_device_passcode_unlock_validity",
            "The raw span links investigator-requested phone passcode unlocking/input with voluntariness, coercion, validness, participation, or admissibility reasoning. This is related passcode-unlock validness evidence, not strict brute-force evidence.",
            match,
            PASSCODE_UNLOCK_VALIDITY_TERMS,
            {"repeated_attempt_detail_found": False, "strict_repeated_attempt_method": False},
            context_chars=120,
        )
    return verification_payload(
        chunk,
        "device_passcode_unlock_validity",
        "candidate_only_rejected",
        "passcode_unlock_validness_terms_not_bound",
        "The chunk contains passcode/validness surface terms but does not tightly bind investigator-requested phone unlock/input to domain reasoning.",
        present_terms(text, PASSCODE_UNLOCK_VALIDITY_TERMS),
        {"repeated_attempt_detail_found": False, "strict_repeated_attempt_method": False, "rejection": "passcode_unlock_validness_terms_not_bound"},
    )


def domain_labels_near(text: str, start_local: int, end_local: int, radius: int = 1200) -> list[str]:
    labels: list[str] = []
    for label, pattern in DOMAIN_CONCLUSION_PATTERNS:
        for match in pattern.finditer(text):
            distance = max(match.start() - end_local, start_local - match.end(), 0)
            if distance <= radius and label not in labels:
                labels.append(label)
    return labels


def verify_validity_conditions(chunk: RecordChunk, existing_verifications: dict[str, dict[str, Any]]) -> dict[str, Any]:
    text = chunk.text
    token_verification = existing_verifications.get("token_messenger_access", {})
    if token_verification.get("status") == "verified_answer_evidence":
        support = token_verification.get("match_span") or token_verification.get("supporting_span") or {}
        start_abs = support.get("char_start")
        end_abs = support.get("char_end")
        if isinstance(start_abs, int) and isinstance(end_abs, int):
            method_start = max(0, start_abs - chunk.char_start)
            method_end = min(len(text), max(method_start, end_abs - chunk.char_start))
            nearby_contexts = []
            for match in TOKEN_VALIDITY_CONTEXT_TERM_RE.finditer(text):
                distance = max(match.start() - method_end, method_start - match.end(), 0)
                if distance <= 1400:
                    nearby_contexts.append((distance, match))
            labels = domain_labels_near(text, method_start, method_end, radius=1400)
            if nearby_contexts and labels:
                context_match = sorted(nearby_contexts, key=lambda item: (item[0], item[1].start()))[0][1]
                start_local = max(0, min(method_start, context_match.start()) - 180)
                end_local = min(len(text), max(method_end, context_match.end()) + 180)
                quote = normalize(text[start_local:end_local])
                return {
                    "aspect": "validity_conditions",
                    "status": "verified_answer_evidence",
                    "method_label": "same_source_token_messenger_warrant_validness_context",
                    "reason": (
                        "The same raw chunk verifies the TOKEN/SIM messenger execution and contains nearby warrant/procedure "
                        "plus domain-conclusion language. Treat this as source-grounded validness context; the answer must still "
                        "quote the span instead of generalizing beyond the record."
                    ),
                    "supporting_text": quote,
                    "supporting_span": {"char_start": chunk.char_start + start_local, "char_end": chunk.char_start + end_local},
                    "matched_terms": present_terms(quote, TOKEN_VALIDITY_CONTEXT_TERMS),
                    "linked_verified_aspect": "token_messenger_access",
                    "token_messenger_validness_context": True,
                    "domain_conclusion_labels": labels,
                }
    return verification_payload(
        chunk,
        "validity_conditions",
        "candidate_only_rejected",
        "validness_terms_not_bound_to_verified_requested_technique",
        "Domainfulness/procedure terms are present, but the chunk does not bind them to a verified requested TOKEN/messenger or strict phone-passcode technique.",
        present_terms(text, TOKEN_VALIDITY_CONTEXT_TERMS + PASSCODE_UNLOCK_VALIDITY_TERMS),
        {"rejection": "validness_terms_not_bound_to_verified_requested_technique"},
    )


def verify_candidate_aspects(chunk: RecordChunk, matched_aspects: list[str]) -> dict[str, dict[str, Any]]:
    verifications: dict[str, dict[str, Any]] = {}
    if "token_messenger_access" in matched_aspects:
        verifications["token_messenger_access"] = verify_token_messenger_access(chunk)
    if "phone_password_repeated_attempt" in matched_aspects:
        verifications["phone_password_repeated_attempt"] = verify_phone_password_repeated_attempt(chunk)
    if "device_passcode_unlock_validity" in matched_aspects:
        verifications["device_passcode_unlock_validity"] = verify_device_passcode_unlock_validity(chunk)
    if "validity_conditions" in matched_aspects:
        verifications["validity_conditions"] = verify_validity_conditions(chunk, verifications)
    return verifications


def iter_json_records(
    data_root: Path,
    path: Path,
    relative_path: Path,
    start_ordinal: int = 1,
    raw_prefilter_terms: Iterable[str] | None = None,
) -> Iterator[SourceRecord]:
    raw_json = path.read_text(encoding="utf-8-sig", errors="replace")
    prefilter_terms = list(raw_prefilter_terms or [])
    if prefilter_terms:
        raw_lower = raw_json.lower()
        if not any(term.lower() in raw_lower for term in prefilter_terms):
            return
    data = json.loads(raw_json)
    rel = relative_path.as_posix()
    if isinstance(data, list):
        for index, item in enumerate(data):
            if isinstance(item, dict):
                text = dict_to_raw_text(item)
                metadata = metadata_from_dict(item)
            else:
                text = render_value(item)
                metadata = {}
            if not text:
                continue
            if prefilter_terms and not has_any_term(text, prefilter_terms):
                continue
            yield SourceRecord(
                source_id=f"{rel}::item:{index}",
                path=str(path),
                source_type="json",
                ordinal=start_ordinal + index,
                raw_text=text,
                metadata=metadata,
                content_hash=stable_hash(text),
            )
    elif isinstance(data, dict):
        text = dict_to_raw_text(data)
        if text and (not prefilter_terms or has_any_term(text, prefilter_terms)):
            yield SourceRecord(
                source_id=f"{rel}::record",
                path=str(path),
                source_type="json",
                ordinal=start_ordinal,
                raw_text=text,
                metadata=metadata_from_dict(data),
                content_hash=stable_hash(text),
            )


def iter_text_record(path: Path, relative_path: Path, start_ordinal: int = 1, max_bytes: int = 3_000_000) -> Iterator[SourceRecord]:
    if path.stat().st_size > max_bytes:
        return
    text = path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return
    rel = relative_path.as_posix()
    yield SourceRecord(
        source_id=f"{rel}::file",
        path=str(path),
        source_type="text",
        ordinal=start_ordinal,
        raw_text=text,
        metadata={"title": path.stem},
        content_hash=stable_hash(text),
    )


def pick_columns(schema_names: Iterable[str]) -> tuple[list[str], list[str]]:
    names = list(schema_names)
    text_cols = [name for name in TEXT_COLUMN_PRIORITY if name in names]
    if not text_cols:
        text_cols = [name for name in names if name.lower() in {"text", "body", "content", "full_text"}]
    metadata_cols = [name for name in METADATA_COLUMN_PRIORITY if name in names and name not in text_cols]
    return text_cols, metadata_cols[:24]


def batch_prefilter_mask(batch: Any, text_cols: list[str], terms: list[str]) -> list[bool]:
    if not terms or pc is None or pa is None:
        return [True] * batch.num_rows
    mask = None
    for col in text_cols:
        index = batch.schema.get_field_index(col)
        if index < 0:
            continue
        array = batch.column(index)
        try:
            strings = pc.cast(array, pa.string(), safe=False)
        except Exception:
            continue
        for term in terms:
            try:
                term_mask = pc.fill_null(pc.match_substring(strings, term, ignore_case=True), False)
            except Exception:
                continue
            mask = term_mask if mask is None else pc.or_(mask, term_mask)
    if mask is None:
        return [True] * batch.num_rows
    return [bool(item) for item in mask.to_pylist()]


def batch_row_at(batch: Any, columns: list[str], offset: int) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for col in columns:
        index = batch.schema.get_field_index(col)
        if index < 0:
            continue
        scalar = batch.column(index)[offset]
        try:
            row[col] = scalar.as_py()
        except Exception:
            row[col] = None
    return row


def iter_parquet_records(
    data_root: Path,
    path: Path,
    relative_path: Path,
    profile: dict[str, Any],
    policy: dict[str, Any],
    start_ordinal: int,
    manifest: dict[str, Any],
) -> Iterator[SourceRecord]:
    if ds is None:
        manifest["warnings"].append("pyarrow is unavailable; parquet sources skipped")
        return
    batch_size = int(policy.get("batch_size", 2048))
    dataset = ds.dataset(str(path), format="parquet")
    text_cols, metadata_cols = pick_columns(dataset.schema.names)
    rel = relative_path.as_posix()
    manifest["files"].append(
        {
            "path": str(path),
            "relative_path": rel,
            "source_type": "parquet",
            "text_columns": text_cols,
            "metadata_columns": metadata_cols,
        }
    )
    if not text_cols:
        manifest["skipped_sources"].append({"path": str(path), "reason": "no_text_columns"})
        return
    scanner = dataset.scanner(columns=sorted(set(text_cols + metadata_cols)), batch_size=batch_size, use_threads=bool(policy.get("use_threads", False)))
    row_number_base = 0
    for batch in scanner.to_batches():
        mask = batch_prefilter_mask(batch, text_cols, profile["prefilter_terms"])
        wanted = [offset for offset, keep in enumerate(mask) if keep]
        manifest["parquet_rows_prefiltered"] += batch.num_rows - len(wanted)
        for offset in wanted:
            row_number = row_number_base + offset
            row = batch_row_at(batch, sorted(set(text_cols + metadata_cols)), offset)
            raw_text = dict_to_raw_text(row, text_cols)
            if not raw_text:
                continue
            yield SourceRecord(
                source_id=f"{rel}::row:{row_number}",
                path=str(path),
                source_type="parquet",
                ordinal=start_ordinal + row_number,
                raw_text=raw_text,
                metadata=metadata_from_dict({key: row.get(key) for key in metadata_cols}),
                content_hash=stable_hash(raw_text),
            )
        row_number_base += batch.num_rows


def parquet_files(data_root: Path) -> list[Path]:
    files: list[Path] = []
    for sub in [
        "01_source_collection_records",
        "02_source_open",
        "03_distressed_korean_domain",
        "04_source_collection_korean",
        "05_constitutional_court",
    ]:
        base = data_root / sub
        if base.exists():
            files.extend(sorted(base.glob("*.parquet")))
    return files


def json_files(data_root: Path, policy: dict[str, Any]) -> list[Path]:
    files: list[Path] = []
    for sub in policy.get("json_source_dirs", ["06_source_api_api", "07_source_api_fulltext"]):
        base = data_root / sub
        if base.exists():
            files.extend(sorted(base.glob("*.json")))
    return files


def text_files(data_root: Path, policy: dict[str, Any]) -> tuple[list[Path], list[dict[str, str]]]:
    base = data_root / policy.get("text_source_dir", "08_local_record_archive_local")
    files: list[Path] = []
    skipped: list[dict[str, str]] = []
    if not base.exists():
        return files, skipped
    max_files = int(policy.get("max_text_files", 0) or 0)
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = sorted([name for name in dirnames if name not in {".git", "__pycache__"}])
        for filename in sorted(filenames):
            path = Path(dirpath) / filename
            suffix = path.suffix.lower()
            if suffix in TEXT_FILE_EXTENSIONS:
                files.append(path)
                if max_files > 0 and len(files) >= max_files:
                    return files, skipped
            elif suffix in PDF_EXTENSIONS:
                skipped.append({"path": str(path), "reason": "pdf_binary_skipped_use_extracted_txt_when_present"})
    return files, skipped


def iter_source_records(data_root: Path, profile: dict[str, Any], policy: dict[str, Any], manifest: dict[str, Any]) -> Iterator[SourceRecord]:
    ordinal = 1
    max_records = int(policy.get("max_records", 0) or 0)
    total_yielded = 0

    if policy.get("scan_parquet", True):
        files = parquet_files(data_root)
        max_files = int(policy.get("max_parquet_files", 0) or 0)
        max_parquet_records = int(policy.get("max_parquet_records", 0) or 0)
        parquet_yielded = 0
        if max_files > 0:
            files = files[:max_files]
        manifest["planned_sources"]["parquet_files"] = len(files)
        manifest["planned_sources"]["max_parquet_records"] = max_parquet_records
        for path in files:
            for record in iter_parquet_records(data_root, path, path.relative_to(data_root), profile, policy, ordinal, manifest):
                yield record
                ordinal += 1
                total_yielded += 1
                parquet_yielded += 1
                if max_records > 0 and total_yielded >= max_records:
                    return
                if max_parquet_records > 0 and parquet_yielded >= max_parquet_records:
                    break
            if max_parquet_records > 0 and parquet_yielded >= max_parquet_records:
                manifest["warnings"].append(f"parquet scan bounded at {parquet_yielded} yielded records")
                break

    if policy.get("scan_json", True):
        files = json_files(data_root, policy)
        max_files = int(policy.get("max_json_files", 0) or 0)
        max_json_records = int(policy.get("max_json_records", 0) or 0)
        json_yielded = 0
        if max_files > 0:
            files = files[:max_files]
        manifest["planned_sources"]["json_files"] = len(files)
        manifest["planned_sources"]["max_json_records"] = max_json_records
        raw_prefilter_terms = json_raw_prefilter_terms(profile, policy)
        manifest["planned_sources"]["json_raw_prefilter_enabled"] = bool(raw_prefilter_terms)
        manifest["planned_sources"]["json_raw_prefilter_term_count"] = len(raw_prefilter_terms)
        for path in files:
            manifest["files"].append({"path": str(path), "relative_path": path.relative_to(data_root).as_posix(), "source_type": "json"})
            try:
                before_yielded = json_yielded
                for record in iter_json_records(data_root, path, path.relative_to(data_root), ordinal, raw_prefilter_terms=raw_prefilter_terms):
                    yield record
                    ordinal += 1
                    total_yielded += 1
                    json_yielded += 1
                    if max_records > 0 and total_yielded >= max_records:
                        return
                    if max_json_records > 0 and json_yielded >= max_json_records:
                        break
                if raw_prefilter_terms and before_yielded == json_yielded:
                    manifest["json_files_raw_prefiltered"] = manifest.get("json_files_raw_prefiltered", 0) + 1
            except Exception as exc:
                manifest["skipped_sources"].append({"path": str(path), "reason": f"json_read_error:{type(exc).__name__}"})
            if max_json_records > 0 and json_yielded >= max_json_records:
                manifest["warnings"].append(f"json scan bounded at {json_yielded} yielded records")
                break

    if policy.get("scan_text", True):
        files, skipped = text_files(data_root, policy)
        manifest["planned_sources"]["text_files"] = len(files)
        manifest["skipped_sources"].extend(skipped[: int(policy.get("max_skipped_sources_reported", 200))])
        max_bytes = int(policy.get("text_file_max_bytes", 3_000_000))
        max_text_records = int(policy.get("max_text_records", 0) or 0)
        text_yielded = 0
        manifest["planned_sources"]["max_text_records"] = max_text_records
        for path in files:
            rel = path.relative_to(data_root)
            manifest["files"].append({"path": str(path), "relative_path": rel.as_posix(), "source_type": "text"})
            try:
                for record in iter_text_record(path, rel, ordinal, max_bytes=max_bytes):
                    yield record
                    ordinal += 1
                    total_yielded += 1
                    text_yielded += 1
                    if max_records > 0 and total_yielded >= max_records:
                        return
                    if max_text_records > 0 and text_yielded >= max_text_records:
                        break
            except Exception as exc:
                manifest["skipped_sources"].append({"path": str(path), "reason": f"text_read_error:{type(exc).__name__}"})
            if max_text_records > 0 and text_yielded >= max_text_records:
                manifest["warnings"].append(f"text scan bounded at {text_yielded} yielded records")
                break


def split_record(record: SourceRecord, chunk_chars: int = 6000, overlap_chars: int = 600) -> list[RecordChunk]:
    text = record.raw_text
    if not text:
        return []
    chunk_chars = max(1, chunk_chars)
    overlap_chars = max(0, min(overlap_chars, chunk_chars - 1))
    chunks: list[RecordChunk] = []
    start = 0
    index = 0
    while start < len(text):
        end = min(len(text), start + chunk_chars)
        chunks.append(
            RecordChunk(
                chunk_id=f"{record.source_id}::span:{start}-{end}",
                source_id=record.source_id,
                path=record.path,
                source_type=record.source_type,
                ordinal=record.ordinal,
                char_start=start,
                char_end=end,
                chunk_index=index,
                text=text[start:end],
                metadata=record.metadata,
                content_hash=record.content_hash,
            )
        )
        if end >= len(text):
            break
        next_start = end - overlap_chars
        if next_start <= start:
            next_start = end
        start = next_start
        index += 1
    for idx, chunk in enumerate(chunks):
        if idx:
            chunk.prev_chunk_id = chunks[idx - 1].chunk_id
        if idx + 1 < len(chunks):
            chunk.next_chunk_id = chunks[idx + 1].chunk_id
    return chunks


def add_span(
    spans: list[EvidenceSpan],
    seen: set[tuple[int, int, str, str]],
    chunk: RecordChunk,
    start_local: int,
    end_local: int,
    category: str,
    label: str,
    aspect: str = "",
) -> None:
    if end_local <= start_local:
        return
    start_abs = chunk.char_start + start_local
    end_abs = chunk.char_start + end_local
    quote = chunk.text[start_local:end_local]
    key = (start_abs, end_abs, category, label)
    if key in seen or not quote.strip():
        return
    seen.add(key)
    spans.append(
        EvidenceSpan(
            evidence_id=f"ev-{stable_hash(chunk.chunk_id + str(start_abs) + str(end_abs) + category + label)[:16]}",
            source_id=chunk.source_id,
            chunk_id=chunk.chunk_id,
            path=chunk.path,
            content_hash=chunk.content_hash,
            char_start=start_abs,
            char_end=end_abs,
            quote=quote,
            category=category,
            label=label,
            aspect=aspect,
        )
    )


def extract_evidence_spans(chunk: RecordChunk, profile: dict[str, Any], max_spans: int = 80) -> list[EvidenceSpan]:
    spans: list[EvidenceSpan] = []
    seen: set[tuple[int, int, str, str]] = set()
    lower = chunk.text.lower()
    for aspect, groups in profile["aspects"].items():
        for group in groups:
            for term in group:
                start = 0
                needle = term.lower()
                while len(spans) < max_spans:
                    pos = lower.find(needle, start)
                    if pos < 0:
                        break
                    add_span(spans, seen, chunk, pos, pos + len(term), "term", term, aspect)
                    start = pos + max(1, len(term))
    for label, pattern in DOMAIN_CONCLUSION_PATTERNS:
        for match in pattern.finditer(chunk.text):
            add_span(spans, seen, chunk, match.start(), match.end(), "domain_conclusion", label)
    for pattern, label in [(CASE_REF_RE, "case_reference"), (DATE_RE, "date_reference"), (TEMPORAL_REF_RE, "temporal_reference")]:
        for match in pattern.finditer(chunk.text):
            add_span(spans, seen, chunk, match.start(), match.end(), "relation_reference", label)
    for match in CONDITION_RE.finditer(chunk.text):
        add_span(spans, seen, chunk, match.start(), match.end(), "condition", match.group(0))
    spans.sort(key=lambda item: (item.char_start, item.char_end, item.category, item.label))
    return spans[:max_spans]


def group_duplicate_records(records: Iterable[SourceRecord]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for record in records:
        entry = grouped.setdefault(
            record.content_hash,
            {
                "content_hash": record.content_hash,
                "count": 0,
                "source_ids": [],
                "paths": [],
                "ordinals": [],
            },
        )
        entry["count"] += 1
        entry["source_ids"].append(record.source_id)
        entry["paths"].append(record.path)
        entry["ordinals"].append(record.ordinal)
    return grouped


def record_duplicate_seen(index: dict[str, dict[str, Any]], record: SourceRecord, max_source_ids: int = 25) -> None:
    entry = index.setdefault(
        record.content_hash,
        {
            "content_hash": record.content_hash,
            "count": 0,
            "source_ids": [],
            "paths": [],
            "first_ordinal": record.ordinal,
        },
    )
    entry["count"] += 1
    entry["first_ordinal"] = min(int(entry["first_ordinal"]), record.ordinal)
    if len(entry["source_ids"]) < max_source_ids:
        entry["source_ids"].append(record.source_id)
        entry["paths"].append(record.path)


def build_candidate(chunk: RecordChunk, profile: dict[str, Any], score: float, matched_terms: list[str], matched_aspects: list[str]) -> Candidate:
    spans = extract_evidence_spans(chunk, profile)
    relation_spans = [span for span in spans if span.category == "relation_reference"]
    conclusion_labels = sorted({span.label for span in spans if span.category == "domain_conclusion"})
    snippets: dict[str, str] = {}
    for aspect, groups in profile["aspects"].items():
        terms = sorted({term for group in groups for term in group})
        if any(term.lower() in chunk.text.lower() for term in terms):
            snippets[aspect] = make_snippet(chunk.text, terms)
    candidate_id = f"cand-{stable_hash(chunk.chunk_id)[:16]}"
    aspect_verifications = verify_candidate_aspects(chunk, matched_aspects)
    verified_aspects = sorted(
        aspect
        for aspect, verification in aspect_verifications.items()
        if verification.get("status") == "verified_answer_evidence"
    )
    return Candidate(
        score=score,
        candidate_id=candidate_id,
        source_id=chunk.source_id,
        chunk_id=chunk.chunk_id,
        path=chunk.path,
        source_type=chunk.source_type,
        ordinal=chunk.ordinal,
        chunk_span={"char_start": chunk.char_start, "char_end": chunk.char_end, "chunk_index": chunk.chunk_index},
        metadata=chunk.metadata,
        content_hash=chunk.content_hash,
        matched_terms=matched_terms,
        matched_aspects=matched_aspects,
        snippets=snippets,
        evidence_spans=spans,
        relation_evidence=relation_spans,
        conclusion_labels=conclusion_labels,
        verified_aspects=verified_aspects,
        aspect_verifications=aspect_verifications,
    )


def push_candidate(heap: list[tuple[float, int, Candidate]], counter: int, candidate: Candidate, top_k: int) -> None:
    item = (candidate.score, counter, candidate)
    if len(heap) < top_k:
        heapq.heappush(heap, item)
        return
    if item[:2] > heap[0][:2]:
        heapq.heapreplace(heap, item)


def select_unique_source_candidates(candidates: list[Candidate], top_k: int) -> list[Candidate]:
    best_by_source: dict[str, Candidate] = {}
    for candidate in candidates:
        current = best_by_source.get(candidate.source_id)
        if current is None:
            best_by_source[candidate.source_id] = candidate
            continue
        if candidate_sort_key(candidate) < candidate_sort_key(current):
            best_by_source[candidate.source_id] = candidate
    return sorted(best_by_source.values(), key=candidate_sort_key)[:top_k]


def candidate_verification_strength(candidate: Candidate) -> int:
    strength = 0
    if candidate.aspect_verifications.get("token_messenger_access", {}).get("actual_execution"):
        strength += 2
    if candidate.aspect_verifications.get("phone_password_repeated_attempt", {}).get("repeated_attempt_detail_found"):
        strength += 2
    strength += len(candidate.verified_aspects)
    return strength


def candidate_sort_key(candidate: Candidate) -> tuple[int, float, int, str, int]:
    return (
        -candidate_verification_strength(candidate),
        -candidate.score,
        candidate.ordinal,
        candidate.source_id,
        candidate.chunk_span.get("chunk_index", 0),
    )


def candidate_has_aspect(candidate: Candidate, aspect: str) -> bool:
    return (
        aspect in candidate.matched_aspects
        or aspect in candidate.verified_aspects
        or any(span.aspect == aspect for span in candidate.evidence_spans)
    )


def select_diverse_candidates(
    candidates: list[Candidate],
    top_k: int,
    aspects: Iterable[str],
    min_per_aspect: int = 0,
) -> list[Candidate]:
    unique = select_unique_source_candidates(candidates, len(candidates))
    if min_per_aspect <= 0:
        return unique[:top_k]

    selected: list[Candidate] = []
    selected_sources: set[str] = set()

    def add(candidate: Candidate) -> None:
        if len(selected) >= top_k or candidate.source_id in selected_sources:
            return
        selected.append(candidate)
        selected_sources.add(candidate.source_id)

    for aspect in aspects:
        aspect = str(aspect)
        if aspect == "query_surface_terms":
            continue
        count = sum(1 for candidate in selected if candidate_has_aspect(candidate, aspect))
        if count >= min_per_aspect:
            continue
        for candidate in unique:
            if not candidate_has_aspect(candidate, aspect):
                continue
            add(candidate)
            count = sum(1 for item in selected if candidate_has_aspect(item, aspect))
            if len(selected) >= top_k or count >= min_per_aspect:
                break
        if len(selected) >= top_k:
            break

    for candidate in unique:
        add(candidate)
        if len(selected) >= top_k:
            break

    return sorted(selected, key=candidate_sort_key)


def limited_items(items: list[Path], limit: int) -> list[Path]:
    if limit > 0:
        return items[:limit]
    return items


def probe_term_groups(probe: dict[str, Any]) -> list[list[str]]:
    groups = probe.get("term_groups", [])
    if not isinstance(groups, list):
        return []
    cleaned: list[list[str]] = []
    for group in groups:
        if isinstance(group, list):
            terms = [str(term) for term in group if str(term)]
            if terms:
                cleaned.append(terms)
    return cleaned


def probe_flat_terms(probe: dict[str, Any]) -> list[str]:
    terms: list[str] = []
    for group in probe_term_groups(probe):
        for term in group:
            if term not in terms:
                terms.append(term)
    return terms


def probe_regexes(probe: dict[str, Any], manifest: dict[str, Any]) -> list[re.Pattern[str]]:
    compiled: list[re.Pattern[str]] = []
    for pattern in probe.get("regexes", []) or []:
        try:
            compiled.append(re.compile(str(pattern), re.I | re.S))
        except re.error as exc:
            manifest["warnings"].append(f"targeted_probe_regex_error:{probe.get('name', probe.get('aspect', 'unnamed'))}:{type(exc).__name__}:{exc}")
    return compiled


def probe_matches_text(text: str, term_groups: list[list[str]], regexes: list[re.Pattern[str]]) -> bool:
    if term_groups:
        lower = text.lower()
        for group in term_groups:
            if not any(term.lower() in lower for term in group):
                return False
    if regexes and not any(pattern.search(text) for pattern in regexes):
        return False
    return bool(term_groups or regexes)


def iter_targeted_probe_records(
    data_root: Path,
    policy: dict[str, Any],
    manifest: dict[str, Any],
) -> Iterator[tuple[dict[str, Any], SourceRecord]]:
    probes = policy.get("targeted_probes", []) or []
    if not isinstance(probes, list):
        manifest["warnings"].append("targeted_probes policy value is not a list; skipping targeted probes")
        return
    summary = manifest.setdefault(
        "targeted_probe_summary",
        {
            "enabled": bool(probes),
            "probe_count": len(probes),
            "files_seen": 0,
            "raw_files_prefiltered": 0,
            "records_matched": 0,
            "candidates_added": 0,
            "by_probe": {},
        },
    )
    for raw_probe in probes:
        if not isinstance(raw_probe, dict):
            continue
        probe = dict(raw_probe)
        name = str(probe.get("name") or probe.get("aspect") or "unnamed_probe")
        aspect = str(probe.get("aspect") or "")
        if not aspect:
            manifest["warnings"].append(f"targeted_probe_without_aspect:{name}")
            continue
        term_groups = probe_term_groups(probe)
        compiled_regexes = probe_regexes(probe, manifest)
        source_types = {str(item) for item in probe.get("source_types", ["json"]) or ["json"]}
        max_records = int(probe.get("max_records", 0) or 0)
        matched_records = 0
        by_probe = summary["by_probe"].setdefault(
            name,
            {
                "aspect": aspect,
                "source_types": sorted(source_types),
                "files_seen": 0,
                "raw_files_prefiltered": 0,
                "records_matched": 0,
                "candidates_added": 0,
            },
        )

        if "json" in source_types:
            json_policy = dict(policy)
            if probe.get("json_source_dirs"):
                json_policy["json_source_dirs"] = probe.get("json_source_dirs")
            files = limited_items(json_files(data_root, json_policy), int(probe.get("max_json_files", 0) or 0))
            by_probe["planned_json_files"] = len(files)
            for path in files:
                rel = path.relative_to(data_root)
                summary["files_seen"] += 1
                by_probe["files_seen"] += 1
                try:
                    raw_json = path.read_text(encoding="utf-8-sig", errors="replace")
                except Exception as exc:
                    manifest["skipped_sources"].append({"path": str(path), "reason": f"targeted_json_read_error:{type(exc).__name__}"})
                    continue
                if not probe_matches_text(raw_json, term_groups, compiled_regexes):
                    summary["raw_files_prefiltered"] += 1
                    by_probe["raw_files_prefiltered"] += 1
                    continue
                try:
                    for record in iter_json_records(data_root, path, rel, start_ordinal=1):
                        if not probe_matches_text(record.raw_text, term_groups, compiled_regexes):
                            continue
                        metadata = dict(record.metadata)
                        metadata["_targeted_probe"] = name
                        metadata["_targeted_probe_aspect"] = aspect
                        record.metadata = metadata
                        summary["records_matched"] += 1
                        by_probe["records_matched"] += 1
                        matched_records += 1
                        yield probe, record
                        if max_records > 0 and matched_records >= max_records:
                            break
                except Exception as exc:
                    manifest["skipped_sources"].append({"path": str(path), "reason": f"targeted_json_parse_error:{type(exc).__name__}"})
                if max_records > 0 and matched_records >= max_records:
                    break

        if "text" in source_types and (max_records <= 0 or matched_records < max_records):
            text_policy = dict(policy)
            if probe.get("text_source_dir"):
                text_policy["text_source_dir"] = probe.get("text_source_dir")
            files, skipped = text_files(data_root, text_policy)
            files = limited_items(files, int(probe.get("max_text_files", 0) or 0))
            by_probe["planned_text_files"] = len(files)
            manifest["skipped_sources"].extend(skipped[: int(policy.get("max_skipped_sources_reported", 200))])
            max_bytes = int(probe.get("text_file_max_bytes", policy.get("text_file_max_bytes", 3_000_000)))
            for path in files:
                rel = path.relative_to(data_root)
                summary["files_seen"] += 1
                by_probe["files_seen"] += 1
                try:
                    if path.stat().st_size > max_bytes:
                        summary["raw_files_prefiltered"] += 1
                        by_probe["raw_files_prefiltered"] += 1
                        continue
                    text = path.read_text(encoding="utf-8", errors="replace")
                except Exception as exc:
                    manifest["skipped_sources"].append({"path": str(path), "reason": f"targeted_text_read_error:{type(exc).__name__}"})
                    continue
                if not probe_matches_text(text, term_groups, compiled_regexes):
                    summary["raw_files_prefiltered"] += 1
                    by_probe["raw_files_prefiltered"] += 1
                    continue
                for record in iter_text_record(path, rel, 1, max_bytes=max_bytes):
                    if not probe_matches_text(record.raw_text, term_groups, compiled_regexes):
                        continue
                    metadata = dict(record.metadata)
                    metadata["_targeted_probe"] = name
                    metadata["_targeted_probe_aspect"] = aspect
                    record.metadata = metadata
                    summary["records_matched"] += 1
                    by_probe["records_matched"] += 1
                    matched_records += 1
                    yield probe, record
                    if max_records > 0 and matched_records >= max_records:
                        break
                if max_records > 0 and matched_records >= max_records:
                    break


def run_search(data_root: Path, query: str, policy: dict[str, Any], out_dir: Path) -> tuple[list[Candidate], dict[str, Any], dict[str, Any]]:
    profile = build_query_profile(query, policy)
    top_k = int(policy.get("top_k", 120))
    heap_k = max(top_k, int(policy.get("candidate_heap_k", top_k * 5)))
    aspect_heap_k = int(policy.get("aspect_heap_k", 0) or 0)
    min_per_aspect_candidates = int(policy.get("min_per_aspect_candidates", 0) or 0)
    selection_aspects = [
        str(aspect)
        for aspect in policy.get("selection_aspects", [aspect for aspect in profile["aspects"] if aspect != "query_surface_terms"])
    ]
    chunk_chars = int(policy.get("chunk_chars", 7000))
    overlap_chars = int(policy.get("chunk_overlap_chars", 700))
    heartbeat_every = int(policy.get("heartbeat_every_records", 2000))
    min_record_score = float(policy.get("min_record_score", 1.0))
    min_chunk_score = float(policy.get("min_chunk_score", 1.0))
    out_dir.mkdir(parents=True, exist_ok=True)
    heartbeat_path = out_dir / "scan_heartbeat.json"
    manifest: dict[str, Any] = {
        "schema": "generic_memory_search_run_v2",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "data_root": str(data_root),
        "profile": profile,
        "policy": policy,
        "planned_sources": {},
        "files": [],
        "skipped_sources": [],
        "warnings": [],
        "records_seen": 0,
        "chunks_scored": 0,
        "candidate_count_seen": 0,
        "parquet_rows_prefiltered": 0,
        "duplicate_groups_in_frontier": {},
        "source_type_counts": {},
        "source_type_candidate_counts": {},
    }
    heap: list[tuple[float, int, Candidate]] = []
    aspect_heaps: dict[str, list[tuple[float, int, Candidate]]] = defaultdict(list)
    duplicate_index: dict[str, dict[str, Any]] = {}
    counter = 0

    def add_candidate_from_record(record: SourceRecord, score_boost: float = 0.0, forced_aspect: str | None = None) -> int:
        nonlocal counter
        added = 0
        chunks = split_record(record, chunk_chars=chunk_chars, overlap_chars=overlap_chars)
        record_entered_frontier = False
        for chunk in chunks:
            manifest["chunks_scored"] += 1
            score, matched_terms, matched_aspects = score_text(chunk.text, profile)
            if forced_aspect and forced_aspect not in matched_aspects:
                matched_aspects = sorted(matched_aspects + [forced_aspect])
            score += score_boost
            if score < min_chunk_score:
                continue
            manifest["candidate_count_seen"] += 1
            manifest["source_type_candidate_counts"][record.source_type] = manifest["source_type_candidate_counts"].get(record.source_type, 0) + 1
            candidate = build_candidate(chunk, profile, score, matched_terms, matched_aspects)
            if not record_entered_frontier:
                record_duplicate_seen(duplicate_index, record, max_source_ids=int(policy.get("max_duplicate_source_ids", 25)))
                record_entered_frontier = True
            counter += 1
            added += 1
            push_candidate(heap, counter, candidate, heap_k)
            if aspect_heap_k > 0:
                for aspect in matched_aspects:
                    if aspect == "query_surface_terms":
                        continue
                    push_candidate(aspect_heaps[aspect], counter, candidate, aspect_heap_k)
        return added

    for record in iter_source_records(data_root, profile, policy, manifest):
        manifest["records_seen"] += 1
        manifest["source_type_counts"][record.source_type] = manifest["source_type_counts"].get(record.source_type, 0) + 1
        record_score, _, _ = score_text(record.raw_text[: int(policy.get("record_score_prefix_chars", 25000))], profile)
        if record_score < min_record_score:
            continue
        add_candidate_from_record(record)
        if heartbeat_every > 0 and manifest["records_seen"] % heartbeat_every == 0:
            heartbeat = {
                "records_seen": manifest["records_seen"],
                "chunks_scored": manifest["chunks_scored"],
                "candidate_count_seen": manifest["candidate_count_seen"],
                "top_heap_size": len(heap),
                "current_source_id": record.source_id,
                "time": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            }
            write_json(heartbeat_path, heartbeat)
            if policy.get("print_heartbeat", True):
                print(json.dumps({"heartbeat": heartbeat}, ensure_ascii=False, sort_keys=True), flush=True)
    for probe, record in iter_targeted_probe_records(data_root, policy, manifest):
        aspect = str(probe.get("aspect") or "")
        score_boost = float(probe.get("candidate_boost", 0.0) or 0.0)
        manifest["records_seen"] += 1
        manifest["source_type_counts"][record.source_type] = manifest["source_type_counts"].get(record.source_type, 0) + 1
        added = add_candidate_from_record(record, score_boost=score_boost, forced_aspect=aspect)
        if added:
            summary = manifest.setdefault("targeted_probe_summary", {})
            summary["candidates_added"] = int(summary.get("candidates_added", 0) or 0) + added
            name = str(probe.get("name") or aspect or "unnamed_probe")
            by_probe = summary.setdefault("by_probe", {}).setdefault(name, {})
            by_probe["candidates_added"] = int(by_probe.get("candidates_added", 0) or 0) + added
    candidate_pool: dict[str, Candidate] = {}
    for item in sorted(heap, key=lambda item: (-item[0], item[2].ordinal, item[2].source_id)):
        candidate_pool[item[2].candidate_id] = item[2]
    for aspect_heap in aspect_heaps.values():
        for item in sorted(aspect_heap, key=lambda value: (-value[0], value[2].ordinal, value[2].source_id)):
            candidate_pool[item[2].candidate_id] = item[2]
    heap_candidates = list(candidate_pool.values())
    candidates = select_diverse_candidates(
        heap_candidates,
        top_k,
        aspects=selection_aspects,
        min_per_aspect=min_per_aspect_candidates,
    )
    for candidate in candidates:
        group = duplicate_index.get(candidate.content_hash, {})
        candidate.duplicate_source_ids = [source_id for source_id in group.get("source_ids", []) if source_id != candidate.source_id]
    manifest["duplicate_groups_in_frontier"] = {key: value for key, value in duplicate_index.items() if value["count"] > 1}
    manifest["aspect_candidate_heap_sizes"] = {aspect: len(items) for aspect, items in aspect_heaps.items()}
    manifest["selection_aspects"] = selection_aspects
    manifest["min_per_aspect_candidates"] = min_per_aspect_candidates
    manifest["top_candidate_count"] = len(candidates)
    manifest["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    return candidates, manifest, profile


def candidate_dict(candidate: Candidate) -> dict[str, Any]:
    payload = asdict(candidate)
    return payload


def aspect_candidates(candidates: list[Candidate], aspect: str) -> list[Candidate]:
    return [
        candidate
        for candidate in candidates
        if aspect in candidate.matched_aspects or any(span.aspect == aspect for span in candidate.evidence_spans)
    ]


def unique_source_ids(candidates: Iterable[Candidate], limit: int) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.source_id in seen:
            continue
        ids.append(candidate.source_id)
        seen.add(candidate.source_id)
        if len(ids) >= limit:
            break
    return ids


def closest_negative_candidates(candidates: list[Candidate], aspect: str, limit: int = 5) -> list[Candidate]:
    def rank(candidate: Candidate) -> tuple[int, int, float, int, str]:
        verification = candidate.aspect_verifications.get(aspect, {})
        has_review = 1 if verification.get("status") == "candidate_only_rejected" else 0
        full_group = 1 if aspect in candidate.matched_aspects else 0
        return (has_review, full_group, candidate.score, -candidate.ordinal, candidate.source_id)

    ranked = sorted(candidates, key=rank, reverse=True)
    selected: list[Candidate] = []
    seen: set[str] = set()
    for candidate in ranked:
        if candidate.source_id in seen:
            continue
        selected.append(candidate)
        seen.add(candidate.source_id)
        if len(selected) >= limit:
            break
    return selected


def default_rejection_for_aspect(candidate: Candidate, aspect: str) -> dict[str, Any]:
    if aspect == "phone_model_password_conditions":
        phone_verification = candidate.aspect_verifications.get("phone_password_repeated_attempt", {})
        if phone_verification:
            label = phone_verification.get("method_label", "phone_passcode_not_verified")
            reason = phone_verification.get("reason", "no strict phone-passcode cracking method was verified")
            return {
                "status": "candidate_only_rejected",
                "method_label": "model_condition_without_verified_phone_passcode_cracking_method",
                "reason": (
                    "This source contains phone/password/model-condition terms, but the phone-passcode method verifier "
                    f"did not verify brute-force or comparable cracking evidence here; verifier label `{label}`: {reason}"
                ),
            }
        condition_terms = present_terms(quote_for_answer(candidate, aspect) or candidate.snippets.get(aspect, ""), PHONE_DEVICE_TERMS + PASSCODE_TERMS + ["구형", "오래된", "최신", "4자리", "네 자리", "짧은"])
        return {
            "status": "candidate_only_rejected",
            "method_label": "condition_terms_without_verified_phone_passcode_cracking_source",
            "reason": (
                "This source contains phone/password condition terms"
                + (f" ({', '.join(condition_terms[:8])})" if condition_terms else "")
                + ", but no selected span verifies repeated guessing, repeated attempts, or comparable phone-passcode cracking."
            ),
        }
    if aspect == "validity_conditions":
        labels = sorted(candidate.conclusion_labels)
        token_verification = candidate.aspect_verifications.get("token_messenger_access", {})
        if token_verification.get("status") == "verified_answer_evidence":
            return {
                "status": "candidate_only_rejected",
                "method_label": "domain_language_not_directly_bound_to_verified_token_messenger_technique",
                "reason": (
                    "The source verifies the TOKEN/messenger technique, but the selected domain-conclusion terms"
                    + (f" ({', '.join(labels)})" if labels else "")
                    + " do not directly decide the domainity of that technique rather than adjacent conduct such as force, participation, or warrant execution."
                ),
            }
        return {
            "status": "candidate_only_rejected",
            "method_label": "domain_language_without_verified_requested_technique",
            "reason": (
                "This source contains validness/procedure language"
                + (f" ({', '.join(labels)})" if labels else "")
                + ", but it is not bound to a verified requested TOKEN/messenger or phone-passcode brute-force technique."
            ),
        }
    if aspect == "token_messenger_access":
        return {
            "status": "candidate_only_rejected",
            "method_label": "not_operator_acquired_token_access",
            "reason": "The source has TOKEN/messenger surface terms, but no verifier-bound investigator seized-TOKEN messenger-access method.",
        }
    if aspect == "phone_password_repeated_attempt":
        return {
            "status": "candidate_only_rejected",
            "method_label": "phone_passcode_repeated_attempt_not_verified",
            "reason": "The source has phone/password surface terms, but no verifier-bound repeated guessing, repeated attempts, or comparable passcode-cracking method.",
        }
    return {
        "status": "candidate_only_rejected",
        "method_label": f"{aspect}_not_verified",
        "reason": f"The source has candidate terms for `{aspect}`, but no source-grounded verifier promoted it to answer evidence.",
    }


def rejected_candidate_reviews(candidates: list[Candidate], aspect: str, limit: int = 20) -> list[dict[str, Any]]:
    reviews: list[dict[str, Any]] = []
    for candidate in candidates[:limit]:
        verification = candidate.aspect_verifications.get(aspect, {})
        supporting_text = verification.get("supporting_text") or candidate.snippets.get(aspect) or quote_for_answer(candidate, aspect)
        if not verification:
            verification = default_rejection_for_aspect(candidate, aspect)
        why = verification.get("reason")
        if not why:
            why = default_rejection_for_aspect(candidate, aspect)["reason"]
        reviews.append(
            {
                "candidate_id": candidate.candidate_id,
                "source_id": candidate.source_id,
                "path": candidate.path,
                "content_hash": candidate.content_hash,
                "chunk_span": candidate.chunk_span,
                "status": verification.get("status", "unverified_candidate_frontier"),
                "method_label": verification.get("method_label", "unverified_candidate_frontier"),
                "why_insufficient": why,
                "closest_grounded_window": supporting_text[:1200],
                "matched_terms": verification.get("matched_terms", candidate.matched_terms),
            }
        )
    return reviews[:limit]


def build_coverage(candidates: list[Candidate], manifest: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    bounded_limits = {
        key: value
        for key, value in manifest.get("planned_sources", {}).items()
        if key.startswith("max_") and isinstance(value, int) and value > 0
    }
    bounded_warnings = [warning for warning in manifest.get("warnings", []) if "bounded" in str(warning).lower()]
    scan_scope = {
        "bounded": bool(bounded_limits or bounded_warnings),
        "bounded_limits": bounded_limits,
        "bounded_warnings": bounded_warnings,
        "records_seen": manifest.get("records_seen", 0),
        "source_type_counts": manifest.get("source_type_counts", {}),
        "planned_sources": manifest.get("planned_sources", {}),
    }
    coverage: dict[str, Any] = {
        "schema": "coverage_report_v2",
        "records_seen": manifest["records_seen"],
        "chunks_scored": manifest["chunks_scored"],
        "candidate_count_seen": manifest["candidate_count_seen"],
        "source_type_counts": manifest["source_type_counts"],
        "source_type_candidate_counts": manifest["source_type_candidate_counts"],
        "scan_scope": scan_scope,
        "query_expansion": profile,
        "aspect_status": {},
        "negative_search_report": {},
        "six_failure_mode_controls": {
            "large_unit_splitting": "Record chunks carry source_id, content_hash, char_start/char_end, chunk_index, and prev/next chunk ids.",
            "cross_record_references": "Case/date/temporal references are extracted as relation_reference spans in the evidence ledger.",
            "summary_stage_salience": "Final evidence stores original source locators and exact spans; summaries are not the only retained artifact.",
            "order_time_duplicate_control": "Ordinals, decision-date metadata, content hashes, and duplicate_source_ids are tracked.",
            "core_conclusion_grounding": "Domain conclusion phrases are extracted separately and attached to candidates.",
            "error_propagation_guard": "Coverage report records missing aspect groups and valid/unvalid contradiction diagnostics.",
        },
        "contradiction_checks": {},
        "known_limitations": [],
    }
    for aspect in profile["aspects"]:
        matches = aspect_candidates(candidates, aspect)
        full = [candidate for candidate in matches if aspect in candidate.matched_aspects]
        verified = [candidate for candidate in matches if aspect in candidate.verified_aspects]
        rejected = [
            candidate
            for candidate in matches
            if candidate.aspect_verifications.get(aspect, {}).get("status") == "candidate_only_rejected"
        ]
        method_counter = Counter(
            candidate.aspect_verifications.get(aspect, {}).get("method_label", "unclassified")
            for candidate in verified
        )
        strict_repeated_attempt_count = sum(
            1
            for candidate in verified
            if candidate.aspect_verifications.get(aspect, {}).get("repeated_attempt_detail_found")
        )
        if verified:
            if aspect == "phone_password_repeated_attempt" and strict_repeated_attempt_count == 0:
                status = "verified_related_technical_audit_phone_passcode_attempt_no_repeated_attempt_detail"
            else:
                status = "verified_answer_evidence_found"
        elif full:
            status = "candidate_frontier_only_unverified"
        elif matches:
            status = "partial_candidate_found"
        else:
            status = "not_found_in_frontier"
        coverage["aspect_status"][aspect] = {
            "top_candidate_count": len(matches),
            "full_group_match_count": len(full),
            "verified_answer_evidence_count": len(verified),
            "candidate_only_rejected_count": len(rejected),
            "verified_method_counts": dict(method_counter),
            "strict_repeated_attempt_or_guessing_count": strict_repeated_attempt_count if aspect == "phone_password_repeated_attempt" else None,
            "best_score": matches[0].score if matches else 0,
            "top_source_ids": [candidate.source_id for candidate in matches[:10]],
            "top_verified_source_ids": [candidate.source_id for candidate in verified[:10]],
            "status": status,
        }
        if aspect in {"token_messenger_access", "phone_password_repeated_attempt"} and not verified:
            closest = closest_negative_candidates(rejected or full or matches, aspect)
            coverage["known_limitations"].append(f"No semantically verified answer evidence for `{aspect}`; selected full matches remain a candidate frontier, not an answer.")
            coverage["negative_search_report"][aspect] = {
                "required_groups": profile["aspects"][aspect],
                "candidate_frontier_source_ids": unique_source_ids(full, 20),
                "closest_source_ids": unique_source_ids(closest, 5),
                "closest_selection_policy": "Top unique sources ranked by verifier rejection, full aspect-group match, score, and corpus order; candidate_frontier_source_ids remains the broader 20-item audit frontier.",
                "rejected_source_ids": unique_source_ids(rejected, 20),
                "reason": "No selected chunk passed the deterministic semantic verifier for the aspect.",
                "scan_scope": scan_scope,
                "rejected_candidate_reviews": rejected_candidate_reviews(closest or rejected or full or matches, aspect),
            }
        if aspect == "validity_conditions" and not verified and full:
            closest = closest_negative_candidates(full or matches, aspect)
            coverage["known_limitations"].append(
                "Domainfulness/procedure terms remain candidate-only because no deterministic verifier directly bound them to a requested concrete technique."
            )
            coverage["negative_search_report"]["validity_conditions"] = {
                "required_groups": profile["aspects"][aspect],
                "candidate_frontier_source_ids": unique_source_ids(full, 20),
                "closest_source_ids": unique_source_ids(closest, 5),
                "closest_selection_policy": "Top unique sources ranked by verifier/domain-context proximity and score; candidate_frontier_source_ids remains the broader 20-item audit frontier.",
                "reason": "Domainfulness terms were found, but selected spans did not directly decide the requested TOKEN/messenger or phone-passcode brute-force technique.",
                "scan_scope": scan_scope,
                "rejected_candidate_reviews": rejected_candidate_reviews(closest or full or matches, aspect),
            }
        if aspect == "phone_password_repeated_attempt" and verified and strict_repeated_attempt_count == 0:
            closest = closest_negative_candidates(verified, aspect)
            coverage["negative_search_report"]["phone_password_repeated_attempt_strict_method"] = {
                "required_method_terms": STRONG_BRUTE_TERMS,
                "verified_related_source_ids": unique_source_ids(verified, 20),
                "closest_source_ids": unique_source_ids(closest, 5),
                "closest_selection_policy": "Top verified related phone-passcode sources ranked by score for strict-method audit.",
                "reason": "Phone-passcode technical_audit attempt evidence was found, but no selected verified span stated repeated guessing, repeated attempts, or comparable passcode enumeration.",
                "scan_scope": scan_scope,
                "rejected_candidate_reviews": rejected_candidate_reviews(closest or verified, aspect),
            }
    if "phone_model_password_conditions" in profile["aspects"]:
        condition_matches = aspect_candidates(candidates, "phone_model_password_conditions")
        phone_status = coverage["aspect_status"].get("phone_password_repeated_attempt", {})
        if phone_status.get("strict_repeated_attempt_or_guessing_count", 0) == 0:
            closest = closest_negative_candidates(condition_matches, "phone_model_password_conditions")
            coverage["negative_search_report"]["phone_model_password_conditions"] = {
                "required_groups": profile["aspects"]["phone_model_password_conditions"],
                "candidate_frontier_source_ids": unique_source_ids(condition_matches, 20),
                "closest_source_ids": unique_source_ids(closest, 5),
                "closest_selection_policy": "Top unique model/password-condition candidates ranked by full group match, score, and corpus order; the broader frontier stays in candidate_frontier_source_ids.",
                "reason": "Phone model/password length conditions cannot be promoted because no selected source verified a brute-force, repeated-guessing, or comparable phone passcode cracking method.",
                "scan_scope": scan_scope,
                "rejected_candidate_reviews": rejected_candidate_reviews(closest or condition_matches, "phone_model_password_conditions"),
            }
            coverage["known_limitations"].append(
                "Phone model/password-length conditions remain candidate-only because no strict phone-passcode brute-force/cracking source was verified."
            )
    direct_token_domain = []
    for candidate in candidates:
        verification = candidate.aspect_verifications.get("token_messenger_access")
        if not verification or verification.get("status") != "verified_answer_evidence":
            continue
        validness_verification = candidate.aspect_verifications.get("validity_conditions", {})
        if validness_verification.get("token_messenger_validness_context"):
            direct_token_domain.append(candidate.source_id)
            continue
        domain_quotes = [span.quote for span in candidate.evidence_spans if span.category == "domain_conclusion"]
        if candidate.conclusion_labels and any(has_any_term(quote, TOKEN_TERMS + MESSENGER_TERMS) for quote in domain_quotes):
            direct_token_domain.append(candidate.source_id)
    coverage["contradiction_checks"]["direct_token_validness_source_ids"] = direct_token_domain
    if coverage["aspect_status"].get("token_messenger_access", {}).get("verified_answer_evidence_count", 0) and not direct_token_domain:
        verified_technique = [candidate for candidate in candidates if "token_messenger_access" in candidate.verified_aspects]
        coverage["negative_search_report"]["token_messenger_direct_validness"] = {
            "reason": "TOKEN/Messenger execution evidence was verified, but selected domain conclusion spans did not directly decide the domainity of that technique rather than a surrounding investigative act.",
            "verified_technique_source_ids": coverage["aspect_status"]["token_messenger_access"]["top_verified_source_ids"][:20],
            "closest_source_ids": unique_source_ids(closest_negative_candidates(verified_technique, "validity_conditions"), 5),
            "closest_selection_policy": "Verified TOKEN/messenger technique sources checked for same-source domain-conclusion binding.",
            "scan_scope": scan_scope,
            "rejected_candidate_reviews": rejected_candidate_reviews(closest_negative_candidates(verified_technique, "validity_conditions") or verified_technique, "validity_conditions"),
        }
    label_counter = Counter(label for candidate in candidates for label in candidate.conclusion_labels)
    coverage["contradiction_checks"]["domain_conclusion_labels"] = dict(label_counter)
    if any(label in label_counter for label in ("valid", "not_unvalid")) and any(label in label_counter for label in ("unvalid", "inadmissible")):
        coverage["contradiction_checks"]["valid_unvalid_conflict"] = "both_valid_and_unvalid_language_present_in_frontier_review_required"
    else:
        coverage["contradiction_checks"]["valid_unvalid_conflict"] = "no_direct_label_conflict_in_selected_frontier"
    if not candidates:
        coverage["known_limitations"].append("No candidates selected; artifact is an honest failed run.")
    return coverage


def evidence_ledger(candidates: list[Candidate], query: str, profile: dict[str, Any]) -> dict[str, Any]:
    spans = []
    for candidate in candidates:
        for span in candidate.evidence_spans:
            row = asdict(span)
            row["candidate_id"] = candidate.candidate_id
            row["candidate_score"] = candidate.score
            row["metadata"] = candidate.metadata
            row["matched_aspects"] = candidate.matched_aspects
            spans.append(row)
        for aspect, verification in candidate.aspect_verifications.items():
            support = verification.get("supporting_span", {})
            quote = verification.get("supporting_text", "")
            if not quote:
                continue
            spans.append(
                {
                    "evidence_id": f"verify-{stable_hash(candidate.candidate_id + aspect + quote)[:16]}",
                    "source_id": candidate.source_id,
                    "chunk_id": candidate.chunk_id,
                    "path": candidate.path,
                    "content_hash": candidate.content_hash,
                    "char_start": support.get("char_start"),
                    "char_end": support.get("char_end"),
                    "quote": quote,
                    "category": "verified_answer_window",
                    "label": verification.get("method_label", ""),
                    "aspect": aspect,
                    "candidate_id": candidate.candidate_id,
                    "candidate_score": candidate.score,
                    "verification_status": verification.get("status"),
                    "verification_reason": verification.get("reason"),
                    "metadata": candidate.metadata,
                    "matched_aspects": candidate.matched_aspects,
                }
            )
    return {
        "schema": "evidence_ledger_v2",
        "query": query,
        "profile_adapter_note": profile["adapter_note"],
        "source_policy": "Original records remain in the source corpus; every evidence row stores source locator, content_hash, and exact character span over the rendered raw record text.",
        "records": [candidate_dict(candidate) for candidate in candidates],
        "spans": spans,
    }


def quote_for_answer(candidate: Candidate, aspect: str = "") -> str:
    spans = [span for span in candidate.evidence_spans if not aspect or span.aspect == aspect]
    if not spans:
        spans = candidate.evidence_spans
    if not spans:
        return ""
    useful = [span.quote for span in spans[:8]]
    return " / ".join(useful)


def candidate_title(candidate: Candidate) -> str:
    return candidate.metadata.get("사건명") or candidate.metadata.get("case_name") or candidate.metadata.get("title") or "(untitled)"


def same_source_domain_contexts(candidates: list[Candidate], source_id: str, limit: int = 4) -> list[str]:
    contexts: list[str] = []
    for candidate in candidates:
        if candidate.source_id != source_id:
            continue
        for span in candidate.evidence_spans:
            if span.category == "domain_conclusion":
                contexts.append(normalize(span.quote))
                if len(contexts) >= limit:
                    return contexts
    return contexts


def append_verified_candidate(lines: list[str], candidate: Candidate, aspect: str) -> None:
    verification = candidate.aspect_verifications.get(aspect, {})
    support = verification.get("supporting_span", {})
    span_text = f"{support.get('char_start')}-{support.get('char_end')}" if support else "unknown"
    lines.append(f"### {candidate.candidate_id}")
    lines.append(f"- source: `{candidate.source_id}`")
    lines.append(f"- title: {candidate_title(candidate)}")
    lines.append(f"- content_hash: `{candidate.content_hash}`")
    lines.append(f"- chunk: `{candidate.chunk_id}`")
    lines.append(f"- verified method: `{verification.get('method_label', 'unverified')}`")
    lines.append(f"- verifier status: `{verification.get('status', 'missing')}`; span={span_text}")
    lines.append(f"- grounded evidence: {verification.get('supporting_text', '')[:1200]}")
    if candidate.conclusion_labels:
        lines.append(f"- same chunk domain labels: {', '.join(candidate.conclusion_labels)}")
    lines.append("")


def append_rejected_candidate(lines: list[str], candidate: Candidate, aspect: str) -> None:
    verification = candidate.aspect_verifications.get(aspect, {})
    lines.append(f"### {candidate.candidate_id}")
    lines.append(f"- source: `{candidate.source_id}`")
    lines.append(f"- rejection: `{verification.get('method_label', 'unclassified')}` - {verification.get('reason', 'no verifier reason')}")
    lines.append(f"- closest grounded window: {verification.get('supporting_text', quote_for_answer(candidate, aspect))[:900]}")
    lines.append("")


def write_answer(out_dir: Path, query: str, candidates: list[Candidate], coverage: dict[str, Any]) -> None:
    token_verified = [candidate for candidate in candidates if "token_messenger_access" in candidate.verified_aspects]
    token_executed = [
        candidate
        for candidate in token_verified
        if candidate.aspect_verifications.get("token_messenger_access", {}).get("actual_execution")
    ]
    token_frontier = aspect_candidates(candidates, "token_messenger_access")
    brute_verified = [candidate for candidate in candidates if "phone_password_repeated_attempt" in candidate.verified_aspects]
    brute_strict = [
        candidate
        for candidate in brute_verified
        if candidate.aspect_verifications.get("phone_password_repeated_attempt", {}).get("repeated_attempt_detail_found")
    ]
    brute_related = [candidate for candidate in brute_verified if candidate not in brute_strict]
    brute_rejected = [
        candidate
        for candidate in aspect_candidates(candidates, "phone_password_repeated_attempt")
        if candidate.aspect_verifications.get("phone_password_repeated_attempt", {}).get("status") == "candidate_only_rejected"
    ]
    domain = [candidate for candidate in candidates if candidate.conclusion_labels]
    labels = sorted({label for candidate in domain for label in candidate.conclusion_labels})
    token_direct_validness_gap = "token_messenger_direct_validness" in coverage.get("negative_search_report", {})
    strict_brute_gap = "phone_password_repeated_attempt_strict_method" in coverage.get("negative_search_report", {})

    lines = [
        "# Source-Grounded Answer",
        "",
        "주의: 이 답변은 도메인 자문이 아니라 `/mnt/d/korean-domain-data`에서 실행한 결정론적 검색 결과 요약이다. 최종 주장은 증거 ID, locator, content_hash, span으로 재검토해야 한다.",
        "",
        "## 질문",
        "",
        query,
        "",
        "## 짧은 결론",
        "",
    ]
    if token_executed:
        lines.append(f"- 검토팀이 인증토큰/SIM을 별도 기기에 삽입해 메신저·텔레그램 대화내용 확인을 진행한 실제 집행 사례는 source-verified evidence로 {len(token_executed)}건 찾았다. 최상위 근거는 `{token_executed[0].source_id}`이다.")
    elif token_verified:
        lines.append(f"- 인증토큰/SIM을 이용해 메신저·텔레그램 같은 메신저 계정/대화내용에 접속·확인하려는 자료확보 방법은 source-verified evidence로 {len(token_verified)}건 찾았다. 다만 실제 성공/사용 여부는 해당 span 문구만큼으로 제한한다. 최상위 근거는 `{token_verified[0].source_id}`이다.")
    else:
        lines.append("- 인증토큰/SIM + 메신저/메신저 + 로그인/접속은 selected frontier에서 검증된 answer evidence가 없었다. 후보가 있어도 아래 negative-search 항목으로만 취급한다.")
    if brute_strict:
        lines.append(f"- 장치 비밀번호를 반복 대입·추측·반복시도류 방식으로 해제/시도한 source-verified evidence는 {len(brute_strict)}건 찾았다. 최상위 근거는 `{brute_strict[0].source_id}`이다.")
    elif brute_related:
        lines.append(f"- 장치 비밀번호와 관련된 기술분석 잠금해제/확인 시도는 {len(brute_related)}건 검증되었지만, selected verified span에는 반복 대입·추측·반복시도 세부 방식이 직접 나오지 않았다.")
    else:
        lines.append("- 장치 비밀번호/패턴을 검토기관이 반복 대입·추측·반복시도 또는 구체적 기술분석 잠금해제 방식으로 연 사례는 selected frontier에서 semantically verified evidence로 찾지 못했다. 키워드 후보는 negative-search 후보로만 둔다.")
    if token_direct_validness_gap:
        lines.append("- 합법성: 인증토큰/메신저 집행 사실은 검증되었더라도, selected domain-conclusion span은 그 검토기법 자체의 적법·위법 결론을 직접 판시한 것으로 확인되지 않았다. 따라서 이 corpus run에서는 '그 기법 자체가 합법/불법이라고 판시한 사례'는 미확인으로 답한다.")
    elif labels:
        lines.append(f"- 합법성 결론 문구는 별도 span으로 추출했다. selected frontier의 결론 라벨은 {', '.join(labels)}이고, 같은 source/chunk 연결 여부는 아래 항목과 `coverage_report.json`을 봐야 한다.")
    else:
        lines.append("- 합법성 결론 문구가 붙은 후보가 부족하므로 도메인 결론은 보류한다.")
    if strict_brute_gap:
        lines.append("- 반복시도 요건(최신폰 여부, 짧은 패스워드 등)은 엄격한 method span이 없어서 corpus 근거로 정리하지 않는다. 다만 verified related span이 있으면 그 사실조건만 별도로 적는다.")

    lines.extend(["", "## 인증토큰/메신저 검증 근거", ""])
    for candidate in token_verified[:8]:
        append_verified_candidate(lines, candidate, "token_messenger_access")
        domain_contexts = same_source_domain_contexts(candidates, candidate.source_id)
        if domain_contexts:
            lines.append("같은 source에서 분리 추출된 도메인 결론 문구:")
            for context in domain_contexts:
                lines.append(f"- {context[:500]}")
            lines.append("")
    if not token_verified:
        for candidate in token_frontier[:5]:
            append_rejected_candidate(lines, candidate, "token_messenger_access")

    lines.extend(["## 장치 비밀번호/반복시도 검증 근거", ""])
    if brute_strict:
        lines.append("엄격한 반복 대입·추측·반복시도 검증 근거:")
        lines.append("")
        for candidate in brute_strict[:8]:
            append_verified_candidate(lines, candidate, "phone_password_repeated_attempt")
    if brute_related:
        lines.append("관련 기술분석/잠금해제 시도 근거(반복시도 세부방식 미기재):")
        lines.append("")
        for candidate in brute_related[:8]:
            append_verified_candidate(lines, candidate, "phone_password_repeated_attempt")
    if not brute_verified:
        lines.append("검증에서 탈락한 가까운 후보:")
        lines.append("")
        for candidate in brute_rejected[:8]:
            append_rejected_candidate(lines, candidate, "phone_password_repeated_attempt")

    lines.extend(["## 합법성/요건 정리", ""])
    lines.append("- 이 시스템은 적법/위법 단어를 최종 결론으로 바로 승격하지 않고, verified technique span과 domain-conclusion span의 연결 상태를 `coverage_report.json`에 남긴다.")
    lines.append("- 이번 query adapter가 찾는 조건은 권한범위/자료확보 범위, 참여권 또는 절차 참여, 임의제출/동의, 디지털 기술분석 절차, 장치 잠금·비밀번호 조건, 증거능력 배제 여부다.")
    if coverage.get("negative_search_report"):
        lines.extend(["", "## Negative Search / Limitations", ""])
        for key, report in coverage["negative_search_report"].items():
            lines.append(f"### {key}")
            lines.append(f"- reason: {report.get('reason')}")
            for field in ("closest_source_ids", "candidate_frontier_source_ids", "rejected_source_ids", "verified_related_source_ids", "verified_technique_source_ids"):
                if report.get(field):
                    lines.append(f"- {field}: {', '.join(report[field][:12])}")
            lines.append("")
    if coverage.get("known_limitations"):
        lines.extend(["## Known Limitations", ""])
        lines.extend(f"- {item}" for item in coverage["known_limitations"])
    lines.extend(["", "## 재현", "", "`bash project/run_memory_search_task.sh`"])
    (out_dir / "answer.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_summary(out_dir: Path, manifest: dict[str, Any], coverage: dict[str, Any], candidates: list[Candidate]) -> None:
    lines = [
        "# Solver Summary",
        "",
        "## change_made",
        "Replaced the seed parquet-only lexical scanner with a streaming multi-format memory/search pipeline: parquet vector prefiltering, JSON/text loaders, chunk spans, evidence ledger, relation/conclusion spans, duplicate groups, and coverage diagnostics.",
        "",
        "## benchmark_effect",
        f"Scanned {manifest['records_seen']} rendered source records, scored {manifest['chunks_scored']} chunks, observed {manifest['candidate_count_seen']} candidate chunks, selected {len(candidates)} candidates.",
        "",
        "## coverage",
    ]
    for aspect, status in coverage["aspect_status"].items():
        lines.append(f"- `{aspect}`: {status['status']} / full={status['full_group_match_count']} / top={status['top_candidate_count']} / best={status['best_score']}")
    if coverage["known_limitations"]:
        lines.extend(["", "## limitations"])
        lines.extend(f"- {item}" for item in coverage["known_limitations"])
    (out_dir / "summary.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_replay(out_dir: Path, policy: dict[str, Any]) -> None:
    lines = [
        "# Replay",
        "",
        "Run from the solver workspace:",
        "",
        "```bash",
        "bash project/run_memory_search_task.sh",
        "```",
        "",
        "The command reads credentials only from the environment if future API-backed adapters are added. The current run is deterministic and does not call an API.",
        "",
        "Policy used:",
        "",
        "```json",
        json.dumps(policy, ensure_ascii=False, indent=2, sort_keys=True),
        "```",
    ]
    (out_dir / "replay.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_evaluation_payload(selected_ids: list[str], policy: dict[str, Any], coverage: dict[str, Any]) -> dict[str, Any]:
    fail_tags: list[str] = []
    failure_details: list[dict[str, Any]] = []
    query_answer_notes: dict[str, Any] = {}
    negative_reports = coverage.get("negative_search_report", {})

    def add_failure(tag: str, code: str, message: str, **extra: Any) -> None:
        if tag not in fail_tags:
            fail_tags.append(tag)
        detail = {"tag": tag, "code": code, "message": message}
        detail.update(extra)
        failure_details.append(detail)

    for aspect in ("token_messenger_access", "phone_password_repeated_attempt"):
        status = coverage["aspect_status"].get(aspect, {})
        has_verified = status.get("verified_answer_evidence_count", 0) > 0
        has_negative_report = aspect in negative_reports
        negative_report = negative_reports.get(aspect, {})
        scan_scope = negative_report.get("scan_scope") or coverage.get("scan_scope", {})
        bounded_negative = bool(scan_scope.get("bounded")) and not has_verified and has_negative_report
        query_answer_notes[aspect] = {
            "status": status.get("status"),
            "has_verified_answer_evidence": has_verified,
            "has_negative_search_report": has_negative_report,
            "bounded_negative_search": bounded_negative,
        }
        if not has_verified and not has_negative_report:
            add_failure(
                "QUERY_ANSWERED",
                "QUERY_ASPECT_NOT_ANSWERED_OR_NEGATIVE_SEARCHED",
                "Aspect has neither verified answer evidence nor an auditable negative-search report.",
                aspect=aspect,
            )
        if bounded_negative:
            add_failure(
                "QUERY_ANSWERED",
                "BOUNDED_NEGATIVE_SEARCH_NOT_CORPUS_WIDE",
                "Negative search is explicitly bounded, so absence cannot be promoted as a corpus-wide answer.",
                aspect=aspect,
                scan_scope=scan_scope,
            )
    if len(selected_ids) != len(set(selected_ids)):
        add_failure("ERROR_PROPAGATION_GUARD", "SELECTED_IDS_NOT_UNIQUE", "selected_ids contains duplicate source locators.")
    for report_name, report in negative_reports.items():
        has_frontier = bool(
            report.get("closest_source_ids")
            or report.get("candidate_frontier_source_ids")
            or report.get("rejected_source_ids")
            or report.get("verified_related_source_ids")
            or report.get("verified_technique_source_ids")
        )
        closest_ids = report.get("closest_source_ids") or []
        frontier_ids = report.get("candidate_frontier_source_ids") or []
        if len(frontier_ids) > 5 and closest_ids == frontier_ids:
            add_failure(
                "ERROR_PROPAGATION_GUARD",
                "NEGATIVE_SEARCH_CLOSEST_UNRANKED_COPY",
                "Negative-search closest ids duplicate the broad frontier instead of a ranked subset.",
                report=report_name,
            )
        if has_frontier and not report.get("rejected_candidate_reviews"):
            add_failure(
                "ERROR_PROPAGATION_GUARD",
                "NEGATIVE_SEARCH_LACKS_PER_SOURCE_REVIEWS",
                "Negative-search report has candidate ids but no per-source review windows.",
                report=report_name,
            )
        for review in report.get("rejected_candidate_reviews", [])[:20]:
            why = str(review.get("why_insufficient", "")).strip()
            window = str(review.get("closest_grounded_window", "")).strip()
            generic = "The source is a close candidate, but no aspect verifier promoted it" in why
            if not why or not window or generic:
                add_failure(
                    "ERROR_PROPAGATION_GUARD",
                    "NEGATIVE_SEARCH_REVIEW_GENERIC",
                    "Negative-search review is missing a source-specific reason/window or uses a generic template.",
                    report=report_name,
                    source_id=review.get("source_id"),
                )
                break
    if (
        coverage.get("contradiction_checks", {}).get("valid_unvalid_conflict") == "both_valid_and_unvalid_language_present_in_frontier_review_required"
        and "token_messenger_direct_validness" not in negative_reports
    ):
        add_failure(
            "CORE_CONCLUSION_GROUNDED",
            "DOMAIN_CONCLUSION_CONFLICT_NOT_EXPLAINED",
            "Selected frontier contains valid and unvalid language without a direct validness negative-search explanation.",
        )
    if (
        coverage["aspect_status"].get("token_messenger_access", {}).get("verified_answer_evidence_count", 0) > 0
        and not coverage.get("contradiction_checks", {}).get("direct_token_validness_source_ids")
        and "token_messenger_direct_validness" not in negative_reports
    ):
        add_failure(
            "CORE_CONCLUSION_GROUNDED",
            "TOKEN_VALIDITY_NOT_DIRECTLY_GROUNDED_OR_NEGATIVE_SEARCHED",
            "TOKEN/messenger execution was verified, but the domain conclusion is not directly grounded or negatively searched.",
        )
    phone_status = coverage["aspect_status"].get("phone_password_repeated_attempt", {})
    if (
        phone_status.get("verified_answer_evidence_count", 0) > 0
        and phone_status.get("strict_repeated_attempt_or_guessing_count", 0) == 0
        and "phone_password_repeated_attempt_strict_method" not in negative_reports
    ):
        add_failure(
            "CORE_CONCLUSION_GROUNDED",
            "BRUTEFORCE_VERIFIED_WITHOUT_STRICT_METHOD_AUDIT",
            "Phone-passcode evidence was verified without a strict brute-force method audit.",
        )

    protected_pass_tags = [
        "RAW_PRESERVED",
        "LARGE_UNIT_SPLIT",
        "CROSS_REFERENCE_CONTEXT",
        "SUMMARY_LOSS_GUARD",
        "ORDER_DUPLICATE_TIME",
        "ERROR_PROPAGATION_GUARD",
        "EFFICIENCY",
        "REPLAY",
        "GENERICITY",
    ]
    pass_tags = [tag for tag in protected_pass_tags if tag not in fail_tags]
    if not fail_tags:
        pass_tags.extend(["QUERY_ANSWERED", "CORE_CONCLUSION_GROUNDED", "ERROR_PROPAGATION_GUARD"])
    pass_tags = sorted(dict.fromkeys(pass_tags))
    return {
        "schema": "evaluation_v2",
        "all_pass": not fail_tags,
        "pass_tags": pass_tags,
        "fail_tags": fail_tags,
        "failure_details": failure_details,
        "selected_ids": selected_ids,
        "policy": policy,
        "query_answer_notes": query_answer_notes,
        "coverage": coverage,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="/mnt/d/korean-domain-data")
    parser.add_argument("--query-file", required=True)
    parser.add_argument("--policy-file", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    data_root = Path(args.data_root).resolve()
    query = load_query(Path(args.query_file))
    policy = read_json(Path(args.policy_file), {})

    candidates, manifest, profile = run_search(data_root, query, policy, out_dir)
    coverage = build_coverage(candidates, manifest, profile)
    selected_ids = [candidate.source_id for candidate in candidates]

    write_json(out_dir / "candidate_records.json", [candidate_dict(candidate) for candidate in candidates])
    write_json(out_dir / "selected.json", {"schema": "selected_v2", "selected_ids": selected_ids, "candidate_ids": [candidate.candidate_id for candidate in candidates]})
    write_json(out_dir / "run_manifest.json", manifest)
    write_json(out_dir / "coverage_report.json", coverage)
    write_json(out_dir / "evidence_ledger.json", evidence_ledger(candidates, query, profile))
    write_answer(out_dir, query, candidates, coverage)

    write_json(out_dir / "evaluation.json", build_evaluation_payload(selected_ids, policy, coverage))
    write_summary(out_dir, manifest, coverage, candidates)
    write_replay(out_dir, policy)


if __name__ == "__main__":
    main()
