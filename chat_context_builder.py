"""
Reduz o contexto enviado ao LLM: extrai registros tabulares, aplica filtros
heurísticos a partir da pergunta (PT-BR) e limita tamanho — economiza tokens
e deixa explícito quando o recorte é parcial.
"""
from __future__ import annotations

import json
import re
from typing import Any, Mapping

# Limites padrão (ajustáveis via payload opcional `limites_chat`)
DEFAULT_MAX_ROWS = 120
DEFAULT_MAX_CHARS = 14_000


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def extract_records(contexto: Any) -> list[dict[str, Any]] | None:
    """Tenta obter uma lista de objetos (linhas) a partir de formatos comuns de gráfico/API."""
    if isinstance(contexto, list):
        if contexto and all(isinstance(x, dict) for x in contexto):
            return contexto  # type: ignore[return-value]
        return None

    if not isinstance(contexto, dict):
        return None

    for key in ("records", "dados", "data", "rows", "pontos", "registros"):
        v = contexto.get(key)
        if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
            return v  # type: ignore[return-value]

    # Formato estilo Chart.js: labels + datasets[].data
    if "labels" in contexto and "datasets" in contexto:
        labels = contexto.get("labels") or []
        datasets = contexto.get("datasets") or []
        if not isinstance(labels, list) or not isinstance(datasets, list):
            return None
        out: list[dict[str, Any]] = []
        for ds in datasets:
            if not isinstance(ds, dict):
                continue
            sname = ds.get("label", "serie")
            data = ds.get("data") or []
            if not isinstance(data, list):
                continue
            for i, val in enumerate(data):
                cat = labels[i] if i < len(labels) else i
                out.append({"categoria": cat, "serie": sname, "valor": val})
        return out or None

    return None


def extract_records_nested(contexto: Any) -> list[dict[str, Any]] | None:
    """Como extract_records, mas tenta uma camada de aninhamento (grafico, chart, etc.)."""
    r = extract_records(contexto)
    if r is not None:
        return r
    if isinstance(contexto, dict):
        for inner in ("grafico", "chart", "payload", "visualizacao", "dados_grafico"):
            sub = contexto.get(inner)
            r = extract_records(sub)
            if r is not None:
                return r
    return None


def _collect_string_values(records: list[dict[str, Any]], key: str, cap: int = 80) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for row in records:
        v = row.get(key)
        if v is None or isinstance(v, (list, dict)):
            continue
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= cap:
            break
    return out


def infer_filters_from_question(
    mensagem: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Heurística leve (sem LLM): anos, sexo comum, e coincidência texto↔valor por coluna.
    """
    q = _norm(mensagem)
    filtros: dict[str, Any] = {}

    if not records:
        return filtros

    row0 = records[0]

    years = [int(y) for y in re.findall(r"\b(19\d{2}|20\d{2})\b", mensagem)]
    if years:
        yi = years[0]
        if "ano" in row0:
            filtros["ano"] = yi
        elif "year" in row0:
            filtros["year"] = yi
        elif "Ano" in row0:
            filtros["Ano"] = yi

    if re.search(r"\b(masculino|homem|sexo\s*m\b|\bmasc\b)", q):
        for key in ("sexo", "Sexo", "genero", "gênero"):
            if key in row0:
                filtros[key] = "M"
                break
    elif re.search(r"\b(feminino|mulher|sexo\s*f\b|\bfem\b)", q):
        for key in ("sexo", "Sexo", "genero", "gênero"):
            if key in row0:
                filtros[key] = "F"
                break

    keys = [k for k in row0 if not str(k).startswith("_")]
    for key in keys:
        if key in filtros:
            continue
        for val in _collect_string_values(records, key, cap=80):
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                continue
            vs = _norm(str(val))
            if len(vs) < 2:
                continue
            compact_q = q.replace(" ", "")
            if vs in q or vs.replace(" ", "") in compact_q:
                filtros[key] = val
                break

    return filtros


def _row_matches(row: dict[str, Any], filtros: dict[str, Any]) -> bool:
    for k, want in filtros.items():
        if k not in row:
            continue
        got = row.get(k)
        if isinstance(want, (int, float)) and not isinstance(want, bool):
            try:
                if float(want) != float(got):
                    return False
            except (TypeError, ValueError):
                if str(got).strip().lower() != str(want).strip().lower():
                    return False
        elif str(got).strip().lower() != str(want).strip().lower():
            return False
    return True


def _manifest(records: list[dict[str, Any]], keys: list[str]) -> dict[str, Any]:
    """Resumo compacto: colunas + alguns valores distintos por coluna (cabeçalho semântico)."""
    man: dict[str, Any] = {"colunas": keys, "n_linhas": len(records)}
    for k in keys[:24]:
        vals = _collect_string_values(records, k, cap=12)
        if vals:
            man.setdefault("amostra_valores", {})[k] = vals
    return man


def build_reduced_context(
    mensagem: str,
    contexto: Any,
    filtros_visiveis: Mapping[str, Any] | None = None,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> tuple[str, dict[str, Any]]:
    """
    Retorna (texto_para_prompt, meta) onde meta descreve recorte (útil para debug/log).
    """
    meta: dict[str, Any] = {
        "modo": "json_bruto_truncado",
        "max_rows": max_rows,
        "max_chars": max_chars,
    }
    records = extract_records_nested(contexto)

    if records is None:
        raw = json.dumps(contexto, ensure_ascii=False, separators=(",", ":"))
        if len(raw) > max_chars:
            raw = raw[: max_chars - 80] + "…[TRUNCADO]"
            meta["truncado"] = True
        meta["modo"] = "contexto_sem_tabela"
        return raw, meta

    total = len(records)
    meta["linhas_totais"] = total
    keys = list(records[0].keys()) if records else []

    merged = infer_filters_from_question(mensagem, records)
    if filtros_visiveis:
        merged.update(dict(filtros_visiveis))

    filtrados = [r for r in records if _row_matches(r, merged)] if merged else list(records)
    if merged and len(filtrados) == 0:
        meta["filtro_sem_resultado"] = True
        filtrados = list(records)

    meta["filtros_aplicados"] = merged
    meta["linhas_apos_filtro"] = len(filtrados)

    usar = filtrados
    if len(usar) > max_rows:
        # Mantém início e fim para séries temporais não perderem cauda
        metade = max_rows // 2
        usar = usar[:metade] + usar[-metade:]
        meta["amostragem"] = f"primeiras_{metade}_e_ultimas_{metade}"
        meta["linhas_enviadas"] = len(usar)
    else:
        meta["linhas_enviadas"] = len(usar)

    manifest = _manifest(records, keys)
    payload = {
        "resumo_conjunto": manifest,
        "filtros_usados_na_selecao": merged or None,
        "linhas_para_analise": usar,
        "nota": (
            "Use somente os campos em linhas_para_analise e resumo_conjunto. "
            "Se a pergunta exigir linhas não incluídas após filtros ou limite de linhas, "
            "diga explicitamente que os dados fornecidos são parciais ou insuficientes."
        ),
    }

    texto = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(texto) > max_chars:
        # Último recurso: menos linhas
        while len(usar) > 5 and len(texto) > max_chars:
            usar = usar[: max(5, len(usar) * 2 // 3)]
            payload["linhas_para_analise"] = usar
            texto = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        if len(texto) > max_chars:
            texto = texto[: max_chars - 60] + "…[TRUNCADO]"
            meta["truncado"] = True
        meta["linhas_enviadas"] = len(usar)

    meta["chars_enviados"] = len(texto)
    return texto, meta
