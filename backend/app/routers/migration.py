"""Hive→Glue 字段迁移专用路由：核验、对账、双读、切换、回切。

独立于通用资源路由，因为这些接口是有状态的迁移演练动作而非 CRUD。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.services.behavior_migration import service

router = APIRouter(prefix="/api/migration/behavior-log", tags=["Hive-Glue字段迁移"])


@router.post("/bootstrap")
def bootstrap() -> dict:
    return service.bootstrap()


@router.get("/versions")
def versions() -> dict:
    snap = service.snapshot()
    return {"active_version": snap["active_version"], "versions": snap["versions"]}


@router.post("/verify/{version_id}")
def verify(version_id: str, field_name: str = "event_timestamp") -> dict:
    try:
        return service.verify(version_id, field_name)
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/reconcile")
def reconcile() -> dict:
    try:
        return service.reconcile()
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/reads/legacy")
def read_legacy() -> dict:
    return service.legacy_read()


@router.get("/reads/glue")
def read_glue() -> dict:
    try:
        return service.glue_read()
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/quarantine")
def quarantine() -> dict:
    return service.quarantine()


@router.post("/cutover")
def cutover() -> dict:
    try:
        return service.cutover()
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/rollback")
def rollback() -> dict:
    try:
        return service.rollback()
    except (PermissionError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/snapshot")
def snapshot() -> dict:
    return service.snapshot()
