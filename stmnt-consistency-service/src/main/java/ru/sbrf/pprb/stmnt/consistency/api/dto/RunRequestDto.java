package ru.sbrf.pprb.stmnt.consistency.api.dto;

/**
 * Body for POST /api/consistency/run.
 * cacheName == null means "all configured caches".
 */
public record RunRequestDto(String cacheName) {}
