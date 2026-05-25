package ru.sbrf.pprb.stmnt.consistency.api.dto;

import java.util.Map;

/**
 * clusterId -> per-cluster operation result (object shape depends on operation).
 * "ok": boolean, "result": payload, "error": optional message.
 */
public record AdminFanOutResultDto(
        String operation,
        Map<String, Map<String, Object>> perCluster
) {}
