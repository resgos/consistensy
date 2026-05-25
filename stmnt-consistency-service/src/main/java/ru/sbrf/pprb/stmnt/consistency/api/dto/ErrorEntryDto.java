package ru.sbrf.pprb.stmnt.consistency.api.dto;

import java.time.Instant;
import java.util.Map;

public record ErrorEntryDto(
        long id,
        Instant occurredAt,
        String source,
        String level,
        String code,
        String message,
        Map<String, Object> details,
        Long relatedRunId,
        String clusterId,
        String cacheName,
        Instant resolvedAt,
        String resolutionNotes
) {}
