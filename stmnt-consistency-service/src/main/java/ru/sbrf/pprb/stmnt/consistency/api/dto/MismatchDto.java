package ru.sbrf.pprb.stmnt.consistency.api.dto;

import java.time.Instant;
import java.util.Map;

public record MismatchDto(
        long id,
        long runId,
        String cacheName,
        String businessKey,
        Map<String, String> clusterHashes,
        Instant detectedAt,
        Instant resolvedAt,
        String notes
) {}
