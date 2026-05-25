package ru.sbrf.pprb.stmnt.consistency.api.dto;

import java.time.Instant;

public record ConsistencyRunDto(
        long id,
        Instant startedAt,
        Instant finishedAt,
        String cacheName,
        String status,
        int mismatchCount,
        String errorMessage
) {}
