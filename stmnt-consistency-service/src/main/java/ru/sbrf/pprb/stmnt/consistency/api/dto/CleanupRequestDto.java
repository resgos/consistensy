package ru.sbrf.pprb.stmnt.consistency.api.dto;

public record CleanupRequestDto(
        String beforeDate,           // ISO YYYY-MM-DD
        String registerFilter,       // nullable
        java.util.List<String> clusterIds   // optional cluster filter; null = all
) {}
