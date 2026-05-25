package ru.sbrf.pprb.stmnt.consistency.api.dto;

import java.math.BigDecimal;

/**
 * Body for POST /api/admin/init.
 * clusterIds == null  → run on all configured clusters.
 */
public record InitRequestDto(
        String registerId,
        String fromDate,           // ISO YYYY-MM-DD
        String toDate,             // ISO YYYY-MM-DD
        BigDecimal openingBalance,
        BigDecimal openingBalanceNat,
        java.util.List<String> clusterIds   // optional filter; null = all
) {}
