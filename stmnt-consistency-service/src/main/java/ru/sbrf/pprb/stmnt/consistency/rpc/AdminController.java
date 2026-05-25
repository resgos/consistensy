package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.api.dto.AdminFanOutResultDto;
import ru.sbrf.pprb.stmnt.consistency.api.dto.CleanupRequestDto;
import ru.sbrf.pprb.stmnt.consistency.api.dto.InitRequestDto;
import ru.sbrf.pprb.stmnt.consistency.lib.AdminFanOut;

import java.util.Map;

/**
 * Fan-out admin operations to all configured Ignite clusters at once.
 *
 * Usage:
 *   curl -X POST http://localhost:8080/api/admin/init -H 'Content-Type: application/json' \
 *        -d '{"registerId":"R001","fromDate":"2026-01-01","toDate":"2026-05-24",
 *             "openingBalance":1000.00}'
 *
 *   curl -X POST http://localhost:8080/api/admin/cleanup -H 'Content-Type: application/json' \
 *        -d '{"beforeDate":"2025-11-25"}'
 */
@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
public class AdminController {

    private final AdminFanOut fanOut;

    @PostMapping("/init")
    public AdminFanOutResultDto init(@RequestBody InitRequestDto req) {
        Map<String, Map<String, Object>> per = fanOut.init(
                req.registerId(), req.fromDate(), req.toDate(),
                req.openingBalance(), req.openingBalanceNat(),
                req.clusterIds());
        return new AdminFanOutResultDto("init", per);
    }

    @PostMapping("/cleanup")
    public AdminFanOutResultDto cleanup(@RequestBody CleanupRequestDto req) {
        Map<String, Map<String, Object>> per = fanOut.cleanup(
                req.beforeDate(), req.registerFilter(), req.clusterIds());
        return new AdminFanOutResultDto("cleanup", per);
    }

    @PostMapping("/recalc")
    public AdminFanOutResultDto recalc(@RequestBody InitRequestDto req) {
        // reuse InitRequestDto since same fields (registerId, fromDate, toDate, clusterIds)
        Map<String, Map<String, Object>> per = fanOut.recalc(
                req.registerId(), req.fromDate(), req.toDate(), req.clusterIds());
        return new AdminFanOutResultDto("recalc", per);
    }
}
