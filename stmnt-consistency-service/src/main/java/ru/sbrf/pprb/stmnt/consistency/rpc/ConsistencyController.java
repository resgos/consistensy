package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.api.dto.*;
import ru.sbrf.pprb.stmnt.consistency.lib.ConsistencyJob;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.ConsistencyRunRepository;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.MismatchRepository;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/consistency")
@RequiredArgsConstructor
public class ConsistencyController {

    private final ConsistencyJob job;
    private final ConsistencyRunRepository runRepo;
    private final MismatchRepository mismatchRepo;

    @PostMapping("/run")
    public RunResponseDto run(@RequestBody(required = false) RunRequestDto body) {
        String cacheName = body != null ? body.cacheName() : null;
        long id = job.runAll(cacheName);
        var run = runRepo.findById(id);
        return new RunResponseDto(id, run.map(ConsistencyRunDto::status).orElse("UNKNOWN"));
    }

    @GetMapping("/runs")
    public List<ConsistencyRunDto> runs(@RequestParam(defaultValue = "20") int limit,
                                         @RequestParam(required = false) String status) {
        return runRepo.findRecent(limit, status);
    }

    @GetMapping("/runs/{id}")
    public ResponseEntity<ConsistencyRunDto> runById(@PathVariable long id) {
        return runRepo.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/mismatches")
    public List<MismatchDto> mismatchesList(@RequestParam(required = false) String cacheName,
                                             @RequestParam(required = false) String since,
                                             @RequestParam(defaultValue = "false") boolean unresolvedOnly,
                                             @RequestParam(defaultValue = "100") int limit) {
        Instant s = (since == null || since.isBlank()) ? null : Instant.parse(since);
        return mismatchRepo.find(cacheName, s, unresolvedOnly, limit);
    }

    @PostMapping("/mismatches/{id}/resolve")
    public Map<String, Object> resolve(@PathVariable long id,
                                        @RequestBody(required = false) Map<String, String> body) {
        String notes = body != null ? body.get("notes") : null;
        return Map.of("updated", mismatchRepo.resolve(id, notes));
    }
}
