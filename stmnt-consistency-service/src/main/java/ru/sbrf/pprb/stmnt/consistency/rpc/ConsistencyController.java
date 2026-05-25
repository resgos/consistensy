package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.api.dto.*;
import ru.sbrf.pprb.stmnt.consistency.lib.ConsistencyJob;
import ru.sbrf.pprb.stmnt.consistency.lib.MismatchRepository;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/consistency")
@RequiredArgsConstructor
public class ConsistencyController {

    private final ConsistencyJob job;
    private final MismatchRepository repo;

    /** Start an ad-hoc run synchronously and return its id. */
    @PostMapping("/run")
    public RunResponseDto run(@RequestBody(required = false) RunRequestDto body) {
        String cacheName = body != null ? body.cacheName() : null;
        long id = job.runAll(cacheName);
        var run = repo.getRun(id);
        return new RunResponseDto(id, run != null ? run.status() : "UNKNOWN");
    }

    @GetMapping("/runs")
    public List<ConsistencyRunDto> runs(@RequestParam(defaultValue = "20") int limit,
                                         @RequestParam(required = false) String status) {
        return repo.listRuns(limit, status);
    }

    @GetMapping("/runs/{id}")
    public ResponseEntity<ConsistencyRunDto> runById(@PathVariable long id) {
        ConsistencyRunDto dto = repo.getRun(id);
        return dto == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(dto);
    }

    @GetMapping("/mismatches")
    public List<MismatchDto> mismatches(@RequestParam(required = false) String cacheName,
                                         @RequestParam(required = false) String since,
                                         @RequestParam(defaultValue = "false") boolean unresolvedOnly,
                                         @RequestParam(defaultValue = "100") int limit) {
        Instant sinceInstant = (since == null || since.isBlank()) ? null : Instant.parse(since);
        return repo.listMismatches(cacheName, sinceInstant, unresolvedOnly, limit);
    }

    @PostMapping("/mismatches/{id}/resolve")
    public Map<String, Object> resolve(@PathVariable long id,
                                        @RequestBody(required = false) Map<String, String> body) {
        String notes = body != null ? body.get("notes") : null;
        int updated = repo.resolveMismatch(id, notes);
        return Map.of("updated", updated);
    }
}
