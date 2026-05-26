package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.api.dto.ErrorEntryDto;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.ErrorRepository;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/errors")
@RequiredArgsConstructor
public class ErrorController {

    private final ErrorRepository errorRepo;

    @GetMapping
    public List<ErrorEntryDto> list(@RequestParam(required = false) String source,
                                     @RequestParam(required = false) String level,
                                     @RequestParam(required = false) String code,
                                     @RequestParam(required = false) String clusterId,
                                     @RequestParam(required = false) String cacheName,
                                     @RequestParam(required = false) String since,
                                     @RequestParam(defaultValue = "false") boolean unresolvedOnly,
                                     @RequestParam(defaultValue = "100") int limit) {
        Instant s = (since == null || since.isBlank()) ? null : Instant.parse(since);
        return errorRepo.find(
                new ErrorRepository.Filter(source, level, code, clusterId, cacheName, s, unresolvedOnly),
                limit);
    }

    @GetMapping("/stats")
    public Map<String, ?> stats(@RequestParam(required = false) String since) {
        Instant s = (since == null || since.isBlank()) ? null : Instant.parse(since);
        return errorRepo.aggregateStats(s);
    }

    @PostMapping("/{id}/resolve")
    public Map<String, Object> resolve(@PathVariable long id,
                                        @RequestBody(required = false) Map<String, String> body) {
        String notes = body != null ? body.get("notes") : null;
        return Map.of("updated", errorRepo.resolve(id, notes));
    }
}
