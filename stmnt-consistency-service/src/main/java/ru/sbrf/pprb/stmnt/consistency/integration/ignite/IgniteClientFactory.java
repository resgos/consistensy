package ru.sbrf.pprb.stmnt.consistency.integration.ignite;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Owns one IgniteClient per configured cluster.
 * Clients are reusable across many SQL queries and thread-safe.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IgniteClientFactory implements InitializingBean {

    private final ConsistencyProperties props;

    private final Map<String, IgniteClient> clients = new LinkedHashMap<>();

    @Override
    public void afterPropertiesSet() {
        for (ConsistencyProperties.Cluster c : props.getClusters()) {
            try {
                ClientConfiguration cfg = new ClientConfiguration()
                        .setAddresses(c.getAddresses().toArray(new String[0]))
                        .setTimeout(30_000)
                        .setSendBufferSize(64 * 1024)
                        .setReceiveBufferSize(64 * 1024);
                IgniteClient client = Ignition.startClient(cfg);
                clients.put(c.getId(), client);
                log.info("Ignite thin client connected: cluster={} addresses={}",
                        c.getId(), c.getAddresses());
            } catch (ClientException e) {
                log.error("Failed to connect cluster={} addresses={} — will skip in runs",
                        c.getId(), c.getAddresses(), e);
            }
        }
    }

    /** Get client by cluster id. Returns null if cluster was unreachable at startup. */
    public IgniteClient get(String clusterId) {
        return clients.get(clusterId);
    }

    public Map<String, IgniteClient> all() {
        return clients;
    }

    @PreDestroy
    public void shutdown() {
        clients.forEach((id, c) -> {
            try { c.close(); } catch (Exception e) {
                log.warn("Error closing client cluster={}: {}", id, e.toString());
            }
        });
    }
}
