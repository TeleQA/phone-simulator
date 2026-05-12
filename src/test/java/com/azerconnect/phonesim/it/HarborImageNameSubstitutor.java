package com.azerconnect.phonesim.it;

import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

/**
 * Redirects Testcontainers-namespaced image pulls (ryuk and friends) to the
 * internal Harbor mirror at {@code harbor.azerconnect.az/infra/testcontainers/}.
 * Images outside the {@code testcontainers/} namespace pass through unchanged
 * — callers are expected to reference those by their explicit Harbor path
 * (e.g. {@code harbor.azerconnect.az/infra/redis:7-alpine}).
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code testcontainers/ryuk:0.9.0} → {@code harbor.azerconnect.az/infra/testcontainers/ryuk:0.9.0}</li>
 *   <li>{@code redis:7-alpine} → unchanged</li>
 *   <li>{@code harbor.azerconnect.az/infra/redis:7-alpine} → unchanged</li>
 * </ul>
 *
 * <p>Registered via SPI in
 * {@code META-INF/services/org.testcontainers.utility.ImageNameSubstitutor}.
 */
public class HarborImageNameSubstitutor extends ImageNameSubstitutor {

    private static final String HARBOR_REGISTRY = "harbor.azerconnect.az";
    private static final String HARBOR_PATH_PREFIX = "infra/testcontainers/";
    private static final String TESTCONTAINERS_NAMESPACE = "testcontainers/";

    @Override
    public DockerImageName apply(DockerImageName original) {
        if (HARBOR_REGISTRY.equals(original.getRegistry())) {
            return original;
        }
        String repository = original.getRepository();
        if (repository == null || !repository.startsWith(TESTCONTAINERS_NAMESPACE)) {
            return original;
        }
        String basename = repository.substring(TESTCONTAINERS_NAMESPACE.length());
        String tag = original.getVersionPart();
        String replacement = HARBOR_REGISTRY + "/" + HARBOR_PATH_PREFIX + basename
                + (tag == null || tag.isEmpty() ? "" : ":" + tag);
        return DockerImageName.parse(replacement).asCompatibleSubstituteFor(original);
    }

    @Override
    protected String getDescription() {
        return "Azerconnect Harbor (infra/testcontainers) substitutor";
    }
}
