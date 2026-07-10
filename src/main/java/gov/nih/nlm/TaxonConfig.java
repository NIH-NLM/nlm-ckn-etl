package gov.nih.nlm;

import java.util.Set;

/**
 * Single source of truth for the taxa the Cell-KN graph is restricted to.
 *
 * <p>The graph is intended to describe a fixed set of organisms (currently only human, NCBITaxon:9606). Both the
 * {@link OntologySlimmer} (which decides, per ontology, which classes to keep) and the {@link OntologyGraphBuilder}
 * (which decides which taxon vertices and taxon-constraint relations to load) derive their behavior from
 * {@link #PERMITTED_TAXA}, so that permitting additional taxa is a single data change here rather than a code change in
 * multiple places.
 *
 * <p>To permit an additional taxon (e.g. mouse), add its NCBITaxon numeric id to {@link #PERMITTED_TAXA}:
 * {@code Set.of("9606", "10090")}. The slimmer keeps a class relevant to <em>any</em> permitted taxon, and the graph
 * builder then loads a vertex for each permitted taxon; no other change is required.
 */
public final class TaxonConfig {

    /**
     * The NCBITaxon numeric ids the graph is restricted to. A class is retained by the slimmer when it is relevant to at
     * least one of these taxa, and the graph builder loads exactly one taxon vertex per id.
     */
    public static final Set<String> PERMITTED_TAXA = Set.of("9606"); // human; add "10090" for mouse, etc.

    private TaxonConfig() {
    }
}
