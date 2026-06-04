package gov.nih.nlm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OntologySlimmerTest {

    private static final Path testOboDir = Paths.get(System.getProperty("user.dir"),
            "src/test/data/obo");

    @Test
    void slimOntology_keepsHumanClasses(@TempDir Path tempDir) throws IOException, XMLStreamException {
        Path input = testOboDir.resolve("pr-test.owl");
        Path output = tempDir.resolve("pr-slim.owl");

        int count = OntologySlimmer.slimOntology(input, output, "9606");

        assertEquals(2, count);
    }

    @Test
    void slimOntology_outputContainsHumanClass(@TempDir Path tempDir) throws IOException, XMLStreamException {
        Path input = testOboDir.resolve("pr-test.owl");
        Path output = tempDir.resolve("pr-slim.owl");

        OntologySlimmer.slimOntology(input, output, "9606");

        String content = Files.readString(output);
        assertTrue(content.contains("PR_000000001"), "Should contain human class PR_000000001");
        assertTrue(content.contains("PR_000000004"), "Should contain human class PR_000000004");
    }

    @Test
    void slimOntology_excludesNonHumanClasses(@TempDir Path tempDir) throws IOException, XMLStreamException {
        Path input = testOboDir.resolve("pr-test.owl");
        Path output = tempDir.resolve("pr-slim.owl");

        OntologySlimmer.slimOntology(input, output, "9606");

        String content = Files.readString(output);
        assertFalse(content.contains("PR_000000002"), "Should not contain mouse class PR_000000002");
        assertFalse(content.contains("PR_000000003"), "Should not contain species-neutral class PR_000000003");
    }

    @Test
    void slimOntology_dropsAxioms(@TempDir Path tempDir) throws IOException, XMLStreamException {
        Path input = testOboDir.resolve("pr-test.owl");
        Path output = tempDir.resolve("pr-slim.owl");

        OntologySlimmer.slimOntology(input, output, "9606");

        String content = Files.readString(output);
        assertFalse(content.contains("owl:Axiom"), "Should not contain any owl:Axiom elements");
    }

    @Test
    void slimOntology_retainsHeader(@TempDir Path tempDir) throws IOException, XMLStreamException {
        Path input = testOboDir.resolve("pr-test.owl");
        Path output = tempDir.resolve("pr-slim.owl");

        OntologySlimmer.slimOntology(input, output, "9606");

        String content = Files.readString(output);
        assertTrue(content.contains("owl:Ontology"), "Should contain ontology header");
        assertTrue(content.contains("AnnotationProperty"), "Should contain annotation properties");
        assertTrue(content.contains("ObjectProperty"), "Should contain object properties");
    }

    @Test
    void slimOntology_outputIsValidXml(@TempDir Path tempDir) throws IOException, XMLStreamException {
        Path input = testOboDir.resolve("pr-test.owl");
        Path output = tempDir.resolve("pr-slim.owl");

        OntologySlimmer.slimOntology(input, output, "9606");

        String content = Files.readString(output);
        assertTrue(content.contains("<?xml"), "Should have XML declaration");
        assertTrue(content.contains("</rdf:RDF>"), "Should have closing rdf:RDF tag");
    }

    @Test
    void slimOntology_differentTaxon(@TempDir Path tempDir) throws IOException, XMLStreamException {
        Path input = testOboDir.resolve("pr-test.owl");
        Path output = tempDir.resolve("pr-slim.owl");

        int count = OntologySlimmer.slimOntology(input, output, "10090");

        assertEquals(1, count);
        String content = Files.readString(output);
        assertTrue(content.contains("PR_000000002"), "Should contain mouse class");
        assertFalse(content.contains("PR_000000001"), "Should not contain human class");
    }

    @Test
    void buildTaxonLineage_includesAncestorsOfHuman() throws IOException, XMLStreamException {
        Path taxslim = testOboDir.resolve("taxslim-test.owl");

        Set<String> lineage = OntologySlimmer.buildTaxonLineage(taxslim, "9606");

        assertTrue(lineage.contains("http://purl.obolibrary.org/obo/NCBITaxon_9606"), "Should contain the taxon itself");
        assertTrue(lineage.contains("http://purl.obolibrary.org/obo/NCBITaxon_40674"), "Should contain Mammalia");
        assertTrue(lineage.contains("http://purl.obolibrary.org/obo/NCBITaxon_7711"), "Should contain Chordata");
        assertTrue(lineage.contains("http://purl.obolibrary.org/obo/NCBITaxon_33208"), "Should contain Metazoa");
        assertTrue(lineage.contains("http://purl.obolibrary.org/obo/NCBITaxon_1"), "Should contain root");
    }

    @Test
    void buildTaxonLineage_excludesOffLineageTaxa() throws IOException, XMLStreamException {
        Path taxslim = testOboDir.resolve("taxslim-test.owl");

        Set<String> lineage = OntologySlimmer.buildTaxonLineage(taxslim, "9606");

        assertFalse(lineage.contains("http://purl.obolibrary.org/obo/NCBITaxon_8782"), "Should not contain Aves");
        assertFalse(lineage.contains("http://purl.obolibrary.org/obo/NCBITaxon_7147"), "Should not contain Insecta");
    }

    @Test
    void slimByTaxonExclusion_keepsUnconstrainedAndNonHumanExcludingClasses(@TempDir Path tempDir)
            throws IOException, XMLStreamException {
        Path taxslim = testOboDir.resolve("taxslim-test.owl");
        Path input = testOboDir.resolve("uberon-test.owl");
        Path output = tempDir.resolve("uberon-human.owl");
        Set<String> lineage = OntologySlimmer.buildTaxonLineage(taxslim, "9606");

        int count = OntologySlimmer.slimByTaxonExclusion(input, output, lineage);

        // heart (no constraint) and mammary gland (never_in_taxon Aves) are kept
        assertEquals(2, count);
        String content = Files.readString(output);
        assertTrue(content.contains("UBERON_0000004"), "Should keep pan-taxonomic heart");
        assertTrue(content.contains("UBERON_0000002"), "Should keep mammary gland (never_in_taxon Aves)");
    }

    @Test
    void slimByTaxonExclusion_dropsHumanExcludingClasses(@TempDir Path tempDir)
            throws IOException, XMLStreamException {
        Path taxslim = testOboDir.resolve("taxslim-test.owl");
        Path input = testOboDir.resolve("uberon-test.owl");
        Path output = tempDir.resolve("uberon-human.owl");
        Set<String> lineage = OntologySlimmer.buildTaxonLineage(taxslim, "9606");

        OntologySlimmer.slimByTaxonExclusion(input, output, lineage);

        String content = Files.readString(output);
        assertFalse(content.contains("UBERON_0000001"), "Should drop feather (never_in_taxon Mammalia, annotation form)");
        assertFalse(content.contains("UBERON_0000003"), "Should drop insect wing (only_in_taxon Insecta, restriction form)");
        assertFalse(content.contains("UBERON_0000005"), "Should drop swim bladder (never_in_taxon Mammalia, restriction form)");
    }

    @Test
    void slimByTaxonExclusion_dropsAxioms(@TempDir Path tempDir) throws IOException, XMLStreamException {
        Path taxslim = testOboDir.resolve("taxslim-test.owl");
        Path input = testOboDir.resolve("uberon-test.owl");
        Path output = tempDir.resolve("uberon-human.owl");
        Set<String> lineage = OntologySlimmer.buildTaxonLineage(taxslim, "9606");

        OntologySlimmer.slimByTaxonExclusion(input, output, lineage);

        String content = Files.readString(output);
        assertFalse(content.contains("owl:Axiom"), "Should not contain any owl:Axiom elements");
    }
}
