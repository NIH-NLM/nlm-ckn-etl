package gov.nih.nlm;

import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OntologyGraphBuilderTest {

    // Assign location of test ontology files
    private static final Path USR_DIR = Paths.get(System.getProperty("user.dir"));
    private static final Path OBO_DIR = USR_DIR.resolve("src/test/data/obo");

    // --- createVTuple tests (no ArangoDB needed) ---

    @Test
    void createVTuple_validCLTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/CL_0000235");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("CL_0000235", vtuple.term());
        assertEquals("CL", vtuple.id());
        assertEquals("0000235", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validGOTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/GO_0031268");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("GO_0031268", vtuple.term());
        assertEquals("GO", vtuple.id());
        assertEquals("0031268", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validUBERONTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/UBERON_0000061");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("UBERON_0000061", vtuple.term());
        assertEquals("UBERON", vtuple.id());
        assertEquals("0000061", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_invalidPrefix() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/BFO_0000002");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("BFO_0000002", vtuple.term());
        assertEquals("BFO", vtuple.id());
        assertEquals("0000002", vtuple.number());
        assertFalse(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_nonUriNode() {
        var node = NodeFactory.createLiteralString("not a URI");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertNull(vtuple.term());
        assertNull(vtuple.id());
        assertNull(vtuple.number());
        assertFalse(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_uriWithFragment() {
        var node = NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        // "subClassOf" has no underscore or colon separator, so tokens will be null
        assertNull(vtuple.term());
        assertFalse(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_ncbiTaxon() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/NCBITaxon_9606");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("NCBITaxon_9606", vtuple.term());
        assertEquals("NCBITaxon", vtuple.id());
        assertEquals("9606", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_nonHumanTaxonIsNotValidVertex() {
        // The graph is restricted to the permitted taxa, so a non-human taxon (e.g. Proboscidea/elephants,
        // referenced by an Uberon in_taxon constraint) must not be a valid vertex.
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/NCBITaxon_9779");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("NCBITaxon_9779", vtuple.term());
        assertEquals("NCBITaxon", vtuple.id());
        assertEquals("9779", vtuple.number());
        assertFalse(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validHPTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/HP_0000001");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("HP_0000001", vtuple.term());
        assertEquals("HP", vtuple.id());
        assertEquals("0000001", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validMONDOTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/MONDO_0000001");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("MONDO_0000001", vtuple.term());
        assertEquals("MONDO", vtuple.id());
        assertEquals("0000001", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validCellSetDatasetSourceKey() {
        // A single-organ dataset keeps the bare source dataset_version_id (one
        // underscore after the CSD prefix; the id itself carries hyphens).
        var node = NodeFactory.createURI(
                "http://purl.obolibrary.org/obo/CSD_2b1f9ac3-1234-5678-9abc-def012345678");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("CSD", vtuple.id());
        assertEquals("2b1f9ac3-1234-5678-9abc-def012345678", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validCellSetDatasetCompositeKey() {
        // A dataset filtered for an organ is keyed "<dvid>__<organ>"; the "__"
        // plus the organ must remain part of the number (the Arango _key), not
        // split the term into more than two tokens (Springbok-LLC/nlm-ckn-etl#55
        // regression: organ-keyed CellSetDatasets were silently dropped).
        var node = NodeFactory.createURI(
                "http://purl.obolibrary.org/obo/CSD_2b1f9ac3-1234-5678-9abc-def012345678__respiratory_system");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("CSD", vtuple.id());
        assertEquals("2b1f9ac3-1234-5678-9abc-def012345678__respiratory_system", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_delimiterWithEmptyNumberIsInvalid() {
        // A trailing delimiter with no local identifier is not a vertex.
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/CSD_");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertFalse(vtuple.isValidVertex());
    }

    // --- taxon-constraint predicate tests (no ArangoDB needed) ---

    @Test
    void isTaxonConstraintPredicate_excludesTaxonConstraints() {
        assertTrue(OntologyGraphBuilder.isTaxonConstraintPredicate("RO_0002160"), "only_in_taxon");
        assertTrue(OntologyGraphBuilder.isTaxonConstraintPredicate("RO_0002161"), "never_in_taxon");
        assertTrue(OntologyGraphBuilder.isTaxonConstraintPredicate("RO_0002162"), "in_taxon");
    }

    @Test
    void isTaxonConstraintPredicate_allowsOtherRelations() {
        assertFalse(OntologyGraphBuilder.isTaxonConstraintPredicate("RO_0002175"), "present_in_taxon is kept");
        assertFalse(OntologyGraphBuilder.isTaxonConstraintPredicate("subClassOf"), "subClassOf is kept");
        assertFalse(OntologyGraphBuilder.isTaxonConstraintPredicate("RO_0002202"), "develops_from is kept");
    }

    // --- parsePredicate tests (no ArangoDB needed) ---

    @Test
    void parsePredicate_fragmentUri() {
        // A URI with a fragment should return the fragment
        var node = NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf");
        Map<String, OntologyElementMap> maps = new HashMap<>();
        maps.put("ro", new OntologyElementMap());

        String label = OntologyGraphBuilder.parsePredicate(maps, node).label();
        assertEquals("subClassOf", label);
    }

    @Test
    void parsePredicate_oboTermWithDevelopsFrom() {
        // A URI without fragment, where the term is in the ro map
        List<Path> roFile = List.of(OBO_DIR.resolve("ro.owl"));
        Map<String, OntologyElementMap> maps = OntologyElementParser.parseOntologyElements(roFile);

        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/RO_0002202");
        String label = OntologyGraphBuilder.parsePredicate(maps, node).label();
        assertEquals("develops from", label);
    }

    @Test
    void parsePredicate_oboTermWithCapableOf() {
        List<Path> roFile = List.of(OBO_DIR.resolve("ro.owl"));
        Map<String, OntologyElementMap> maps = OntologyElementParser.parseOntologyElements(roFile);

        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/RO_0002215");
        String label = OntologyGraphBuilder.parsePredicate(maps, node).label();
        assertEquals("capable of", label);
    }

    @Test
    void parsePredicate_nonUriThrows() {
        var node = NodeFactory.createLiteralString("not a URI");
        Map<String, OntologyElementMap> maps = new HashMap<>();
        maps.put("ro", new OntologyElementMap());

        assertThrows(RuntimeException.class, () -> OntologyGraphBuilder.parsePredicate(maps, node));
    }

    // --- normalizeEdgeSource tests ---

    @Test
    void normalizeEdgeSource_mondoSimple() {
        assertEquals("MONDO", OntologyGraphBuilder.normalizeEdgeSource("mondo-simple"));
    }

    @Test
    void normalizeEdgeSource_taxslim() {
        assertEquals("NCBITAXON", OntologyGraphBuilder.normalizeEdgeSource("taxslim"));
    }

    @Test
    void normalizeEdgeSource_goPlus() {
        assertEquals("GO", OntologyGraphBuilder.normalizeEdgeSource("go-plus"));
    }

    @Test
    void normalizeEdgeSource_uberonBase() {
        assertEquals("UBERON", OntologyGraphBuilder.normalizeEdgeSource("uberon-base"));
    }

    @Test
    void normalizeEdgeSource_defaultUpperCase() {
        assertEquals("CL", OntologyGraphBuilder.normalizeEdgeSource("cl"));
        assertEquals("HP", OntologyGraphBuilder.normalizeEdgeSource("hp"));
        assertEquals("PATO", OntologyGraphBuilder.normalizeEdgeSource("pato"));
    }

    // --- normalizeEdgeLabel tests ---

    @Test
    void normalizeEdgeLabel_subClassOf() {
        assertEquals("SUB_CLASS_OF", OntologyGraphBuilder.normalizeEdgeLabel("subClassOf"));
    }

    @Test
    void normalizeEdgeLabel_disjointWith() {
        assertEquals("DISJOINT_WITH", OntologyGraphBuilder.normalizeEdgeLabel("disjointWith"));
    }

    @Test
    void normalizeEdgeLabel_selectivelyExpresses() {
        // The UI filters marker gene edges on this label, and its counterpart, EXPRESSES.
        assertEquals("SELECTIVELY_EXPRESSES", OntologyGraphBuilder.normalizeEdgeLabel("selectively expresses"));
        assertEquals("EXPRESSES", OntologyGraphBuilder.normalizeEdgeLabel("expresses"));
    }

    @Test
    void normalizeEdgeLabel_isAbout() {
        assertEquals("IS_ABOUT", OntologyGraphBuilder.normalizeEdgeLabel("is about"));
    }

    @Test
    void normalizeEdgeLabel_crossSpeciesExactMatch() {
        assertEquals("CROSS_SPECIES_EXACT_MATCH", OntologyGraphBuilder.normalizeEdgeLabel("crossSpeciesExactMatch"));
    }

    @Test
    void normalizeEdgeLabel_exactMatch() {
        assertEquals("EXACT_MATCH", OntologyGraphBuilder.normalizeEdgeLabel("exactMatch"));
    }

    @Test
    void normalizeEdgeLabel_equivalentClass() {
        assertEquals("EQUIVALENT_CLASS", OntologyGraphBuilder.normalizeEdgeLabel("equivalentClass"));
    }

    @Test
    void normalizeEdgeLabel_seeAlso() {
        assertEquals("SEE_ALSO", OntologyGraphBuilder.normalizeEdgeLabel("seeAlso"));
    }

    @Test
    void normalizeEdgeLabel_defaultWithSpaces() {
        assertEquals("DEVELOPS_FROM", OntologyGraphBuilder.normalizeEdgeLabel("develops from"));
        assertEquals("CAPABLE_OF", OntologyGraphBuilder.normalizeEdgeLabel("capable of"));
        assertEquals("PART_OF", OntologyGraphBuilder.normalizeEdgeLabel("part of"));
    }

    @Test
    void normalizeEdgeLabel_defaultUpperCase() {
        assertEquals("LABEL", OntologyGraphBuilder.normalizeEdgeLabel("label"));
    }

    // --- getDocumentCollectionName tests ---

    @Test
    void getDocumentCollectionName_vertexId() {
        assertEquals("CL", OntologyGraphBuilder.getDocumentCollectionName("CL/0000235"));
    }

    @Test
    void getDocumentCollectionName_edgeId() {
        assertEquals("CL-GO", OntologyGraphBuilder.getDocumentCollectionName("CL-GO/0000235-0031268"));
    }

    @Test
    void getDocumentCollectionName_nullInput() {
        assertNull(OntologyGraphBuilder.getDocumentCollectionName(null));
    }

    @Test
    void getDocumentCollectionName_noSlash() {
        assertNull(OntologyGraphBuilder.getDocumentCollectionName("CL0000235"));
    }

    // --- getDocumentKey tests ---

    @Test
    void getDocumentKey_vertexId() {
        assertEquals("0000235", OntologyGraphBuilder.getDocumentKey("CL/0000235"));
    }

    @Test
    void getDocumentKey_edgeId() {
        assertEquals("0000235-0031268", OntologyGraphBuilder.getDocumentKey("CL-GO/0000235-0031268"));
    }

    @Test
    void getDocumentKey_nullInput() {
        assertNull(OntologyGraphBuilder.getDocumentKey(null));
    }

    @Test
    void getDocumentKey_noSlash() {
        assertNull(OntologyGraphBuilder.getDocumentKey("CL0000235"));
    }

}
