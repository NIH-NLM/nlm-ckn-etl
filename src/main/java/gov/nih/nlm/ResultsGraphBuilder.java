package gov.nih.nlm;

import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoEdgeCollection;
import com.arangodb.ArangoGraph;
import com.arangodb.ArangoVertexCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static gov.nih.nlm.OntologyElementParser.parseOntologyElements;
import static gov.nih.nlm.OntologyGraphBuilder.createVTuple;
import static gov.nih.nlm.OntologyGraphBuilder.insertEdges;
import static gov.nih.nlm.OntologyGraphBuilder.insertVertices;
import static gov.nih.nlm.OntologyGraphBuilder.normalizeEdgeLabel;
import static gov.nih.nlm.OntologyGraphBuilder.parsePredicate;
import static gov.nih.nlm.PathUtilities.OBO_DIR;
import static gov.nih.nlm.PathUtilities.USR_DIR;
import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

/**
 * Loads tuples parsed from results JSON files into a local ArangoDB server instance.
 */
public class ResultsGraphBuilder {

    // Assign location of tuples and schema files
    public static final Path TUPLES_DIR = USR_DIR.resolve("data/tuples");

    // Connect to a local ArangoDB server instance
    private static final ArangoDbUtilities arangoDbUtilities = new ArangoDbUtilities();

    // Assign triple indices
    private static final int TRIPLE_SUBJECT_IDX = 0;
    private static final int TRIPLE_PREDICATE_IDX = 1;
    private static final int TRIPLE_OBJECT_IDX = 2;

    // Assign quadruple indices
    private static final int QUADRUPLE_SUBJECT_IDX = 0;
    private static final int QUADRUPLE_OBJECT_IDX = 1;
    private static final int QUADRUPLE_PREDICATE_IDX = 2;
    private static final int QUADRUPLE_LITERAL_IDX = 3;

    /**
     * Read a JSON file containing tuples, parsing each element as a URI or literal node, and validating the resulting
     * triples and quadruples.
     *
     * @param jsonFilePath Path to the JSON file containing tuples
     * @return List of tuples, each represented as a list of nodes
     * @throws IOException if the file cannot be read, contains invalid JSON, or contains invalid tuples
     */
    public static ArrayList<ArrayList<Node>> readJsonFile(String jsonFilePath) throws IOException {
        ArrayList<ArrayList<Node>> tuplesArrayList = new ArrayList<>();
        try (JsonParser parser = new JsonFactory().createParser(new FileInputStream(jsonFilePath))) {
            advanceToTuplesArray(parser, jsonFilePath);
            while (parser.nextToken() == JsonToken.START_ARRAY) {
                ArrayList<Node> tuple = parseTupleFromStream(parser);
                validateTuple(tuple);
                tuplesArrayList.add(tuple);
            }
        }
        return tuplesArrayList;
    }

    /**
     * Advance a JsonParser to the start of the "tuples" array in the JSON object.
     *
     * @param parser            JsonParser positioned at the start of the file
     * @param sourceDescription Description of the source (file path) for error messages
     * @throws IOException if the JSON is malformed or the "tuples" field is missing
     */
    private static void advanceToTuplesArray(JsonParser parser, String sourceDescription) throws IOException {
        if (parser.nextToken() != JsonToken.START_OBJECT) {
            throw new IOException("Error parsing JSON in file: " + sourceDescription);
        }
        while (parser.nextToken() != null) {
            if (parser.currentToken() == JsonToken.FIELD_NAME && "tuples".equals(parser.getCurrentName())) {
                if (parser.nextToken() != JsonToken.START_ARRAY) {
                    throw new IOException("Error parsing JSON in file: " + sourceDescription);
                }
                return;
            }
        }
        throw new IOException("Error parsing JSON in file: " + sourceDescription);
    }

    /**
     * Parse one tuple (inner array) from a JsonParser positioned at START_ARRAY.
     *
     * @param parser JsonParser positioned at START_ARRAY of the tuple
     * @return List of nodes parsed from the tuple elements
     * @throws IOException if the JSON is malformed
     */
    private static ArrayList<Node> parseTupleFromStream(JsonParser parser) throws IOException {
        ArrayList<Node> tuple = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            String value = parser.getText();
            tuple.add(value.contains("http") ? NodeFactory.createURI(value) : NodeFactory.createLiteral(value));
        }
        return tuple;
    }

    /**
     * Validate a tuple, throwing if it is not a valid triple or quadruple.
     *
     * @param tuple List of nodes to validate
     * @throws IOException if the tuple is an invalid triple or quadruple
     */
    private static void validateTuple(ArrayList<Node> tuple) throws IOException {
        if (tuple.size() == 3 && !(tuple.get(TRIPLE_SUBJECT_IDX).isURI() && tuple.get(TRIPLE_PREDICATE_IDX).isURI() && (tuple.get(
                TRIPLE_OBJECT_IDX).isURI() || tuple.get(TRIPLE_OBJECT_IDX).isLiteral()))) {
            throw new IOException("Invalid triple " + tuple);
        }
        if (tuple.size() == 4 && !(tuple.get(QUADRUPLE_SUBJECT_IDX).isURI() && tuple.get(QUADRUPLE_OBJECT_IDX).isURI() && tuple.get(
                QUADRUPLE_PREDICATE_IDX).isURI() && tuple.get(QUADRUPLE_LITERAL_IDX).isLiteral())) {
            throw new IOException("Invalid quadruple " + tuple);
        }
    }

    /**
     * Create a vertex collection and document for the given VTuple if they do not already exist.
     *
     * @param vtuple            VTuple identifying the vertex
     * @param graph             ArangoDB graph
     * @param vertexKeys        Set of existing vertex keys per collection, for deduplication
     * @param vertexCollections ArangoDB vertex collections
     * @param vertexDocuments   ArangoDB vertex documents
     * @return true if a new vertex was created, false if it already existed
     */
    private static boolean createVertexIfAbsent(OntologyGraphBuilder.VTuple vtuple,
                                                ArangoGraph graph,
                                                Map<String, Set<String>> vertexKeys,
                                                Map<String, ArangoVertexCollection> vertexCollections,
                                                Map<String, Map<String, BaseDocument>> vertexDocuments) {
        if (!vertexCollections.containsKey(vtuple.id())) {
            vertexCollections.put(vtuple.id(), arangoDbUtilities.createOrGetVertexCollection(graph, vtuple.id()));
            vertexDocuments.put(vtuple.id(), new HashMap<>());
            vertexKeys.put(vtuple.id(), new HashSet<>());
        }
        if (!vertexKeys.get(vtuple.id()).contains(vtuple.number())) {
            vertexDocuments.get(vtuple.id()).put(vtuple.number(), new BaseDocument(vtuple.number()));
            vertexKeys.get(vtuple.id()).add(vtuple.number());
            return true;
        }
        return false;
    }

    /**
     * Create an edge collection and document for the given subject and object VTuples if they do not already exist. The
     * Label attribute is not set here; callers are responsible for setting it.
     *
     * @param subjectVTuple   VTuple identifying the edge source vertex
     * @param objectVTuple    VTuple identifying the edge target vertex
     * @param graph           ArangoDB graph
     * @param edgeKeys        Set of existing edge keys per collection, for deduplication
     * @param edgeCollections ArangoDB edge collections
     * @param edgeDocuments   ArangoDB edge documents
     * @return true if a new edge was created, false if it already existed
     */
    private static boolean createEdgeIfAbsent(OntologyGraphBuilder.VTuple subjectVTuple,
                                              OntologyGraphBuilder.VTuple objectVTuple,
                                              ArangoGraph graph,
                                              Map<String, Set<String>> edgeKeys,
                                              Map<String, ArangoEdgeCollection> edgeCollections,
                                              Map<String, Map<String, BaseEdgeDocument>> edgeDocuments) {
        String idPair = subjectVTuple.id() + "-" + objectVTuple.id();
        if (!edgeCollections.containsKey(idPair)) {
            edgeCollections.put(idPair,
                    arangoDbUtilities.createOrGetEdgeCollection(graph, subjectVTuple.id(), objectVTuple.id()));
            edgeDocuments.put(idPair, new HashMap<>());
            edgeKeys.put(idPair, new HashSet<>());
        }
        String key = subjectVTuple.number() + "-" + objectVTuple.number();
        if (!edgeKeys.get(idPair).contains(key)) {
            BaseEdgeDocument doc = new BaseEdgeDocument(key,
                    subjectVTuple.id() + "/" + subjectVTuple.number(),
                    objectVTuple.id() + "/" + objectVTuple.number());
            edgeDocuments.get(idPair).put(key, doc);
            edgeKeys.get(idPair).add(key);
            return true;
        }
        return false;
    }

    /**
     * Process a single tuples file in a single streaming pass, constructing and updating vertices and edges as each
     * tuple is parsed. For each tuple, the subject and object vertices are created before any updates are applied, and
     * for quadruples the edge is created before its attributes are updated. This eliminates the need to accumulate all
     * tuples in memory and makes the processing order within the file irrelevant.
     *
     * @param tuplesFile                Path to the JSON file containing tuples
     * @param ontologyElementMaps       Maps terms and labels
     * @param ontologyGraph             ArangoDB graph
     * @param ontologyVertexKeys        ArangoDB vertex keys for deduplication
     * @param ontologyVertexCollections ArangoDB vertex collections
     * @param ontologyVertexDocuments   ArangoDB vertex documents
     * @param ontologyEdgeKeys          ArangoDB edge keys for deduplication
     * @param ontologyEdgeCollections   ArangoDB edge collections
     * @param ontologyEdgeDocuments     ArangoDB edge documents
     * @throws IOException if the file cannot be read or contains invalid JSON
     */
    private static void processFileStreaming(Path tuplesFile,
                                             Map<String, OntologyElementMap> ontologyElementMaps,
                                             ArangoGraph ontologyGraph,
                                             Map<String, Set<String>> ontologyVertexKeys,
                                             Map<String, ArangoVertexCollection> ontologyVertexCollections,
                                             Map<String, Map<String, BaseDocument>> ontologyVertexDocuments,
                                             Map<String, Set<String>> ontologyEdgeKeys,
                                             Map<String, ArangoEdgeCollection> ontologyEdgeCollections,
                                             Map<String, Map<String, BaseEdgeDocument>> ontologyEdgeDocuments) throws IOException {

        int nVertices = 0, nEdges = 0;
        Set<String> updatedVertices = new HashSet<>();
        Set<String> updatedEdges = new HashSet<>();
        System.out.println("Processing tuples file " + tuplesFile);
        long startTime = System.nanoTime();

        try (JsonParser parser = new JsonFactory().createParser(new FileInputStream(tuplesFile.toFile()))) {
            advanceToTuplesArray(parser, tuplesFile.toString());

            while (parser.nextToken() == JsonToken.START_ARRAY) {
                ArrayList<Node> tuple = parseTupleFromStream(parser);

                if (tuple.size() == 3) {
                    OntologyGraphBuilder.VTuple subjectVTuple = createVTuple(tuple.get(TRIPLE_SUBJECT_IDX));
                    OntologyGraphBuilder.VTuple objectVTuple = createVTuple(tuple.get(TRIPLE_OBJECT_IDX));

                    // Create subject and object vertices before applying any updates
                    if (subjectVTuple.isValidVertex() && createVertexIfAbsent(subjectVTuple,
                            ontologyGraph,
                            ontologyVertexKeys,
                            ontologyVertexCollections,
                            ontologyVertexDocuments)) {
                        nVertices++;
                    }
                    if (objectVTuple.isValidVertex() && createVertexIfAbsent(objectVTuple,
                            ontologyGraph,
                            ontologyVertexKeys,
                            ontologyVertexCollections,
                            ontologyVertexDocuments)) {
                        nVertices++;
                    }

                    Node o = tuple.get(TRIPLE_OBJECT_IDX);
                    if (o.isLiteral() && subjectVTuple.isValidVertex()) {
                        // Update subject vertex with the literal attribute
                        String attribute = parsePredicate(ontologyElementMaps, tuple.get(TRIPLE_PREDICATE_IDX));
                        String literal = o.getLiteralValue().toString();
                        BaseDocument doc = ontologyVertexDocuments.get(subjectVTuple.id()).get(subjectVTuple.number());
                        updatedVertices.add(subjectVTuple.id() + "-" + subjectVTuple.number());
                        if (doc.getAttribute(attribute) == null) {
                            doc.addAttribute(attribute, literal);
                        } else {
                            doc.updateAttribute(attribute, literal);
                        }
                    } else if (subjectVTuple.isValidVertex() && objectVTuple.isValidVertex()) {
                        // Create edge if absent, then always set/update the Label from this triple's predicate
                        String label = normalizeEdgeLabel(parsePredicate(ontologyElementMaps,
                                tuple.get(TRIPLE_PREDICATE_IDX)));
                        if (createEdgeIfAbsent(subjectVTuple,
                                objectVTuple,
                                ontologyGraph,
                                ontologyEdgeKeys,
                                ontologyEdgeCollections,
                                ontologyEdgeDocuments)) {
                            nEdges++;
                        }
                        String idPair = subjectVTuple.id() + "-" + objectVTuple.id();
                        String key = subjectVTuple.number() + "-" + objectVTuple.number();
                        BaseEdgeDocument doc = ontologyEdgeDocuments.get(idPair).get(key);
                        if (doc.getAttribute("Label") == null) {
                            doc.addAttribute("Label", label);
                        } else {
                            doc.updateAttribute("Label", label);
                        }
                    }

                } else if (tuple.size() == 4) {
                    OntologyGraphBuilder.VTuple subjectVTuple = createVTuple(tuple.get(QUADRUPLE_SUBJECT_IDX));
                    OntologyGraphBuilder.VTuple objectVTuple = createVTuple(tuple.get(QUADRUPLE_OBJECT_IDX));

                    if (!subjectVTuple.isValidVertex() || !objectVTuple.isValidVertex()) continue;

                    // Create subject and object vertices before creating the edge
                    if (createVertexIfAbsent(subjectVTuple,
                            ontologyGraph,
                            ontologyVertexKeys,
                            ontologyVertexCollections,
                            ontologyVertexDocuments)) {
                        nVertices++;
                    }
                    if (createVertexIfAbsent(objectVTuple,
                            ontologyGraph,
                            ontologyVertexKeys,
                            ontologyVertexCollections,
                            ontologyVertexDocuments)) {
                        nVertices++;
                    }

                    // Create edge before updating its attribute (Label will be set when the construct triple arrives)
                    if (createEdgeIfAbsent(subjectVTuple,
                            objectVTuple,
                            ontologyGraph,
                            ontologyEdgeKeys,
                            ontologyEdgeCollections,
                            ontologyEdgeDocuments)) {
                        nEdges++;
                    }

                    // Update edge attribute
                    String idPair = subjectVTuple.id() + "-" + objectVTuple.id();
                    String key = subjectVTuple.number() + "-" + objectVTuple.number();
                    String attribute = parsePredicate(ontologyElementMaps, tuple.get(QUADRUPLE_PREDICATE_IDX));
                    String literal = tuple.get(QUADRUPLE_LITERAL_IDX).getLiteralValue().toString();
                    BaseEdgeDocument doc = ontologyEdgeDocuments.get(idPair).get(key);
                    updatedEdges.add(subjectVTuple.id() + "/" + subjectVTuple.number() + "-" + objectVTuple.id() + "/" + objectVTuple.number());
                    if (doc.getAttribute(attribute) == null) {
                        doc.addAttribute(attribute, literal);
                    } else {
                        doc.updateAttribute(attribute, literal);
                    }
                }
            }
        }

        long stopTime = System.nanoTime();
        System.out.println("Constructed " + nVertices + " vertices, updated " + updatedVertices.size() + " vertices, constructed " + nEdges + " edges, updated " + updatedEdges.size() + " edges from " + tuplesFile.getFileName() + " in " + (stopTime - startTime) / 1e9 + " s");
    }

    /**
     * Load tuples parsed from a results file into a local ArangoDB server instance.
     *
     * @param args (None expected)
     */
    public static void main(String[] args) {

        // Identify the results tuples files
        String tuplesPath = TUPLES_DIR.toString();
        String tuplesPattern = ".*\\.json";
        List<Path> tuplesFiles;
        try {
            tuplesFiles = listFilesMatchingPattern(tuplesPath, tuplesPattern);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (tuplesFiles.isEmpty()) {
            System.out.println("No tuples files found matching pattern " + tuplesPattern);
            System.exit(1);
        }

        // Map terms and labels
        String oboPath = OBO_DIR.toString();
        String oboPattern = "ro.owl";
        List<Path> oboFiles;
        try {
            oboFiles = listFilesMatchingPattern(oboPath, oboPattern);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Map<String, OntologyElementMap> ontologyElementMaps = null;
        if (oboFiles.isEmpty()) {
            System.out.println("No OBO files found matching pattern " + oboPattern);
            System.exit(2);
        } else {
            ontologyElementMaps = parseOntologyElements(oboFiles);
        }

        // Create the database and graph
        String ontologyDatabaseName = "Cell-KN-Ontologies";
        ArangoDatabase ontologyDb = arangoDbUtilities.createOrGetDatabase(ontologyDatabaseName);
        String ontologyGraphName = "KN-Ontologies-v2.0";
        ArangoGraph ontologyGraph = arangoDbUtilities.createOrGetGraph(ontologyDb, ontologyGraphName);

        // Collect vertex keys for each vertex collection to prevent constructing
        // duplicate vertices in the vertex collection
        Map<String, Set<String>> ontologyVertexKeys = new HashMap<>();

        // Collect edge keys in each edge collection to prevent constructing duplicate
        // edges in the edge collection
        Map<String, Set<String>> ontologyEdgeKeys = new HashMap<>();

        // Collect all vertices and edges before inserting them into the graph for improved performance
        Map<String, ArangoVertexCollection> ontologyVertexCollections = new HashMap<>();
        Map<String, Map<String, BaseDocument>> ontologyVertexDocuments = new HashMap<>();
        Map<String, ArangoEdgeCollection> ontologyEdgeCollections = new HashMap<>();
        Map<String, Map<String, BaseEdgeDocument>> ontologyEdgeDocuments = new HashMap<>();

        // Process each tuples file in a single streaming pass
        for (Path tuplesFile : tuplesFiles) {
            try {
                processFileStreaming(tuplesFile,
                        ontologyElementMaps,
                        ontologyGraph,
                        ontologyVertexKeys,
                        ontologyVertexCollections,
                        ontologyVertexDocuments,
                        ontologyEdgeKeys,
                        ontologyEdgeCollections,
                        ontologyEdgeDocuments);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Insert vertices, and edges
        try {
            insertVertices(ontologyVertexCollections, ontologyVertexDocuments);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        insertEdges(ontologyVertexCollections, ontologyEdgeCollections, ontologyEdgeDocuments);

        // Disconnect from a local ArangoDB server instance
        arangoDbUtilities.arangoDB.shutdown();
    }
}
