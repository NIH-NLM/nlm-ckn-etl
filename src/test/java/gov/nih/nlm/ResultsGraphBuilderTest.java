package gov.nih.nlm;

import org.apache.jena.graph.Node;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultsGraphBuilderTest {

    private static final Path TEST_DATA_DIR = Paths.get(System.getProperty("user.dir"))
            .resolve("src/test/data/tuples");

    @Test
    void readJsonFile_validTriples() throws IOException {
        ArrayList<ArrayList<Node>> tuples = ResultsGraphBuilder.readJsonFile(
                TEST_DATA_DIR.resolve("test-triples.json").toString());

        // File contains 3 triples and 1 quadruple = 4 tuples total
        assertEquals(4, tuples.size());

        // First tuple is a triple with all URIs
        ArrayList<Node> triple = tuples.get(0);
        assertEquals(3, triple.size());
        assertTrue(triple.get(0).isURI());
        assertTrue(triple.get(1).isURI());
        assertTrue(triple.get(2).isURI());

        // Third tuple is a triple with a literal object
        ArrayList<Node> literalTriple = tuples.get(2);
        assertEquals(3, literalTriple.size());
        assertTrue(literalTriple.get(0).isURI());
        assertTrue(literalTriple.get(1).isURI());
        assertTrue(literalTriple.get(2).isLiteral());
        assertEquals("macrophage", literalTriple.get(2).getLiteralValue().toString());
    }

    @Test
    void readJsonFile_validQuadruples() throws IOException {
        ArrayList<ArrayList<Node>> tuples = ResultsGraphBuilder.readJsonFile(
                TEST_DATA_DIR.resolve("test-triples.json").toString());

        // Fourth tuple is a quadruple
        ArrayList<Node> quadruple = tuples.get(3);
        assertEquals(4, quadruple.size());
        assertTrue(quadruple.get(0).isURI());
        assertTrue(quadruple.get(1).isURI());
        assertTrue(quadruple.get(2).isURI());
        assertTrue(quadruple.get(3).isLiteral());
    }

    @Test
    void readJsonFile_invalidTriple() {
        IOException ex = assertThrows(IOException.class, () ->
                ResultsGraphBuilder.readJsonFile(
                        TEST_DATA_DIR.resolve("test-invalid.json").toString()));
        assertTrue(ex.getMessage().contains("Invalid triple"));
    }

    @Test
    void readJsonFile_missingFile() {
        assertThrows(IOException.class, () ->
                ResultsGraphBuilder.readJsonFile(
                        TEST_DATA_DIR.resolve("nonexistent.json").toString()));
    }

    @Test
    void readJsonFile_invalidJson() throws IOException {
        // Create a temp file with invalid JSON
        Path tempFile = java.nio.file.Files.createTempFile("test-invalid", ".json");
        java.nio.file.Files.writeString(tempFile, "not valid json");
        try {
            assertThrows(IOException.class, () ->
                    ResultsGraphBuilder.readJsonFile(tempFile.toString()));
        } finally {
            java.nio.file.Files.deleteIfExists(tempFile);
        }
    }
}
