package gov.nih.nlm;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OntologyDownloaderTest {

    private static final Path testOboDir = Paths.get(System.getProperty("user.dir")).resolve("src/test/data/obo");

    // --- findOboVersion tests ---

    @Test
    void findOboVersion_fromVersionInfo() {
        // version-info-test.owl has owl:versionInfo with text "2024-01-15"
        String version = OntologyDownloader.findOboVersion(testOboDir.resolve("version-info-test.owl"));
        assertEquals("2024-01-15", version);
    }

    @Test
    void findOboVersion_fromVersionIRI() {
        // macrophage.owl has owl:versionIRI but no owl:versionInfo
        String version = OntologyDownloader.findOboVersion(testOboDir.resolve("macrophage.owl"));
        assertEquals("2024-09-26", version);
    }

    @Test
    void findOboVersion_prefersVersionInfo() {
        // ro.owl has both owl:versionInfo and owl:versionIRI with the same date
        String version = OntologyDownloader.findOboVersion(testOboDir.resolve("ro.owl"));
        assertEquals("2024-04-24", version);
    }

    @Test
    void findOboVersion_noVersion() {
        // no-version-test.owl has neither owl:versionInfo nor owl:versionIRI
        String version = OntologyDownloader.findOboVersion(testOboDir.resolve("no-version-test.owl"));
        assertNull(version);
    }

    // --- OBO_PURLS tests ---

    @Test
    void oboPurls_containsExpectedUrls() {
        assertEquals(10, OntologyDownloader.OBO_PURLS.size());
        assertTrue(OntologyDownloader.OBO_PURLS.contains("https://purl.obolibrary.org/obo/cl.owl"));
        assertTrue(OntologyDownloader.OBO_PURLS.contains("https://purl.obolibrary.org/obo/ro.owl"));
    }

    // --- installDownload tests (no network) ---

    private static String owl(String version) {
        String info = version == null ? "" : "<owl:versionInfo>" + version + "</owl:versionInfo>";
        return "<?xml version=\"1.0\"?>\n"
                + "<rdf:RDF xmlns:owl=\"http://www.w3.org/2002/07/owl#\""
                + " xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n"
                + "<owl:Ontology rdf:about=\"http://purl.obolibrary.org/obo/t.owl\">" + info + "</owl:Ontology>\n"
                + "</rdf:RDF>\n";
    }

    private static void write(Path path, String version) throws IOException {
        Files.writeString(path, owl(version), StandardCharsets.UTF_8);
    }

    @Test
    void installDownload_newerVersionReplacesAndArchivesCurrent(@TempDir Path dir) throws IOException {
        write(dir.resolve("t.owl"), "2024-01-01");
        write(dir.resolve("t-new.owl"), "2024-02-01");

        OntologyDownloader.installDownload(dir.resolve("t-new.owl"), dir, "t", ".owl");

        assertEquals("2024-02-01", OntologyDownloader.findOboVersion(dir.resolve("t.owl")));
        assertTrue(Files.exists(dir.resolve(".archive/t-2024-01-01.owl")));
        assertFalse(Files.exists(dir.resolve("t-new.owl")));
    }

    @Test
    void installDownload_olderVersionIsDiscarded(@TempDir Path dir) throws IOException {
        write(dir.resolve("t.owl"), "2024-02-01");
        write(dir.resolve("t-new.owl"), "2024-01-01");

        OntologyDownloader.installDownload(dir.resolve("t-new.owl"), dir, "t", ".owl");

        assertEquals("2024-02-01", OntologyDownloader.findOboVersion(dir.resolve("t.owl")));
        assertFalse(Files.exists(dir.resolve("t-new.owl")));
    }

    @Test
    void installDownload_sameVersionIsDiscarded(@TempDir Path dir) throws IOException {
        write(dir.resolve("t.owl"), "2024-01-01");
        write(dir.resolve("t-new.owl"), "2024-01-01");

        OntologyDownloader.installDownload(dir.resolve("t-new.owl"), dir, "t", ".owl");

        assertFalse(Files.exists(dir.resolve("t-new.owl")));
        assertFalse(Files.exists(dir.resolve(".archive")));
    }

    @Test
    void installDownload_unreadableNewVersionKeepsNewAndArchivesCurrent(@TempDir Path dir) throws IOException {
        write(dir.resolve("t.owl"), "2024-01-01");
        write(dir.resolve("t-new.owl"), null);

        OntologyDownloader.installDownload(dir.resolve("t-new.owl"), dir, "t", ".owl");

        assertNull(OntologyDownloader.findOboVersion(dir.resolve("t.owl")));
        assertTrue(Files.exists(dir.resolve(".archive/t-2024-01-01.owl")));
        assertFalse(Files.exists(dir.resolve("t-new.owl")));
    }

    @Test
    void installDownload_unreadableCurrentVersionKeepsNewAndArchivesCurrent(@TempDir Path dir) throws IOException {
        write(dir.resolve("t.owl"), null);
        write(dir.resolve("t-new.owl"), "2024-02-01");

        OntologyDownloader.installDownload(dir.resolve("t-new.owl"), dir, "t", ".owl");

        assertEquals("2024-02-01", OntologyDownloader.findOboVersion(dir.resolve("t.owl")));
        try (var archived = Files.list(dir.resolve(".archive"))) {
            assertEquals(1, archived.filter(p -> p.getFileName().toString().startsWith("t-unversioned-")).count());
        }
    }

    @Test
    void installDownload_unreadableCurrentVersionArchiveTagsAreUnique(@TempDir Path dir) throws IOException {
        // Two installs back-to-back, each with an unreadable current version,
        // must not collide on the same ".archive" path even within the same
        // millisecond (System.currentTimeMillis() resolution).
        write(dir.resolve("t.owl"), null);
        write(dir.resolve("t-new.owl"), null);
        OntologyDownloader.installDownload(dir.resolve("t-new.owl"), dir, "t", ".owl");

        write(dir.resolve("t-new.owl"), null);
        OntologyDownloader.installDownload(dir.resolve("t-new.owl"), dir, "t", ".owl");

        try (var archived = Files.list(dir.resolve(".archive"))) {
            assertEquals(
                    2, archived.filter(p -> p.getFileName().toString().startsWith("t-unversioned-")).count());
        }
    }

    @Test
    void installDownload_noCurrentFileInstallsNew(@TempDir Path dir) throws IOException {
        write(dir.resolve("t-new.owl"), null);

        OntologyDownloader.installDownload(dir.resolve("t-new.owl"), dir, "t", ".owl");

        assertTrue(Files.exists(dir.resolve("t.owl")));
        assertFalse(Files.exists(dir.resolve("t-new.owl")));
    }

    // --- updateDownloads tests (local HTTP server) ---

    @Test
    void updateDownloads_failedUrlDoesNotStopTheOthers(@TempDir Path dir) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/good.owl", exchange -> {
            byte[] body = owl("2024-03-01").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/missing.owl", exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();
        try {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            write(dir.resolve("missing.owl"), "2024-01-01");

            IOException e = assertThrows(IOException.class, () -> OntologyDownloader.updateDownloads(
                    List.of(base + "/missing.owl", base + "/good.owl"), dir, HttpClient.newHttpClient()));

            // The failing URL is reported, the URL after it was still downloaded, and the current file was kept
            assertTrue(e.getMessage().contains("/missing.owl"));
            assertFalse(e.getMessage().contains("/good.owl"));
            assertEquals("2024-03-01", OntologyDownloader.findOboVersion(dir.resolve("good.owl")));
            assertEquals("2024-01-01", OntologyDownloader.findOboVersion(dir.resolve("missing.owl")));
            assertFalse(Files.exists(dir.resolve("missing-new.owl")));
        } finally {
            server.stop(0);
        }
    }

    // --- updateDownloads integration test ---

    @Tag("integration")
    @Test
    void updateDownloads_downloadAndCompareVersions() throws IOException, InterruptedException {
        Path tempDir = Files.createTempDirectory("obo-download-test");
        try {
            // Download a small OBO file (ro.owl)
            List<String> urls = List.of("https://purl.obolibrary.org/obo/ro.owl");
            OntologyDownloader.updateDownloads(urls, tempDir);

            // Verify the file was downloaded and renamed from ro-new.owl to ro.owl
            Path downloadedFile = tempDir.resolve("ro.owl");
            assertTrue(Files.exists(downloadedFile), "ro.owl should exist after first download");
            assertTrue(Files.size(downloadedFile) > 0, "ro.owl should not be empty");

            // Verify version can be extracted
            String version = OntologyDownloader.findOboVersion(downloadedFile);
            assertNotNull(version, "Downloaded ro.owl should have a parseable version");
            assertTrue(version.matches("\\d{4}-\\d{2}-\\d{2}"), "Version should be YYYY-MM-DD format");

            // Download again — should detect same version and remove the new file
            OntologyDownloader.updateDownloads(urls, tempDir);
            assertTrue(Files.exists(downloadedFile), "ro.owl should still exist after second download");
            Path newFile = tempDir.resolve("ro-new.owl");
            assertTrue(!Files.exists(newFile), "ro-new.owl should be removed (same version)");
        } finally {
            // Clean up temp directory
            try (var stream = Files.walk(tempDir)) {
                stream.sorted(java.util.Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(java.io.File::delete);
            }
        }
    }
}
