package gov.nih.nlm;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static gov.nih.nlm.PathUtilities.OBO_DIR;

/**
 * Filters large OWL ontology files by taxon using StAX streaming, so the files can be processed with minimal memory.
 *
 * <p>Two complementary filters are provided:
 * <ul>
 *   <li><b>Inclusion</b> ({@link #slimOntology}) — retain only classes positively asserted to be in the target taxon
 *   via an {@code owl:someValuesFrom} restriction. Correct for ontologies whose classes are species-specific by design
 *   (e.g. the Protein Ontology, where every class is {@code only_in_taxon} some organism).</li>
 *   <li><b>Exclusion</b> ({@link #slimByTaxonExclusion}) — retain every class except those whose taxon constraints rule
 *   the target taxon out. Correct for ontologies that are predominantly pan-taxonomic (e.g. Uberon, where most
 *   anatomical classes carry no taxon constraint and apply across species).</li>
 * </ul>
 */
public class OntologySlimmer {

    private static final String OWL_NS = "http://www.w3.org/2002/07/owl#";
    private static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    private static final String RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#";
    private static final String OBO_NS = "http://purl.obolibrary.org/obo/";

    private static final String NCBITAXON_PREFIX = OBO_NS + "NCBITaxon_";

    // RO taxon-constraint properties (used both as restriction onProperty URIs and as direct annotation local names).
    private static final String NEVER_IN_TAXON_URI = OBO_NS + "RO_0002161"; // never_in_taxon
    private static final String ONLY_IN_TAXON_URI = OBO_NS + "RO_0002160";  // only_in_taxon
    private static final String NEVER_IN_TAXON_LOCAL = "RO_0002161";
    private static final String ONLY_IN_TAXON_LOCAL = "RO_0002160";

    /**
     * Decides whether a fully-buffered top-level {@code owl:Class} should be written to the output.
     */
    @FunctionalInterface
    private interface ClassFilter {
        boolean keep(List<XMLEvent> classEvents);
    }

    /**
     * Create an {@link XMLInputFactory} hardened against XXE by disabling DTDs and external entities. The ontology
     * inputs do not use DTDs or entity references, so disabling them is safe and prevents external-entity attacks.
     *
     * @return A hardened input factory
     */
    private static XMLInputFactory hardenedInputFactory() {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        return factory;
    }

    /**
     * Filter an OWL file to retain only classes with a taxon restriction for the given NCBI taxon ID. Copies the
     * ontology header, all annotation and object property declarations, and only those {@code owl:Class} elements that
     * contain an {@code owl:someValuesFrom} restriction referencing the specified taxon. Drops all {@code owl:Axiom}
     * elements.
     *
     * @param inputFile  Path to the full OWL file
     * @param outputFile Path to write the filtered OWL file
     * @param taxonId    NCBI taxon ID to filter on (e.g., "9606" for human)
     * @return The number of classes written
     * @throws IOException        if an I/O error occurs
     * @throws XMLStreamException if an XML parsing error occurs
     */
    public static int slimOntology(Path inputFile, Path outputFile, String taxonId) throws IOException, XMLStreamException {
        String taxonURI = NCBITAXON_PREFIX + taxonId;
        return filterClasses(inputFile, outputFile, events -> hasSomeValuesFrom(events, taxonURI));
    }

    /**
     * Filter an OWL file to retain every class except those whose taxon constraints exclude the target taxon. Copies the
     * ontology header, all annotation and object property declarations, and every {@code owl:Class} element that is not
     * excluded. Drops all {@code owl:Axiom} elements.
     *
     * <p>A class is excluded when it asserts either:
     * <ul>
     *   <li>{@code never_in_taxon} (RO:0002161) {@code T} where the target taxon is {@code T} or a descendant of
     *   {@code T} (i.e. {@code T} is in {@code targetLineage}); or</li>
     *   <li>{@code only_in_taxon} (RO:0002160) restricted to taxa none of which cover the target taxon.</li>
     * </ul>
     * Both the direct annotation form (e.g. {@code <obo:RO_0002161 rdf:resource="NCBITaxon_..."/>}) and the logical
     * restriction form ({@code owl:onProperty} + {@code owl:someValuesFrom}) are recognized.
     *
     * @param inputFile     Path to the full OWL file
     * @param outputFile    Path to write the filtered OWL file
     * @param targetLineage The target taxon together with all of its ancestors (see {@link #buildTaxonLineage})
     * @return The number of classes written
     * @throws IOException        if an I/O error occurs
     * @throws XMLStreamException if an XML parsing error occurs
     */
    public static int slimByTaxonExclusion(Path inputFile, Path outputFile, Set<String> targetLineage) throws IOException, XMLStreamException {
        return filterClasses(inputFile, outputFile, events -> !isExcludedForTaxon(events, targetLineage));
    }

    /**
     * Stream an OWL file, passing through the header and all top-level non-class, non-axiom content, buffering each
     * top-level {@code owl:Class} and writing it only when the supplied filter keeps it. All {@code owl:Axiom} elements
     * are dropped.
     *
     * @param inputFile  Path to the full OWL file
     * @param outputFile Path to write the filtered OWL file
     * @param filter     Decides whether each buffered class is written
     * @return The number of classes written
     * @throws IOException        if an I/O error occurs
     * @throws XMLStreamException if an XML parsing error occurs
     */
    private static int filterClasses(Path inputFile, Path outputFile, ClassFilter filter) throws IOException, XMLStreamException {
        XMLInputFactory inputFactory = hardenedInputFactory();
        XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        int classesWritten = 0;
        int classesSkipped = 0;

        try (var fis = new BufferedInputStream(new FileInputStream(inputFile.toFile()));
             var fos = new BufferedOutputStream(new FileOutputStream(outputFile.toFile()))) {

            XMLEventReader reader = inputFactory.createXMLEventReader(fis);
            XMLEventWriter writer = outputFactory.createXMLEventWriter(fos, "UTF-8");

            // State machine: buffer each owl:Class, decide keep/discard at its end tag
            List<XMLEvent> buffer = new ArrayList<>();
            boolean inClass = false;   // inside a top-level owl:Class (has rdf:about)
            boolean inAxiom = false;   // inside an owl:Axiom (always discarded)
            int depth = 0;             // XML nesting depth
            int elementStartDepth = 0; // depth when the current class/axiom opened

            while (reader.hasNext()) {
                XMLEvent event = reader.nextEvent();

                if (event.isStartElement()) {
                    depth++;
                    String localName = event.asStartElement().getName().getLocalPart();
                    String nsURI = event.asStartElement().getName().getNamespaceURI();

                    // Start buffering a top-level owl:Class; we decide keep/discard at its end tag. A named class may be
                    // serialized with either rdf:about or rdf:ID; accept both so neither bypasses the filter.
                    if (nsURI.equals(OWL_NS) && localName.equals("Class") && !inClass && !inAxiom) {
                        var aboutAttr = event.asStartElement().getAttributeByName(new QName(RDF_NS, "about"));
                        var idAttr = event.asStartElement().getAttributeByName(new QName(RDF_NS, "ID"));
                        if (aboutAttr != null || idAttr != null) {
                            inClass = true;
                            elementStartDepth = depth;
                            buffer.clear();
                            buffer.add(event);
                            continue;
                        }
                    }

                    // Skip owl:Axiom elements entirely
                    if (nsURI.equals(OWL_NS) && localName.equals("Axiom") && !inClass) {
                        inAxiom = true;
                        elementStartDepth = depth;
                        continue;
                    }

                    if (inClass) {
                        buffer.add(event);
                        continue;
                    }

                    if (inAxiom) {
                        continue;
                    }

                    // Top-level non-class, non-axiom elements pass through (header, properties, etc.)
                    writer.add(event);

                } else if (event.isEndElement()) {
                    if (inClass) {
                        buffer.add(event);
                        // Class fully parsed: flush buffer if the filter keeps it, otherwise discard
                        if (depth == elementStartDepth) {
                            inClass = false;
                            if (filter.keep(buffer)) {
                                for (XMLEvent e : buffer) {
                                    writer.add(e);
                                }
                                classesWritten++;
                            } else {
                                classesSkipped++;
                            }
                            buffer.clear();
                        }
                    } else if (inAxiom) {
                        if (depth == elementStartDepth) {
                            inAxiom = false;
                        }
                    } else {
                        writer.add(event);
                    }
                    depth--;

                } else {
                    // Characters, comments, processing instructions, etc.
                    if (inClass) {
                        buffer.add(event);
                    } else if (!inAxiom) {
                        writer.add(event);
                    }
                }
            }

            writer.flush();
            reader.close();
        }

        System.out.println("Wrote " + classesWritten + " classes, skipped " + classesSkipped
                + " classes and all axioms");
        return classesWritten;
    }

    /**
     * Whether the buffered class contains an {@code owl:someValuesFrom} restriction referencing the target URI.
     *
     * @param classEvents Buffered events for a single class
     * @param targetURI   The taxon URI to match
     * @return {@code true} if a matching restriction is present
     */
    private static boolean hasSomeValuesFrom(List<XMLEvent> classEvents, String targetURI) {
        for (XMLEvent e : classEvents) {
            if (e.isStartElement()) {
                StartElement se = e.asStartElement();
                if (OWL_NS.equals(se.getName().getNamespaceURI())
                        && "someValuesFrom".equals(se.getName().getLocalPart())
                        && targetURI.equals(getResource(se))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether the buffered class is excluded for the target taxon, given the target's lineage (the taxon and all of its
     * ancestors). Recognizes {@code never_in_taxon} and {@code only_in_taxon} constraints in both the direct annotation
     * form and the logical {@code owl:onProperty}/{@code owl:someValuesFrom} restriction form.
     *
     * @param classEvents   Buffered events for a single class
     * @param targetLineage The target taxon together with all of its ancestors
     * @return {@code true} if the class should be excluded
     */
    static boolean isExcludedForTaxon(List<XMLEvent> classEvents, Set<String> targetLineage) {
        Set<String> neverInTaxon = new HashSet<>();
        Set<String> onlyInTaxon = new HashSet<>();
        String restrictionProperty = null; // RO property of the enclosing owl:Restriction, if relevant

        for (XMLEvent e : classEvents) {
            if (e.isStartElement()) {
                StartElement se = e.asStartElement();
                String ns = se.getName().getNamespaceURI();
                String ln = se.getName().getLocalPart();

                if (OWL_NS.equals(ns) && "Restriction".equals(ln)) {
                    restrictionProperty = null;
                } else if (OWL_NS.equals(ns) && "onProperty".equals(ln)) {
                    String p = getResource(se);
                    if (NEVER_IN_TAXON_URI.equals(p) || ONLY_IN_TAXON_URI.equals(p)) {
                        restrictionProperty = p;
                    }
                } else if (OWL_NS.equals(ns) && "someValuesFrom".equals(ln)) {
                    String t = getResource(se);
                    if (t != null && restrictionProperty != null) {
                        (NEVER_IN_TAXON_URI.equals(restrictionProperty) ? neverInTaxon : onlyInTaxon).add(t);
                    }
                } else if (OBO_NS.equals(ns) && NEVER_IN_TAXON_LOCAL.equals(ln)) {
                    String t = getResource(se);
                    if (t != null) {
                        neverInTaxon.add(t);
                    }
                } else if (OBO_NS.equals(ns) && ONLY_IN_TAXON_LOCAL.equals(ln)) {
                    String t = getResource(se);
                    if (t != null) {
                        onlyInTaxon.add(t);
                    }
                }
            } else if (e.isEndElement()) {
                if (OWL_NS.equals(e.asEndElement().getName().getNamespaceURI())
                        && "Restriction".equals(e.asEndElement().getName().getLocalPart())) {
                    restrictionProperty = null;
                }
            }
        }

        // never_in_taxon T excludes the target when the target is T or a descendant of T (T in the target lineage).
        for (String t : neverInTaxon) {
            if (targetLineage.contains(t)) {
                return true;
            }
        }
        // only_in_taxon excludes the target when none of the named taxa cover it.
        if (!onlyInTaxon.isEmpty()) {
            for (String t : onlyInTaxon) {
                if (targetLineage.contains(t)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Build the set containing the given taxon and all of its ancestors by walking the {@code rdfs:subClassOf} chain in
     * an NCBITaxon ontology file (e.g. taxslim.owl). The returned set is the taxon for which a {@code never_in_taxon}
     * constraint excludes a class, and against which {@code only_in_taxon} membership is tested.
     *
     * @param taxslimFile Path to an NCBITaxon OWL file containing the taxon hierarchy
     * @param taxonId     NCBI taxon ID whose lineage to build (e.g., "9606" for human)
     * @return The taxon URI together with all ancestor taxon URIs
     * @throws IOException        if an I/O error occurs
     * @throws XMLStreamException if an XML parsing error occurs
     */
    public static Set<String> buildTaxonLineage(Path taxslimFile, String taxonId) throws IOException, XMLStreamException {
        Map<String, Set<String>> childToParents = new HashMap<>();
        XMLInputFactory inputFactory = hardenedInputFactory();

        try (var fis = new BufferedInputStream(new FileInputStream(taxslimFile.toFile()))) {
            XMLEventReader reader = inputFactory.createXMLEventReader(fis);
            String currentTaxon = null; // rdf:about of the NCBITaxon class currently being read
            int classDepth = -1;
            int depth = 0;

            while (reader.hasNext()) {
                XMLEvent event = reader.nextEvent();
                if (event.isStartElement()) {
                    depth++;
                    StartElement se = event.asStartElement();
                    String ns = se.getName().getNamespaceURI();
                    String ln = se.getName().getLocalPart();

                    if (OWL_NS.equals(ns) && "Class".equals(ln) && currentTaxon == null) {
                        var about = se.getAttributeByName(new QName(RDF_NS, "about"));
                        if (about != null && about.getValue().startsWith(NCBITAXON_PREFIX)) {
                            currentTaxon = about.getValue();
                            classDepth = depth;
                        }
                    } else if (currentTaxon != null && RDFS_NS.equals(ns) && "subClassOf".equals(ln)) {
                        String parent = getResource(se);
                        if (parent != null && parent.startsWith(NCBITAXON_PREFIX)) {
                            childToParents.computeIfAbsent(currentTaxon, k -> new HashSet<>()).add(parent);
                        }
                    }
                } else if (event.isEndElement()) {
                    if (currentTaxon != null && depth == classDepth) {
                        currentTaxon = null;
                        classDepth = -1;
                    }
                    depth--;
                }
            }
            reader.close();
        }

        // Walk up from the target taxon, collecting it and all of its ancestors
        Set<String> lineage = new HashSet<>();
        Deque<String> stack = new ArrayDeque<>();
        stack.push(NCBITAXON_PREFIX + taxonId);
        while (!stack.isEmpty()) {
            String taxon = stack.pop();
            if (lineage.add(taxon)) {
                for (String parent : childToParents.getOrDefault(taxon, Set.of())) {
                    stack.push(parent);
                }
            }
        }
        return lineage;
    }

    /**
     * Read the value of the {@code rdf:resource} attribute of a start element, or {@code null} if absent.
     *
     * @param se Start element
     * @return The {@code rdf:resource} value, or {@code null}
     */
    private static String getResource(StartElement se) {
        var attr = se.getAttributeByName(new QName(RDF_NS, "resource"));
        return attr == null ? null : attr.getValue();
    }

    /**
     * Slim downloaded ontology files in data/obo before graph loading:
     * <ul>
     *   <li>{@code pr.owl} → {@code pr-slim.owl}: retain only human (NCBITaxon 9606) protein classes (inclusion).</li>
     *   <li>{@code uberon-base.owl} → {@code uberon-human.owl}: retain all anatomy except classes a taxon constraint
     *   excludes from human (exclusion), using the NCBITaxon hierarchy in {@code taxslim.owl}.</li>
     * </ul>
     * Each slimmed input is moved to {@code .archive/} after a successful slim.
     *
     * @param args (None expected)
     */
    public static void main(String[] args) {
        Path archiveDir = OBO_DIR.resolve(".archive");

        try {
            // Inclusion slim: pr.owl -> pr-slim.owl
            Path prFull = OBO_DIR.resolve("pr.owl");
            Path prSlim = OBO_DIR.resolve("pr-slim.owl");
            if (!Files.exists(prFull)) {
                throw new RuntimeException("pr.owl not found in " + OBO_DIR);
            }
            System.out.println("Slimming " + prFull + " to taxon 9606 (inclusion)");
            long startTime = System.nanoTime();
            slimOntology(prFull, prSlim, "9606");
            System.out.println("Slimmed in " + (System.nanoTime() - startTime) / 1e9 + " s");
            Files.createDirectories(archiveDir);
            Path prArchive = archiveDir.resolve("pr.owl");
            System.out.println("Moving " + prFull + " to " + prArchive);
            Files.move(prFull, prArchive, StandardCopyOption.REPLACE_EXISTING);

            // Exclusion slim: uberon-base.owl -> uberon-human.owl, using the NCBITaxon hierarchy in taxslim.owl
            Path taxslimFile = OBO_DIR.resolve("taxslim.owl");
            Path uberonFull = OBO_DIR.resolve("uberon-base.owl");
            Path uberonSlim = OBO_DIR.resolve("uberon-human.owl");
            if (!Files.exists(uberonFull)) {
                throw new RuntimeException("uberon-base.owl not found in " + OBO_DIR);
            }
            if (!Files.exists(taxslimFile)) {
                throw new RuntimeException("taxslim.owl not found in " + OBO_DIR);
            }
            System.out.println("Building human (NCBITaxon 9606) lineage from " + taxslimFile);
            Set<String> humanLineage = buildTaxonLineage(taxslimFile, "9606");
            System.out.println("Human lineage spans " + humanLineage.size() + " taxa");
            System.out.println("Slimming " + uberonFull + " to taxon 9606 (exclusion)");
            startTime = System.nanoTime();
            int kept = slimByTaxonExclusion(uberonFull, uberonSlim, humanLineage);
            System.out.println("Kept " + kept + " UBERON classes; slimmed in " + (System.nanoTime() - startTime) / 1e9 + " s");
            Path uberonArchive = archiveDir.resolve("uberon-base.owl");
            System.out.println("Moving " + uberonFull + " to " + uberonArchive);
            Files.move(uberonFull, uberonArchive, StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException | XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }
}
