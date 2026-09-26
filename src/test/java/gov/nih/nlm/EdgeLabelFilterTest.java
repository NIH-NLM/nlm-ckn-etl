package gov.nih.nlm;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeLabelFilterTest {

    private static final List<String> CL_UBERON_KEPT = List.of("PART_OF",
            "HAS_SOMA_LOCATION",
            "HAS_SYNAPTIC_IO_IN_REGION",
            "AXON_SYNAPSES_IN",
            "HAS_DENDRITE_LOCATION",
            "HAS_PRESYNAPTIC_TERMINAL_IN",
            "HAS_SENSORY_DENDRITE_IN",
            "HAS_SYNAPTIC_TERMINAL_IN");

    private static final List<String> CL_UBERON_REMOVED = List.of("LOCATED_IN",
            "ADJACENT_TO",
            "SUB_CLASS_OF",
            "PRODUCES",
            "DEVELOPS_FROM",
            "OVERLAPS",
            "CONNECTED_TO",
            "EXISTENCE_ENDS_DURING",
            "EXISTENCE_STARTS_AND_ENDS_DURING",
            "EXISTENCE_STARTS_DURING",
            "FASCICULATES_WITH");

    private static final List<String> UBERON_CL_KEPT = List.of("HAS_PART", "COMPOSED_PRIMARILY_OF");

    private static final List<String> UBERON_CL_REMOVED = List.of("OVERLAPS",
            "SURROUNDS",
            "PART_OF",
            "CHANNEL_FOR",
            "PRODUCED_BY",
            "BOUNDING_LAYER_OF",
            "DEVELOPS_FROM",
            "ADJACENT_TO",
            "EXTENDS_FIBERS_INTO",
            "HAS_POTENTIAL_TO_DEVELOPMENTALLY_CONTRIBUTE_TO",
            "SYNAPSED_BY");

    @Test
    void keep_keepsReviewedLabelsFromCellTypeToAnatomy() {
        EdgeLabelFilter filter = new EdgeLabelFilter();
        CL_UBERON_KEPT.forEach(label -> assertTrue(filter.keep("CL-UBERON", label), label));
        assertEquals(0, filter.getDroppedCount("CL-UBERON"));
    }

    @Test
    void keep_dropsRemovedLabelsFromCellTypeToAnatomy() {
        EdgeLabelFilter filter = new EdgeLabelFilter();
        CL_UBERON_REMOVED.forEach(label -> assertFalse(filter.keep("CL-UBERON", label), label));
        assertEquals(CL_UBERON_REMOVED.size(), filter.getDroppedCount("CL-UBERON"));
    }

    @Test
    void keep_keepsReviewedLabelsFromAnatomyToCellType() {
        EdgeLabelFilter filter = new EdgeLabelFilter();
        UBERON_CL_KEPT.forEach(label -> assertTrue(filter.keep("UBERON-CL", label), label));
        assertEquals(0, filter.getDroppedCount("UBERON-CL"));
    }

    @Test
    void keep_dropsRemovedLabelsFromAnatomyToCellType() {
        EdgeLabelFilter filter = new EdgeLabelFilter();
        UBERON_CL_REMOVED.forEach(label -> assertFalse(filter.keep("UBERON-CL", label), label));
        assertEquals(UBERON_CL_REMOVED.size(), filter.getDroppedCount("UBERON-CL"));
    }

    @Test
    void keep_dependsOnDirection() {
        EdgeLabelFilter filter = new EdgeLabelFilter();
        assertTrue(filter.keep("CL-UBERON", "PART_OF"));
        assertFalse(filter.keep("UBERON-CL", "PART_OF"));
        assertTrue(filter.keep("UBERON-CL", "HAS_PART"));
        assertFalse(filter.keep("CL-UBERON", "HAS_PART"));
    }

    @Test
    void keep_dropsUnreviewedLabelOnFilteredPairs() {
        EdgeLabelFilter filter = new EdgeLabelFilter();
        assertFalse(filter.keep("CL-UBERON", "SOME_NEW_RELATION"));
        assertFalse(filter.keep("UBERON-CL", "SOME_NEW_RELATION"));
    }

    @Test
    void keep_keepsEverythingOnOtherPairs() {
        EdgeLabelFilter filter = new EdgeLabelFilter();
        for (String pair : List.of("CL-CL", "UBERON-UBERON", "GO-GO", "CL-GO", "CS-CL", "UBERON-NCBITaxon")) {
            for (String label : List.of("PART_OF", "SUB_CLASS_OF", "SOME_NEW_RELATION")) {
                assertTrue(filter.keep(pair, label), pair + " " + label);
            }
        }
        assertTrue(filter.summarize().isEmpty());
    }

    @Test
    void getDroppedCounts_countsRepeatedDropsByLabel() {
        EdgeLabelFilter filter = new EdgeLabelFilter();
        filter.keep("CL-UBERON", "LOCATED_IN");
        filter.keep("CL-UBERON", "LOCATED_IN");
        filter.keep("CL-UBERON", "ADJACENT_TO");
        filter.keep("CL-UBERON", "PART_OF");
        assertEquals(Map.of("LOCATED_IN", 2L, "ADJACENT_TO", 1L), filter.getDroppedCounts("CL-UBERON"));
        assertEquals(3, filter.getDroppedCount("CL-UBERON"));
        assertEquals(Map.of(), filter.getDroppedCounts("UBERON-CL"));
    }

    @Test
    void summarize_listsMostFrequentLabelFirstForEachPair() {
        EdgeLabelFilter filter = new EdgeLabelFilter();
        filter.keep("CL-UBERON", "ADJACENT_TO");
        filter.keep("CL-UBERON", "LOCATED_IN");
        filter.keep("CL-UBERON", "LOCATED_IN");
        filter.keep("UBERON-CL", "PART_OF");
        assertEquals(List.of("Dropped 3 edges from CL-UBERON: LOCATED_IN=2, ADJACENT_TO=1",
                "Dropped 1 edges from UBERON-CL: PART_OF=1"), filter.summarize());
    }

    @Test
    void keptLabels_matchTheReviewedListSizes() {
        assertEquals(CL_UBERON_KEPT.size(), EdgeLabelFilter.KEPT_LABELS_BY_PAIR.get("CL-UBERON").size());
        assertEquals(UBERON_CL_KEPT.size(), EdgeLabelFilter.KEPT_LABELS_BY_PAIR.get("UBERON-CL").size());
    }
}
