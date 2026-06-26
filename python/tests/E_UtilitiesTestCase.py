from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from E_Utilities import element_text, find_names_or_none, parse_xml


class E_UtilitiesTestCase(unittest.TestCase):
    """Pure unit tests for E_Utilities functions."""

    # find_names_or_none tests

    def test_find_names_or_none_nested_tags(self):
        """Traverses nested tags to find text."""
        xml = "<root><AuthorList><Author><LastName>Smith</LastName></Author></AuthorList></root>"
        root = parse_xml(xml)
        result = find_names_or_none(root, ["AuthorList", "Author", "LastName"])
        self.assertEqual(result, "Smith")

    def test_find_names_or_none_missing_tag(self):
        """Returns None when intermediate tag is missing."""
        xml = "<root><AuthorList><Author><LastName>Smith</LastName></Author></AuthorList></root>"
        root = parse_xml(xml)
        result = find_names_or_none(root, ["AuthorList", "Missing"])
        self.assertIsNone(result)

    def test_find_names_or_none_with_attribute(self):
        """Extracts tag attribute value."""
        xml = '<root><Type value="protein-coding"/></root>'
        root = parse_xml(xml)
        result = find_names_or_none(root, ["Type"], attribute="value")
        self.assertEqual(result, "protein-coding")

    def test_find_names_or_none_single_tag(self):
        """Finds text in a single tag name."""
        xml = "<root><Title>Some Title</Title></root>"
        root = parse_xml(xml)
        result = find_names_or_none(root, ["Title"])
        self.assertEqual(result, "Some Title")

    def test_find_names_or_none_missing_first_tag(self):
        """Returns None when first tag is not found."""
        xml = "<root><Other>text</Other></root>"
        root = parse_xml(xml)
        result = find_names_or_none(root, ["Missing"])
        self.assertIsNone(result)

    def test_find_names_or_none_missing_attribute(self):
        """Returns None when attribute does not exist on found tag."""
        xml = "<root><Type>protein-coding</Type></root>"
        root = parse_xml(xml)
        result = find_names_or_none(root, ["Type"], attribute="nonexistent")
        self.assertIsNone(result)

    def test_find_names_or_none_joins_inline_markup(self):
        """Concatenates descendant text the way BeautifulSoup's .text did."""
        xml = "<root><Title>Role of <i>TP53</i> in <sub>2</sub> cells</Title></root>"
        root = parse_xml(xml)
        result = find_names_or_none(root, ["Title"])
        self.assertEqual(result, "Role of TP53 in 2 cells")

    def test_element_text_joins_descendants(self):
        """element_text mirrors Tag.text by joining nested text nodes."""
        root = parse_xml("<Title>a <i>b</i> c</Title>")
        self.assertEqual(element_text(root), "a b c")
