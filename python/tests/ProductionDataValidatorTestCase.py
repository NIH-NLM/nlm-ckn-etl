import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd

import ProductionDataSpecification as spec
import ProductionDataValidator as v


class SpecConsistencyTestCase(unittest.TestCase):
    """The spec must stay the single source of truth for the readers."""

    def test_min_cluster_size_single_sourced(self):
        import LoaderUtilities

        self.assertEqual(LoaderUtilities.MIN_CLUSTER_SIZE, spec.MIN_CLUSTER_SIZE)

    def test_obo_purl_to_curie_matches_reader(self):
        from TupleWriterUtilities import purl_to_curie

        for s in (
            "http://purl.obolibrary.org/obo/CL_0000235",
            "https://purl.obolibrary.org/obo/UBERON_0002048",
            "CL:0000235",
            "not-a-purl",
        ):
            self.assertEqual(spec.obo_purl_to_curie(s), purl_to_curie(s))

    def test_companion_basename(self):
        ns = "results_ensg_liver_Foo_2020_ct_X_umap_ab12cd.csv"
        self.assertEqual(
            spec.companion_basename(ns, spec.SUMMARY_PREFIX),
            "master_dataset_summary_liver_Foo_2020_ct_X_umap_ab12cd.csv",
        )


class ValidatorFixtureTestCase(unittest.TestCase):
    """Each check fires on a purpose-built violation and stays quiet on a
    well-formed tree."""

    def _write(self, path, df):
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)

    def _good_tree(self, root):
        """Build a minimal, conformant dataset under *root*."""
        stem = "liver_Foo_2020_ct_X_umap_ab12cd"
        ds = root / "liver" / "results" / "liver-Foo-2020"
        self._write(
            ds / f"results_ensg_{stem}.csv",
            pd.DataFrame(
                {
                    "cluster_header": ["ct", "ct"],
                    "clusterName": ["A", "B"],
                    "clusterSize": [50, 60],
                    "f_score": [0.9, 0.8],
                    "NSForest_markers": ["['ENSG1']", "['ENSG2']"],
                    "binary_genes": ["['ENSG1']", "['ENSG2']"],
                }
            ),
        )
        self._write(
            ds / f"master_dataset_summary_{stem}.csv",
            pd.DataFrame({"dataset_version_id": ["dv-1"], "organ": ["liver"]}),
        )
        self._write(
            ds / f"silhouette_fscore_summary_{stem}.csv",
            pd.DataFrame(
                {
                    "ct": ["A", "B"],
                    "mean": [0.1, 0.2],
                    "std": [0.0, 0.0],
                    "median": [0.1, 0.2],
                    "count": [50, 60],
                    "q1": [0.0, 0.1],
                    "q3": [0.2, 0.3],
                    "f_score": [0.9, 0.8],
                }
            ),
        )
        self._write(
            root / "liver" / "reference" / f"cluster_cid_mapping_{stem}.csv",
            pd.DataFrame(
                {
                    "cluster_name": ["A", "B"],
                    "skos": ["exactMatch", "exactMatch"],
                    "manual_mapped_cid": ["CL:0000235", "CL:0000236"],
                    "cell_ontology_id": ["CL:0000235", "CL:0000236"],
                }
            ),
        )
        self._write(
            root / "liver" / "cellxgene-harvester" / "homo_sapiens_liver_harvester_final.csv",
            pd.DataFrame({"dataset_version_id": ["dv-1"]}),
        )
        self._write(
            ds / "master_s3_manifest.csv",
            pd.DataFrame(
                {"filename": [f"results_ensg_{stem}.csv"], "s3_path": ["s3://x/y"]}
            ),
        )
        return f"results_ensg_{stem}.csv", stem, ds

    def test_good_tree_has_no_errors(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            self._good_tree(root)
            report = v.validate(root)
            self.assertEqual(report.errors(), [], msg=[vars(f) for f in report.errors()])

    def test_no_results(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            report = v.validate(Path(d))
            self.assertTrue(any(f.check == "no-results" for f in report.errors()))

    def test_missing_summary_companion(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            _, stem, ds = self._good_tree(root)
            (ds / f"master_dataset_summary_{stem}.csv").unlink()
            report = v.validate(root)
            self.assertTrue(any(f.check == "companion-missing" for f in report.errors()))

    def test_cluster_header_missing_with_silhouette(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            name, stem, ds = self._good_tree(root)
            rp = ds / name
            df = pd.read_csv(rp).drop(columns=["cluster_header"])
            df.to_csv(rp, index=False)
            report = v.validate(root)
            self.assertTrue(
                any(f.check == "cluster-header-missing" for f in report.errors())
            )

    def test_silhouette_join_column_mismatch(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            name, stem, ds = self._good_tree(root)
            sp = ds / f"silhouette_fscore_summary_{stem}.csv"
            df = pd.read_csv(sp).rename(columns={"ct": "WRONG"})
            df.to_csv(sp, index=False)
            report = v.validate(root)
            self.assertTrue(
                any(f.check == "silhouette-join-column" for f in report.errors())
            )

    def test_flatten_collision(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            name, stem, ds = self._good_tree(root)
            # Same results basename in a second dataset dir → collision.
            dup = root / "liver" / "results" / "liver-Foo-2020-DUP" / name
            self._write(dup, pd.read_csv(ds / name))
            report = v.validate(root)
            self.assertTrue(any(f.check == "flatten-collision" for f in report.errors()))

    def test_mapping_bad_cl_curie_warns(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            _, stem, _ = self._good_tree(root)
            mp = root / "liver" / "reference" / f"cluster_cid_mapping_{stem}.csv"
            df = pd.read_csv(mp)
            df.loc[0, "manual_mapped_cid"] = "garbage"
            df.loc[0, "cell_ontology_id"] = "garbage"
            df.to_csv(mp, index=False)
            report = v.validate(root)
            self.assertTrue(any(f.check == "mapping-cl-curie" for f in report.warnings()))


if __name__ == "__main__":
    unittest.main()
