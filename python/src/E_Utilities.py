import json
import os
from pathlib import Path
from pprint import pprint
import re
from time import sleep
from urllib import parse

import bs4
import requests

EUTILS_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
NCBI_EMAIL = os.environ.get("NCBI_EMAIL")
NCBI_API_KEY = os.environ.get("NCBI_API_KEY")
NCBI_API_SLEEP = 1


def find_names_or_none(soup, names, attribute=None):
    """Find the text, or specified attribute, in the last named tag,
    if all previously named tags are found.

    Parameters
    ----------
    soup : bs4.element.Tag
        Any soup returned by BeautifulSoup
    names : list(str)
        List of tag names to find in order
    attribute : str
        Attribute of the last named tag

    Returns
    -------
    str
        text, or attribute, in the last named tag, or None
    """
    soup = soup.find(names[0])
    for name in names[1:]:
        if soup:
            soup = soup.find(name)
    if soup:
        if attribute:
            return soup.get(attribute)
        else:
            return soup.text
    else:
        return soup


def get_xml_for_pmid(pmid):
    """Fetch XML from PubMed describing a PMID.

    Parameters
    ----------
    pmid : str
        The PubMed identifier to use in the fetch

    Returns
    -------
    pmid_xml : str
       String containing the response text XML for a specified PMID
    """
    # Need a default return value
    pmid_xml = None

    # Fetch from PubMed
    print(f"Getting data for PMID: '{pmid}'")
    fetch_url = EUTILS_URL + "efetch.fcgi"
    params = {
        "db": "pubmed",
        "id": pmid,
        "rettype": "xml",
        "email": NCBI_EMAIL,
        "api_key": NCBI_API_KEY,
    }
    sleep(NCBI_API_SLEEP)
    response = requests.get(fetch_url, params=parse.urlencode(params, safe=","))
    if response.status_code == 200:
        pmid_xml = response.text

    else:
        print(f"Encountered error in fetching from PubMed: {response.status_code}")

    return pmid_xml


def extract_pmid_data_from_pmid_xml(pmid_xml):
    """Extract all required fields from the PMID XML.

    Parameters
    ----------
    pmid_xml : str
       String containing the response text XML for a specified PMID

    Returns
    -------
    pmid_data : dict
        Dictionary containing all required fields
    """
    pmid_data = {}
    root = bs4.BeautifulSoup(pmid_xml, "xml").find("Article")
    if root:
        pmid_data["Author"] = find_names_or_none(
            root, ["AuthorList", "Author", "LastName"]
        )  # First author
        if len(find_names_or_none(root, ["AuthorList"])) > 1:
            pmid_data["Author"] += " et al."
        pmid_data["Journal"] = find_names_or_none(root, ["Journal", "ISOAbbreviation"])
        pmid_data["Title"] = find_names_or_none(root, ["ArticleTitle"])
        pmid_data["Year"] = find_names_or_none(root, ["ArticleDate", "Year"])
        pmid_data["Citation"] = (
            f"{pmid_data['Author']} ({pmid_data['Year']}) {pmid_data['Journal']}"
        )
    return pmid_data


def find_gene_id_for_gene_name(name, do_write=False):
    """Search Gene using a gene name to find the corresponding gene
    id.

    Parameters
    ----------
    name : str
       The gene name for which to search
    do_write : bool
        Flag to write fetched results, or not (default: False)

    Returns
    -------
    str
       The gene id
    """
    # Need a default return value
    gene_id = None

    # Search Gene
    print(f"Searching Gene for name: '{name}'")
    search_url = EUTILS_URL + "esearch.fcgi"
    params = {
        "db": "gene",
        "term": f"{name}[Gene Name] AND 9606[Taxonomy ID]",
        "sort": "relevance",
        "retmax": 1,
        "retmode": "json",
        "email": NCBI_EMAIL,
        "api_key": NCBI_API_KEY,
    }
    sleep(NCBI_API_SLEEP)
    response = requests.get(search_url, params=parse.urlencode(params, safe=","))
    if response.status_code == 200:
        json_data = response.json()
        if do_write:
            with open(f"{name}.json", "w") as fp:
                json.dump(json_data, fp, indent=4)

        # Got the response, so assign the gene id
        if len(json_data["esearchresult"]["idlist"]) > 0:
            gene_id = json_data["esearchresult"]["idlist"][0]
            print(f"Found gene id {gene_id} while searching Gene for name {name}")

        else:
            print(f"No gene id found while searching Gene for name {name}")

    else:
        print(
            f"Encountered error in searching Gene for name {name}: {response.status_code}"
        )

    return gene_id


def get_xml_for_gene_id(gene_id):
    """Fetch XML from Gene describing a gene id.

    Parameters
    ----------
    gene_id : str
        The Gene identifier to use in the fetch

    Returns
    -------
    gene_xml : str
       String containing the response text XML for a specified gene id
    """
    # Need a default return value
    gene_xml = None

    # Fetch from Gene
    print(f"Getting data for gene id: '{gene_id}'")
    fetch_url = EUTILS_URL + "efetch.fcgi"
    params = {
        "db": "gene",
        "id": gene_id,
        "retmode": "xml",
        "email": NCBI_EMAIL,
        "api_key": NCBI_API_KEY,
    }
    sleep(NCBI_API_SLEEP)
    response = requests.get(fetch_url, params=parse.urlencode(params, safe=","))
    if response.status_code == 200:
        gene_xml = response.text

    else:
        print(f"Encountered error in fetching from Gene: {response.status_code}")

    return gene_xml


def extract_gene_data_from_gene_xml(gene_xml):
    """Extract all required fields from the gene XML.

    Parameters
    ----------
    gene_xml : str
       String containing the response text XML for a specified gene id

    Returns
    -------
    gene_data : dict
        Dictionary containing all required fields
    """
    gene_data = {}
    tags = bs4.BeautifulSoup(gene_xml, "xml").find_all("Entrezgene")
    if len(tags) > 1:
        raise Exception("Expect a single Entrezgene element")
    root = tags[0]
    # TODO: Find the gene id in the gene xml
    # gene_data["Gene_ID"] = gene_id
    gene_data["Official_symbol"] = find_names_or_none(
        root,
        [
            "Entrezgene_gene",
            "Gene-ref",
            "Gene-ref_formal-name",
            "Gene-nomenclature_symbol",
        ],
    )
    gene_data["Official_full_name"] = find_names_or_none(
        root,
        [
            "Entrezgene_gene",
            "Gene-ref",
            "Gene-ref_formal-name",
            "Gene-nomenclature_name",
        ],
    )
    gene_data["Gene_type"] = find_names_or_none(
        root, ["Entrezgene_type"], attribute="value"
    )
    for child in root.find_all("Other-source_url"):
        if "www.uniprot.org" in child.text:
            gene_data["Link_to_UniProt_ID"] = child.text
    gene_data["Organism"] = find_names_or_none(
        root,
        [
            "Entrezgene_source",
            "BioSource",
            "BioSource_org",
            "Org-ref",
            "Org-ref_taxname",
        ],
    )
    gene_data["RefSeq_gene_ID"] = None
    for child in root.find_all("Gene-commentary_heading"):
        if "GCF_" in child.text:
            m = re.search(r":\s*(GCF_.*)", child.text)
            if m:
                gene_data["RefSeq_gene_ID"] = m.group(1)
    gene_data["Also_known_as"] = []
    for child in root.find_all("Gene-ref_syn_E"):
        gene_data["Also_known_as"].append(child.text)
    gene_data["Summary"] = find_names_or_none(root, ["Entrezgene_summary"])
    pr_desc = find_names_or_none(root, ["Entrezgene_prot", "Prot-ref_desc"])
    gene_data["UniProt_name"] = Path(
        parse.urlparse(gene_data["Link_to_UniProt_ID"]).path
    ).stem
    for product in root.find_all("Gene-commentary_products"):
        if find_names_or_none(product, ["Gene-commentary_type"], "value") == "mRNA":
            nm_id = None
            np_id = None
            for accession in product.find_all("Gene-commentary_accession"):
                if "NM_" in accession.text:
                    nm_id = accession.text
                elif "NP_" in accession.text:
                    np_id = accession.text
            if nm_id and np_id and pr_desc:
                gene_data["mRNA_(NM)_and_protein_(NP)_sequences"] = (
                    f"{nm_id} -> {np_id}, {pr_desc}"
                )
            break

    return gene_data


def main():
    pmid_xml = get_xml_for_pmid("37291214")
    pprint(extract_pmid_data_from_pmid_xml(pmid_xml))
    gene_xml = get_xml_for_gene_id(3777)
    pprint(extract_gene_data_from_gene_xml(gene_xml))


if __name__ == "__main__":
    main()
