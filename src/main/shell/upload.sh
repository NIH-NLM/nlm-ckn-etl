#!/usr/bin/env bash
# Print usage
usage() {
    cat << EOF

NAME
    upload - Upload NLM-CKN ETL archives to S3

SYNOPSIS
    upload [OPTIONS]

DESCRIPTION
    Rename the obo and arangodb archives produced by etl.sh using the
    Java and Python package versions, then upload them along with a
    build info file to S3 under a date-stamped folder. Must be run
    from the src/main/shell directory.

    The archives are expected in the repo root:
      obo.tar.gz
      arangodb.tar.gz

    Files are uploaded to:
      s3://cell-kn-arangodb-data-952291113202/YYYY-MM-DD/obo-JAVA_VER-PY_VER.tar.gz
      s3://cell-kn-arangodb-data-952291113202/YYYY-MM-DD/arangodb-JAVA_VER-PY_VER.tar.gz
      s3://cell-kn-arangodb-data-952291113202/YYYY-MM-DD/build-info.txt

OPTIONS
    -h    Help

    -e    Exit immediately if a command returns a non-zero status

    -x    Print a trace of simple commands

EOF
}

# Parse command line options
while getopts ":hex" opt; do
    case $opt in
        h)
            usage
            exit 0
            ;;
        e)
            set -e
            ;;
        x)
            set -x
            ;;
        \?)
            echo "Invalid option: -${OPTARG}" >&2
            usage
            exit 1
            ;;
        \:)
            echo "Option -${OPTARG} requires an argument" >&2
            usage
            exit 1
            ;;
    esac
done

# Parse command line arguments
shift `expr ${OPTIND} - 1`
if [[ "$#" -ne 0 ]]; then
    echo "No arguments required"
    usage
    exit 1
fi

S3_BUCKET="cell-kn-arangodb-data-952291113202"

# Navigate to the repo root
pushd "../../.." > /dev/null

# Check that source archives exist
if [[ ! -f "obo.tar.gz" ]]; then
    echo "obo.tar.gz not found; run etl.sh -a first"
    exit 1
fi
if [[ ! -f "arangodb.tar.gz" ]]; then
    echo "arangodb.tar.gz not found; run etl.sh -a first"
    exit 1
fi

# Extract versions from manifests
java_version=$(grep -m1 '<version>' pom.xml | sed 's|.*<version>\(.*\)</version>.*|\1|' | tr -d '[:space:]')
py_version=$(grep -m1 '^version = ' python/pyproject.toml | sed 's/version = "\(.*\)"/\1/' | tr -d '[:space:]')
commit_hash=$(git rev-parse --short HEAD)
date_stamp=$(date +%Y-%m-%d)

version="${java_version}-${py_version}"
s3_prefix="s3://${S3_BUCKET}/${date_stamp}"

echo "Java version:   ${java_version}"
echo "Python version: ${py_version}"
echo "Commit hash:    ${commit_hash}"
echo "S3 prefix:      ${s3_prefix}/"

# Write build info file
build_info="build-info.txt"
cat > "$build_info" << EOF
Date:           ${date_stamp}
Commit:         ${commit_hash}
Java version:   ${java_version}
Python version: ${py_version}
EOF

# Rename archives with the combined version
obo_archive="obo-${version}.tar.gz"
arangodb_archive="arangodb-${version}.tar.gz"

mv obo.tar.gz "$obo_archive"
mv arangodb.tar.gz "$arangodb_archive"

# Upload to S3 under the date-stamped folder
aws s3 cp "$obo_archive" "${s3_prefix}/${obo_archive}" \
|| { echo "Failed to upload ${obo_archive}"; exit 1; }

aws s3 cp "$arangodb_archive" "${s3_prefix}/${arangodb_archive}" \
|| { echo "Failed to upload ${arangodb_archive}"; exit 1; }

aws s3 cp "$build_info" "${s3_prefix}/${build_info}" \
|| { echo "Failed to upload ${build_info}"; exit 1; }

echo "Uploaded to ${s3_prefix}/"

popd > /dev/null
