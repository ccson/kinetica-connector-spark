#!/usr/bin/env bash
# The directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR=${SCRIPT_DIR}/..
#import common functions
source $SCRIPT_DIR/make-dist-common.sh

LOG="$SCRIPT_DIR/makerpm.log"

DIST_DIR="$SCRIPT_DIR/dist"
SKIP_BUILD=0

while [[ $# > 0 ]]; do
    key="$1"
    shift

    case $key in
        -d|--dist-dir)
            DIST_DIR="$1"
            shift
            ;;
        -s|--skip-build)
            SKIP_BUILD=1
            ;;
        *)
            echo "Unknown option: '$key', exiting."
            echo "$USAGE_STR"
            exit 1
    ;;
esac
done

# ---------------------------------------------------------------------------
# make sure we have a clean build
if [ $SKIP_BUILD -eq 0 ]; then
    pushd_cmd $PROJECT_DIR
    run_cmd "mvn clean package -DskipTests"
    popd_cmd
    mkdir -p $DIST_DIR
    rm -rf $DIST_DIR/*

    run_cmd "cp -p $PROJECT_DIR/target/gpudb-spark*.jar $DIST_DIR"
    run_cmd "cp -p $PROJECT_DIR/README.md $DIST_DIR"
    run_cmd "cp -p $PROJECT_DIR/example.sh $DIST_DIR"
    run_cmd "cp -p $PROJECT_DIR/example.properties $DIST_DIR"
fi

# ---------------------------------------------------------------------------
# Ensure that the install directory exists
DIST_DIR=$(readlink -m $DIST_DIR)
if [ ! -d $DIST_DIR ]; then
    echo "ERROR: Install directory $DIST_DIR does not exist!"
    echo $USAGE_STR
    exit 1
fi

# ---------------------------------------------------------------------------
# Clean up and create RPM directories
RPM_BUILD_DIR=$SCRIPT_DIR/rpmbuild
for dir in SPECS SOURCES RPMS BUILDROOT
do
    [[ -d $RPM_BUILD_DIR/$dir ]] && rm -Rf $RPM_BUILD_DIR/$dir
    mkdir -p $RPM_BUILD_DIR/$dir
done

# ---------------------------------------------------------------------------
# Grab version
pushd $DIST_DIR
VERSION=$(find *.jar | tail -1 | awk -F'-' '{print $3}' )
popd
log "Detected version: $VERSION"

# ---------------------------------------------------------------------------
# Archive files
TARBALL=$RPM_BUILD_DIR/SOURCES/files.tgz
pushd_cmd $DIST_DIR
run_cmd "tar -cvzf $TARBALL *"
popd_cmd

# ---------------------------------------------------------------------------
# Copy and fill in the SPEC file.
SPEC_FILE=$RPM_BUILD_DIR/SPECS/gpudb-connector-spark.spec
run_cmd "cp $SCRIPT_DIR/gpudb-connector-spark.spec $SPEC_FILE"

# Add the list of files to the .spec file automatically, so we don't have to write them all out.
INSTALL_FILES="$SCRIPT_DIR/install-files.txt"
get_file_attrs $DIST_DIR $INSTALL_FILES '' '' ''
echo
echo INSTALL_FILES=
cat $INSTALL_FILES
echo

run_cmd "sed -i -e \"/TEMPLATE_RPM_FILES/{r \"$INSTALL_FILES\"\" -e 'd}' $SPEC_FILE"
run_cmd "sed -i s/TEMPLATE_RPM_VERSION/\"$VERSION\"/g $SPEC_FILE"
run_cmd "sed -i s/TEMPLATE_RPM_RELEASE/\"$(get_git_build_number)\"/g $SPEC_FILE"

if grep TEMPLATE_RPM_ $SPEC_FILE ; then
    echo "ERROR: There's some unconfigured TEMPLATE_RPM_* variables in $SPEC_FILE"
    exit 1
fi

# ---------------------------------------------------------------------------
# Run RPMBuild
pushd_cmd $RPM_BUILD_DIR
run_cmd "rpmbuild -vv --define \"_topdir $(pwd)\" -bb $SPEC_FILE"
#run_cmd "find RPMS/ -type f -name \"*.rpm\" -exec sh -c 'RPM={}; cd ${RPM%/*}; FILE=${RPM##*/}; md5sum ${FILE} > ${FILE%.*}.md5; rpm -qlp ${FILE} > ${FILE%.*}.mf; cd -' \;"
popd_cmd

echo "SUCCESS!"
