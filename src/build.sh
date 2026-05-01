branch=$(git rev-parse --abbrev-ref HEAD)
commit=$(git rev-parse --short=12 HEAD)
version=${branch}-${commit}
build_date=$(date -u +%Y-%m-%dT%H:%M:%SZ)

docker build \
	--build-arg APP_VERSION="$version" \
	--build-arg GIT_COMMIT="$commit" \
	--build-arg GIT_BRANCH="$branch" \
	--build-arg BUILD_DATE="$build_date" \
	-t rbrandstaedter/solarflow-control:local .

docker image push rbrandstaedter/solarflow-control:local