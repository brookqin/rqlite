#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="${1:-${repo_root}/.qiankun-baseline}"
mkdir -p "${output_dir}"

for required_tool in git go jq file shasum perl; do
  if ! command -v "${required_tool}" >/dev/null 2>&1; then
    printf 'required tool is missing: %s\n' "${required_tool}" >&2
    exit 1
  fi
done

clean_cache="$(mktemp -d "${TMPDIR:-/tmp}/qiankun-rqlite-gocache.XXXXXX")"
cleanup() {
  case "${clean_cache}" in
    "${TMPDIR:-/tmp}"/qiankun-rqlite-gocache.*)
      rm -rf "${clean_cache}"
      ;;
    *)
      printf 'refusing to remove unexpected cache path: %s\n' "${clean_cache}" >&2
      ;;
  esac
}
trap cleanup EXIT

cd "${repo_root}"

git status --porcelain=v1 >"${output_dir}/git-status.txt"
git rev-parse HEAD >"${output_dir}/git-commit.txt"
git describe --tags --always --dirty >"${output_dir}/git-describe.txt"
git remote -v >"${output_dir}/git-remotes.txt"
go env -json >"${output_dir}/go-env.json"
go version >"${output_dir}/go-version.txt"
go mod graph >"${output_dir}/module-graph.txt"
go list -m -json all >"${output_dir}/modules.json"
go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./cmd/rqlited | sed '/^$/d' | sort -u >"${output_dir}/rqlited-dependencies.txt"
go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./store ./db ./command/... | sed '/^$/d' | sort -u >"${output_dir}/core-dependencies.txt"
{
  printf '%s\n' 'cyclonedx-gomod=v1.10.0'
  printf '%s\n' 'go-licenses=v2.0.1'
} >"${output_dir}/baseline-tool-versions.txt"
go run github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.10.0 \
  mod -licenses -json -output "${output_dir}/bom.json" .
jq '{
  spec_version: .specVersion,
  component_count: (.components | length),
  dependency_count: (.dependencies | length),
  components_without_license_evidence: [
    .components[]
    | select(((.licenses // []) | length) == 0 and ((.evidence.licenses // []) | length) == 0)
    | {name, version, purl}
  ]
}' "${output_dir}/bom.json" >"${output_dir}/bom-summary.json"
go run github.com/google/go-licenses/v2@v2.0.1 report ./cmd/rqlited \
  >"${output_dir}/licenses.csv"

now_milliseconds() {
  perl -MTime::HiRes=time -e 'printf "%.0f\n", time() * 1000'
}

clean_started="$(now_milliseconds)"
env GOCACHE="${clean_cache}" go build -trimpath -o "${output_dir}/rqlited-clean" ./cmd/rqlited
clean_finished="$(now_milliseconds)"

warm_started="$(now_milliseconds)"
env GOCACHE="${clean_cache}" go build -trimpath -o "${output_dir}/rqlited-warm" ./cmd/rqlited
warm_finished="$(now_milliseconds)"

{
  printf 'clean_milliseconds=%s\n' "$((clean_finished - clean_started))"
  printf 'warm_milliseconds=%s\n' "$((warm_finished - warm_started))"
  printf 'clean_bytes=%s\n' "$(wc -c <"${output_dir}/rqlited-clean" | tr -d ' ')"
  printf 'warm_bytes=%s\n' "$(wc -c <"${output_dir}/rqlited-warm" | tr -d ' ')"
} >"${output_dir}/build-metrics.txt"

file "${output_dir}/rqlited-clean" >"${output_dir}/binary-file.txt"
shasum -a 256 "${output_dir}/rqlited-clean" >"${output_dir}/binary-sha256.txt"
go version -m "${output_dir}/rqlited-clean" >"${output_dir}/binary-buildinfo.txt"
"${output_dir}/rqlited-clean" -version >"${output_dir}/binary-version.txt" 2>&1

case "$(go env GOOS)" in
  darwin)
    otool -L "${output_dir}/rqlited-clean" >"${output_dir}/binary-dynamic-dependencies.txt"
    ;;
  linux)
    ldd "${output_dir}/rqlited-clean" >"${output_dir}/binary-dynamic-dependencies.txt"
    ;;
  *)
    printf 'dynamic dependency inspection not implemented for GOOS=%s\n' "$(go env GOOS)" >"${output_dir}/binary-dynamic-dependencies.txt"
    ;;
esac

go run ./tools/qiankun-baseline >"${output_dir}/sqlite-self-check.json"
go test -count=1 -run 'Test_(OpenEmptyInWALMode|TableCreationFK|DB_CompileOptions|DB_Backup)$' ./db >"${output_dir}/sqlite-focused-tests.txt"
go test -count=1 -run 'Test_(SingleNodeSnapshot|OpenStoreCloseUserSnapshot)$' ./store >"${output_dir}/snapshot-focused-tests.txt"

printf 'baseline artifacts: %s\n' "${output_dir}"
