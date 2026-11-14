#!/usr/bin/env bash
#
# release.sh — publish a release that already exists locally to GitHub.
#
# The failure this exists to prevent: a session does all the engineering, tags
# the release locally, writes a commit message saying "released vX.Y.Z", and
# never pushes. The repo then looks released to whoever did the work and looks
# untouched (0.6.0, every issue open) to everybody else.
#
# What it does, in order, skipping anything already done:
#   1. checks the working tree is clean, on the release branch, and that the
#      version agrees across builder-config.yaml, examples/quickstart/manifest.yaml
#      and the README changelog
#   2. go build / go vet / go test (add --race for -race)
#   3. creates the annotated tag at HEAD if it is missing
#   4. pushes the branch, then the tag
#   5. creates the GitHub release, with notes taken from the README changelog
#   6. closes the issues referenced in those notes, commenting with the release
#      link (skip with --no-close-issues; PROTECTED_ISSUES are never closed)
#   7. re-reads remote state and prints it, so "released" is verified, not claimed
#
# Every step is idempotent. If it fails halfway — push fine, release creation
# failed — rerun the same command and it resumes.
#
# Usage:
#   scripts/release.sh                 # release the version in builder-config.yaml
#   scripts/release.sh v0.7.0          # release an explicit version
#   scripts/release.sh --dry-run       # print what it would do, change nothing
#   scripts/release.sh --skip-tests    # trust the last test run
#   scripts/release.sh --race          # run tests with -race
#   scripts/release.sh -y              # no confirmation prompt
#   scripts/release.sh --no-close-issues
#   scripts/release.sh --update-notes  # rewrite notes on an existing release
#
# Environment:
#   RELEASE_REMOTE      default origin
#   RELEASE_BRANCH      default main
#   PROTECTED_ISSUES    default 17 (space-separated issue numbers never closed)
#
set -euo pipefail

REMOTE="${RELEASE_REMOTE:-origin}"
BRANCH="${RELEASE_BRANCH:-main}"
PROTECTED_ISSUES="${PROTECTED_ISSUES:-17}"

VERSION=""
DRY_RUN=0
SKIP_TESTS=0
USE_RACE=0
ASSUME_YES=0
CLOSE_ISSUES=1
UPDATE_NOTES=0

usage() {
    sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'
    exit "${1:-0}"
}

log()  { printf '%s\n' "$*"; }
step() { printf '\n==> %s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

# Run a command, or print it under --dry-run.
run() {
    if [ "$#" -eq 0 ]; then return 0; fi
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '    [dry-run] %s\n' "$*"
        return 0
    fi
    "$@"
}

repo_root() {
    git rev-parse --show-toplevel 2>/dev/null || die "not inside a git repository"
}

trim() { sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'; }

# Notes for a version, lifted out of the README changelog.
extract_notes() {
    awk -v ver="$1" '
        substr($0, 1, 5) == "### v" { if (inblock) exit; if (substr($0, 6) == ver) { inblock = 1; next } }
        inblock && substr($0, 1, 3) == "## " { exit }
        inblock { print }
    ' README.md
}

# Strip leading/trailing blank lines.
strip_blank_edges() {
    awk 'NF { found = 1 } found { buf[++n] = $0 } END { while (n > 0 && buf[n] ~ /^[[:space:]]*$/) n--; for (i = 1; i <= n; i++) print buf[i] }'
}

# True when $1 < $2, both bare semver (no leading v).
version_lt() {
    awk -v a="${1#v}" -v b="${2#v}" 'BEGIN {
        n = split(a, A, "."); m = split(b, B, ".")
        k = (n > m ? n : m)
        for (i = 1; i <= k; i++) {
            x = A[i] + 0; y = B[i] + 0
            if (x < y) exit 0
            if (x > y) exit 1
        }
        exit 1
    }'
}

# The release-notes lines that mention a given issue, so the closing comment
# says what shipped rather than just "released".
issue_evidence() {
    printf '%s\n' "$NOTES" | grep -E "#${1}([^0-9]|$)" | sed -e 's/^[[:space:]]*[-*][[:space:]]*//' || true
}

issue_comment_body() {
    local n="$1" evidence
    evidence=$(issue_evidence "$n")
    printf 'Fixed in %s.\n\n' "$TAG"
    if [ -n "$evidence" ]; then
        printf '%s\n' "$evidence"
    else
        printf 'Resolved by the %s release; see the release notes for the details.\n' "$TAG"
    fi
    printf '\nRelease: %s\nCommit: %s\n' "$release_url" "https://github.com/$repo/commit/$head_sha"
}

issue_is_protected() {
    local want="$1" n
    for n in $PROTECTED_ISSUES; do
        [ "$n" = "$want" ] && return 0
    done
    return 1
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --dry-run|-n)       DRY_RUN=1 ;;
        --skip-tests)       SKIP_TESTS=1 ;;
        --race)             USE_RACE=1 ;;
        --yes|-y)           ASSUME_YES=1 ;;
        --no-close-issues)  CLOSE_ISSUES=0 ;;
        --update-notes)     UPDATE_NOTES=1 ;;
        --help|-h)          usage 0 ;;
        -*)                 die "unknown flag: $1 (try --help)" ;;
        *)
            [ -n "$VERSION" ] && die "version given twice: $VERSION and $1"
            VERSION="$1"
            ;;
    esac
    shift
done

cd "$(repo_root)"

# ---------------------------------------------------------------- preflight

step "preflight"

command -v gh >/dev/null 2>&1 || die "gh CLI not found"
gh auth status >/dev/null 2>&1 || die "gh is not authenticated (run: gh auth login)"

[ -z "$(git status --porcelain)" ] || {
    git status --short
    die "working tree is dirty; commit or stash first"
}

current_branch=$(git rev-parse --abbrev-ref HEAD)
[ "$current_branch" = "$BRANCH" ] || die "on branch '$current_branch', expected '$BRANCH'"

git remote get-url "$REMOTE" >/dev/null 2>&1 || die "no such remote: $REMOTE"

if [ "$DRY_RUN" -eq 0 ]; then
    git fetch --quiet --tags "$REMOTE" || die "git fetch from $REMOTE failed"
fi

if [ -z "$VERSION" ]; then
    VERSION=$(awk '/^dist:/{d=1} d && /^[[:space:]]+version:/{print $2; exit}' builder-config.yaml)
    [ -n "$VERSION" ] || die "could not read dist.version from builder-config.yaml; pass the version as an argument"
    log "    version from builder-config.yaml: $VERSION"
fi

VERSION="${VERSION#v}"
TAG="v$VERSION"
TAG="$(printf '%s' "$TAG" | tr -d '[:space:]')"
[ -n "$VERSION" ] || die "empty version"
case "$VERSION" in
    *[!0-9.]*|.*|*.) die "version '$VERSION' is not a bare X.Y.Z" ;;
esac

log "    tag:  $TAG"
log "    from: $REMOTE/$BRANCH"

# Version must agree everywhere a user can read it.
dist_version=$(awk '/^dist:/{d=1} d && /^[[:space:]]+version:/{print $2; exit}' builder-config.yaml)
[ "$dist_version" = "$VERSION" ] || \
    die "builder-config.yaml dist.version is '$dist_version', releasing '$VERSION'; fix it first"

manifest_bad=$(grep -L "logs-to-spans-otel v$VERSION" examples/quickstart/manifest.yaml || true)
[ -z "$manifest_bad" ] || \
    die "examples/quickstart/manifest.yaml does not reference v$VERSION; fix it first"

grep -q "logs-to-spans-otel v$VERSION" builder-config.yaml || \
    die "builder-config.yaml connectors[].gomod does not reference v$VERSION; fix it first"

grep -q "^### v$VERSION\$" README.md || \
    die "README.md has no '### v$VERSION' changelog section; write the release notes first"

NOTES=$(extract_notes "$VERSION" | strip_blank_edges)
[ -n "$NOTES" ] || die "README.md changelog section for v$VERSION is empty"

# Nothing published yet may be overwritten from here on: if the tag exists
# remotely it must already point at the commit we are about to push.
head_sha=$(git rev-parse HEAD)
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null 2>&1; then
    tag_sha=$(git rev-parse "$TAG^{commit}")
    if [ "$tag_sha" != "$head_sha" ]; then
        die "local tag $TAG points at $tag_sha, HEAD is $head_sha; move the tag or release the commit it points at"
    fi
    log "    local tag $TAG already exists at HEAD"
else
    latest_tag=$(git tag --sort=-v:refname | head -1)
    if [ -n "$latest_tag" ] && version_lt "$VERSION" "${latest_tag#v}"; then
        die "$TAG is older than the latest tag $latest_tag"
    fi
fi

log "    HEAD: ${head_sha:0:12} $(git log -1 --format=%s)"

# -------------------------------------------------------------------- gate

if [ "$SKIP_TESTS" -eq 1 ]; then
    step "tests (skipped: --skip-tests)"
else
    step "tests"
    run go build ./...
    run go vet ./...
    if [ "$USE_RACE" -eq 1 ]; then
        run go test -race -count=1 ./...
    else
        run go test -count=1 ./...
    fi
fi

# ------------------------------------------------------------- confirmation

if [ "$DRY_RUN" -eq 0 ] && [ "$ASSUME_YES" -eq 0 ]; then
    step "confirmation"
    log "Publishing $TAG from $REMOTE/$BRANCH at ${head_sha:0:12}:"
    log "  - push $BRANCH"
    log "  - push tag $TAG"
    log "  - create GitHub release $TAG"
    if [ "$CLOSE_ISSUES" -eq 1 ]; then
        log "  - close issues referenced in the notes (never: $PROTECTED_ISSUES)"
    fi
    printf 'Continue? [y/N] '
    read -r reply </dev/tty || reply=n
    case "$reply" in
        y|Y|yes|YES) ;;
        *) die "aborted; nothing was pushed" ;;
    esac
fi

# --------------------------------------------------------------------- tag

step "tag"
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null 2>&1; then
    log "    $TAG exists locally, leaving it alone"
else
    log "    creating annotated tag $TAG at ${head_sha:0:12}"
    run git tag -a "$TAG" -m "Release $TAG"
fi

# -------------------------------------------------------------------- push

step "push branch $BRANCH"
local_sha=$(git rev-parse "$BRANCH")
remote_sha=$(git ls-remote "$REMOTE" "refs/heads/$BRANCH" | awk 'NR==1{print $1}')
log "    local  ${local_sha:0:12}"
log "    remote ${remote_sha:0:12}"
if [ "$local_sha" = "$remote_sha" ]; then
    log "    already up to date"
else
    if [ -n "$remote_sha" ] && ! git merge-base --is-ancestor "$remote_sha" "$local_sha"; then
        die "$REMOTE/$BRANCH has commits you do not have; pull and re-run"
    fi
    run git push "$REMOTE" "$BRANCH"
fi

step "push tag $TAG"
remote_tag_sha=$(git ls-remote "$REMOTE" "refs/tags/$TAG^{}" | awk 'NR==1{print $1}')
if [ -z "$remote_tag_sha" ]; then
    remote_tag_sha=$(git ls-remote "$REMOTE" "refs/tags/$TAG" | awk 'NR==1{print $1}')
fi
if [ -n "$remote_tag_sha" ]; then
    if [ "$remote_tag_sha" = "$head_sha" ]; then
        log "    already published at ${head_sha:0:12}"
    else
        die "remote tag $TAG points at ${remote_tag_sha:0:12}, not ${head_sha:0:12}; refusing to rewrite a published tag"
    fi
else
    run git push "$REMOTE" "refs/tags/$TAG"
fi

# ----------------------------------------------------------------- release

step "GitHub release $TAG"
repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
release_url="https://github.com/$repo/releases/tag/$TAG"

notes_file=$(mktemp "${TMPDIR:-/tmp}/release-notes.XXXXXX")
printf '%s\n' "$NOTES" >"$notes_file"
trap 'rm -f "$notes_file"' EXIT

previous_tag=$(git tag --sort=-v:refname | grep -v "^${TAG}\$" | head -1)
if [ -n "$previous_tag" ]; then
    printf '\n**Full changelog**: https://github.com/%s/compare/%s...%s\n' \
        "$repo" "$previous_tag" "$TAG" >>"$notes_file"
fi

if gh release view "$TAG" >/dev/null 2>&1; then
    if [ "$UPDATE_NOTES" -eq 1 ]; then
        log "    release exists, updating notes (--update-notes)"
        run gh release edit "$TAG" --notes-file "$notes_file"
    else
        log "    release exists, leaving it alone (use --update-notes to rewrite the body)"
    fi
else
    log "    creating release from README changelog section ($(printf '%s\n' "$NOTES" | wc -l | tr -d ' ') lines)"
    run gh release create "$TAG" --title "$TAG" --notes-file "$notes_file" --verify-tag
fi

# ------------------------------------------------------------------ issues

step "issues"
closed=0
commented=0
issue_numbers=$(printf '%s\n' "$NOTES" | grep -oE '#[0-9]+' | tr -d '#' | sort -n -u || true)
if [ -z "$issue_numbers" ]; then
    log "    no issue references in the notes, nothing to close"
elif [ "$CLOSE_ISSUES" -eq 0 ]; then
    log "    closing disabled (--no-close-issues); referenced: $(printf '%s ' $issue_numbers)"
else
    for n in $issue_numbers; do
        state=$(gh issue view "$n" --json state --jq .state 2>/dev/null || echo "MISSING")
        case "$state" in
            MISSING)
                warn "#$n referenced in the notes but does not exist"
                continue
                ;;
            CLOSED)
                log "    #$n already closed"
                continue
                ;;
        esac
        if issue_is_protected "$n"; then
            log "    #$n protected, leaving open"
            continue
        fi
        body=$(issue_comment_body "$n")
        if [ "$DRY_RUN" -eq 1 ]; then
            log "    [dry-run] comment on #$n, then close:"
            printf '%s\n' "$body" | sed -e 's/^/      | /'
            continue
        fi
        # Comment first, close second: gh does both in one call, but a failed
        # comment must not leave the issue closed with no explanation.
        if gh issue comment "$n" --body "$body" >/dev/null 2>&1; then
            commented=$((commented + 1))
            log "    commented on #$n"
        else
            warn "could not comment on #$n; leaving it open"
            continue
        fi
        if gh issue close "$n" >/dev/null 2>&1; then
            closed=$((closed + 1))
            log "    closed #$n"
        else
            warn "commented on #$n but could not close it"
        fi
    done
fi

# ------------------------------------------------------------------- verify

step "verify"
if [ "$DRY_RUN" -eq 1 ]; then
    log "    dry run: nothing was pushed, so there is nothing to verify"
    exit 0
fi

ok=1

pushed_sha=$(git ls-remote "$REMOTE" "refs/heads/$BRANCH" | awk 'NR==1{print $1}')
if [ "$pushed_sha" = "$head_sha" ]; then
    log "    $REMOTE/$BRANCH -> ${pushed_sha:0:12} ok"
else
    log "    $REMOTE/$BRANCH -> ${pushed_sha:0:12} MISMATCH (expected ${head_sha:0:12})"
    ok=0
fi

published_tag_sha=$(git ls-remote "$REMOTE" "refs/tags/$TAG^{}" | awk 'NR==1{print $1}')
[ -n "$published_tag_sha" ] || published_tag_sha=$(git ls-remote "$REMOTE" "refs/tags/$TAG" | awk 'NR==1{print $1}')
if [ "$published_tag_sha" = "$head_sha" ]; then
    log "    tag $TAG -> ${published_tag_sha:0:12} ok"
else
    log "    tag $TAG -> '${published_tag_sha:0:12}' MISSING OR WRONG"
    ok=0
fi

if gh release view "$TAG" --json tagName,url --jq '"    release " + .tagName + " -> " + .url' 2>/dev/null; then
    :
else
    log "    release $TAG MISSING"
    ok=0
fi

if [ "$CLOSE_ISSUES" -eq 1 ]; then
    open_count=$(gh issue list --state open --limit 100 --json number --jq 'length')
    log "    commented on $commented issue(s), closed $closed, open now: $open_count"
fi

if [ "$ok" -eq 1 ]; then
    step "done: $TAG is published"
    log "    $release_url"
else
    step "incomplete: rerun this script once the problems above are fixed"
    exit 1
fi
