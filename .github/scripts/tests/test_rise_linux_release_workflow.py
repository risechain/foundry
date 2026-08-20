import pathlib
import unittest


ROOT = pathlib.Path(__file__).parents[3]
WORKFLOW = ROOT / ".github" / "workflows" / "rise-linux-release.yml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
MONAD_WORKFLOW = ROOT / ".github" / "workflows" / "monad.yml"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
DOCKER_WORKFLOW = ROOT / ".github" / "workflows" / "docker-publish.yml"
MAKEFILE = ROOT / "Makefile"


class RiseLinuxReleaseWorkflowTest(unittest.TestCase):
    def workflow(self):
        self.assertTrue(WORKFLOW.is_file(), f"missing workflow: {WORKFLOW}")
        return WORKFLOW.read_text(encoding="utf-8")

    def test_builds_on_compatible_github_hosted_linux_runner(self):
        workflow = self.workflow()

        self.assertIn("build:\n    runs-on: ubuntu-22.04", workflow)
        self.assertNotIn("depot-", workflow)
        self.assertNotIn("self-hosted", workflow)

    def test_runs_for_changes_and_rise_release_tags(self):
        workflow = self.workflow()

        self.assertIn("pull_request:", workflow)
        self.assertIn("push:", workflow)
        self.assertIn("branches: [master]", workflow)
        self.assertIn('tags: ["rise-v*"]', workflow)
        self.assertIn("workflow_dispatch:", workflow)

    def test_fork_ci_excludes_unused_monad_jobs(self):
        ci_workflow = CI_WORKFLOW.read_text(encoding="utf-8")

        self.assertNotIn("uses: ./.github/workflows/monad.yml", ci_workflow)
        self.assertNotIn("      - monad\n", ci_workflow)
        self.assertFalse(MONAD_WORKFLOW.exists())

    def test_packaged_builds_exclude_unused_monad_feature(self):
        for build_path in (WORKFLOW, RELEASE_WORKFLOW, DOCKER_WORKFLOW, MAKEFILE):
            with self.subTest(build=build_path.name):
                build_config = build_path.read_text(encoding="utf-8")
                self.assertNotRegex(
                    build_config,
                    r"(?m)^(?:\s*RUST_FEATURES:|\s*FEATURES \?=).*\bmonad\b",
                )

    def test_validates_cpu_trace_and_builds_dist_binaries(self):
        workflow = self.workflow()

        self.assertIn(
            "dtolnay/rust-toolchain@6c977a6ca4077a0ceb28ffbe03f59d46e9ac8772",
            workflow,
        )
        self.assertIn("toolchain: 1.96", workflow)
        self.assertIn("test_cmd::cpu_trace::", workflow)
        self.assertIn(
            'run: "cargo test --locked -p forge --test cli '
            'test_cmd::cpu_trace:: -- --nocapture"',
            workflow,
        )
        self.assertIn('cargo build "${flags[@]}"', workflow)
        self.assertIn('TAG_NAME="${VERSION_NAME#rise-}"', workflow)
        self.assertIn("export TAG_NAME", workflow)
        self.assertIn('--profile "$RUST_PROFILE"', workflow)
        self.assertIn("--bins", workflow)
        self.assertIn("--no-default-features", workflow)
        self.assertIn('SVM_TARGET_PLATFORM: "linux-amd64"', workflow)

    def test_release_build_does_not_restore_mutable_cache(self):
        workflow = self.workflow()

        self.assertNotIn("Swatinem/rust-cache@", workflow)

    def test_packages_versioned_archive_and_checksum(self):
        workflow = self.workflow()

        self.assertIn('archive="foundry_${VERSION_NAME}_linux_amd64.tar.gz"', workflow)
        self.assertIn('checksum="foundry_${VERSION_NAME}_linux_amd64.sha256"', workflow)
        self.assertIn(
            'tar -czf "$archive" -C "$OUT_DIR" forge cast anvil chisel solar', workflow
        )
        self.assertIn('sha256sum "$archive" > "$checksum"', workflow)
        self.assertIn(
            "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a", workflow
        )

    def test_publishes_only_trusted_rise_tags_with_provenance(self):
        workflow = self.workflow()

        self.assertIn("if: startsWith(github.ref, 'refs/tags/rise-v')", workflow)
        self.assertIn("contents: write", workflow)
        self.assertIn("attestations: write", workflow)
        self.assertIn("id-token: write", workflow)
        self.assertIn(
            "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c", workflow
        )
        self.assertIn("actions/attest@508db95dd578ae2727ebd6217d5ba78e4fbda05d", workflow)
        self.assertIn('gh release create "$GITHUB_REF_NAME"', workflow)
        self.assertIn("--draft", workflow)
        self.assertIn("--verify-tag", workflow)

    def test_marks_cpu_builds_as_prereleases(self):
        workflow = self.workflow()

        self.assertIn('[[ "$GITHUB_REF_NAME" == *-cpu.* ]]', workflow)
        self.assertIn("release_args+=(--prerelease)", workflow)

    def test_release_publish_is_safe_to_rerun(self):
        workflow = self.workflow()

        self.assertIn('gh release view "$GITHUB_REF_NAME"', workflow)
        self.assertIn('gh release upload "$GITHUB_REF_NAME"', workflow)
        self.assertIn("--clobber", workflow)
        self.assertIn('gh release edit "$GITHUB_REF_NAME" --draft=false', workflow)
        self.assertIn('gh release download "$GITHUB_REF_NAME"', workflow)
        self.assertIn('sha256sum --check "$checksum_name"', workflow)
        self.assertNotIn("cmp --silent", workflow)

    def test_rejects_malformed_rise_tag_before_compilation(self):
        workflow = self.workflow()

        self.assertIn("- name: Validate Rise release tag", workflow)
        validation = workflow.index("- name: Validate Rise release tag")
        compilation = workflow.index("- name: Test CPU tracing")
        self.assertLess(validation, compilation)
        self.assertIn(
            "^rise-v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\."
            "(0|[1-9][0-9]*)(-cpu\\.([1-9][0-9]*))?$",
            workflow,
        )


if __name__ == "__main__":
    unittest.main()
