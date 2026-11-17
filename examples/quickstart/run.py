import failslice


def main() -> None:
    project = failslice.load_project("failslice.yaml")

    report = failslice.check(project)  # FK orphans + determinism of the predicate
    print("check ok:", report.ok)

    result = failslice.reduce(project, isolate=True)
    print(result.status, "|", result.minimality_label, "|", result.verification.status)
    print(result.stats.sizes_before, "->", result.stats.sizes_after)
    print("run directory:", result.run_dir)


if __name__ == "__main__":  # required: isolate=True starts a child process
    main()
