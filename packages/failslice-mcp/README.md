# failslice-mcp

The MCP server of [FailSlice](https://github.com/sdmhdn/failslice) for AI agents (Claude
Code, Claude Desktop, Cursor and other MCP clients). It runs FailSlice's loop over stdio:
inspect the files, check a failure predicate, reduce the data to a small FK-consistent
reproducer, verify it in a fresh process and export a pytest regression test.

This distribution only provides the `failslice-mcp` command; the server is part of the
[`failslice`](https://pypi.org/project/failslice/) package, pinned to the same version.

```bash
uvx failslice-mcp --root /absolute/path/to/your/data
```

Every path an agent passes is resolved inside `--root`. Client configuration, tools and
resources: [docs/mcp.md](https://github.com/sdmhdn/failslice/blob/main/docs/mcp.md).

FailSlice isolates rows; it does not determine the root cause.

Created and maintained by Saeed Mahdian. MIT license.

<!-- mcp-name: io.github.sdmhdn/failslice -->
