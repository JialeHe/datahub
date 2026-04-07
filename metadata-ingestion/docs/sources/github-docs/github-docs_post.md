### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features.

#### Common Use Cases

##### 1. Import All Markdown from a Repo (Default)

```yaml
source:
  type: github-docs
  config:
    repo: "my-org/my-repo"
    token: "${GITHUB_TOKEN}"
```

##### 2. Import a Specific Directory

```yaml
source:
  type: github-docs
  config:
    repo: "my-org/my-repo"
    token: "${GITHUB_TOKEN}"
    path_prefix: "docs/guides"
    file_extensions: [".md"]
```

##### 3. External Reference Mode

Import documents as external references linking back to GitHub instead of copying the full content:

```yaml
source:
  type: github-docs
  config:
    repo: "my-org/my-repo"
    token: "${GITHUB_TOKEN}"
    import_as_native: false
```

##### 4. AI-Only Context Documents

Import documents hidden from the global sidebar, accessible only through related assets:

```yaml
source:
  type: github-docs
  config:
    repo: "my-org/my-repo"
    token: "${GITHUB_TOKEN}"
    path_prefix: "context"
    show_in_global_context: false
```

##### 5. Flat Import (No Hierarchy)

Import files without creating folder documents:

```yaml
source:
  type: github-docs
  config:
    repo: "my-org/my-repo"
    token: "${GITHUB_TOKEN}"
    preserve_hierarchy: false
```

##### 6. Production Setup with Stateful Ingestion

```yaml
source:
  type: github-docs
  config:
    repo: "my-org/my-repo"
    token: "${GITHUB_TOKEN}"
    path_prefix: "docs"
    file_extensions: [".md", ".rst"]
    max_documents: 5000
    stateful_ingestion:
      enabled: true
```

##### 7. Path Allow/Deny Filtering

```yaml
source:
  type: github-docs
  config:
    repo: "my-org/my-repo"
    token: "${GITHUB_TOKEN}"
    paths:
      allow:
        - "docs/public"
        - "docs/guides"
      deny:
        - "docs/public/internal"
```

### Limitations

#### GitHub API Limits

- **Rate limits**: Authenticated requests are limited to 5,000 per hour. Each file requires one API call, so large repositories may hit limits.
- **Tree API**: The recursive tree endpoint works for repositories with fewer than 100,000 entries.
- **File size**: Files larger than 1 MB (configurable via `max_file_size_bytes`) are skipped. GitHub's Contents API has a 100 MB hard limit.

#### Content Support

- **Supported formats**: Any text-based file format (`.md`, `.txt`, `.rst`, `.html`, `.yaml`, `.json`, etc.)
- **Not supported**: Binary files (images, PDFs, DOCX) are not extracted. Only files matching `file_extensions` are processed.

#### Performance Considerations

- Each file is fetched individually via the GitHub Contents API (no batch endpoint exists).
- For repositories with thousands of matching files, consider using `max_documents` to limit the import.
- First run imports all matching files. Subsequent runs with stateful ingestion only detect deletions; content change detection is not yet implemented.

### Troubleshooting

#### Common Issues

**"401 Unauthorized" errors:**

- Verify the GitHub token is valid and not expired
- Ensure the token has `Contents: Read` permission

**"404 Not Found" errors:**

- Check that the repository identifier is correct (`owner/repo`)
- Verify the branch name exists
- For private repositories, ensure the token owner has access

**No files discovered:**

- Check `path_prefix` matches the actual directory structure (no leading slash)
- Verify `file_extensions` includes the target file types (e.g., `[".md"]`)
- Check `paths.deny` isn't excluding everything

**Rate limit errors:**

- Reduce `max_documents` to import fewer files per run
- Schedule ingestion during off-peak hours
- Use a token with higher rate limits (GitHub Apps get 5,000 requests per installation per hour)
