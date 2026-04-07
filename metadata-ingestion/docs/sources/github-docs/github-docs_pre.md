### Overview

The GitHub Docs source imports documents from a GitHub repository into DataHub using the GitHub REST API. No git clone or local disk I/O is required.

#### Key Features

##### 1. Native and External Import Modes

- **Native mode** (default): Full content is copied into DataHub as a first-class document, independent of GitHub
- **External mode**: Documents are created as external references linking back to GitHub, with content indexed for search

##### 2. Folder Hierarchy

- Preserves the GitHub directory structure as parent-child document relationships
- Folder documents are automatically created for intermediate directories
- Can be disabled with `preserve_hierarchy: false` for flat import

##### 3. Stateful Ingestion

- Tracks emitted document URNs across runs
- Automatically removes documents that no longer exist in the repository
- Enable with `stateful_ingestion.enabled: true`

##### 4. Flexible Filtering

- Filter by file extension (`.md`, `.txt`, `.rst`, etc.)
- Filter by path prefix to target specific directories
- Allow/deny path lists for fine-grained control
- Configurable file size and document count limits

### Prerequisites

#### 1. GitHub Token

Create a Personal Access Token (PAT) or fine-grained token:

1. Go to https://github.com/settings/tokens
2. Click **"Generate new token"** (classic or fine-grained)
3. Grant **"Contents: Read"** permission (fine-grained) or **"repo"** scope (classic)
4. Copy the token

#### 2. Repository Access

The token must have read access to the target repository. For private repositories, ensure the token owner has access.
