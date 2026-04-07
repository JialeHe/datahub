## Overview

GitHub is a platform for version control and collaboration. Learn more in the [official GitHub documentation](https://docs.github.com).

The DataHub integration for GitHub Docs imports documents from GitHub repositories as DataHub Document entities, preserving the folder hierarchy as parent-child relationships. It supports both native document import (full content copy) and external reference mode, with stateful ingestion for automatic stale document removal.

## Concept Mapping

| Source Concept    | DataHub Concept | Notes                                                             |
| ----------------- | --------------- | ----------------------------------------------------------------- |
| Repository        | Source config    | Each ingestion source targets one repository and branch.          |
| File (.md, .txt)  | Document         | Imported as native or external document based on configuration.   |
| Directory         | Document         | Optionally created as parent documents to preserve hierarchy.     |
| Branch + Commit   | Custom property  | Stored as provenance metadata on each document.                   |
