"""
MicroStrategy API constants.
"""

# Platform name
PLATFORM_NAME = "microstrategy"

# IServer REST errors (JSON body: iServerCode)
ISERVER_PROJECT_UNAVAILABLE = -2147209151
ISERVER_PROJECT_UNAVAILABLE_DETAIL = (
    "Project unavailable or not loaded on IServer (idle/unloaded). "
    "Try another project or load the project on the server; "
    "or set include_unloaded_projects: true only if you intend to ingest idle projects."
)

# Search API object types (/api/searches/results type=...)
SEARCH_OBJECT_TYPE_CUBE = 776
SEARCH_OBJECT_TYPE_WAREHOUSE_TABLE = 53
