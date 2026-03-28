"""
MicroStrategy REST API Client.

Handles:
- Authentication and session management
- Request retry logic and error handling
- Pagination
- Token refresh for long-running ingestions
- Guaranteed session cleanup
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.microstrategy.config import (
    MicroStrategyConnectionConfig,
)
from datahub.ingestion.source.microstrategy.constants import (
    ISERVER_PROJECT_UNAVAILABLE,
    ISERVER_PROJECT_UNAVAILABLE_DETAIL,
    SEARCH_OBJECT_TYPE_WAREHOUSE_TABLE,
)

logger = logging.getLogger(__name__)


class MicroStrategyAPIError(Exception):
    """Base exception for MicroStrategy API errors."""

    pass


class MicroStrategyAuthenticationError(MicroStrategyAPIError):
    """Raised when authentication fails."""

    pass


class MicroStrategyPermissionError(MicroStrategyAPIError):
    """Raised when API returns permission denied."""

    pass


class MicroStrategyProjectUnavailableError(MicroStrategyAPIError):
    """Raised when IServer reports the project as unavailable / not loaded."""

    def __init__(
        self,
        message: str,
        *,
        i_server_code: int,
        ticket_id: str = "",
        endpoint: str = "",
    ) -> None:
        super().__init__(message)
        self.i_server_code = i_server_code
        self.ticket_id = ticket_id
        self.endpoint = endpoint


class MicroStrategyClient:
    """
    Client for MicroStrategy REST API.

    Features:
    - Automatic session management (login/logout)
    - Token refresh before expiration
    - Exponential backoff retry logic
    - Pagination support
    - Context manager for guaranteed cleanup

    Usage:
        with MicroStrategyClient(config) as client:
            projects = client.get_projects()
            for project in projects:
                objects = client.search_objects(project["id"], object_type=55)
    """

    # API Endpoints
    AUTH_LOGIN = "/api/auth/login"
    AUTH_LOGOUT = "/api/auth/logout"
    SESSIONS = "/api/sessions"

    # Session timeout defaults (MicroStrategy standard is 30 minutes)
    DEFAULT_SESSION_TIMEOUT = timedelta(minutes=30)
    REFRESH_THRESHOLD = timedelta(minutes=5)

    def __init__(self, config: MicroStrategyConnectionConfig):
        self.config = config
        self.base_url = config.base_url.rstrip("/")

        # Session state
        self.auth_token: Optional[str] = None
        self.token_created_at: Optional[datetime] = None
        self.session_timeout = self.DEFAULT_SESSION_TIMEOUT

        # HTTP session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _response_json_dict(response: requests.Response) -> Optional[Dict[str, Any]]:
        try:
            if not response.content:
                return None
            data = response.json()
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _raise_if_iserver_project_unavailable(
        self, response: requests.Response, endpoint: str
    ) -> None:
        body = self._response_json_dict(response)
        if body and body.get("iServerCode") == ISERVER_PROJECT_UNAVAILABLE:
            msg = str(body.get("message") or ISERVER_PROJECT_UNAVAILABLE_DETAIL)
            raise MicroStrategyProjectUnavailableError(
                msg,
                i_server_code=int(body["iServerCode"]),
                ticket_id=str(body.get("ticketId", "")),
                endpoint=endpoint,
            )

    @staticmethod
    def _is_legacy_classcast_message(body: Any) -> bool:
        if body is None:
            return False
        text = str(body.get("message", "")) if isinstance(body, dict) else str(body)
        lower = text.lower()
        return "cannot be cast" in lower or "classcast" in lower

    def _with_project(self, project_id: str) -> Dict[str, Any]:
        """Return a headers snapshot with X-MSTR-ProjectID set."""
        return {**self.session.headers, "X-MSTR-ProjectID": project_id}

    def _set_project_header(self, project_id: str) -> Dict[str, Any]:
        """Set project header and return original headers for restoration."""
        original = dict(self.session.headers)
        self.session.headers.update({"X-MSTR-ProjectID": project_id})
        return original

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "MicroStrategyClient":
        self.login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.logout()
        except Exception as e:
            logger.error("Session cleanup error during exit: %s", e)
            if exc_type is None:
                raise
        return False

    # ── Authentication ────────────────────────────────────────────────────────

    def login(self) -> None:
        logger.info("Authenticating to MicroStrategy...")
        login_mode = 8 if self.config.use_anonymous else 1
        login_data: Dict[str, Any] = {"loginMode": login_mode}
        if not self.config.use_anonymous:
            if not self.config.username or not self.config.password:
                raise MicroStrategyAuthenticationError(
                    "Username and password required for non-anonymous access"
                )
            login_data["username"] = self.config.username
            login_data["password"] = self.config.password.get_secret_value()
        try:
            response = self.session.post(
                f"{self.base_url}{self.AUTH_LOGIN}",
                json=login_data,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            self.auth_token = response.headers.get("X-MSTR-AuthToken")
            if not self.auth_token:
                raise MicroStrategyAuthenticationError(
                    "Authentication response missing X-MSTR-AuthToken header"
                )
            self.token_created_at = datetime.now()
            self.session.headers.update({"X-MSTR-AuthToken": self.auth_token})
            self._activate_session()
            logger.info("Successfully authenticated to MicroStrategy")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [401, 403]:
                raise MicroStrategyAuthenticationError(
                    f"Authentication failed: {e.response.text}"
                ) from e
            raise MicroStrategyAPIError(f"Login request failed: {e}") from e
        except requests.exceptions.RequestException as e:
            raise MicroStrategyAPIError(
                f"Failed to connect to MicroStrategy: {e}"
            ) from e

    def _activate_session(self) -> None:
        try:
            response = self.session.put(
                f"{self.base_url}{self.SESSIONS}",
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            logger.debug("Session activated successfully")
        except Exception as e:
            logger.warning("Failed to activate session (non-fatal): %s", e)

    def _ensure_valid_token(self) -> None:
        if not self.auth_token or not self.token_created_at:
            logger.info("No active session, logging in...")
            self.login()
            return
        elapsed = datetime.now() - self.token_created_at
        if elapsed >= (self.session_timeout - self.REFRESH_THRESHOLD):
            logger.info("Token nearing expiration, refreshing session...")
            try:
                self._activate_session()
                self.token_created_at = datetime.now()
                logger.info("Session refreshed successfully")
            except Exception as e:
                logger.warning("Token refresh failed, re-authenticating: %s", e)
                self.auth_token = None
                self.login()

    def logout(self) -> None:
        if not self.auth_token:
            logger.debug("No active session to logout")
            return
        try:
            response = self.session.post(
                f"{self.base_url}{self.AUTH_LOGOUT}",
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            logger.debug("Successfully logged out MicroStrategy session")
        except Exception as e:
            logger.error("Failed to logout MicroStrategy session: %s", e)
            raise MicroStrategyAPIError(f"Logout failed: {e}") from e
        finally:
            self.auth_token = None
            self.token_created_at = None
            self.session.headers.pop("X-MSTR-AuthToken", None)

    # ── Core request machinery ────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[int] = None,
        legacy_classcast_500_returns_empty: bool = False,
    ) -> Any:
        """
        Make authenticated API request with automatic token refresh.

        Args:
            method: HTTP method
            endpoint: API endpoint path
            params: Query parameters
            json: Request body
            timeout: Override default timeout (seconds). Use for slow endpoints
                     such as document instance creation which can take 60–90s.
            legacy_classcast_500_returns_empty: Return {} on ClassCast 500 instead of raising.

        Returns:
            Parsed JSON (dict or list) or empty dict / empty list.

        Raises:
            MicroStrategyPermissionError: 403
            MicroStrategyProjectUnavailableError: iServerCode project unavailable
            MicroStrategyAPIError: Other API errors
        """
        self._ensure_valid_token()
        url = f"{self.base_url}{endpoint}"
        effective_timeout = (
            timeout if timeout is not None else self.config.timeout_seconds
        )

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=effective_timeout,
            )

            if response.status_code == 403:
                raise MicroStrategyPermissionError(
                    f"Permission denied for {endpoint}: {response.text}"
                )

            self._raise_if_iserver_project_unavailable(response, endpoint)

            if legacy_classcast_500_returns_empty and response.status_code == 500:
                body = self._response_json_dict(response)
                if self._is_legacy_classcast_message(body):
                    logger.warning(
                        "Ignoring legacy MicroStrategy ClassCast error for %s", endpoint
                    )
                    return {}

            response.raise_for_status()
            return response.json() if response.content else {}

        except MicroStrategyProjectUnavailableError:
            raise
        except requests.exceptions.HTTPError as e:
            self._raise_if_iserver_project_unavailable(e.response, endpoint)
            if e.response.status_code == 404:
                logger.debug("Resource not found: %s", url)
                return {}
            raise MicroStrategyAPIError(
                f"HTTP {e.response.status_code} error for {endpoint}: {e.response.text}"
            ) from e
        except requests.exceptions.Timeout:
            raise MicroStrategyAPIError(f"Request timeout for {endpoint}") from None
        except requests.exceptions.RequestException as e:
            raise MicroStrategyAPIError(f"Request failed for {endpoint}: {e}") from e

    def _paginated_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 1000,
    ) -> Iterator[Dict[str, Any]]:
        """Yield items from all pages of a paginated endpoint."""
        params = params or {}
        params["limit"] = page_size
        offset = 0
        while True:
            params["offset"] = offset
            response = self._request("GET", endpoint, params=params)
            items: List[Dict[str, Any]] = []
            if isinstance(response, list):
                items = response
            elif isinstance(response, dict):
                items = (
                    response.get("result")
                    or response.get("value")
                    or response.get("data")
                    or response.get("items")
                    or []
                )
            if not items:
                break
            for item in items:
                yield item
            if len(items) < page_size:
                break
            offset += page_size

    # ── Connection test ───────────────────────────────────────────────────────

    def test_connection(self) -> bool:
        try:
            with self:
                pass
            return True
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    # ── Project / folder / user endpoints ────────────────────────────────────

    def get_projects(self) -> List[Dict[str, Any]]:
        response = self._request("GET", "/api/projects")
        projects = response if isinstance(response, list) else response.get("value", [])
        logger.info("Found %s projects", len(projects))
        return projects

    def get_project(self, project_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v2/projects/{project_id}")

    def get_folders(self, project_id: str) -> List[Dict[str, Any]]:
        original = self._set_project_header(project_id)
        try:
            return list(
                self._paginated_request(f"/api/v2/projects/{project_id}/folders")
            )
        finally:
            self.session.headers = original

    def get_folder(self, folder_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v2/folders/{folder_id}")

    def get_users(self) -> List[Dict[str, Any]]:
        return list(self._paginated_request("/api/v2/users"))

    def get_user(self, user_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v2/users/{user_id}")

    # ── Object search (primary discovery method) ──────────────────────────────

    def search_objects(
        self, project_id: str, object_type: int, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Search for objects using /api/searches/results.

        This is the authoritative discovery method — the /v2/projects/{id}/dashboards
        and similar endpoints often return empty results, while the search API
        reliably returns all objects.

        Object type codes:
            3   = Report + Intelligent Cube (subtype 776 = cube)
            8   = Folder
            53  = Warehouse table
            55  = Dashboard/Dossier/Document (subtype 14081=legacy, 14336=modern)
            776 = Intelligent Cube (search API, subtype of type 3)
        """
        original = self._set_project_header(project_id)
        try:
            objects = list(
                self._paginated_request(
                    "/api/searches/results",
                    params={"type": object_type},
                    page_size=limit,
                )
            )
            logger.info(
                "Found %s objects (type=%s) in project %s",
                len(objects),
                object_type,
                project_id,
            )
            return objects
        finally:
            self.session.headers = original

    def get_object(
        self, object_id: str, object_type: int, project_id: str
    ) -> Dict[str, Any]:
        original = self._set_project_header(project_id)
        try:
            data = self._request(
                "GET", f"/api/objects/{object_id}", params={"type": object_type}
            )
            return data if isinstance(data, dict) else {}
        finally:
            self.session.headers = original

    def search_warehouse_tables(
        self, project_id: str, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        return self.search_objects(
            project_id, SEARCH_OBJECT_TYPE_WAREHOUSE_TABLE, limit=limit
        )

    # ── Cube endpoints ────────────────────────────────────────────────────────

    def get_cube(self, cube_id: str, project_id: str) -> Dict[str, Any]:
        original = self._set_project_header(project_id)
        try:
            return self._request(
                "GET",
                f"/api/v2/cubes/{cube_id}",
                legacy_classcast_500_returns_empty=True,
            )
        finally:
            self.session.headers = original

    def get_cube_schema(self, cube_id: str, project_id: str) -> Dict[str, Any]:
        """
        Get cube schema (attributes and metrics).

        FIX: Uses GET /api/v2/cubes/{id} (the full cube definition endpoint)
        instead of /api/v2/cubes/{id}/schema, which doesn't exist in MSTR REST API.
        The definition response nests schema under definition.availableObjects —
        source.py reads the correct path after this call.
        """
        original = self._set_project_header(project_id)
        try:
            return self._request(
                "GET",
                f"/api/v2/cubes/{cube_id}",
                legacy_classcast_500_returns_empty=True,
            )
        finally:
            self.session.headers = original

    def get_cube_sql_view(self, cube_id: str, project_id: str) -> str:
        """
        Get the SQL generated by MicroStrategy for an Intelligent Cube.

        Returns the raw SQL string for warehouse lineage extraction.
        Returns empty string if cube is not published or dynamic-sourcing only.

        Confirmed working via live testing (16 tables from XRBIA_DM schema, JCP).
        Does NOT require 'Use Architect Editors' privilege — only standard access.
        """
        original = self._set_project_header(project_id)
        try:
            data = self._request("GET", f"/api/v2/cubes/{cube_id}/sqlView")
            return data.get("sqlStatement", "") if isinstance(data, dict) else ""
        finally:
            self.session.headers = original

    # ── Report endpoints ──────────────────────────────────────────────────────

    def get_report(self, report_id: str, project_id: str) -> Dict[str, Any]:
        original = self._set_project_header(project_id)
        try:
            return self._request("GET", f"/api/v2/reports/{report_id}")
        finally:
            self.session.headers = original

    def create_report_instance(self, report_id: str, project_id: str) -> Optional[str]:
        """
        Create a report instance for SQL view retrieval.

        Uses executionStage=resolve_prompts so the query plan is built without
        executing the report against the warehouse — fast and safe for ingestion.
        Requires DssXmlPrivilegesWebReportSQL privilege on the service account.

        Returns instanceId string, or None if creation fails.
        """
        original = self._set_project_header(project_id)
        try:
            data = self._request(
                "POST",
                f"/api/v2/reports/{report_id}/instances",
                params={"executionStage": "resolve_prompts"},
                json={},
            )
            if isinstance(data, dict):
                return data.get("instanceId")
            return None
        except MicroStrategyPermissionError:
            logger.warning(
                "Cannot create report instance for %s — missing DssXmlPrivilegesWebReportSQL "
                "privilege. Grant 'Web Report SQL' in MSTR Security Roles.",
                report_id,
            )
            return None
        except Exception as e:
            logger.debug("Failed to create report instance for %s: %s", report_id, e)
            return None
        finally:
            self.session.headers = original

    def get_report_sql_view(
        self, report_id: str, instance_id: str, project_id: str
    ) -> str:
        """
        Get the SQL statement for a report instance.

        Returns the raw SQL string, or empty string on failure.
        Confirmed working: 8 source tables parsed from 'Spring OOS Date Receipt
        Cst by Week Div 5' on jcpenney-qa.cloud.strategy.com.
        """
        original = self._set_project_header(project_id)
        try:
            data = self._request(
                "GET",
                f"/api/v2/reports/{report_id}/instances/{instance_id}/sqlView",
            )
            return data.get("sqlStatement", "") if isinstance(data, dict) else ""
        except Exception as e:
            logger.debug(
                "Failed to get report SQL view for %s/%s: %s", report_id, instance_id, e
            )
            return ""
        finally:
            self.session.headers = original

    def delete_report_instance(
        self, report_id: str, instance_id: str, project_id: str
    ) -> None:
        """Delete a report instance. Best-effort — errors are logged but not raised."""
        original = self._set_project_header(project_id)
        try:
            self.session.delete(
                f"{self.base_url}/api/v2/reports/{report_id}/instances/{instance_id}",
                timeout=self.config.timeout_seconds,
            )
        except Exception as e:
            logger.debug("Failed to delete report instance %s: %s", instance_id, e)
        finally:
            self.session.headers = original

    # ── Document/Dossier endpoints ────────────────────────────────────────────

    def get_document_definition(
        self, document_id: str, project_id: str
    ) -> Dict[str, Any]:
        """
        Get legacy document definition (subtype 14081).

        Uses GET /api/documents/{id}/definition which returns:
          { "datasets": [ { "id": ..., "name": ..., "availableObjects": [...] } ] }

        This is different from /api/v2/dossiers/{id}/definition which returns
        chapters/pages/visualizations and returns ClassCastException 500 for subtype 14081.
        """
        original = self._set_project_header(project_id)
        try:
            return self._request(
                "GET",
                f"/api/documents/{document_id}/definition",
                legacy_classcast_500_returns_empty=True,
            )
        finally:
            self.session.headers = original

    def get_dossier_definition(
        self, dossier_id: str, project_id: str
    ) -> Dict[str, Any]:
        """
        Get modern dossier definition (subtype 14336).

        Uses GET /api/v2/dossiers/{id}/definition which returns:
          { "chapters": [ { "pages": [ { "visualizations": [...] } ] } ] }

        Note the pages layer — source.py traverses chapters → pages → visualizations.
        """
        original = self._set_project_header(project_id)
        try:
            return self._request(
                "GET",
                f"/api/v2/dossiers/{dossier_id}/definition",
                legacy_classcast_500_returns_empty=True,
            )
        finally:
            self.session.headers = original

    def get_dashboard_definition(
        self, dashboard_id: str, project_id: str
    ) -> Dict[str, Any]:
        """
        Legacy compatibility shim — prefer get_document_definition or get_dossier_definition.

        Source.py now routes by subtype before calling the client, so this method
        is kept only for backward compatibility with existing callers.
        Defaults to the dossier (modern) endpoint.
        """
        return self.get_dossier_definition(dashboard_id, project_id)

    # ── Dataset endpoints ─────────────────────────────────────────────────────

    def get_datasets(self, project_id: str) -> List[Dict[str, Any]]:
        original = self._set_project_header(project_id)
        try:
            datasets = list(
                self._paginated_request(f"/api/v2/projects/{project_id}/datasets")
            )
            logger.info("Found %s datasets in project %s", len(datasets), project_id)
            return datasets
        finally:
            self.session.headers = original

    def get_dataset(self, dataset_id: str, project_id: str) -> Dict[str, Any]:
        original = self._set_project_header(project_id)
        try:
            return self._request("GET", f"/api/v2/datasets/{dataset_id}")
        finally:
            self.session.headers = original

    # ── Model / lineage endpoints (require Architect Editors privilege) ────────

    def get_model_cube(self, cube_id: str, project_id: str) -> Dict[str, Any]:
        """
        Get cube model (physical tables). Requires 'Use Architect Editors' privilege.
        Returns 403 on most service accounts — use get_cube_sql_view() instead.
        Kept for environments where the privilege is available.
        """
        original = self._set_project_header(project_id)
        try:
            data = self._request("GET", f"/api/model/cubes/{cube_id}")
            return data if isinstance(data, dict) else {}
        finally:
            self.session.headers = original

    def get_model_tables(
        self, cube_id: str, project_id: str, *, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Requires 'Use Architect Editors' privilege."""
        original = self._set_project_header(project_id)
        try:
            data = self._request(
                "GET", "/api/model/tables", params={"cubeId": cube_id, "limit": limit}
            )
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return list(data.get("tables", []))
            return []
        finally:
            self.session.headers = original

    def get_model_facts(
        self, cube_id: str, project_id: str, *, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Requires 'Use Architect Editors' privilege."""
        original = self._set_project_header(project_id)
        try:
            data = self._request(
                "GET", "/api/model/facts", params={"cubeId": cube_id, "limit": limit}
            )
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return list(data.get("facts", []))
            return []
        finally:
            self.session.headers = original

    def get_lineage_for_object(
        self, object_id: str, project_id: str, object_type: int
    ) -> Dict[str, Any]:
        """Get MSTR lineage API results. Returns {} if endpoint is 404 (not available on all versions)."""
        original = self._set_project_header(project_id)
        try:
            data = self._request(
                "GET", f"/api/lineage/objects/{object_id}", params={"type": object_type}
            )
            return data if isinstance(data, dict) else {}
        finally:
            self.session.headers = original
